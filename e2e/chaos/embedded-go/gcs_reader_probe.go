package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

const gcsIAMProbeVersion = 1

var gcsIAMProbeMetadataURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"

var gcsIAMProbeCredentialOverrides = []string{
	"STORAGE_EMULATOR_HOST", "GCE_METADATA_HOST", "GCE_METADATA_IP",
	"GOOGLE_APPLICATION_CREDENTIALS", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
	"http_proxy", "https_proxy", "all_proxy", "CLOUDSDK_CONFIG",
	"CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE",
}

type gcsIAMProbeConfig struct {
	Bucket             string
	Prefix             string
	RunID              string
	Nonce              string
	ExpectedGSA        string
	ManifestGeneration int64
}

type gcsIAMProbeManifest struct {
	Version           int    `json:"version"`
	RunID             string `json:"run_id"`
	Nonce             string `json:"nonce"`
	ReadKey           string `json:"read_key"`
	ReadGeneration    int64  `json:"read_generation"`
	ReadSHA256        string `json:"read_sha256"`
	UpdateKey         string `json:"update_key"`
	UpdateGeneration  int64  `json:"update_generation"`
	DeleteKey         string `json:"delete_key"`
	DeleteGeneration  int64  `json:"delete_generation"`
	ControlKey        string `json:"control_key"`
	ControlGeneration int64  `json:"control_generation"`
}

type gcsIAMProbeStore interface {
	Read(context.Context, string) ([]byte, int64, error)
	ReadVersion(context.Context, string, int64) ([]byte, error)
	Create(context.Context, string, []byte) (int64, error)
	Update(context.Context, string, int64) (int64, error)
	Delete(context.Context, string, int64) error
	Cleanup(context.Context, []gcsIAMProbeOwnedGeneration) error
}

type gcsIAMProbeOwnedGeneration struct {
	key        string
	generation int64
	expected   []byte
}

type gcsIAMProbeCleanupPending struct {
	items []gcsIAMProbeCleanupItem
}

func (e *gcsIAMProbeCleanupPending) Error() string { return "GCS IAM preflight exact cleanup pending" }

type gcsIAMProbeCleanupItem struct {
	label      string
	generation int64
}

type gcsIAMProbeCreateUnknown struct{ labels []string }

func (e *gcsIAMProbeCreateUnknown) Error() string { return "GCS IAM preflight create outcome unknown" }

type gcsIAMProbeBucket struct {
	bucket *storage.BucketHandle
}

func runGCSIAMProbeCommand(mode string) error {
	if mode != "writer-prepare" && mode != "reader" && mode != "writer-cleanup" {
		return errors.New("GCS IAM preflight mode rejected")
	}
	cfg, err := gcsIAMProbeConfigFromEnv()
	if err != nil {
		return err
	}
	if err := rejectGCSIAMProbeOverrides(getenv); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := verifyGCSIAMProbeIdentity(ctx, cfg.ExpectedGSA, http.DefaultClient); err != nil {
		return err
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.New("GCS IAM preflight client unavailable")
	}
	defer client.Close()
	store := &gcsIAMProbeBucket{bucket: client.Bucket(cfg.Bucket)}
	var runErr error
	switch mode {
	case "writer-prepare":
		runErr = prepareGCSIAMProbe(ctx, &cfg, store)
	case "reader":
		runErr = verifyGCSIAMProbe(ctx, cfg, store)
	case "writer-cleanup":
		runErr = cleanupGCSIAMProbe(ctx, cfg, store)
	}
	if runErr != nil {
		handled := false
		var pending *gcsIAMProbeCleanupPending
		if errors.As(runErr, &pending) {
			for _, item := range pending.items {
				fmt.Printf("gcs-iam-probe=cleanup-pending:%s:%d\n", item.label, item.generation)
			}
			handled = true
		}
		var unknown *gcsIAMProbeCreateUnknown
		if errors.As(runErr, &unknown) {
			for _, label := range unknown.labels {
				fmt.Printf("gcs-iam-probe=create-outcome-unknown:%s\n", label)
			}
			handled = true
		}
		if handled {
			return errors.New("GCS IAM preflight failed with unresolved cleanup evidence")
		}
		return fmt.Errorf("GCS IAM preflight %s phase failed", mode)
	}
	if mode == "writer-prepare" {
		fmt.Printf("gcs-iam-probe=prepared:%d\n", cfg.ManifestGeneration)
		return nil
	}
	fmt.Printf("gcs-iam-probe=%s\n", map[string]string{
		"reader":         "reader_verified",
		"writer-cleanup": "cleaned",
	}[mode])
	return nil
}

func getenv(name string) string {
	return os.Getenv(name)
}

func rejectGCSIAMProbeOverrides(getenv func(string) string) error {
	for _, name := range gcsIAMProbeCredentialOverrides {
		if strings.TrimSpace(getenv(name)) != "" {
			return errors.New("GCS IAM preflight credential or endpoint override rejected")
		}
	}
	home := strings.TrimSpace(getenv("HOME"))
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	if home != "" {
		_, err := os.Stat(filepath.Join(home, ".config", "gcloud", "application_default_credentials.json"))
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			return errors.New("GCS IAM preflight local ADC credentials rejected")
		}
	}
	return nil
}

func gcsIAMProbeConfigFromEnv() (gcsIAMProbeConfig, error) {
	return gcsIAMProbeConfigFromValues(
		getenv("RHIZA_GCS_BUCKET"), getenv("RHIZA_GCS_PREFIX"),
		getenv("RHIZA_RUN_ID"), getenv("RHIZA_GCS_IAM_PROBE_NONCE"),
		getenv("RHIZA_GCS_IAM_PROBE_EXPECTED_GSA"), getenv("RHIZA_GCS_IAM_PROBE_MANIFEST_GENERATION"))
}

func verifyGCSIAMProbeIdentity(ctx context.Context, expected string, client *http.Client) error {
	if expected == "" || client == nil {
		return errors.New("GCS IAM preflight identity configuration rejected")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, gcsIAMProbeMetadataURL, nil)
	if err != nil {
		return errors.New("GCS IAM preflight identity check unavailable")
	}
	request.Header.Set("Metadata-Flavor", "Google")
	identityClient := *client
	identityClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := identityClient.Do(request)
	if err != nil {
		return errors.New("GCS IAM preflight identity check unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("GCS IAM preflight identity check rejected")
	}
	identity, err := io.ReadAll(io.LimitReader(response.Body, 512))
	if err != nil || bytes.Equal(bytes.TrimSpace(identity), []byte(expected)) == false {
		return errors.New("GCS IAM preflight identity mismatch")
	}
	return nil
}

func prepareGCSIAMProbe(ctx context.Context, cfg *gcsIAMProbeConfig, store gcsIAMProbeStore) error {
	keys := gcsIAMProbeKeys(*cfg)
	payload := func(label string) []byte { return gcsIAMProbePayload(*cfg, label) }
	var manifest gcsIAMProbeManifest
	manifest.Version = gcsIAMProbeVersion
	manifest.RunID = cfg.RunID
	manifest.Nonce = cfg.Nonce
	manifest.ReadKey, manifest.UpdateKey, manifest.DeleteKey, manifest.ControlKey = keys.read, keys.update, keys.delete, keys.control
	created := []struct {
		key     string
		content []byte
		gen     *int64
	}{
		{keys.read, payload("read"), &manifest.ReadGeneration},
		{keys.update, payload("update"), &manifest.UpdateGeneration},
		{keys.delete, payload("delete"), &manifest.DeleteGeneration},
		{keys.control, payload("control"), &manifest.ControlGeneration},
	}
	owned := make([]gcsIAMProbeOwnedGeneration, 0, len(created)+1)
	cleanupOwned := func() error {
		if len(owned) == 0 {
			return nil
		}
		return store.Cleanup(ctx, append([]gcsIAMProbeOwnedGeneration(nil), owned...))
	}
	for _, object := range created {
		generation, err := store.Create(ctx, object.key, object.content)
		if err != nil || generation <= 0 {
			cleanupErr := cleanupOwned()
			if generation <= 0 && (err == nil || !isGCSIAMPermissionDenied(err)) {
				return errors.Join(&gcsIAMProbeCreateUnknown{labels: []string{gcsIAMProbeKeyLabel(object.key)}}, cleanupErr)
			}
			if err != nil && !isGCSIAMPermissionDenied(err) {
				return errors.Join(&gcsIAMProbeCreateUnknown{labels: []string{gcsIAMProbeKeyLabel(object.key)}}, cleanupErr)
			}
			return errors.Join(errors.New("GCS IAM preflight canary creation failed"), cleanupErr)
		}
		*object.gen = generation
		owned = append(owned, gcsIAMProbeOwnedGeneration{key: object.key, generation: generation, expected: append([]byte(nil), object.content...)})
		body, readGeneration, err := store.Read(ctx, object.key)
		if err != nil || readGeneration != generation || !bytes.Equal(body, object.content) {
			return errors.Join(errors.New("GCS IAM preflight canary readback failed"), cleanupOwned())
		}
	}
	digest := sha256.Sum256(payload("read"))
	manifest.ReadSHA256 = hex.EncodeToString(digest[:])
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return errors.New("GCS IAM preflight manifest encoding failed")
	}
	manifestGeneration, err := store.Create(ctx, keys.manifest, encoded)
	if err != nil || manifestGeneration <= 0 {
		cleanupErr := cleanupOwned()
		if manifestGeneration <= 0 && (err == nil || !isGCSIAMPermissionDenied(err)) {
			return errors.Join(&gcsIAMProbeCreateUnknown{labels: []string{"manifest"}}, cleanupErr)
		}
		if err != nil && !isGCSIAMPermissionDenied(err) {
			return errors.Join(&gcsIAMProbeCreateUnknown{labels: []string{"manifest"}}, cleanupErr)
		}
		return errors.Join(errors.New("GCS IAM preflight manifest creation failed"), cleanupErr)
	}
	owned = append(owned, gcsIAMProbeOwnedGeneration{key: keys.manifest, generation: manifestGeneration, expected: append([]byte(nil), encoded...)})
	readback, readGeneration, err := store.Read(ctx, keys.manifest)
	if err != nil || readGeneration != manifestGeneration || !bytes.Equal(readback, encoded) {
		return errors.Join(errors.New("GCS IAM preflight manifest readback failed"), cleanupOwned())
	}
	cfg.ManifestGeneration = manifestGeneration
	return nil
}

func verifyGCSIAMProbe(ctx context.Context, cfg gcsIAMProbeConfig, store gcsIAMProbeStore) error {
	keys := gcsIAMProbeKeys(cfg)
	manifestBytes, manifestGeneration, err := store.Read(ctx, keys.manifest)
	if err != nil || manifestGeneration <= 0 {
		return errors.New("GCS IAM preflight manifest read failed")
	}
	if manifestGeneration != cfg.ManifestGeneration {
		return errors.New("GCS IAM preflight manifest generation mismatch")
	}
	manifest, err := decodeGCSIAMProbeManifest(manifestBytes, cfg, keys)
	if err != nil {
		return errors.New("GCS IAM preflight manifest rejected")
	}
	readData, readGeneration, err := store.Read(ctx, manifest.ReadKey)
	if err != nil || readGeneration != manifest.ReadGeneration {
		return errors.New("GCS IAM preflight positive data read failed")
	}
	digest := sha256.Sum256(readData)
	if hex.EncodeToString(digest[:]) != manifest.ReadSHA256 || !bytes.Equal(readData, gcsIAMProbePayload(cfg, "read")) {
		return errors.New("GCS IAM preflight positive data read mismatched")
	}
	if _, _, err := store.Read(ctx, manifest.ControlKey); !isGCSIAMPermissionDenied(err) {
		return errors.New("GCS IAM preflight control read was not denied")
	}
	createGeneration, err := store.Create(ctx, keys.create, []byte("unexpected-create"))
	if err == nil && createGeneration > 0 {
		if cleanupErr := store.Delete(ctx, keys.create, createGeneration); cleanupErr != nil {
			return &gcsIAMProbeCleanupPending{items: []gcsIAMProbeCleanupItem{{label: "create", generation: createGeneration}}}
		}
		return errors.New("GCS IAM preflight create permission was not denied")
	}
	if err == nil || createGeneration > 0 || !isGCSIAMPermissionDenied(err) {
		return errors.New("GCS IAM preflight create permission was not denied")
	}
	updatedGeneration, err := store.Update(ctx, manifest.UpdateKey, manifest.UpdateGeneration)
	if err == nil && updatedGeneration > 0 {
		var pending []gcsIAMProbeCleanupItem
		generations := []int64{updatedGeneration}
		if manifest.UpdateGeneration != updatedGeneration {
			generations = append(generations, manifest.UpdateGeneration)
		}
		for _, generation := range generations {
			if cleanupErr := store.Delete(ctx, manifest.UpdateKey, generation); cleanupErr != nil {
				pending = append(pending, gcsIAMProbeCleanupItem{label: "update", generation: generation})
			}
		}
		if len(pending) > 0 {
			return &gcsIAMProbeCleanupPending{items: pending}
		}
		return errors.New("GCS IAM preflight update permission was not denied")
	}
	if !isGCSIAMPermissionDenied(err) {
		return errors.New("GCS IAM preflight update permission was not denied")
	}
	if err := store.Delete(ctx, manifest.DeleteKey, manifest.DeleteGeneration); !isGCSIAMPermissionDenied(err) {
		return errors.New("GCS IAM preflight delete permission was not denied")
	}
	return nil
}

func cleanupGCSIAMProbe(ctx context.Context, cfg gcsIAMProbeConfig, store gcsIAMProbeStore) error {
	keys := gcsIAMProbeKeys(cfg)
	exact := make([]gcsIAMProbeOwnedGeneration, 0, 8)
	var problems []error
	addExact := func(item gcsIAMProbeOwnedGeneration) {
		for _, existing := range exact {
			if existing.key == item.key && existing.generation == item.generation {
				if !bytes.Equal(existing.expected, item.expected) {
					problems = append(problems, &gcsIAMProbeCreateUnknown{labels: []string{gcsIAMProbeKeyLabel(item.key)}})
				}
				return
			}
		}
		exact = append(exact, item)
	}
	manifestBytes, manifestGeneration, manifestErr := store.Read(ctx, keys.manifest)
	var manifest gcsIAMProbeManifest
	manifestValid := false
	if manifestErr == nil && manifestGeneration > 0 {
		if cfg.ManifestGeneration > 0 && manifestGeneration != cfg.ManifestGeneration {
			problems = append(problems, &gcsIAMProbeCreateUnknown{labels: []string{"manifest"}})
		} else if decoded, err := decodeGCSIAMProbeManifest(manifestBytes, cfg, keys); err != nil {
			problems = append(problems, &gcsIAMProbeCreateUnknown{labels: []string{"manifest"}})
		} else {
			manifest, manifestValid = decoded, true
			addExact(gcsIAMProbeOwnedGeneration{key: keys.manifest, generation: manifestGeneration, expected: append([]byte(nil), manifestBytes...)})
		}
	} else if !isGCSIAMNotFound(manifestErr) {
		problems = append(problems, &gcsIAMProbeCreateUnknown{labels: []string{"manifest"}})
	}
	if cfg.ManifestGeneration > 0 && (manifestErr != nil || manifestGeneration != cfg.ManifestGeneration) {
		problems = append(problems, &gcsIAMProbeCreateUnknown{labels: []string{"manifest"}})
	}

	candidates := []struct {
		key     string
		label   string
		payload []byte
	}{
		{keys.read, "read", gcsIAMProbePayload(cfg, "read")},
		{keys.update, "update", gcsIAMProbePayload(cfg, "update")},
		{keys.delete, "delete", gcsIAMProbePayload(cfg, "delete")},
		{keys.control, "control", gcsIAMProbePayload(cfg, "control")},
		{keys.create, "create", []byte("unexpected-create")},
	}
	for _, candidate := range candidates {
		body, generation, err := store.Read(ctx, candidate.key)
		if isGCSIAMNotFound(err) {
			continue
		}
		if err != nil || generation <= 0 || !bytes.Equal(body, candidate.payload) {
			problems = append(problems, &gcsIAMProbeCreateUnknown{labels: []string{candidate.label}})
			continue
		}
		addExact(gcsIAMProbeOwnedGeneration{key: candidate.key, generation: generation, expected: candidate.payload})
	}
	if manifestValid {
		for _, entry := range []gcsIAMProbeOwnedGeneration{
			{key: keys.read, generation: manifest.ReadGeneration, expected: gcsIAMProbePayload(cfg, "read")},
			{key: keys.update, generation: manifest.UpdateGeneration, expected: gcsIAMProbePayload(cfg, "update")},
			{key: keys.delete, generation: manifest.DeleteGeneration, expected: gcsIAMProbePayload(cfg, "delete")},
			{key: keys.control, generation: manifest.ControlGeneration, expected: gcsIAMProbePayload(cfg, "control")},
		} {
			addExact(entry)
		}
	}
	for envName, key := range map[string]string{
		"RHIZA_GCS_IAM_PROBE_CREATE_GENERATION": keys.create,
		"RHIZA_GCS_IAM_PROBE_UPDATE_GENERATION": keys.update,
	} {
		value := getenv(envName)
		if value == "" || value == "0" {
			continue
		}
		generation, err := strconv.ParseInt(value, 10, 64)
		if err != nil || generation <= 0 {
			return errors.New("GCS IAM preflight cleanup generation rejected")
		}
		label := gcsIAMProbeKeyLabel(key)
		payload := gcsIAMProbePayload(cfg, label)
		if label == "create" {
			payload = []byte("unexpected-create")
		}
		addExact(gcsIAMProbeOwnedGeneration{key: key, generation: generation, expected: payload})
	}
	cleanupErr := store.Cleanup(ctx, exact)
	if cleanupErr != nil {
		problems = append(problems, cleanupErr)
	}
	return errors.Join(problems...)
}

func gcsIAMProbePayload(cfg gcsIAMProbeConfig, label string) []byte {
	return []byte("rhiza-gcs-iam-v1/" + cfg.RunID + "/" + cfg.Nonce + "/" + label)
}

type gcsIAMProbeObjectKeys struct {
	read, update, delete, control, manifest, create string
}

func gcsIAMProbeKeys(cfg gcsIAMProbeConfig) gcsIAMProbeObjectKeys {
	dataBase := cfg.Prefix + "data/iam-preflight/" + cfg.RunID + "/" + cfg.Nonce + "/"
	controlBase := cfg.Prefix + "control/iam-preflight/" + cfg.RunID + "/" + cfg.Nonce + "/"
	return gcsIAMProbeObjectKeys{
		read: dataBase + "read", update: dataBase + "update", delete: dataBase + "delete",
		manifest: dataBase + "manifest.json", create: dataBase + "create",
		control: controlBase + "read",
	}
}

func decodeGCSIAMProbeManifest(data []byte, cfg gcsIAMProbeConfig, keys gcsIAMProbeObjectKeys) (gcsIAMProbeManifest, error) {
	var manifest gcsIAMProbeManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return gcsIAMProbeManifest{}, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return gcsIAMProbeManifest{}, errors.New("trailing manifest data")
	}
	if manifest.Version != gcsIAMProbeVersion || manifest.RunID != cfg.RunID || manifest.Nonce != cfg.Nonce ||
		manifest.ReadKey != keys.read || manifest.UpdateKey != keys.update ||
		manifest.DeleteKey != keys.delete || manifest.ControlKey != keys.control ||
		manifest.ReadGeneration <= 0 || manifest.UpdateGeneration <= 0 ||
		manifest.DeleteGeneration <= 0 || manifest.ControlGeneration <= 0 ||
		!regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(manifest.ReadSHA256) {
		return gcsIAMProbeManifest{}, errors.New("manifest identity mismatch")
	}
	return manifest, nil
}

func isGCSIAMPermissionDenied(err error) bool {
	var apiError *googleapi.Error
	return errors.As(err, &apiError) && apiError.Code == http.StatusForbidden
}

func (s *gcsIAMProbeBucket) Read(ctx context.Context, key string) ([]byte, int64, error) {
	reader, err := s.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return nil, 0, err
	}
	attrs := reader.Attrs
	body, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, 0, readErr
	}
	if closeErr != nil {
		return nil, 0, closeErr
	}
	if attrs.Generation <= 0 {
		return nil, 0, errors.New("object generation unavailable")
	}
	return body, attrs.Generation, nil
}

func (s *gcsIAMProbeBucket) ReadVersion(ctx context.Context, key string, generation int64) ([]byte, error) {
	if generation <= 0 {
		return nil, errors.New("object generation unavailable")
	}
	reader, err := s.bucket.Object(key).Generation(generation).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	attrs := reader.Attrs
	body, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if attrs.Generation != generation {
		return nil, errors.New("object generation mismatch")
	}
	return body, nil
}

func (s *gcsIAMProbeBucket) Create(ctx context.Context, key string, payload []byte) (int64, error) {
	writer := s.bucket.Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	_, writeErr := writer.Write(payload)
	closeErr := writer.Close()
	if writeErr != nil {
		return 0, writeErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	attrs := writer.Attrs()
	if attrs == nil || attrs.Generation <= 0 {
		return 0, errors.New("created object generation unavailable")
	}
	return attrs.Generation, nil
}

func (s *gcsIAMProbeBucket) Update(ctx context.Context, key string, generation int64) (int64, error) {
	attrs, err := s.bucket.Object(key).If(storage.Conditions{GenerationMatch: generation}).Update(ctx,
		storage.ObjectAttrsToUpdate{Metadata: map[string]string{"rhiza-iam-preflight": "unexpected"}})
	if err != nil {
		return 0, err
	}
	if attrs == nil || attrs.Generation <= 0 {
		return 0, errors.New("updated object generation unavailable")
	}
	return attrs.Generation, nil
}

func (s *gcsIAMProbeBucket) Delete(ctx context.Context, key string, generation int64) error {
	return s.bucket.Object(key).If(storage.Conditions{GenerationMatch: generation}).Delete(ctx)
}

func (s *gcsIAMProbeBucket) Cleanup(ctx context.Context, exact []gcsIAMProbeOwnedGeneration) error {
	var pending []gcsIAMProbeCleanupItem
	for _, item := range exact {
		key, generation := item.key, item.generation
		if generation <= 0 || len(item.expected) == 0 {
			pending = append(pending, gcsIAMProbeCleanupItem{label: gcsIAMProbeKeyLabel(key), generation: generation})
			continue
		}
		body, err := s.ReadVersion(ctx, key, generation)
		if isGCSIAMNotFound(err) {
			continue
		}
		if err != nil || !bytes.Equal(body, item.expected) {
			pending = append(pending, gcsIAMProbeCleanupItem{label: gcsIAMProbeKeyLabel(key), generation: generation})
			continue
		}
		object := s.bucket.Object(key).Generation(generation)
		if err := object.Delete(ctx); err != nil && !isGCSIAMNotFound(err) {
			pending = append(pending, gcsIAMProbeCleanupItem{label: gcsIAMProbeKeyLabel(key), generation: generation})
			continue
		}
		if _, err := object.Attrs(ctx); err == nil || !isGCSIAMNotFound(err) {
			pending = append(pending, gcsIAMProbeCleanupItem{label: gcsIAMProbeKeyLabel(key), generation: generation})
		}
	}
	if len(pending) > 0 {
		return &gcsIAMProbeCleanupPending{items: pending}
	}
	return nil
}

func isGCSIAMNotFound(err error) bool {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return true
	}
	var apiError *googleapi.Error
	return errors.As(err, &apiError) && apiError.Code == http.StatusNotFound
}

func gcsIAMProbeKeyLabel(key string) string {
	if strings.HasSuffix(key, "/manifest.json") {
		return "manifest"
	}
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		return key[i+1:]
	}
	return "unknown"
}

func gcsIAMProbeConfigFromValues(bucket, prefix, runID, nonce, expectedGSA, manifestGenerationText string) (gcsIAMProbeConfig, error) {
	manifestGeneration, err := strconv.ParseInt(manifestGenerationText, 10, 64)
	if err != nil || manifestGeneration < 0 {
		return gcsIAMProbeConfig{}, errors.New("GCS IAM preflight configuration rejected")
	}
	if !regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`).MatchString(bucket) ||
		!regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*/$`).MatchString(prefix) ||
		!regexp.MustCompile(`^[a-f0-9]{8}$`).MatchString(runID) ||
		!regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(nonce) ||
		!strings.HasSuffix(prefix, "/"+runID+"/") ||
		!regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$`).MatchString(expectedGSA) {
		return gcsIAMProbeConfig{}, errors.New("GCS IAM preflight configuration rejected")
	}
	for _, component := range strings.Split(strings.TrimSuffix(prefix, "/"), "/") {
		if component == "" || component == "." || component == ".." {
			return gcsIAMProbeConfig{}, errors.New("GCS IAM preflight configuration rejected")
		}
	}
	return gcsIAMProbeConfig{Bucket: bucket, Prefix: prefix, RunID: runID, Nonce: nonce, ExpectedGSA: expectedGSA, ManifestGeneration: manifestGeneration}, nil
}
