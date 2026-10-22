package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
)

type fakeGCSIAMProbeObject struct {
	body       []byte
	generation int64
}

type fakeGCSIAMProbeStore struct {
	objects         map[string]fakeGCSIAMProbeObject
	versions        map[string]map[int64][]byte
	unknownVersions map[string]map[int64]bool
	deniedReads     map[string]bool
	missingReads    map[string]bool
	allowCreate     bool
	allowUpdate     bool
	sameGenUpdate   bool
	allowDelete     bool
	calls           []string
	cleanupKeys     []gcsIAMProbeOwnedGeneration
	cleanupErr      error
	nextGeneration  int64
}

func newFakeGCSIAMProbeStore() *fakeGCSIAMProbeStore {
	return &fakeGCSIAMProbeStore{
		objects:         make(map[string]fakeGCSIAMProbeObject),
		versions:        make(map[string]map[int64][]byte),
		unknownVersions: make(map[string]map[int64]bool),
		deniedReads:     make(map[string]bool),
		missingReads:    make(map[string]bool),
		nextGeneration:  1,
	}
}

func (s *fakeGCSIAMProbeStore) Read(_ context.Context, key string) ([]byte, int64, error) {
	s.calls = append(s.calls, "read:"+key)
	if s.deniedReads[key] {
		return nil, 0, &googleapi.Error{Code: http.StatusForbidden}
	}
	if s.missingReads[key] {
		return nil, 0, &googleapi.Error{Code: http.StatusNotFound}
	}
	object, ok := s.objects[key]
	if !ok {
		return nil, 0, &googleapi.Error{Code: http.StatusNotFound}
	}
	return append([]byte(nil), object.body...), object.generation, nil
}

func (s *fakeGCSIAMProbeStore) Create(_ context.Context, key string, payload []byte) (int64, error) {
	s.calls = append(s.calls, "create:"+key)
	if !s.allowCreate {
		return 0, &googleapi.Error{Code: http.StatusForbidden}
	}
	s.nextGeneration++
	s.objects[key] = fakeGCSIAMProbeObject{body: append([]byte(nil), payload...), generation: s.nextGeneration}
	if s.versions[key] == nil {
		s.versions[key] = make(map[int64][]byte)
	}
	s.versions[key][s.nextGeneration] = append([]byte(nil), payload...)
	return s.nextGeneration, nil
}

func (s *fakeGCSIAMProbeStore) ReadVersion(_ context.Context, key string, generation int64) ([]byte, error) {
	if s.unknownVersions[key][generation] {
		return nil, errors.New("version read unavailable")
	}
	body, ok := s.versions[key][generation]
	if !ok {
		return nil, &googleapi.Error{Code: http.StatusNotFound}
	}
	return append([]byte(nil), body...), nil
}

func (s *fakeGCSIAMProbeStore) Update(_ context.Context, key string, generation int64) (int64, error) {
	s.calls = append(s.calls, "update:"+key)
	if !s.allowUpdate {
		return 0, &googleapi.Error{Code: http.StatusForbidden}
	}
	object, ok := s.objects[key]
	if !ok || object.generation != generation {
		return 0, &googleapi.Error{Code: http.StatusPreconditionFailed}
	}
	if !s.sameGenUpdate {
		object.generation++
	}
	if s.versions[key] == nil {
		s.versions[key] = make(map[int64][]byte)
	}
	object.body = append([]byte(nil), object.body...)
	s.versions[key][object.generation] = append([]byte(nil), object.body...)
	s.objects[key] = object
	return object.generation, nil
}

func (s *fakeGCSIAMProbeStore) Delete(_ context.Context, key string, generation int64) error {
	s.calls = append(s.calls, "delete:"+key)
	if !s.allowDelete {
		return &googleapi.Error{Code: http.StatusForbidden}
	}
	if _, ok := s.versions[key][generation]; !ok {
		return &googleapi.Error{Code: http.StatusPreconditionFailed}
	}
	object := s.objects[key]
	delete(s.versions[key], generation)
	if object.generation == generation {
		var latest int64
		var body []byte
		for version, candidate := range s.versions[key] {
			if version > latest {
				latest, body = version, candidate
			}
		}
		if latest == 0 {
			delete(s.objects, key)
		} else {
			s.objects[key] = fakeGCSIAMProbeObject{body: append([]byte(nil), body...), generation: latest}
		}
	}
	return nil
}

func (s *fakeGCSIAMProbeStore) Cleanup(_ context.Context, exact []gcsIAMProbeOwnedGeneration) error {
	s.cleanupKeys = append([]gcsIAMProbeOwnedGeneration(nil), exact...)
	if s.cleanupErr != nil {
		return s.cleanupErr
	}
	for _, item := range exact {
		body, err := s.ReadVersion(context.Background(), item.key, item.generation)
		if isGCSIAMNotFound(err) {
			continue
		}
		if err != nil || !bytes.Equal(body, item.expected) {
			return &gcsIAMProbeCleanupPending{items: []gcsIAMProbeCleanupItem{{label: gcsIAMProbeKeyLabel(item.key), generation: item.generation}}}
		}
		delete(s.versions[item.key], item.generation)
		if object, ok := s.objects[item.key]; ok && object.generation == item.generation {
			var latest int64
			var latestBody []byte
			for version, candidate := range s.versions[item.key] {
				if version > latest {
					latest, latestBody = version, candidate
				}
			}
			if latest == 0 {
				delete(s.objects, item.key)
			} else {
				s.objects[item.key] = fakeGCSIAMProbeObject{body: append([]byte(nil), latestBody...), generation: latest}
			}
		}
	}
	return nil
}

func gcsIAMProbeTestConfig() gcsIAMProbeConfig {
	cfg, err := gcsIAMProbeConfigFromValues("rhiza-test-bucket", "qual/run/1234abcd/", "1234abcd", strings.Repeat("b", 32), "rhiza-readonly@rhiza-test-project.iam.gserviceaccount.com", "0")
	if err != nil {
		panic(err)
	}
	return cfg
}

func prepareFakeGCSIAMProbe(t *testing.T) (*fakeGCSIAMProbeStore, gcsIAMProbeConfig) {
	t.Helper()
	t.Setenv("RHIZA_GCS_IAM_PROBE_CREATE_GENERATION", "0")
	t.Setenv("RHIZA_GCS_IAM_PROBE_UPDATE_GENERATION", "0")
	store := newFakeGCSIAMProbeStore()
	store.allowCreate = true
	cfg := gcsIAMProbeTestConfig()
	if err := prepareGCSIAMProbe(context.Background(), &cfg, store); err != nil {
		t.Fatalf("prepare preflight: %v", err)
	}
	store.allowCreate = false
	keys := gcsIAMProbeKeys(cfg)
	store.deniedReads[keys.control] = true
	return store, cfg
}

func TestGCSIAMProbeRequiresPositiveReadAndExact403Denials(t *testing.T) {
	store, cfg := prepareFakeGCSIAMProbe(t)
	store.calls = nil
	if err := verifyGCSIAMProbe(context.Background(), cfg, store); err != nil {
		t.Fatalf("valid read-only identity rejected: %v", err)
	}
	keys := gcsIAMProbeKeys(cfg)
	want := []string{
		"read:" + keys.manifest,
		"read:" + keys.read,
		"read:" + keys.control,
		"create:" + keys.create,
		"update:" + keys.update,
		"delete:" + keys.delete,
	}
	if !reflect.DeepEqual(store.calls, want) {
		t.Fatalf("operation order=%v want=%v", store.calls, want)
	}
}

func TestGCSIAMProbeDoesNotTreat404AsDeniedPermission(t *testing.T) {
	store, cfg := prepareFakeGCSIAMProbe(t)
	keys := gcsIAMProbeKeys(cfg)
	store.calls = nil
	store.deniedReads[keys.control] = false
	delete(store.objects, keys.control)
	if err := verifyGCSIAMProbe(context.Background(), cfg, store); err == nil || !strings.Contains(err.Error(), "control read was not denied") {
		t.Fatalf("missing control object was accepted as an IAM denial: %v", err)
	}
	for _, call := range store.calls {
		if strings.HasPrefix(call, "create:") || strings.HasPrefix(call, "update:") || strings.HasPrefix(call, "delete:") {
			t.Fatalf("write probes ran after control 404: %v", store.calls)
		}
	}
}

func TestGCSIAMProbeStopsBeforeDenialChecksWithoutPositiveDataRead(t *testing.T) {
	store, cfg := prepareFakeGCSIAMProbe(t)
	keys := gcsIAMProbeKeys(cfg)
	store.calls = nil
	store.deniedReads[keys.read] = true
	if err := verifyGCSIAMProbe(context.Background(), cfg, store); err == nil || !strings.Contains(err.Error(), "positive data read failed") {
		t.Fatalf("unreadable data canary was accepted: %v", err)
	}
	for _, call := range store.calls {
		if call == "read:"+keys.control || strings.HasPrefix(call, "create:") || strings.HasPrefix(call, "update:") || strings.HasPrefix(call, "delete:") {
			t.Fatalf("denial probes ran before positive data read: %v", store.calls)
		}
	}
}

func TestGCSIAMProbeUnexpectedWritesFailAndCleanupIsExact(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*fakeGCSIAMProbeStore)
		want string
	}{
		{"create", func(s *fakeGCSIAMProbeStore) { s.allowCreate = true; s.allowDelete = true }, "create permission"},
		{"update", func(s *fakeGCSIAMProbeStore) { s.allowUpdate = true; s.allowDelete = true }, "update permission"},
		{"delete", func(s *fakeGCSIAMProbeStore) { s.allowDelete = true }, "delete permission"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, cfg := prepareFakeGCSIAMProbe(t)
			test.set(store)
			if err := verifyGCSIAMProbe(context.Background(), cfg, store); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unexpected write access passed: %v", err)
			}
			keys := gcsIAMProbeKeys(cfg)
			// Cleanup runs as the trusted writer, not the read-only identity
			// whose expected 403 was exercised above.
			store.deniedReads[keys.control] = false
			store.unknownVersions[keys.read] = map[int64]bool{999: true}
			outside := cfg.Prefix + "unowned/object"
			store.objects[outside] = fakeGCSIAMProbeObject{body: []byte("untouched"), generation: 99}
			if test.name == "update" {
				t.Setenv("RHIZA_GCS_IAM_PROBE_UPDATE_GENERATION", "4")
			}
			if err := cleanupGCSIAMProbe(context.Background(), cfg, store); err != nil {
				t.Fatalf("exact cleanup failed: %v", err)
			}
			wantKeys := []gcsIAMProbeOwnedGeneration{
				{key: keys.read, generation: 2, expected: gcsIAMProbePayload(cfg, "read")}, {key: keys.update, generation: 3, expected: gcsIAMProbePayload(cfg, "update")},
				{key: keys.delete, generation: 4, expected: gcsIAMProbePayload(cfg, "delete")}, {key: keys.control, generation: 5, expected: gcsIAMProbePayload(cfg, "control")},
				{key: keys.manifest, generation: cfg.ManifestGeneration},
			}
			if test.name == "update" {
				wantKeys = append(wantKeys, gcsIAMProbeOwnedGeneration{key: keys.update, generation: 4, expected: gcsIAMProbePayload(cfg, "update")})
			}
			if !reflect.DeepEqual(cleanupPairsByKey(store.cleanupKeys), cleanupPairsByKey(wantKeys)) {
				t.Fatalf("cleanup target=%v want=%v", store.cleanupKeys, wantKeys)
			}
			if _, ok := store.objects[outside]; !ok {
				t.Fatal("cleanup touched an object outside its exact run-owned keys")
			}
			if !store.unknownVersions[keys.read][999] {
				t.Fatal("cleanup removed an unrecorded version of a probe object")
			}
		})
	}
}

func TestGCSIAMProbeSameGenerationMetadataUpdateDeduplicatesCleanup(t *testing.T) {
	store, cfg := prepareFakeGCSIAMProbe(t)
	store.allowUpdate = true
	store.allowDelete = true
	store.sameGenUpdate = true
	if err := verifyGCSIAMProbe(context.Background(), cfg, store); err == nil || !strings.Contains(err.Error(), "update permission was not denied") {
		t.Fatalf("same-generation metadata write was accepted: %v", err)
	}
	keys := gcsIAMProbeKeys(cfg)
	deleteCalls := 0
	for _, call := range store.calls {
		if call == "delete:"+keys.update {
			deleteCalls++
		}
	}
	if deleteCalls != 1 {
		t.Fatalf("same-generation update cleanup calls=%d, want one exact-generation delete; calls=%v", deleteCalls, store.calls)
	}
}

func TestGCSIAMCleanupFailurePreservesExactRemainingGeneration(t *testing.T) {
	store, cfg := prepareFakeGCSIAMProbe(t)
	store.deniedReads[gcsIAMProbeKeys(cfg).control] = false
	store.cleanupErr = &gcsIAMProbeCleanupPending{items: []gcsIAMProbeCleanupItem{{label: "read", generation: 2}}}
	err := cleanupGCSIAMProbe(context.Background(), cfg, store)
	var pending *gcsIAMProbeCleanupPending
	if !errors.As(err, &pending) || len(pending.items) != 1 || pending.items[0] != (gcsIAMProbeCleanupItem{label: "read", generation: 2}) {
		t.Fatalf("exact cleanup failure was hidden: %v", err)
	}
}

func cleanupPairsByKey(items []gcsIAMProbeOwnedGeneration) map[string][]int64 {
	result := make(map[string][]int64)
	for _, item := range items {
		result[item.key] = append(result[item.key], item.generation)
	}
	return result
}

func TestGCSIAMProbeRejectsInvalidManifestAndUnsafePrefix(t *testing.T) {
	cfg := gcsIAMProbeTestConfig()
	keys := gcsIAMProbeKeys(cfg)
	valid := gcsIAMProbeManifest{
		Version: gcsIAMProbeVersion, RunID: cfg.RunID, Nonce: cfg.Nonce,
		ReadKey: keys.read, ReadGeneration: 1, ReadSHA256: strings.Repeat("a", 64),
		UpdateKey: keys.update, UpdateGeneration: 2, DeleteKey: keys.delete, DeleteGeneration: 3,
		ControlKey: keys.control, ControlGeneration: 4,
	}
	encoded, err := jsonMarshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGCSIAMProbeManifest(encoded, cfg, keys); err != nil {
		t.Fatalf("valid immutable manifest rejected: %v", err)
	}
	valid.ControlKey = cfg.Prefix + "outside/control"
	encoded, _ = jsonMarshal(valid)
	if _, err := decodeGCSIAMProbeManifest(encoded, cfg, keys); err == nil {
		t.Fatal("manifest redirected access outside the run-owned control key")
	}
	for _, prefix := range []string{"qual/run/1234abcd/../other/1234abcd/", "qual/run//1234abcd/", "qual/run/1234abcd/../"} {
		if _, err := gcsIAMProbeConfigFromValues("rhiza-test-bucket", prefix, "1234abcd", cfg.Nonce, cfg.ExpectedGSA, "0"); err == nil {
			t.Fatalf("unsafe prefix accepted: %q", prefix)
		}
	}
}

func TestGCSIAMProbeIdentityMismatchDoesNotLeakIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			t.Errorf("metadata header=%q", r.Header.Get("Metadata-Flavor"))
		}
		_, _ = io.WriteString(w, "unexpected-private-identity@example.invalid\n")
	}))
	defer server.Close()
	client := server.Client()
	oldURL := gcsIAMProbeMetadataURL
	gcsIAMProbeMetadataURL = server.URL
	t.Cleanup(func() { gcsIAMProbeMetadataURL = oldURL })
	err := verifyGCSIAMProbeIdentity(context.Background(), gcsIAMProbeTestConfig().ExpectedGSA, client)
	if err == nil || strings.Contains(err.Error(), "unexpected-private-identity") {
		t.Fatalf("identity mismatch leaked or passed: %v", err)
	}
}

func TestGCSIAMProbeIdentityDoesNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/redirected-identity", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	oldURL := gcsIAMProbeMetadataURL
	gcsIAMProbeMetadataURL = server.URL
	t.Cleanup(func() { gcsIAMProbeMetadataURL = oldURL })
	if err := verifyGCSIAMProbeIdentity(context.Background(), gcsIAMProbeTestConfig().ExpectedGSA, server.Client()); err == nil {
		t.Fatal("metadata identity check followed an untrusted redirect")
	}
}

func TestGCSIAMPermissionDeniedRequiresHTTP403(t *testing.T) {
	if !isGCSIAMPermissionDenied(&googleapi.Error{Code: http.StatusForbidden}) {
		t.Fatal("HTTP 403 was not recognized")
	}
	for _, status := range []int{http.StatusNotFound, http.StatusUnauthorized, http.StatusInternalServerError} {
		if isGCSIAMPermissionDenied(&googleapi.Error{Code: status}) {
			t.Fatalf("HTTP %d was accepted as permission denial", status)
		}
	}
	if isGCSIAMPermissionDenied(errors.New("permission denied")) {
		t.Fatal("untyped error was accepted as permission denial")
	}
}

func TestGCSIAMProbeRejectsEmulatorAndCredentialOverrides(t *testing.T) {
	for _, name := range gcsIAMProbeCredentialOverrides {
		t.Run(name, func(t *testing.T) {
			if err := rejectGCSIAMProbeOverrides(func(key string) string {
				if key == name {
					return "injected"
				}
				return ""
			}); err == nil || strings.Contains(err.Error(), "injected") {
				t.Fatalf("credential or endpoint override was accepted or leaked: %v", err)
			}
		})
	}
	home := t.TempDir()
	path := filepath.Join(home, ".config", "gcloud", "application_default_credentials.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("not-read"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := rejectGCSIAMProbeOverrides(func(name string) string {
		if name == "HOME" {
			return home
		}
		return ""
	}); err == nil {
		t.Fatal("well-known local ADC credential file was accepted")
	}
}

func TestGCSIAMProbeCleanupAttemptsAndReportsAllExactGenerations(t *testing.T) {
	store, cfg := prepareFakeGCSIAMProbe(t)
	keys := gcsIAMProbeKeys(cfg)
	store.deniedReads[keys.control] = false
	t.Setenv("RHIZA_GCS_IAM_PROBE_UPDATE_GENERATION", "4")
	store.cleanupErr = &gcsIAMProbeCleanupPending{items: []gcsIAMProbeCleanupItem{
		{label: "update", generation: 3}, {label: "update", generation: 4},
	}}
	err := cleanupGCSIAMProbe(context.Background(), cfg, store)
	var pending *gcsIAMProbeCleanupPending
	if !errors.As(err, &pending) || len(pending.items) != 2 ||
		pending.items[0].label != "update" || pending.items[1].label != "update" {
		t.Fatalf("multiple exact generations were hidden: %v", err)
	}
	if len(store.cleanupKeys) != 6 || store.cleanupKeys[2].key != keys.update || store.cleanupKeys[2].generation != 3 {
		t.Fatalf("cleanup scope lost baseline version: %+v", store.cleanupKeys)
	}
}

func TestGCSIAMCleanupRetainsExactVersionWhenPayloadDoesNotMatch(t *testing.T) {
	store, cfg := prepareFakeGCSIAMProbe(t)
	store.deniedReads[gcsIAMProbeKeys(cfg).control] = false
	key := gcsIAMProbeKeys(cfg).read
	version := store.versions[key][2]
	store.versions[key][2] = []byte("different payload")
	store.objects[key] = fakeGCSIAMProbeObject{body: []byte("different payload"), generation: 2}

	err := cleanupGCSIAMProbe(context.Background(), cfg, store)
	var pending *gcsIAMProbeCleanupPending
	if !errors.As(err, &pending) || len(pending.items) == 0 {
		t.Fatalf("mismatched exact generation was not retained as pending: %v", err)
	}
	if got := store.versions[key][2]; !bytes.Equal(got, []byte("different payload")) {
		t.Fatalf("mismatched generation was deleted: %q", got)
	}
	if bytes.Equal(version, store.versions[key][2]) {
		t.Fatal("test mutation did not alter the exact generation")
	}
}

func TestGCSIAMProbeManifestRejectsTrailingData(t *testing.T) {
	cfg := gcsIAMProbeTestConfig()
	keys := gcsIAMProbeKeys(cfg)
	manifest := gcsIAMProbeManifest{Version: 1, RunID: cfg.RunID, Nonce: cfg.Nonce, ReadKey: keys.read, ReadGeneration: 1,
		ReadSHA256: strings.Repeat("a", 64), UpdateKey: keys.update, UpdateGeneration: 2,
		DeleteKey: keys.delete, DeleteGeneration: 3, ControlKey: keys.control, ControlGeneration: 4}
	encoded, err := jsonMarshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGCSIAMProbeManifest(append(encoded, []byte(" {}")...), cfg, keys); err == nil {
		t.Fatal("trailing manifest data accepted")
	}
}

func jsonMarshal(value any) ([]byte, error) {
	var buffer bytes.Buffer
	err := json.NewEncoder(&buffer).Encode(value)
	return bytes.TrimSpace(buffer.Bytes()), err
}
