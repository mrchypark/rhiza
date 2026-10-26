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
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	gcsAckLossConfigMapName          = "rhiza-fencer-ack-loss"
	gcsAckLossConfigPath             = "/var/run/rhiza/gcs-ack-loss/config.json"
	gcsAckLossVolumeName             = "gcs-ack-loss-config"
	gcsAckLossEventPrefix            = "GCS_ACK_LOSS_QUALIFICATION "
	maxAckLossReceiptBytes           = 1 << 20
	maxAckLossPayloadBytes           = int64(4 << 30)
	gcsAckLossAllowWriterObserverEnv = "RHIZA_GCS_ALLOW_WRITER_OBSERVER"
)

var gcsAckLossCredentialEmail = regexp.MustCompile(`^[^@\s]+@[^@\s]+$`)
var gcsAckLossExtentKey = regexp.MustCompile(`^archive/blocks/[a-f0-9]{64}_[0-9]{20}\.bin$`)

// installGCSAckLossConfig opts only the run-owned fencer Deployment into the
// one-shot exact-dispatch fault. The ConfigMap has no credentials and is
// removed only after the original Deployment template has been restored.
func installGCSAckLossConfig(ctx context.Context, source gcsRecoverySource, config operator.GCSMutationAckLossConfig, out string, expectedTemplates ...obj) (string, func(context.Context) error, error) {
	if err := config.Validate(); err != nil {
		return "", nil, err
	}
	if source.Namespace != namespace || (source.StatefulSetUID == "" && config.Bootstrap == nil) || source.ClusterID == "" || source.Bucket != config.Bucket || source.Prefix != config.Prefix {
		return "", nil, fmt.Errorf("acknowledgement-loss config is outside the captured GCS source scope")
	}
	if config.Bootstrap != nil && (source.StatefulSetUID != "" || source.NamespaceUID != config.Bootstrap.NamespaceUID || source.ClusterID != config.Bootstrap.SourceClusterID) {
		return "", nil, fmt.Errorf("bootstrap acknowledgement-loss config differs from the pre-apply namespace/source binding")
	}
	if source.FencerPod == "" || out == "" {
		return "", nil, fmt.Errorf("acknowledgement-loss rollout identity is incomplete")
	}
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return "", nil, err
	}
	deploymentUID := text(nested(deployment, "metadata", "uid"))
	if deploymentUID == "" || text(nested(deployment, "metadata", "namespace")) != namespace ||
		text(nested(deployment, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID || nested(deployment, "spec", "replicas") != float64(1) {
		return "", nil, fmt.Errorf("refusing to alter a fencer Deployment outside this run")
	}
	if len(expectedTemplates) > 1 || len(expectedTemplates) == 1 && !sameJSON(nested(deployment, "spec", "template"), expectedTemplates[0]) {
		return "", nil, fmt.Errorf("fencer Pod template changed after initial-HEAD restore state was captured")
	}
	oldFencerPod, err := get(ctx, "pod", source.FencerPod)
	if err != nil || text(nested(oldFencerPod, "metadata", "namespace")) != namespace ||
		text(nested(oldFencerPod, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
		!ackLossPodOwnedBy(ctx, oldFencerPod, deploymentUID) {
		return "", nil, fmt.Errorf("captured fencer Pod is not owned by the run-scoped Deployment")
	}
	oldFencerPodUID := text(nested(oldFencerPod, "metadata", "uid"))
	if oldFencerPodUID == "" {
		return "", nil, fmt.Errorf("captured fencer Pod has no immutable UID")
	}
	if err := validateAckLossFencerStorageConfig(ctx, deployment, source); err != nil {
		return "", nil, err
	}
	containerIndex, args, mounts, volumes, err := ackLossDeploymentTemplate(deployment)
	if err != nil {
		return "", nil, err
	}
	patchedArgs := append(append([]any(nil), args...), "--gcs-ack-loss-config-file="+gcsAckLossConfigPath)
	patchedMounts := append(append([]any(nil), mounts...), obj{"name": gcsAckLossVolumeName, "mountPath": filepath.Dir(gcsAckLossConfigPath), "readOnly": true})
	patchedVolumes := append(append([]any(nil), volumes...), obj{"name": gcsAckLossVolumeName, "configMap": obj{"name": gcsAckLossConfigMapName, "items": []any{obj{"key": "config.json", "path": "config.json"}}}})
	originalTemplate, err := ackLossPodTemplate(deployment)
	if err != nil {
		return "", nil, err
	}
	patchedTemplate, err := patchAckLossPodTemplate(originalTemplate, containerIndex, patchedArgs, patchedMounts, patchedVolumes)
	if err != nil {
		return "", nil, err
	}
	configMap, err := get(ctx, "configmap", gcsAckLossConfigMapName)
	if err == nil && configMap != nil {
		return "", nil, fmt.Errorf("run-owned acknowledgement-loss ConfigMap already exists")
	}
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "notfound") && !strings.Contains(strings.ToLower(err.Error()), "not found") {
		return "", nil, fmt.Errorf("check acknowledgement-loss ConfigMap absence: %w", err)
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		return "", nil, err
	}
	manifest := obj{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata":  obj{"name": gcsAckLossConfigMapName, "namespace": namespace, "labels": obj{"chaos.rhiza.io/run": qualificationRunID}},
		"immutable": true, "data": obj{"config.json": string(configBytes)},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return "", nil, err
	}
	configPath, err := writePrivateAckLossConfig(out, manifestBytes)
	if err != nil {
		return "", nil, err
	}
	defer os.Remove(configPath)
	if _, err := kubectl(ctx, "create", "-f", configPath); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cleanupErr := reconcileAndDeleteAckLossConfigMap(cleanupCtx, configBytes)
		cancel()
		return "", nil, errors.Join(fmt.Errorf("create exact run-owned acknowledgement-loss ConfigMap: %w", err), cleanupErr)
	}
	createdConfigMap, err := get(ctx, "configmap", gcsAckLossConfigMapName)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cleanupErr := reconcileAndDeleteAckLossConfigMap(cleanupCtx, configBytes)
		cancel()
		return "", nil, errors.Join(fmt.Errorf("read created acknowledgement-loss ConfigMap: %w", err), cleanupErr)
	}
	if text(nested(createdConfigMap, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
		text(nested(createdConfigMap, "data", "config.json")) != string(configBytes) || nested(createdConfigMap, "immutable") != true {
		return "", nil, fmt.Errorf("created acknowledgement-loss ConfigMap differs from the exact run config; refusing deletion and possible residue remains")
	}
	configUID := text(nested(createdConfigMap, "metadata", "uid"))
	if configUID == "" {
		return "", nil, fmt.Errorf("created acknowledgement-loss ConfigMap has no UID")
	}

	patch := ackLossDeploymentPatchWithTemplate(deploymentUID, qualificationRunID, containerIndex, args, mounts, volumes, patchedArgs, patchedMounts, patchedVolumes, originalTemplate)
	if _, err := kubectl(ctx, "patch", "deployment", "rhiza-fencer", "--type=json", "-p", string(encoded(patch))); err != nil {
		current, readErr := get(ctx, "deployment", "rhiza-fencer")
		if readErr == nil && text(nested(current, "metadata", "uid")) == deploymentUID &&
			text(nested(current, "metadata", "labels", "chaos.rhiza.io/run")) == qualificationRunID {
			currentIndex, currentArgs, currentMounts, currentVolumes, templateErr := readAckLossDeploymentTemplate(current)
			if templateErr == nil && currentIndex == containerIndex && sameJSON(nested(current, "spec", "template"), patchedTemplate) && sameJSON(currentArgs, patchedArgs) && sameJSON(currentMounts, patchedMounts) && sameJSON(currentVolumes, patchedVolumes) {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
				cleanupErr := restoreAckLossDeployment(cleanupCtx, deploymentUID, containerIndex, args, mounts, volumes, patchedArgs, patchedMounts, patchedVolumes, originalTemplate, patchedTemplate)
				if cleanupErr == nil {
					_, cleanupErr = kubectl(cleanupCtx, "rollout", "status", "deployment/rhiza-fencer", "--timeout=120s")
				}
				if cleanupErr == nil {
					cleanupErr = deleteAckLossConfigMapExact(cleanupCtx, configUID, gcsInitialHeadDigest(configBytes))
				}
				cancel()
				return "", nil, errors.Join(fmt.Errorf("fencer patch response was lost after applying the exact template: %w", err), cleanupErr)
			}
			if templateErr == nil && currentIndex == containerIndex && sameJSON(nested(current, "spec", "template"), originalTemplate) && sameJSON(currentArgs, args) && sameJSON(currentMounts, mounts) && sameJSON(currentVolumes, volumes) {
				cleanupErr := deleteAckLossConfigMapExact(ctx, configUID, gcsInitialHeadDigest(configBytes))
				return "", nil, errors.Join(fmt.Errorf("patch run-owned fencer Deployment for acknowledgement-loss qualification: %w", err), cleanupErr)
			}
		}
		return "", nil, errors.Join(fmt.Errorf("fencer patch outcome is unknown; leaving run-owned ConfigMap %s for cleanup: %w", configUID, err), readErr)
	}
	cleanup := func(cleanupCtx context.Context) error {
		current, err := get(cleanupCtx, "deployment", "rhiza-fencer")
		if err != nil {
			return fmt.Errorf("read fencer Deployment before restoring qualification config: %w", err)
		}
		if text(nested(current, "metadata", "uid")) != deploymentUID || text(nested(current, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
			return fmt.Errorf("fencer Deployment UID changed; leaving qualification config for operator cleanup")
		}
		currentIndex, currentArgs, currentMounts, currentVolumes, err := readAckLossDeploymentTemplate(current)
		if err != nil {
			return fmt.Errorf("read fencer Deployment template before restore: %w", err)
		}
		if currentIndex != containerIndex || !sameJSON(nested(current, "spec", "template"), patchedTemplate) || !sameJSON(currentArgs, patchedArgs) || !sameJSON(currentMounts, patchedMounts) || !sameJSON(currentVolumes, patchedVolumes) {
			return fmt.Errorf("fencer Deployment template changed during qualification; refusing to overwrite it")
		}
		restorePatch := ackLossDeploymentPatchWithTemplate(deploymentUID, qualificationRunID, containerIndex, patchedArgs, patchedMounts, patchedVolumes, args, mounts, volumes, patchedTemplate)
		if _, err := kubectl(cleanupCtx, "patch", "deployment", "rhiza-fencer", "--type=json", "-p", string(encoded(restorePatch))); err != nil {
			return fmt.Errorf("restore fencer Deployment template: %w", err)
		}
		if _, err := kubectl(cleanupCtx, "rollout", "status", "deployment/rhiza-fencer", "--timeout=120s"); err != nil {
			return fmt.Errorf("wait for original fencer Deployment after restore: %w", err)
		}
		return deleteAckLossConfigMapExact(cleanupCtx, configUID, gcsInitialHeadDigest(configBytes))
	}
	if _, err := kubectl(ctx, "rollout", "status", "deployment/rhiza-fencer", "--timeout=120s"); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		return "", nil, errors.Join(fmt.Errorf("wait for acknowledgement-loss fencer rollout: %w", err), cleanup(cleanupCtx))
	}
	fencerPod, err := waitAckLossFencerRestart(ctx, source.FencerPod, oldFencerPodUID, deploymentUID)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		return "", nil, errors.Join(err, cleanup(cleanupCtx))
	}
	return fencerPod, cleanup, nil
}

// reconcileAndDeleteAckLossConfigMap only adopts cleanup ownership after the
// deterministic name is read back with the exact run label, immutable bit,
// and complete expected config. An unreadable or mismatched object is left in
// place and reported rather than deleted by name.
func reconcileAndDeleteAckLossConfigMap(ctx context.Context, expected []byte) error {
	current, err := get(ctx, "configmap", gcsAckLossConfigMapName)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "notfound") || strings.Contains(strings.ToLower(err.Error()), "not found") {
			return nil
		}
		return fmt.Errorf("could not determine whether acknowledgement-loss ConfigMap remains; possible residue: %w", err)
	}
	uid := text(nested(current, "metadata", "uid"))
	if uid == "" || text(nested(current, "metadata", "namespace")) != namespace ||
		text(nested(current, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
		text(nested(current, "data", "config.json")) != string(expected) || nested(current, "immutable") != true {
		return fmt.Errorf("acknowledgement-loss ConfigMap ownership is not exact; possible residue remains")
	}
	if err := deleteAckLossConfigMapExact(ctx, uid, gcsInitialHeadDigest(expected)); err != nil {
		return fmt.Errorf("delete exact reconciled acknowledgement-loss ConfigMap UID %s; possible residue remains: %w", uid, err)
	}
	return nil
}

type readOnlyAuthorityBucket struct {
	client *storage.Client
	name   string
}

var errGCSAuthorityObserverReadOnly = errors.New("GCS authority observer is read-only")

func isAckLossNotFound(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "notfound") || strings.Contains(lower, "not found")
}

func (readOnlyAuthorityBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	return errGCSAuthorityObserverReadOnly
}

func (readOnlyAuthorityBucket) Delete(context.Context, string) error {
	return errGCSAuthorityObserverReadOnly
}

// The CAS options are advertised only so the authority-store codec can validate
// its envelope contract. This bucket is backed by a read-only OAuth scope and
// every mutation method is rejected locally.
func (b readOnlyAuthorityBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	return []objstore.ObjectUploadOptionType{objstore.IfNotExists, objstore.IfMatch}
}

func (readOnlyAuthorityBucket) Provider() objstore.ObjProvider { return objstore.GCS }
func (b readOnlyAuthorityBucket) Name() string                 { return b.name }
func (readOnlyAuthorityBucket) Close() error                   { return nil }

func (readOnlyAuthorityBucket) SupportedIterOptions() []objstore.IterOptionType {
	return []objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt}
}

func (b readOnlyAuthorityBucket) Iter(ctx context.Context, dir string, fn func(string) error, options ...objstore.IterOption) error {
	var recursive []objstore.IterOption
	for _, option := range options {
		if option.Type == objstore.Recursive {
			recursive = append(recursive, option)
			break
		}
	}
	return b.IterWithAttributes(ctx, dir, func(attrs objstore.IterObjectAttributes) error { return fn(attrs.Name) }, recursive...)
}

func (b readOnlyAuthorityBucket) IterWithAttributes(ctx context.Context, dir string, fn func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if err := objstore.ValidateIterOptions(b.SupportedIterOptions(), options...); err != nil {
		return err
	}
	if dir != "" {
		dir = strings.TrimSuffix(dir, objstore.DirDelim) + objstore.DirDelim
	}
	applied := objstore.ApplyIterOptions(options...)
	delimiter := objstore.DirDelim
	if applied.Recursive {
		delimiter = ""
	}
	query := &storage.Query{Prefix: dir, Delimiter: delimiter}
	attrsToSelect := []string{"Name"}
	if applied.LastModified {
		attrsToSelect = append(attrsToSelect, "Updated")
	}
	if err := query.SetAttrSelection(attrsToSelect); err != nil {
		return err
	}
	it := b.client.Bucket(b.name).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		entry := objstore.IterObjectAttributes{Name: attrs.Prefix + attrs.Name}
		if applied.LastModified {
			entry.SetLastModified(attrs.Updated)
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
}

func (b readOnlyAuthorityBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.client.Bucket(b.name).Object(name).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return objstore.ObjectSizerReadCloser{ReadCloser: reader, Size: func() (int64, error) { return reader.Attrs.Size, nil }}, nil
}

func (b readOnlyAuthorityBucket) GetRange(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	reader, err := b.client.Bucket(b.name).Object(name).NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, err
	}
	size := reader.Remain()
	return objstore.ObjectSizerReadCloser{ReadCloser: reader, Size: func() (int64, error) { return size, nil }}, nil
}

func (b readOnlyAuthorityBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	attrs, err := b.client.Bucket(b.name).Object(name).Attrs(ctx)
	if err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return objstore.ObjectAttributes{Size: attrs.Size, LastModified: attrs.Updated,
		Version: &objstore.ObjectVersion{Type: objstore.Generation, Value: strconv.FormatInt(attrs.Generation, 10)}}, nil
}

func (b readOnlyAuthorityBucket) Exists(ctx context.Context, name string) (bool, error) {
	_, err := b.client.Bucket(b.name).Object(name).Attrs(ctx)
	if err == nil {
		return true, nil
	}
	if b.IsObjNotFoundErr(err) {
		return false, nil
	}
	return false, err
}

func (readOnlyAuthorityBucket) IsObjNotFoundErr(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}
func (readOnlyAuthorityBucket) IsAccessDeniedErr(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == 403
}
func (readOnlyAuthorityBucket) IsConditionNotMetErr(error) bool { return false }

type gcsAckLossReadScope struct {
	Bucket               string
	Prefix               string
	CredentialsJSON      []byte
	CredentialsPath      string
	SharedWriterIdentity bool
}

type gcsAckLossObjectEvidence struct {
	Generation     int64  `json:"generation"`
	Metageneration int64  `json:"metageneration"`
	Size           int64  `json:"size"`
	PayloadSHA256  string `json:"payloadSHA256"`
	TemporaryHold  bool   `json:"temporaryHold"`
}

type gcsAckLossPrivateEvidence struct {
	Version                      int                                `json:"version"`
	BaselineAck                  gcsAcknowledgement                 `json:"baselineAck"`
	FaultReceipt                 operator.GCSMutationAckLossReceipt `json:"faultReceipt"`
	CanonicalObservation         gcsHAAckObservation                `json:"canonicalObservation"`
	Object                       gcsAckLossObjectEvidence           `json:"object"`
	FirstRecovery                obj                                `json:"firstRecovery"`
	SecondRecovery               obj                                `json:"secondRecovery"`
	SecondAck                    gcsAcknowledgement                 `json:"secondAck"`
	ObserverSharedWriterIdentity bool                               `json:"observerSharedWriterIdentity"`
}

type gcsAckLossCredentialIdentity struct {
	Type             string `json:"type"`
	ClientEmail      string `json:"client_email"`
	Audience         string `json:"audience"`
	ImpersonationURL string `json:"service_account_impersonation_url"`
}

func parseGCSAckLossWriterObserverOptIn(value string) (bool, error) {
	switch strings.TrimSpace(value) {
	case "", "no":
		return false, nil
	case "yes":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be yes or no when set", gcsAckLossAllowWriterObserverEnv)
	}
}

func validateGCSAckLossObserverIdentity(identity gcsAckLossCredentialIdentity, readerEmail, writerEmail string, allowWriterObserver bool) (bool, error) {
	readerEmail = strings.TrimSpace(readerEmail)
	if !gcsAckLossCredentialEmail.MatchString(readerEmail) {
		return false, fmt.Errorf("read-only authority reader identity is invalid")
	}
	sharedWriterIdentity := readerEmail == writerEmail
	if sharedWriterIdentity && !allowWriterObserver {
		return false, fmt.Errorf("authority observer must use a distinct explicitly configured reader identity")
	}
	if identity.Type == "service_account" {
		if !gcsAckLossCredentialEmail.MatchString(identity.ClientEmail) || identity.ClientEmail != readerEmail {
			return false, fmt.Errorf("read-only observer service account identity differs from the configured authority reader")
		}
	} else if identity.Type != "external_account" || identity.Audience == "" || !strings.Contains(identity.ImpersonationURL, readerEmail) {
		return false, fmt.Errorf("read-only observer requires explicit service-account or workload-identity credentials")
	}
	return sharedWriterIdentity, nil
}

func newGCSAckLossReadScope(ctx context.Context, source gcsRecoverySource, deployment obj) (gcsAckLossReadScope, error) {
	var result gcsAckLossReadScope
	if source.Bucket == "" || source.ConfigMap == "" || source.ClusterID == "" || source.NamespaceUID == "" {
		return result, fmt.Errorf("GCS ACK observer source identity is incomplete")
	}
	allowWriterObserver, err := parseGCSAckLossWriterObserverOptIn(os.Getenv(gcsAckLossAllowWriterObserverEnv))
	if err != nil {
		return result, err
	}
	credentialsPath := strings.TrimSpace(os.Getenv("RHIZA_GCS_AUTHORITY_READONLY_CREDENTIALS_FILE"))
	if credentialsPath == "" {
		return result, fmt.Errorf("RHIZA_GCS_AUTHORITY_READONLY_CREDENTIALS_FILE is required; refusing writer-credential fallback")
	}
	info, err := os.Lstat(credentialsPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return result, fmt.Errorf("read-only GCS observer credentials must be a regular private file")
	}
	credentials, err := os.ReadFile(credentialsPath)
	if err != nil || len(credentials) == 0 || len(credentials) > 1<<20 {
		return result, fmt.Errorf("read-only GCS observer credentials are unavailable or oversized")
	}
	var credentialIdentity gcsAckLossCredentialIdentity
	if err := json.Unmarshal(credentials, &credentialIdentity); err != nil {
		return result, fmt.Errorf("decode read-only observer credential identity: %w", err)
	}
	readerEmail := strings.TrimSpace(os.Getenv("RHIZA_GCS_AUTHORITY_READER_EMAIL"))
	project, writerID := strings.TrimSpace(os.Getenv("RHIZA_GCP_PROJECT_ID")), strings.TrimSpace(os.Getenv("RHIZA_DATA_GSA_ID"))
	if readerEmail == "" || project == "" || writerID == "" {
		return result, fmt.Errorf("read-only authority reader identity is required")
	}
	writerEmail := writerID + "@" + project + ".iam.gserviceaccount.com"
	sharedWriterIdentity, err := validateGCSAckLossObserverIdentity(credentialIdentity, readerEmail, writerEmail, allowWriterObserver)
	if err != nil {
		return result, err
	}
	for _, override := range []string{"STORAGE_EMULATOR_HOST", "GCE_METADATA_HOST", "GCE_METADATA_IP", "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"} {
		if strings.TrimSpace(os.Getenv(override)) != "" {
			return result, fmt.Errorf("GCS observer credential override %s is not allowed", override)
		}
	}
	container, err := boundContainer(deployment, "authority")
	if err != nil {
		return result, err
	}
	bucket, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_BUCKET")
	if err != nil || bucket != source.Bucket {
		return result, fmt.Errorf("fencer canonical-state bucket differs from the captured source bucket")
	}
	authorityPrefix, err := effectiveGCSSetting(ctx, container, "FENCER_AUTHORITY_STATE_PREFIX")
	if err != nil || authorityPrefix == "" || strings.Trim(authorityPrefix, "/") != authorityPrefix || path.Clean(authorityPrefix) != authorityPrefix || gcsAckLossScopesOverlap(authorityPrefix, source.Prefix) {
		return result, fmt.Errorf("fencer canonical authority prefix is not explicitly configured")
	}
	basePrefix, err := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_PREFIX")
	if err != nil || strings.Trim(basePrefix, "/") != basePrefix || path.Clean(basePrefix) != basePrefix || path.Join(basePrefix, source.ClusterID) != source.Prefix {
		return result, fmt.Errorf("captured GCS data prefix is not canonical")
	}
	result = gcsAckLossReadScope{Bucket: bucket, Prefix: authorityPrefix, CredentialsJSON: credentials, CredentialsPath: credentialsPath, SharedWriterIdentity: sharedWriterIdentity}
	return result, nil
}

func openGCSAckLossAuthorityStores(ctx context.Context, source gcsRecoverySource, scope gcsAckLossReadScope) (*operator.AuthorityStateStore, *operator.AuthorityEvidenceStore, *storage.Client, objstore.Bucket, func() error, error) {
	if scope.Bucket != source.Bucket || scope.Prefix == "" || scope.CredentialsPath == "" || len(scope.CredentialsJSON) == 0 {
		return nil, nil, nil, nil, nil, fmt.Errorf("read-only GCS authority scope is incomplete")
	}
	storageClient, err := storage.NewClient(ctx, option.WithCredentialsJSON(scope.CredentialsJSON), option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("open read-only GCS authority observer")
	}
	reader := readOnlyAuthorityBucket{client: storageClient, name: scope.Bucket}
	state, err := operator.NewAuthorityStateStore(reader, scope.Prefix)
	if err != nil {
		_ = storageClient.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("configure read-only canonical authority store: %w", err)
	}
	evidence, err := operator.NewAuthorityEvidenceStore(reader, scope.Prefix, source.StatefulSetUID, source.ClusterID)
	if err != nil {
		_ = storageClient.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("configure read-only canonical authority evidence: %w", err)
	}
	return state, evidence, storageClient, reader, nil, nil
}

// qualifyGCSHAAckLoss reuses the normal SQL/archive writer path, then proves
// the discarded provider response against canonical authority state and an
// independent read-only GCS object read before running the two recovery cycle.
func qualifyGCSHAAckLoss(ctx context.Context) (resultErr error) {
	out := strings.TrimSpace(os.Getenv("FENCING_E2E_OUT"))
	if out == "" {
		out = "gcs-ack-loss-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(out); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("GCS ACK-loss result directory must be private")
	}
	events, err := os.OpenFile(filepath.Join(out, gcsAckLossEventsName), os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	var eventErr error
	event := func(name string, data any) {
		if eventErr != nil {
			return
		}
		_, eventErr = fmt.Fprintln(events, string(encoded(obj{"event": name, "time": time.Now().UTC(), "data": data})))
	}
	var faults []*gcsChaosFault
	var fencerCleanup func(context.Context) error
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if fencerCleanup != nil {
			if err := fencerCleanup(cleanupCtx); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restore fencer after ACK-loss qualification: %w", err))
			}
		}
		for i := len(faults) - 1; i >= 0; i-- {
			if err := faults[i].Restore(cleanupCtx); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restore owned ACK-loss GCS fault: %w", err))
			}
			event("chaos_cleanup", gcsChaosEvidence(faults[i]))
		}
		if closeErr := events.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close ACK-loss event record: %w", closeErr))
		}
		if eventErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("write ACK-loss event record: %w", eventErr))
		}
	}()

	source, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if source.Durability != "before-ack" || len(source.Targets) < 3 {
		return fmt.Errorf("ACK-loss qualification requires the independent before-ACK GCS profile and a voter cohort")
	}
	fencerDeployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	readScope, err := newGCSAckLossReadScope(ctx, source, fencerDeployment)
	if err != nil {
		return err
	}
	stateStore, evidenceStore, gcsClient, readBucket, closeStores, err := openGCSAckLossAuthorityStores(ctx, source, readScope)
	if err != nil {
		return err
	}
	defer func() {
		if gcsClient != nil {
			resultErr = errors.Join(resultErr, gcsClient.Close())
		}
		if closeStores != nil {
			resultErr = errors.Join(resultErr, closeStores())
		}
	}()
	// Prove the explicit reader identity and both control/data prefixes before
	// changing the fencer Deployment or injecting any fault.
	if _, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID); err != nil {
		return fmt.Errorf("read-only observer cannot read canonical source root; refusing to arm fault")
	}
	if _, err := readBucket.Attributes(ctx, path.Join(source.Prefix, "archive/head.bin")); err != nil {
		return fmt.Errorf("read-only observer cannot read source archive HEAD; refusing to arm fault")
	}

	adminToken, err := sourceAdminToken(ctx, source.StatefulSet, source.Container)
	if err != nil {
		return err
	}
	forward, err := startForward(ctx, source.Targets[0].Pod, out)
	if err != nil {
		return err
	}
	defer forward.close()
	if _, err := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-ack-loss-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		return fmt.Errorf("create ACK-loss baseline table: %w", err)
	}
	baselineAck, err := writeGCSAcknowledgedRow(ctx, forward, adminToken, 1, "gcs-ack-loss-baseline")
	if err != nil {
		return fmt.Errorf("establish committed baseline before arming ACK-loss: %w", err)
	}
	root, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
	if err != nil {
		return fmt.Errorf("re-read canonical source root before arming ACK-loss: %w", err)
	}
	runtime, producerUID, err := captureGCSAckLossRuntime(ctx, stateStore, source, source.Targets[0].PodUID)
	if err != nil {
		return err
	}
	if runtime.NamespaceUID != source.NamespaceUID || runtime.BindingUID != source.BindingUID || runtime.StatefulSetUID != source.StatefulSetUID || runtime.SourceClusterID != source.ClusterID {
		return fmt.Errorf("canonical source runtime differs from captured namespace and workload identities")
	}
	run, ok := root.Ledger.Runs[runtime.RunID]
	if !ok || run.Runtime == nil || *run.Runtime != runtime || run.Identity.PodUID != source.Targets[0].PodUID || run.MutationProducerUID != producerUID || run.MutationProfile != fencing.MutationHeldExecutionProfile {
		return fmt.Errorf("canonical source root does not contain the exact live held-v2 writer run")
	}
	config := operator.GCSMutationAckLossConfig{Version: 1, Selector: operator.GCSMutationAckLossSelectorFirstArchiveExtent,
		Runtime: runtime, Identity: run.Identity, Bucket: source.Bucket, Prefix: source.Prefix}
	fencerPod, cleanup, err := installGCSAckLossConfig(ctx, source, config, out)
	if err != nil {
		return err
	}
	fencerCleanup = cleanup
	event("ack_loss_fault_armed", obj{"runtimeBound": true, "fencerRestarted": true})

	// An idempotent UPDATE creates a real ordinary application decision while
	// leaving the baseline rowset unchanged for strict recovery readback.
	_, triggerErr := call(ctx, forward, "/sql/execute", obj{"request_id": "gcs-ack-loss-trigger", "sql": "UPDATE fencing_items SET value='gcs-ack-loss-baseline' WHERE id=1"}, adminToken)
	event("trigger_response", obj{"returnedError": triggerErr != nil})
	receipt, err := waitForGCSAckLossReceipt(ctx, fencerPod, out)
	if err != nil {
		return err
	}
	observation, err := observeGCSAckLossReceipt(ctx, stateStore, evidenceStore, source, receipt)
	if err != nil {
		return fmt.Errorf("canonical source state did not verify the exact ACK-loss receipt: %w", err)
	}
	objectEvidence, err := verifyGCSAckLossHeldObject(ctx, gcsClient, source, receipt, observation)
	if err != nil {
		return fmt.Errorf("independent GCS readback did not verify held bytes: %w", err)
	}
	event("ack_loss_canonical_and_object_verified", obj{"sequence": observation.Sequence, "guardSHA256": observation.GuardSHA256, "objectSHA256": objectEvidence.PayloadSHA256, "temporaryHold": objectEvidence.TemporaryHold})

	first, err := gcsLoseQuorumAndRecover(ctx, source, adminToken, `[[1,"gcs-ack-loss-baseline"]]`, "ack-loss-first", false, out, event, &faults)
	if err != nil {
		return fmt.Errorf("first recovery after ACK-loss dispatch: %w", err)
	}
	defer first.Forward.close()
	if err := fencerCleanup(ctx); err != nil {
		return fmt.Errorf("restore fencer after source generation was closed: %w", err)
	}
	fencerCleanup = nil
	secondSource, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return fmt.Errorf("capture first recovered generation after ACK-loss case: %w", err)
	}
	if secondSource.ClusterID != first.Recovery.Status.Target || secondSource.ClusterID == source.ClusterID {
		return fmt.Errorf("first recovered generation does not match the committed recovery target")
	}
	secondAck, err := writeGCSAcknowledgedRow(ctx, first.Forward, first.Admin, 2, "gcs-ack-loss-successor")
	if err != nil {
		return fmt.Errorf("acknowledge successor generation after guarded recovery: %w", err)
	}
	second, err := gcsLoseQuorumAndRecover(ctx, secondSource, first.Admin, `[[1,"gcs-ack-loss-baseline"],[2,"gcs-ack-loss-successor"]]`, "ack-loss-second", false, out, event, &faults)
	if err != nil {
		return fmt.Errorf("second recovery after ACK-loss case: %w", err)
	}
	defer second.Forward.close()
	if err := verifyRecoveryLineage(first.Recovery, second.Recovery, first.Fence, second.Fence); err != nil {
		return err
	}
	private := gcsAckLossPrivateEvidence{Version: 1, BaselineAck: baselineAck, FaultReceipt: receipt, CanonicalObservation: observation, Object: objectEvidence,
		FirstRecovery: gcsRecoveryEvidence(first), SecondRecovery: gcsRecoveryEvidence(second), SecondAck: secondAck,
		ObserverSharedWriterIdentity: readScope.SharedWriterIdentity}
	if err := writePrivateAckLossEvidence(out, private); err != nil {
		return err
	}
	event("ack_loss_two_recoveries_complete", obj{"firstRecoveryVerified": true, "secondRecoveryVerified": true})
	return nil
}

func waitForGCSAckLossReceipt(ctx context.Context, pod, out string) (operator.GCSMutationAckLossReceipt, error) {
	var receipt operator.GCSMutationAckLossReceipt
	var privateLine []byte
	err := wait(ctx, "exact private fencer acknowledgement-loss receipt", func() (bool, error) {
		cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "logs", pod, "-c", "authority", "--tail=256", "--limit-bytes=1048576")
		data, err := cmd.Output()
		if err != nil || len(data) > maxAckLossReceiptBytes {
			return false, nil
		}
		var found []byte
		for _, line := range bytes.Split(data, []byte("\n")) {
			if !bytes.HasPrefix(line, []byte(gcsAckLossEventPrefix)) {
				continue
			}
			if len(found) != 0 || len(line) > maxAckLossReceiptBytes {
				return false, fmt.Errorf("fencer emitted an ambiguous ACK-loss receipt")
			}
			found = append([]byte(nil), bytes.TrimPrefix(line, []byte(gcsAckLossEventPrefix))...)
		}
		if len(found) == 0 {
			return false, nil
		}
		decoder := json.NewDecoder(bytes.NewReader(found))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&receipt); err != nil {
			return false, fmt.Errorf("decode private ACK-loss receipt")
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return false, fmt.Errorf("private ACK-loss receipt has trailing data")
		}
		privateLine = append([]byte(nil), found...)
		return true, nil
	})
	if err != nil {
		return operator.GCSMutationAckLossReceipt{}, err
	}
	if len(privateLine) == 0 {
		return operator.GCSMutationAckLossReceipt{}, fmt.Errorf("private ACK-loss receipt is missing")
	}
	if err := writeNewPrivateAckLossFile(filepath.Join(out, gcsAckLossReceiptName), privateLine); err != nil {
		return operator.GCSMutationAckLossReceipt{}, fmt.Errorf("write private ACK-loss receipt")
	}
	return receipt, nil
}

func verifyGCSAckLossHeldObject(ctx context.Context, client *storage.Client, source gcsRecoverySource, receipt operator.GCSMutationAckLossReceipt, observation gcsHAAckObservation) (gcsAckLossObjectEvidence, error) {
	var result gcsAckLossObjectEvidence
	guarded := observation.Guarded
	if client == nil || observation.Runtime != receipt.Runtime || !gcsAckLossExtentKey.MatchString(receipt.ObjectKey) || receipt.Intent.PayloadSize <= 0 || receipt.Intent.PayloadSize > maxAckLossPayloadBytes ||
		guarded.ObjectGeneration <= 0 || guarded.ObjectMetageneration <= 0 || guarded.ObjectSize != receipt.PayloadSize || guarded.PayloadDigest != receipt.PayloadDigest ||
		!receipt.Preparation.GenerationSet || receipt.Preparation.Generation != 0 {
		return result, fmt.Errorf("exact held-object observation descriptor is invalid")
	}
	key := path.Join(source.Prefix, receipt.ObjectKey)
	object := client.Bucket(source.Bucket).Object(key).Generation(guarded.ObjectGeneration).Retryer(storage.WithPolicy(storage.RetryNever))
	attrs, err := object.Attrs(ctx)
	if err != nil {
		return result, fmt.Errorf("read exact GCS object generation attributes")
	}
	if attrs.Generation != guarded.ObjectGeneration || attrs.Metageneration != guarded.ObjectMetageneration || !attrs.TemporaryHold || attrs.Size != receipt.PayloadSize ||
		attrs.Metadata["rhiza-operation-id"] != receipt.OperationID || attrs.Metadata["rhiza-payload-sha256"] != receipt.PayloadDigest {
		return result, fmt.Errorf("GCS generation lacks the exact canonical held mutation metadata")
	}
	reader, err := object.NewReader(ctx)
	if err != nil {
		return result, fmt.Errorf("read exact GCS object generation bytes")
	}
	hash := sha256.New()
	count, copyErr := io.Copy(hash, io.LimitReader(reader, receipt.PayloadSize+1))
	closeErr := reader.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return result, fmt.Errorf("read exact held GCS bytes")
	}
	finalAttrs, err := object.Attrs(ctx)
	if err != nil || finalAttrs.Generation != attrs.Generation || finalAttrs.Metageneration != attrs.Metageneration || !finalAttrs.TemporaryHold || finalAttrs.Size != attrs.Size {
		return result, fmt.Errorf("held GCS object changed while its exact generation bytes were read")
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if count != receipt.PayloadSize || digest != receipt.PayloadDigest {
		return result, fmt.Errorf("exact held GCS payload differs from the canonical digest or size")
	}
	return gcsAckLossObjectEvidence{Generation: attrs.Generation, Metageneration: attrs.Metageneration, Size: attrs.Size, PayloadSHA256: digest, TemporaryHold: attrs.TemporaryHold}, nil
}

func writePrivateAckLossEvidence(out string, evidence gcsAckLossPrivateEvidence) error {
	data, err := json.Marshal(evidence)
	if err != nil || len(data) > maxAckLossReceiptBytes*4 {
		return fmt.Errorf("encode bounded private ACK-loss evidence")
	}
	if err := writeNewPrivateAckLossFile(filepath.Join(out, gcsAckLossEvidenceName), data); err != nil {
		return fmt.Errorf("write private ACK-loss evidence")
	}
	return nil
}

func validateAckLossFencerStorageConfig(ctx context.Context, deployment obj, source gcsRecoverySource) error {
	container, err := boundContainer(deployment, "authority")
	if err != nil {
		return err
	}
	if !containerUsesConfigMap(container, source.ConfigMap) {
		return fmt.Errorf("fencer does not consume the captured source GCS configuration")
	}
	var gotPrefix string
	var independent, useADC bool
	envValues, ok := ackLossArray(nested(container, "env"))
	if !ok {
		return fmt.Errorf("fencer environment is not a structured list")
	}
	for _, value := range envValues {
		env, ok := ackLossObject(value)
		if !ok {
			return fmt.Errorf("fencer environment contains a malformed entry")
		}
		switch text(env["name"]) {
		case "FENCER_AUTHORITY_STATE_PREFIX":
			gotPrefix = text(env["value"])
		case "FENCER_INDEPENDENT_MUTATION_PROFILE":
			independent = text(env["value"]) == "true"
		case "FENCER_SOURCE_WRITER_USE_ADC":
			useADC = text(env["value"]) == "true"
		}
	}
	bucket, bucketErr := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_BUCKET")
	basePrefix, basePrefixErr := effectiveGCSSetting(ctx, container, "RHIZA_OBJSTORE_PREFIX")
	clusterID, clusterErr := effectiveGCSSetting(ctx, container, "RHIZA_CLUSTER_ID")
	if bucketErr != nil || basePrefixErr != nil || clusterErr != nil || bucket != source.Bucket ||
		strings.Trim(basePrefix, "/") != basePrefix || path.Clean(basePrefix) != basePrefix ||
		path.Join(basePrefix, clusterID) != source.Prefix || clusterID != source.ClusterID ||
		gotPrefix == "" || strings.Trim(gotPrefix, "/") != gotPrefix || path.Clean(gotPrefix) != gotPrefix || !independent || !useADC {
		return fmt.Errorf("fencer authority profile differs from the run-scoped independent GCS configuration")
	}
	return nil
}

func ackLossDeploymentTemplate(deployment obj) (int, []any, []any, []any, error) {
	index, args, mounts, volumes, err := readAckLossDeploymentTemplate(deployment)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	for _, arg := range args {
		if strings.HasPrefix(text(arg), "--gcs-ack-loss-config-file=") {
			return 0, nil, nil, nil, fmt.Errorf("fencer already has an acknowledgement-loss config")
		}
	}
	for _, mount := range mounts {
		m, ok := ackLossObject(mount)
		if !ok {
			return 0, nil, nil, nil, fmt.Errorf("fencer Deployment contains a malformed volume mount")
		}
		if text(m["name"]) == gcsAckLossVolumeName || text(m["mountPath"]) == filepath.Dir(gcsAckLossConfigPath) {
			return 0, nil, nil, nil, fmt.Errorf("fencer already uses acknowledgement-loss config mount")
		}
	}
	for _, volume := range volumes {
		if text(nested(volume, "name")) == gcsAckLossVolumeName {
			return 0, nil, nil, nil, fmt.Errorf("fencer already uses acknowledgement-loss config volume")
		}
	}
	return index, args, mounts, volumes, nil
}

// readAckLossDeploymentTemplate extracts a structurally valid template. Callers
// that inspect an armed template must compare all fields with their saved patch.
func readAckLossDeploymentTemplate(deployment obj) (int, []any, []any, []any, error) {
	containers, ok := ackLossArray(nested(deployment, "spec", "template", "spec", "containers"))
	if !ok {
		return 0, nil, nil, nil, fmt.Errorf("fencer Deployment has no container list")
	}
	index := -1
	var args, mounts []any
	for i, value := range containers {
		container, ok := ackLossObject(value)
		if !ok {
			return 0, nil, nil, nil, fmt.Errorf("fencer Deployment contains a malformed container")
		}
		if text(container["name"]) != "authority" {
			continue
		}
		if index >= 0 {
			return 0, nil, nil, nil, fmt.Errorf("fencer authority container is ambiguous")
		}
		index = i
		args, ok = container["args"].([]any)
		if !ok || len(args) == 0 || text(args[0]) != "serve" {
			return 0, nil, nil, nil, fmt.Errorf("fencer authority container is not running the expected serve command")
		}
		mounts, ok = ackLossArray(container["volumeMounts"])
		if !ok {
			return 0, nil, nil, nil, fmt.Errorf("fencer authority volume mounts are unavailable")
		}
	}
	if index < 0 {
		return 0, nil, nil, nil, fmt.Errorf("fencer authority container is missing")
	}
	volumes, ok := ackLossArray(nested(deployment, "spec", "template", "spec", "volumes"))
	if !ok {
		return 0, nil, nil, nil, fmt.Errorf("fencer Deployment volume list is unavailable")
	}
	return index, append([]any(nil), args...), append([]any(nil), mounts...), append([]any(nil), volumes...), nil
}

func ackLossDeploymentPatch(uid string, index int, oldArgs, oldMounts, oldVolumes, newArgs, newMounts, newVolumes []any) []any {
	base := "/spec/template/spec"
	container := fmt.Sprintf("%s/containers/%d", base, index)
	return []any{
		obj{"op": "test", "path": "/metadata/uid", "value": uid},
		obj{"op": "test", "path": container + "/args", "value": oldArgs},
		obj{"op": "test", "path": container + "/volumeMounts", "value": oldMounts},
		obj{"op": "test", "path": base + "/volumes", "value": oldVolumes},
		obj{"op": "replace", "path": container + "/args", "value": newArgs},
		obj{"op": "replace", "path": container + "/volumeMounts", "value": newMounts},
		obj{"op": "replace", "path": base + "/volumes", "value": newVolumes},
	}
}

func ackLossDeploymentPatchWithTemplate(uid, runID string, index int, oldArgs, oldMounts, oldVolumes, newArgs, newMounts, newVolumes []any, expectedTemplate obj) []any {
	patch := ackLossDeploymentPatch(uid, index, oldArgs, oldMounts, oldVolumes, newArgs, newMounts, newVolumes)
	result := make([]any, 0, len(patch)+2)
	result = append(result, patch[0], obj{"op": "test", "path": "/metadata/labels/chaos.rhiza.io~1run", "value": runID},
		obj{"op": "test", "path": "/spec/template", "value": expectedTemplate})
	return append(result, patch[1:]...)
}

func ackLossPodTemplate(deployment obj) (obj, error) {
	template, ok := ackLossObject(nested(deployment, "spec", "template"))
	if !ok {
		return nil, fmt.Errorf("fencer Deployment Pod template is malformed")
	}
	data, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}
	var snapshot obj
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("copy fencer Deployment Pod template: %w", err)
	}
	if snapshot == nil {
		return nil, fmt.Errorf("copy fencer Deployment Pod template: snapshot is not an object")
	}
	return snapshot, nil
}

func patchAckLossPodTemplate(template obj, index int, args, mounts, volumes []any) (obj, error) {
	data, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}
	var patched obj
	if err := json.Unmarshal(data, &patched); err != nil {
		return nil, fmt.Errorf("copy fencer Pod template for qualification: %w", err)
	}
	if patched == nil {
		return nil, fmt.Errorf("copy fencer Pod template for qualification: snapshot is not an object")
	}
	spec, ok := ackLossObject(patched["spec"])
	if !ok {
		return nil, fmt.Errorf("fencer Pod template spec is malformed")
	}
	containers, ok := ackLossArray(spec["containers"])
	if !ok || index < 0 || index >= len(containers) {
		return nil, fmt.Errorf("fencer Pod template container index is invalid")
	}
	container, ok := ackLossObject(containers[index])
	if !ok || text(container["name"]) != "authority" {
		return nil, fmt.Errorf("fencer Pod template authority container changed")
	}
	container["args"] = args
	container["volumeMounts"] = mounts
	spec["volumes"] = volumes
	return patched, nil
}

func waitAckLossFencerRestart(ctx context.Context, oldPodName, oldPodUID, deploymentUID string) (string, error) {
	var found string
	err := wait(ctx, "run-owned fencer restarted with the acknowledgement-loss config", func() (bool, error) {
		pods, err := get(ctx, "pods", "")
		if err != nil {
			return false, err
		}
		items, _ := pods["items"].([]any)
		for _, item := range items {
			if text(nested(item, "metadata", "labels", "app")) != "rhiza-fencer" || text(nested(item, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID || nested(item, "status", "phase") != "Running" {
				continue
			}
			if text(nested(item, "metadata", "name")) == oldPodName || text(nested(item, "metadata", "uid")) == oldPodUID {
				return false, nil
			}
			if !podReady(item) || !ackLossPodOwnedBy(ctx, item, deploymentUID) {
				return false, nil
			}
			found = text(nested(item, "metadata", "name"))
			return true, nil
		}
		return false, nil
	})
	return found, err
}

func gcsAckLossScopesOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

func ackLossPodOwnedBy(ctx context.Context, pod any, deploymentUID string) bool {
	if deploymentUID == "" {
		return false
	}
	owners, ok := ackLossArray(nested(pod, "metadata", "ownerReferences"))
	if !ok {
		return false
	}
	for _, value := range owners {
		owner, ok := ackLossObject(value)
		if !ok {
			return false
		}
		if text(owner["kind"]) != "ReplicaSet" || text(owner["uid"]) == "" || owner["controller"] != true {
			continue
		}
		rs, err := get(ctx, "replicaset", text(owner["name"]))
		if err != nil || text(nested(rs, "metadata", "uid")) != text(owner["uid"]) {
			return false
		}
		parents, ok := ackLossArray(nested(rs, "metadata", "ownerReferences"))
		if !ok {
			return false
		}
		for _, parent := range parents {
			if text(nested(parent, "kind")) == "Deployment" && text(nested(parent, "uid")) == deploymentUID && nested(parent, "controller") == true {
				return true
			}
		}
	}
	return false
}

func deleteAckLossConfigMap(ctx context.Context, uid string) error {
	return deleteAckLossConfigMapExact(ctx, uid, "")
}

func deleteAckLossConfigMapExact(ctx context.Context, uid, expectedDigest string) error {
	current, err := get(ctx, "configmap", gcsAckLossConfigMapName)
	if err != nil {
		if isAckLossNotFound(err) {
			return nil
		}
		return err
	}
	configBytes := []byte(text(nested(current, "data", "config.json")))
	resourceVersion := text(nested(current, "metadata", "resourceVersion"))
	if uid == "" || text(nested(current, "metadata", "uid")) != uid || text(nested(current, "metadata", "namespace")) != namespace ||
		text(nested(current, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID || nested(current, "immutable") != true ||
		resourceVersion == "" || (expectedDigest != "" && gcsInitialHeadDigest(configBytes) != expectedDigest) {
		return fmt.Errorf("acknowledgement-loss ConfigMap identity changed; refusing deletion")
	}
	return deleteObjectWithUIDAndResourceVersion(ctx, "configmaps", gcsAckLossConfigMapName, uid, resourceVersion)
}

func writePrivateAckLossConfig(dir string, data []byte) (string, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return "", fmt.Errorf("stat acknowledgement-loss output directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("acknowledgement-loss output directory must be private")
	}
	file, err := os.CreateTemp(dir, ".gcs-ack-loss-config-*.json")
	if err != nil {
		return "", err
	}
	name := file.Name()
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		_ = os.Remove(name)
		return "", err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

func writeNewPrivateAckLossFile(name string, data []byte) error {
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return nil
}

func sameJSON(left, right any) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && string(a) == string(b)
}

func restoreAckLossDeployment(ctx context.Context, uid string, index int, args, mounts, volumes, patchedArgs, patchedMounts, patchedVolumes []any, templates ...obj) error {
	current, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return err
	}
	if text(nested(current, "metadata", "uid")) != uid || text(nested(current, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
		return fmt.Errorf("fencer Deployment identity changed before restore")
	}
	if len(templates) != 2 || templates[0] == nil || templates[1] == nil {
		return fmt.Errorf("fencer restore requires saved full Pod template snapshots")
	}
	originalIndex, originalArgs, originalMounts, originalVolumes, err := ackLossDeploymentTemplate(obj{"spec": obj{"template": templates[0]}})
	if err != nil {
		return fmt.Errorf("validate saved original fencer Pod template: %w", err)
	}
	if originalIndex != index || !sameJSON(originalArgs, args) || !sameJSON(originalMounts, mounts) || !sameJSON(originalVolumes, volumes) {
		return fmt.Errorf("saved original fencer Pod template does not match its captured container fields")
	}
	expectedPatched, err := patchAckLossPodTemplate(templates[0], index, patchedArgs, patchedMounts, patchedVolumes)
	if err != nil {
		return fmt.Errorf("reconstruct saved fencer qualification template: %w", err)
	}
	if !sameJSON(expectedPatched, templates[1]) {
		return fmt.Errorf("saved fencer Pod template snapshots do not describe the exact qualification patch")
	}
	currentIndex, currentArgs, currentMounts, currentVolumes, err := readAckLossDeploymentTemplate(current)
	if err != nil {
		return fmt.Errorf("read current fencer Pod template before restore: %w", err)
	}
	if currentIndex != index || !sameJSON(nested(current, "spec", "template"), templates[1]) || !sameJSON(currentArgs, patchedArgs) || !sameJSON(currentMounts, patchedMounts) || !sameJSON(currentVolumes, patchedVolumes) {
		return fmt.Errorf("fencer template differs from the exact applied qualification template")
	}
	patch := ackLossDeploymentPatchWithTemplate(uid, qualificationRunID, index, patchedArgs, patchedMounts, patchedVolumes, args, mounts, volumes, templates[1])
	if _, err := kubectl(ctx, "patch", "deployment", "rhiza-fencer", "--type=json", "-p", string(encoded(patch))); err != nil {
		return err
	}
	return nil
}

func containerUsesConfigMap(container any, name string) bool {
	if name == "" {
		return false
	}
	values, ok := ackLossArray(nested(container, "envFrom"))
	if !ok {
		return false
	}
	for _, value := range values {
		if text(nested(value, "configMapRef", "name")) == name {
			return true
		}
	}
	return false
}

func ackLossArray(value any) ([]any, bool) {
	items, ok := value.([]any)
	return items, ok
}

func ackLossObject(value any) (obj, bool) {
	item, ok := value.(map[string]any)
	return obj(item), ok
}
