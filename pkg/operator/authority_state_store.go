package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/googleapi"
)

var (
	errAuthorityStateNotFound = errors.New("fencing: canonical authority state not found")
	errAuthorityRootLost      = errors.New("fencing: initialized authority root is missing; refusing to bootstrap")
	errAuthorityStateConflict = errors.New("fencing: canonical authority state CAS conflict")
)

const maxAuthorityStateBytes = 1 << 20

// AuthorityStateStore keeps the live fencing ledger and producer journals in
// the configured object store. Kubernetes resources may index this state, but
// are not required to recover it.
type AuthorityStateStore struct {
	bucket objstore.Bucket
	prefix string
}

// authorityConditionalWriteError keeps a failed conditional upload distinct
// from its confirmation read. Both errors remain in the chain, but diagnostics
// classify the write failure as primary when the read cannot confirm it.
type authorityConditionalWriteError struct {
	primary      error
	confirmation error
}

func (e *authorityConditionalWriteError) Error() string {
	return errors.Join(e.primary, e.confirmation).Error()
}

func (e *authorityConditionalWriteError) Unwrap() []error {
	if e.confirmation == nil {
		return []error{e.primary}
	}
	return []error{e.primary, e.confirmation}
}

type authorityStateEnvelope struct {
	Version  int             `json:"version"`
	Identity string          `json:"identity"`
	Revision string          `json:"revision,omitempty"`
	Digest   string          `json:"digest"`
	Value    json.RawMessage `json:"value"`
}

func NewAuthorityStateStore(bucket objstore.Bucket, prefix string) (*AuthorityStateStore, error) {
	if prefix == "." || strings.HasPrefix(prefix, "/") || (prefix != "" && path.Clean(prefix) != prefix) {
		return nil, fmt.Errorf("fencing: canonical authority object store and clean prefix are required")
	}
	if err := validateAuthorityCASBucket(bucket); err != nil {
		return nil, err
	}
	return &AuthorityStateStore{bucket: bucket, prefix: prefix}, nil
}

func (s *AuthorityStateStore) rootKey(uid, source string) (string, string, error) {
	if s == nil || s.bucket == nil || uid == "" || source == "" {
		return "", "", fmt.Errorf("fencing: canonical authority root identity is incomplete")
	}
	identity := hashJSON([]string{"rhiza-authority-root-v1", uid, source})
	return s.key("roots", identity), identity, nil
}

func (s *AuthorityStateStore) producerKey(runtime fencing.RuntimeBinding) (string, string, error) {
	if s == nil || s.bucket == nil || runtime.RunID == "" || runtime.StatefulSetUID == "" || runtime.SourceClusterID == "" {
		return "", "", fmt.Errorf("fencing: canonical producer identity is incomplete")
	}
	identity := hashJSON([]any{"rhiza-mutation-producer-v1", runtime})
	return s.key("producers", identity), identity, nil
}

func (s *AuthorityStateStore) key(kind, identity string) string {
	name := path.Join("rhiza-authority/v1", kind, identity+".json")
	if s.prefix != "" {
		return path.Join(s.prefix, name)
	}
	return name
}

func (s *AuthorityStateStore) genesisKey(identity string) string {
	return s.key("root-genesis", identity)
}

func (s *AuthorityStateStore) archiveScopeClaimKey(scope fencing.ArchiveScope) (string, string, error) {
	hash, err := scope.Hash()
	if err != nil {
		return "", "", err
	}
	identity := hashJSON([]string{"rhiza-archive-scope-claim-v1", hash})
	return s.key("archive-scope-claims", identity), identity, nil
}

// ReadArchiveScopeClaim reads the create-only stable-scope authorization
// record. Missing state is not evidence that an archive namespace is new.
func (s *AuthorityStateStore) ReadArchiveScopeClaim(ctx context.Context, scope fencing.ArchiveScope) (fencing.ArchiveScopeClaim, *objstore.ObjectVersion, error) {
	key, identity, err := s.archiveScopeClaimKey(scope)
	if err != nil {
		return fencing.ArchiveScopeClaim{}, nil, err
	}
	data, version, err := s.read(ctx, key, identity)
	if err != nil {
		return fencing.ArchiveScopeClaim{}, nil, err
	}
	var claim fencing.ArchiveScopeClaim
	if err := decodeAuthorityValue(data, &claim); err != nil || claim.Scope != scope || claim.Validate() != nil {
		return fencing.ArchiveScopeClaim{}, nil, errors.Join(err, fmt.Errorf("fencing: archive scope claim differs"))
	}
	return claim, version, nil
}

// ClaimArchiveScope authorizes a stable archive scope exactly once. A retry is
// idempotent only for the byte-identical first owner and registration.
func (s *AuthorityStateStore) ClaimArchiveScope(ctx context.Context, scope fencing.ArchiveScope, configID uint, ownerUID, ownerSource, registrationIdentity string) (fencing.ArchiveScopeClaim, error) {
	_, identity, err := s.archiveScopeClaimKey(scope)
	if err != nil {
		return fencing.ArchiveScopeClaim{}, err
	}
	hash, _ := scope.Hash()
	want := fencing.ArchiveScopeClaim{Version: fencing.ArchiveGenesisVersion, Scope: scope, ScopeHash: hash, ConfigID: configID, OwnerStatefulSetUID: ownerUID, OwnerSourceClusterID: ownerSource, RegistrationIdentity: registrationIdentity}
	if err := want.Validate(); err != nil {
		return fencing.ArchiveScopeClaim{}, err
	}
	key, _, _ := s.archiveScopeClaimKey(scope)
	writeErr := s.write(ctx, key, identity, want, nil)
	if writeErr == nil {
		return want, nil
	}
	got, _, readErr := s.ReadArchiveScopeClaim(ctx, scope)
	if readErr == nil && got.Scope == want.Scope && got.ScopeHash == want.ScopeHash && got.ConfigID == want.ConfigID &&
		got.OwnerStatefulSetUID == want.OwnerStatefulSetUID && got.OwnerSourceClusterID == want.OwnerSourceClusterID {
		return got, nil
	}
	return fencing.ArchiveScopeClaim{}, errors.Join(writeErr, readErr, errAuthorityStateConflict)
}

// SetArchiveScopeGenesisRef commits the first full genesis reference in the
// stable claim. The exact existing reference is the only successful retry.
func (s *AuthorityStateStore) SetArchiveScopeGenesisRef(ctx context.Context, scope fencing.ArchiveScope, ref fencing.ArchiveGenesisRef) error {
	if err := ref.ValidateForScope(scope); err != nil {
		return fmt.Errorf("fencing: archive genesis reference scope differs")
	}
	for range 8 {
		claim, version, err := s.ReadArchiveScopeClaim(ctx, scope)
		if err != nil {
			return err
		}
		if claim.GenesisRef != nil {
			if hashJSON(claim.GenesisRef) == hashJSON(&ref) {
				return nil
			}
			return fmt.Errorf("fencing: archive genesis reference already committed")
		}
		if claim.ConfigID != ref.ConfigID {
			return fmt.Errorf("fencing: archive genesis config differs from registered scope claim")
		}
		if claim.EmptyRecoveryAuthorization != nil && (ref.Mutation == nil || ref.Mutation.Intent.PayloadDigest != claim.EmptyRecoveryAuthorization.TargetEmptyHeadDigest) {
			return fmt.Errorf("fencing: target genesis mutation differs from empty recovery authorization")
		}
		claim.GenesisRef = &ref
		_, identity, _ := s.archiveScopeClaimKey(scope)
		if err := s.write(ctx, s.key("archive-scope-claims", identity), identity, claim, version); err == nil {
			return nil
		} else if !errors.Is(err, errAuthorityStateConflict) {
			got, _, readErr := s.ReadArchiveScopeClaim(ctx, scope)
			if readErr == nil && got.GenesisRef != nil && hashJSON(got.GenesisRef) == hashJSON(&ref) {
				return nil
			}
			return errors.Join(err, readErr)
		}
	}
	return fmt.Errorf("fencing: archive genesis claim changed too often")
}

// SetArchiveScopeEmptyRecoveryAuthorization stores the immutable proof that
// this target scope may be initialized from a sealed empty recovery source.
// It is separate from GenesisRef, which records the target's actual mutation.
func (s *AuthorityStateStore) SetArchiveScopeEmptyRecoveryAuthorization(ctx context.Context, scope fencing.ArchiveScope, authorization fencing.ArchiveEmptyRecoveryAuthorization) error {
	hash, err := scope.Hash()
	if err != nil || authorization.Validate() != nil || authorization.TargetScopeHash != hash || authorization.ConfigID == 0 {
		return fmt.Errorf("fencing: empty recovery authorization does not match archive scope")
	}
	for range 8 {
		claim, version, err := s.ReadArchiveScopeClaim(ctx, scope)
		if err != nil {
			return err
		}
		if claim.ConfigID != authorization.ConfigID || claim.OwnerStatefulSetUID != authorization.StatefulSetUID || claim.OwnerSourceClusterID != scope.ClusterID {
			return fmt.Errorf("fencing: empty recovery claim belongs to a different root")
		}
		if claim.EmptyRecoveryAuthorization != nil {
			if hashJSON(claim.EmptyRecoveryAuthorization) == hashJSON(&authorization) {
				return nil
			}
			return fmt.Errorf("fencing: empty recovery authorization is immutable")
		}
		claim.EmptyRecoveryAuthorization = &authorization
		_, identity, _ := s.archiveScopeClaimKey(scope)
		if err := s.write(ctx, s.key("archive-scope-claims", identity), identity, claim, version); err == nil {
			return nil
		} else if !errors.Is(err, errAuthorityStateConflict) {
			got, _, readErr := s.ReadArchiveScopeClaim(ctx, scope)
			if readErr == nil && got.EmptyRecoveryAuthorization != nil && hashJSON(got.EmptyRecoveryAuthorization) == hashJSON(&authorization) {
				return nil
			}
			return errors.Join(err, readErr)
		}
	}
	return fmt.Errorf("fencing: empty recovery claim changed too often")
}

// SetRootEmptyRecoveryAuthorization binds the same immutable authorization to
// the canonical root. It never changes ArchiveGenesisRef, the commit receipt.
func (s *AuthorityStateStore) SetRootEmptyRecoveryAuthorization(ctx context.Context, uid, source string, authorization fencing.ArchiveEmptyRecoveryAuthorization) error {
	for range 8 {
		state, version, err := s.ReadRoot(ctx, uid, source)
		if err != nil {
			return err
		}
		if state.StatefulSetUID != authorization.StatefulSetUID || source != authorization.TargetClusterID ||
			state.Ledger.ArchiveScopeHash != authorization.TargetScopeHash || authorization.Validate() != nil {
			return fmt.Errorf("fencing: empty recovery root binding differs")
		}
		if state.Ledger.EmptyRecoveryAuthorization != nil {
			if hashJSON(state.Ledger.EmptyRecoveryAuthorization) == hashJSON(&authorization) {
				return nil
			}
			return fmt.Errorf("fencing: empty recovery root authorization is immutable")
		}
		if state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisEmptyRecoveryPending || state.Ledger.ArchiveGenesisRef != nil {
			return fmt.Errorf("fencing: target root is not reserved for empty recovery")
		}
		state.Ledger.EmptyRecoveryAuthorization = &authorization
		state.Ledger.ArchiveGenesisState = fencing.ArchiveGenesisAwaitingRef
		if err := s.WriteRoot(ctx, uid, source, state, version); err == nil {
			return nil
		} else if !errors.Is(err, errAuthorityStateConflict) {
			confirmed, _, readErr := s.ReadRoot(ctx, uid, source)
			if readErr == nil && confirmed.Ledger.EmptyRecoveryAuthorization != nil && hashJSON(confirmed.Ledger.EmptyRecoveryAuthorization) == hashJSON(&authorization) {
				return nil
			}
			return errors.Join(err, readErr)
		}
	}
	return fmt.Errorf("fencing: empty recovery root changed too often")
}

func (s *AuthorityStateStore) ensureRootGenesis(ctx context.Context, identity string) error {
	key := s.genesisKey(identity)
	value := struct {
		RootIdentity string `json:"rootIdentity"`
	}{identity}
	data, version, err := s.read(ctx, key, identity)
	if err == nil {
		var saved struct {
			RootIdentity string `json:"rootIdentity"`
		}
		if err := decodeAuthorityValue(data, &saved); err != nil || saved != value || version == nil {
			return s.rootDiagnostic("genesis", errors.Join(err, fmt.Errorf("fencing: authority root genesis differs")))
		}
		return nil
	}
	if !errors.Is(err, errAuthorityStateNotFound) {
		return s.rootDiagnostic("genesis", err)
	}
	if err := s.write(ctx, key, identity, value, nil); err != nil && !errors.Is(err, errAuthorityStateConflict) {
		stored, _, readErr := s.read(ctx, key, identity)
		var saved struct {
			RootIdentity string `json:"rootIdentity"`
		}
		if readErr == nil && decodeAuthorityValue(stored, &saved) == nil && saved == value {
			return nil
		}
		return errors.Join(s.rootDiagnostic("genesis", err), readErr)
	}
	stored, _, err := s.read(ctx, key, identity)
	if err != nil {
		return s.rootDiagnostic("genesis", err)
	}
	var saved struct {
		RootIdentity string `json:"rootIdentity"`
	}
	if err := decodeAuthorityValue(stored, &saved); err != nil || saved != value {
		return s.rootDiagnostic("genesis", errors.Join(err, fmt.Errorf("fencing: authority root genesis differs")))
	}
	return nil
}

func (s *AuthorityStateStore) read(ctx context.Context, key, identity string) (json.RawMessage, *objstore.ObjectVersion, error) {
	value, version, _, err := s.readVersioned(ctx, key, identity)
	return value, version, err
}

func (s *AuthorityStateStore) readVersioned(ctx context.Context, key, identity string) (json.RawMessage, *objstore.ObjectVersion, string, error) {
	for attempt := 0; attempt < 4; attempt++ {
		before, err := s.bucket.Attributes(ctx, key)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				return nil, nil, "", errAuthorityStateNotFound
			}
			return nil, nil, "", err
		}
		if !validAuthorityObjectVersion(before.Version) {
			return nil, nil, "", fmt.Errorf("fencing: authority state requires an exact generation or ETag version")
		}
		reader, err := s.bucket.Get(ctx, key)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				continue
			}
			return nil, nil, "", err
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, maxAuthorityStateBytes+1))
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, nil, "", err
		}
		if len(data) > maxAuthorityStateBytes {
			return nil, nil, "", fmt.Errorf("fencing: canonical authority state exceeds capacity")
		}
		after, err := s.bucket.Attributes(ctx, key)
		if err != nil {
			if s.bucket.IsObjNotFoundErr(err) {
				continue
			}
			return nil, nil, "", err
		}
		if after.Version == nil || *after.Version != *before.Version {
			continue
		}
		var envelope authorityStateEnvelope
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&envelope); err != nil {
			return nil, nil, "", err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, nil, "", fmt.Errorf("fencing: trailing canonical authority state")
		}
		digestValid := envelope.Digest == authorityStateDigest(envelope.Value)
		if envelope.Revision != "" {
			digestValid = validAuthorityRevision(envelope.Revision) && envelope.Digest == authorityStateEnvelopeDigest(envelope.Value, envelope.Revision)
		}
		if envelope.Version != 1 || envelope.Identity != identity || !digestValid || len(envelope.Value) == 0 || (before.Version.Type == objstore.ETag && envelope.Revision == "") {
			return nil, nil, "", fmt.Errorf("fencing: canonical authority state identity or digest differs")
		}
		return envelope.Value, before.Version, envelope.Revision, nil
	}
	return nil, nil, "", fmt.Errorf("fencing: canonical authority state changed during read")
}

func (s *AuthorityStateStore) write(ctx context.Context, key, identity string, value any, expected *objstore.ObjectVersion) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > maxAuthorityStateBytes-1024 {
		return fmt.Errorf("fencing: canonical authority state exceeds capacity")
	}
	revision, err := newAuthorityRevision()
	if err != nil {
		return err
	}
	digest := authorityStateEnvelopeDigest(encoded, revision)
	body, err := json.Marshal(authorityStateEnvelope{Version: 1, Identity: identity, Revision: revision, Digest: digest, Value: encoded})
	if err != nil {
		return err
	}
	if len(body) > maxAuthorityStateBytes {
		return fmt.Errorf("fencing: canonical authority state exceeds capacity")
	}
	if expected == nil {
		err = s.bucket.Upload(ctx, key, bytes.NewReader(body), objstore.WithIfNotExists())
	} else {
		if !validAuthorityObjectVersion(expected) {
			return fmt.Errorf("fencing: authority state CAS requires an exact generation or ETag version")
		}
		_, currentVersion, readErr := s.read(ctx, key, identity)
		if readErr != nil {
			return readErr
		}
		if currentVersion.Type != expected.Type {
			return fmt.Errorf("fencing: authority state CAS version type differs from current object")
		}
		version := *expected
		err = s.bucket.Upload(ctx, key, bytes.NewReader(body), objstore.WithIfMatch(&version))
	}
	if err != nil && s.bucket.IsConditionNotMetErr(err) {
		return errAuthorityStateConflict
	}
	if err != nil && expected != nil {
		stored, readVersion, storedRevision, readErr := s.readVersioned(ctx, key, identity)
		if readErr == nil && validAuthorityObjectVersion(readVersion) && readVersion.Type == expected.Type && storedRevision == revision && bytes.Equal(stored, encoded) {
			return nil
		}
		return &authorityConditionalWriteError{primary: err, confirmation: readErr}
	}
	return err
}

func authorityStateDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func authorityStateEnvelopeDigest(value []byte, revision string) string {
	return hashJSON([]string{"rhiza-authority-state-envelope-v1", revision, authorityStateDigest(value)})
}

func (s *AuthorityStateStore) ReadRoot(ctx context.Context, uid, source string) (fenceLedger, *objstore.ObjectVersion, error) {
	key, identity, err := s.rootKey(uid, source)
	if err != nil {
		return fenceLedger{}, nil, s.rootDiagnostic("load", err)
	}
	data, version, err := s.read(ctx, key, identity)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			if _, _, genesisErr := s.read(ctx, s.genesisKey(identity), identity); genesisErr == nil {
				return fenceLedger{}, nil, s.rootDiagnostic("load", errAuthorityRootLost)
			} else if !errors.Is(genesisErr, errAuthorityStateNotFound) {
				return fenceLedger{}, nil, s.rootDiagnostic("load", genesisErr)
			}
		}
		return fenceLedger{}, nil, s.rootDiagnostic("load", err)
	}
	var state fenceLedger
	if err := decodeAuthorityValue(data, &state); err != nil || state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
		return fenceLedger{}, nil, s.rootDiagnostic("validate", errors.Join(err, fmt.Errorf("fencing: canonical authority root identity differs")))
	}
	return state, version, nil
}

func (s *AuthorityStateStore) WriteRoot(ctx context.Context, uid, source string, state fenceLedger, expected *objstore.ObjectVersion) error {
	key, identity, err := s.rootKey(uid, source)
	if err != nil {
		return s.rootDiagnostic("create", err)
	}
	if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
		return s.rootDiagnostic("validate", fmt.Errorf("fencing: canonical authority root identity differs"))
	}
	if err := s.write(ctx, key, identity, state, expected); err != nil {
		return s.rootDiagnostic("create", err)
	}
	return nil
}

// rootDiagnostic adds fixed, non-sensitive phase/cause metadata while
// preserving the underlying error chain for errors.Is and errors.As callers.
func (s *AuthorityStateStore) rootDiagnostic(phase string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("fencing: root-init phase=%s cause=%s: %w", phase, s.rootCause(err), err)
}

func rootHTTPStatusCause(status int) string {
	switch status {
	case 401, 403:
		return "permission"
	case 404:
		return "not_found"
	case 408, 504:
		return "timeout"
	case 409, 412:
		return "precondition"
	case 501:
		return "unsupported"
	case 429:
		return "http_429"
	case 500:
		return "http_500"
	case 502:
		return "http_502"
	case 503:
		return "http_503"
	default:
		return ""
	}
}

func (s *AuthorityStateStore) rootCause(err error) string {
	var writeErr *authorityConditionalWriteError
	if errors.As(err, &writeErr) && writeErr != nil {
		return s.rootCause(writeErr.primary)
	}
	switch {
	case errors.Is(err, errAuthorityStateNotFound):
		return "not_found"
	case errors.Is(err, errAuthorityStateConflict), s != nil && s.bucket != nil && s.bucket.IsConditionNotMetErr(err):
		return "precondition"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	var googleErr *googleapi.Error
	if errors.As(err, &googleErr) && googleErr != nil {
		if cause := rootHTTPStatusCause(googleErr.Code); cause != "" {
			return cause
		}
	}
	var minioValue minio.ErrorResponse
	if errors.As(err, &minioValue) {
		if cause := rootHTTPStatusCause(minioValue.StatusCode); cause != "" {
			return cause
		}
	}
	var minioPointer *minio.ErrorResponse
	if errors.As(err, &minioPointer) && minioPointer != nil {
		if cause := rootHTTPStatusCause(minioPointer.StatusCode); cause != "" {
			return cause
		}
	}
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		if cause := rootHTTPStatusCause(statusErr.HTTPStatusCode()); cause != "" {
			return cause
		}
	}
	if s != nil && s.bucket != nil && s.bucket.IsObjNotFoundErr(err) {
		return "not_found"
	}
	for _, item := range []struct{ message, cause string }{
		{"fencing: initialized authority root is missing", "root_lost"},
		{"fencing: canonical authority root identity differs", "identity_mismatch"},
		{"fencing: authority root genesis differs", "genesis_mismatch"},
		{"fencing: authority state requires an exact generation or ETag version", "version_unsupported"},
		{"fencing: canonical authority state exceeds capacity", "capacity_exceeded"},
	} {
		if strings.Contains(err.Error(), item.message) {
			return item.cause
		}
	}
	return "unknown"
}

func (s *AuthorityStateStore) ReadProducer(ctx context.Context, runtime fencing.RuntimeBinding) (fencing.MutationProducer, string, *objstore.ObjectVersion, error) {
	key, identity, err := s.producerKey(runtime)
	if err != nil {
		return fencing.MutationProducer{}, "", nil, err
	}
	data, version, err := s.read(ctx, key, identity)
	if err != nil {
		return fencing.MutationProducer{}, "", nil, err
	}
	var stored struct {
		ProducerUID string                   `json:"producerUID"`
		Producer    fencing.MutationProducer `json:"producer"`
	}
	if err := decodeAuthorityValue(data, &stored); err != nil || stored.ProducerUID != stableMutationProducerUID(runtime) || stored.Producer.Runtime != runtime || stored.Producer.Validate() != nil {
		return fencing.MutationProducer{}, "", nil, errors.Join(err, fmt.Errorf("fencing: canonical mutation producer identity differs"))
	}
	return stored.Producer, stored.ProducerUID, version, nil
}

func (s *AuthorityStateStore) WriteProducer(ctx context.Context, runtime fencing.RuntimeBinding, producer fencing.MutationProducer, expected *objstore.ObjectVersion) error {
	key, _, err := s.producerKey(runtime)
	if err != nil {
		return err
	}
	if producer.Runtime != runtime || producer.Validate() != nil {
		return fmt.Errorf("fencing: canonical mutation producer state is invalid")
	}
	value := struct {
		ProducerUID string                   `json:"producerUID"`
		Producer    fencing.MutationProducer `json:"producer"`
	}{ProducerUID: stableMutationProducerUID(runtime), Producer: producer}
	_, identity, err := s.producerKey(runtime)
	if err != nil {
		return err
	}
	return s.write(ctx, key, identity, value, expected)
}

func stableMutationProducerUID(runtime fencing.RuntimeBinding) string {
	return "producer-" + hashJSON([]any{"rhiza-mutation-producer-v1", runtime})
}

func decodeAuthorityValue(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing canonical authority value")
	}
	return nil
}
