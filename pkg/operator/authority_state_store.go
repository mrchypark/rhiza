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
	"path"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
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
			return errors.Join(err, fmt.Errorf("fencing: authority root genesis differs"))
		}
		return nil
	}
	if !errors.Is(err, errAuthorityStateNotFound) {
		return err
	}
	if err := s.write(ctx, key, identity, value, nil); err != nil && !errors.Is(err, errAuthorityStateConflict) {
		stored, _, readErr := s.read(ctx, key, identity)
		var saved struct {
			RootIdentity string `json:"rootIdentity"`
		}
		if readErr == nil && decodeAuthorityValue(stored, &saved) == nil && saved == value {
			return nil
		}
		return errors.Join(err, readErr)
	}
	stored, _, err := s.read(ctx, key, identity)
	if err != nil {
		return err
	}
	var saved struct {
		RootIdentity string `json:"rootIdentity"`
	}
	if err := decodeAuthorityValue(stored, &saved); err != nil || saved != value {
		return errors.Join(err, fmt.Errorf("fencing: authority root genesis differs"))
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
		return errors.Join(err, readErr)
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
		return fenceLedger{}, nil, err
	}
	data, version, err := s.read(ctx, key, identity)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			if _, _, genesisErr := s.read(ctx, s.genesisKey(identity), identity); genesisErr == nil {
				return fenceLedger{}, nil, errAuthorityRootLost
			} else if !errors.Is(genesisErr, errAuthorityStateNotFound) {
				return fenceLedger{}, nil, genesisErr
			}
		}
		return fenceLedger{}, nil, err
	}
	var state fenceLedger
	if err := decodeAuthorityValue(data, &state); err != nil || state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
		return fenceLedger{}, nil, errors.Join(err, fmt.Errorf("fencing: canonical authority root identity differs"))
	}
	return state, version, nil
}

func (s *AuthorityStateStore) WriteRoot(ctx context.Context, uid, source string, state fenceLedger, expected *objstore.ObjectVersion) error {
	key, identity, err := s.rootKey(uid, source)
	if err != nil {
		return err
	}
	if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
		return fmt.Errorf("fencing: canonical authority root identity differs")
	}
	return s.write(ctx, key, identity, state, expected)
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
