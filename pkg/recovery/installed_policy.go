package recovery

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"reflect"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const (
	InstalledPolicyProfileAppendOnly = "append-only"
	installedPolicyMaxSize           = 64 << 10
	installedPolicyKeyName           = "recovery/installed-policy.json"
)

type InstalledPolicyBaseKind string

const (
	InstalledPolicyBaseGenesis InstalledPolicyBaseKind = "genesis"
	InstalledPolicyBaseDR      InstalledPolicyBaseKind = "dr"
)

// InstalledPolicyBase distinguishes an explicit genesis definition from an
// externally anchored DR generation. Exactly one of GenesisDigest or
// GenerationAnchorHash is set according to Kind.
type InstalledPolicyBase struct {
	Kind                 InstalledPolicyBaseKind `json:"kind"`
	GenesisDigest        string                  `json:"genesis_digest,omitempty"`
	GenerationAnchorHash string                  `json:"generation_anchor_hash,omitempty"`
}

// InstalledPolicyRecord is immutable installation configuration. It carries
// public identity and policy hashes only; it contains no credentials or
// activation/progress state.
type InstalledPolicyRecord struct {
	ClusterID                 string                      `json:"cluster_id"`
	StorageBindingID          string                      `json:"storage_binding_id"`
	InitialCluster            quepaxa.Cluster             `json:"initial_cluster"`
	Profile                   string                      `json:"profile"`
	Durability                types.ObjectStoreDurability `json:"durability"`
	JoinCapabilityVerifierHex string                      `json:"join_capability_verifier"`
	Base                      InstalledPolicyBase         `json:"base"`
}

// InstalledPolicyExpectation contains values computed independently from
// trusted launch and storage configuration. ExpectedDigest is never inferred
// from the object being verified.
type InstalledPolicyExpectation struct {
	ExpectedDigest   [32]byte
	ClusterID        string
	StorageBindingID string
	InitialCluster   quepaxa.Cluster
	Durability       types.ObjectStoreDurability
}

// VerifiedInstalledPolicy can only be constructed by VerifyInstalledPolicy.
// Getters return copies so callers cannot mutate verified expectations.
type VerifiedInstalledPolicy struct {
	record InstalledPolicyRecord
	digest [32]byte
}

func (p *VerifiedInstalledPolicy) Digest() [32]byte {
	if p == nil {
		return [32]byte{}
	}
	return p.digest
}

func (p *VerifiedInstalledPolicy) ClusterID() string {
	if p == nil {
		return ""
	}
	return p.record.ClusterID
}

func (p *VerifiedInstalledPolicy) StorageBindingID() string {
	if p == nil {
		return ""
	}
	return p.record.StorageBindingID
}

func (p *VerifiedInstalledPolicy) InitialCluster() quepaxa.Cluster {
	if p == nil {
		return quepaxa.Cluster{}
	}
	return cloneCluster(p.record.InitialCluster)
}

func (p *VerifiedInstalledPolicy) Profile() string {
	if p == nil {
		return ""
	}
	return p.record.Profile
}

// ProfileHash identifies the concrete append-only format and durability, not
// any installation-specific identity or membership.
func (p *VerifiedInstalledPolicy) ProfileHash() [32]byte {
	if p == nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte("rhiza-append-only-immutable-manifest-extent-profile\x00" + string(p.record.Durability)))
}

func (p *VerifiedInstalledPolicy) Durability() types.ObjectStoreDurability {
	if p == nil {
		return ""
	}
	return p.record.Durability
}

func (p *VerifiedInstalledPolicy) Base() InstalledPolicyBase {
	if p == nil {
		return InstalledPolicyBase{}
	}
	return p.record.Base
}

// VerifyJoinCapability compares a candidate secret with the generation-scoped
// verifier using constant-time comparison. The public verifier is never itself
// accepted as the secret.
func (p *VerifiedInstalledPolicy) VerifyJoinCapability(secret []byte) bool {
	if p == nil {
		return false
	}
	verifier, err := InstalledJoinCapabilityVerifier(p.record.ClusterID, secret)
	if err != nil {
		return false
	}
	want, err := decodeInstalledPolicyHash(p.record.JoinCapabilityVerifierHex)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(verifier[:], want[:]) == 1
}

// InstalledPolicyKey derives the one policy object key from the caller's
// already-selected bucket prefix and explicit cluster ID.
func InstalledPolicyKey(prefix, clusterID string) (string, error) {
	if clusterID == "" || clusterID == "." || clusterID == ".." || strings.ContainsAny(clusterID, "/\\") {
		return "", fmt.Errorf("installed policy requires a single safe cluster ID")
	}
	if prefix == "" {
		return path.Join(clusterID, installedPolicyKeyName), nil
	}
	normalized, err := normalizeForkPrefix(prefix)
	if err != nil {
		return "", fmt.Errorf("installed policy prefix: %w", err)
	}
	return path.Join(normalized, clusterID, installedPolicyKeyName), nil
}

// InstalledPolicyDigest hashes the canonical JSON encoding of a valid policy.
func InstalledPolicyDigest(record InstalledPolicyRecord) ([32]byte, error) {
	if err := validateInstalledPolicyRecord(record); err != nil {
		return [32]byte{}, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return [32]byte{}, err
	}
	if len(data) > installedPolicyMaxSize {
		return [32]byte{}, fmt.Errorf("installed policy exceeds size limit")
	}
	return sha256.Sum256(data), nil
}

// InstalledJoinCapabilityVerifier derives a public, generation-scoped
// verifier from a secret. The minimum length is only a sanity guard, not proof
// of entropy; callers importing an existing secret are responsible for using
// cryptographically random material.
func InstalledJoinCapabilityVerifier(clusterID string, secret []byte) ([32]byte, error) {
	if clusterID == "" || len(secret) < 32 {
		return [32]byte{}, fmt.Errorf("installed policy join capability requires a cluster ID and at least 32 secret bytes")
	}
	const domain = "rhiza-installed-policy-join-capability-v1\x00"
	data := make([]byte, 0, len(domain)+16+len(clusterID)+len(secret))
	data = append(data, domain...)
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(clusterID)))
	data = append(data, size[:]...)
	data = append(data, clusterID...)
	binary.BigEndian.PutUint64(size[:], uint64(len(secret)))
	data = append(data, size[:]...)
	data = append(data, secret...)
	return sha256.Sum256(data), nil
}

// GenerateInstalledJoinCapability creates a fresh 256-bit generation-scoped
// join secret and its public verifier. The returned secret is 32 random bytes
// encoded as unpadded URL-safe Base64 ASCII so it can be safely carried through
// text-based configuration. Store only the verifier in InstalledPolicyRecord;
// deliver and retain the secret using the caller's protected secret handling.
// The verifier is computed over the exact returned ASCII bytes.
func GenerateInstalledJoinCapability(clusterID string) (secret []byte, verifier [32]byte, err error) {
	if clusterID == "" {
		return nil, [32]byte{}, fmt.Errorf("installed policy join capability requires a cluster ID")
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, [32]byte{}, fmt.Errorf("generate installed policy join capability: %w", err)
	}
	secret = []byte(base64.RawURLEncoding.EncodeToString(random[:]))
	verifier, err = InstalledJoinCapabilityVerifier(clusterID, secret)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return secret, verifier, nil
}

// InstalledGenesisDigest binds the explicit genesis definition to the exact
// initial cluster, recognized profile, and durability policy.
func InstalledGenesisDigest(initial quepaxa.Cluster, profile string, durability types.ObjectStoreDurability) ([32]byte, error) {
	if err := validateInitialCluster(initial); err != nil {
		return [32]byte{}, err
	}
	if err := validateInstalledProfile(profile, durability); err != nil {
		return [32]byte{}, err
	}
	definition := struct {
		Kind           string                      `json:"kind"`
		InitialCluster quepaxa.Cluster             `json:"initial_cluster"`
		Profile        string                      `json:"profile"`
		Durability     types.ObjectStoreDurability `json:"durability"`
	}{"rhiza-explicit-genesis", cloneCluster(initial), profile, durability}
	data, err := json.Marshal(definition)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("rhiza-installed-genesis-base\x00"), data...)), nil
}

// InstallInstalledPolicy is an explicit create-only installer. A later call
// accepts only byte-identical retained policy; ordinary verification never
// creates an object. Unknown upload errors are returned unchanged.
func InstallInstalledPolicy(ctx context.Context, bucket objstore.Bucket, prefix string, record InstalledPolicyRecord) ([32]byte, error) {
	if bucket == nil || nilBucket(bucket) {
		return [32]byte{}, fmt.Errorf("installed policy requires a non-nil bucket")
	}
	data, err := encodeInstalledPolicy(record)
	if err != nil {
		return [32]byte{}, err
	}
	digest := sha256.Sum256(data)
	key, err := InstalledPolicyKey(prefix, record.ClusterID)
	if err != nil {
		return [32]byte{}, err
	}
	if !supportsUploadOption(bucket, objstore.IfNotExists) {
		return [32]byte{}, fmt.Errorf("installed policy store does not support create-only uploads")
	}
	if existing, readErr := readInstalledPolicyBytes(ctx, bucket, key); readErr == nil {
		if bytes.Equal(existing, data) {
			return digest, nil
		}
		return [32]byte{}, fmt.Errorf("installed policy already exists with different bytes")
	} else if !bucket.IsObjNotFoundErr(readErr) {
		return [32]byte{}, readErr
	}

	err = bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists())
	if err != nil {
		if !bucket.IsConditionNotMetErr(err) {
			return [32]byte{}, err
		}
		stored, readErr := readInstalledPolicyBytes(ctx, bucket, key)
		if readErr != nil {
			return [32]byte{}, errors.Join(fmt.Errorf("installed policy create-only conflict: %w", err), readErr)
		}
		if !bytes.Equal(stored, data) {
			return [32]byte{}, fmt.Errorf("installed policy create-only conflict contains different bytes")
		}
		return digest, nil
	}
	stored, err := readInstalledPolicyBytes(ctx, bucket, key)
	if err != nil {
		return [32]byte{}, fmt.Errorf("read back installed policy: %w", err)
	}
	if !bytes.Equal(stored, data) {
		return [32]byte{}, fmt.Errorf("installed policy readback differs from installed bytes")
	}
	return digest, nil
}

// VerifyInstalledPolicy is strictly read-only and requires an independent
// expected digest and trusted configuration expectations.
func VerifyInstalledPolicy(ctx context.Context, bucket objstore.Bucket, prefix string, expected InstalledPolicyExpectation) (*VerifiedInstalledPolicy, error) {
	if expected.ExpectedDigest == ([32]byte{}) {
		return nil, fmt.Errorf("installed policy expected digest is required")
	}
	if bucket == nil || nilBucket(bucket) {
		return nil, fmt.Errorf("installed policy requires a non-nil bucket")
	}
	if expected.ClusterID == "" || expected.StorageBindingID == "" {
		return nil, fmt.Errorf("installed policy expectations require cluster and storage binding IDs")
	}
	if err := validateInitialCluster(expected.InitialCluster); err != nil {
		return nil, fmt.Errorf("invalid expected initial cluster: %w", err)
	}
	key, err := InstalledPolicyKey(prefix, expected.ClusterID)
	if err != nil {
		return nil, err
	}
	data, err := readInstalledPolicyBytes(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("read installed policy: %w", err)
	}
	if sha256.Sum256(data) != expected.ExpectedDigest {
		return nil, fmt.Errorf("installed policy digest does not match independent expectation")
	}
	var record InstalledPolicyRecord
	if err := decodeCanonicalInstalledPolicy(data, &record); err != nil {
		return nil, fmt.Errorf("invalid installed policy: %w", err)
	}
	if err := validateInstalledPolicyRecord(record); err != nil {
		return nil, fmt.Errorf("invalid installed policy: %w", err)
	}
	if record.ClusterID != expected.ClusterID || record.StorageBindingID != expected.StorageBindingID || !sameInstalledCluster(record.InitialCluster, expected.InitialCluster) || record.Durability != expected.Durability {
		return nil, fmt.Errorf("installed policy differs from trusted runtime expectations")
	}
	return &VerifiedInstalledPolicy{record: cloneInstalledPolicy(record), digest: expected.ExpectedDigest}, nil
}

func encodeInstalledPolicy(record InstalledPolicyRecord) ([]byte, error) {
	if err := validateInstalledPolicyRecord(record); err != nil {
		return nil, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(data) > installedPolicyMaxSize {
		return nil, fmt.Errorf("installed policy exceeds size limit")
	}
	return data, nil
}

func decodeCanonicalInstalledPolicy(data []byte, target *InstalledPolicyRecord) error {
	if len(data) == 0 || len(data) > installedPolicyMaxSize {
		return fmt.Errorf("installed policy size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("installed policy has trailing data")
	}
	canonical, err := json.Marshal(target)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, canonical) {
		return fmt.Errorf("installed policy is not canonical JSON")
	}
	return nil
}

func validateInstalledPolicyRecord(record InstalledPolicyRecord) error {
	if record.ClusterID == "" || record.StorageBindingID == "" {
		return fmt.Errorf("installed policy cluster and storage binding IDs are required")
	}
	if err := validateInitialCluster(record.InitialCluster); err != nil {
		return err
	}
	if err := validateInstalledProfile(record.Profile, record.Durability); err != nil {
		return err
	}
	if _, err := decodeInstalledPolicyHash(record.JoinCapabilityVerifierHex); err != nil {
		return fmt.Errorf("installed policy join capability verifier is invalid")
	}
	switch record.Base.Kind {
	case InstalledPolicyBaseGenesis:
		if record.Base.GenerationAnchorHash != "" {
			return fmt.Errorf("genesis installed policy cannot contain a DR anchor")
		}
		want, err := InstalledGenesisDigest(record.InitialCluster, record.Profile, record.Durability)
		if err != nil || hex.EncodeToString(want[:]) != record.Base.GenesisDigest {
			return fmt.Errorf("installed policy genesis definition digest is invalid")
		}
	case InstalledPolicyBaseDR:
		if record.Base.GenesisDigest != "" {
			return fmt.Errorf("DR installed policy cannot contain a genesis digest")
		}
		if anchor, err := decodeInstalledPolicyHash(record.Base.GenerationAnchorHash); err != nil || anchor == ([32]byte{}) {
			return fmt.Errorf("installed policy DR anchor hash is invalid")
		}
	default:
		return fmt.Errorf("installed policy base kind is unsupported")
	}
	return nil
}

func validateInstalledProfile(profile string, durability types.ObjectStoreDurability) error {
	if profile != InstalledPolicyProfileAppendOnly {
		return fmt.Errorf("installed policy profile is unsupported")
	}
	if durability != types.ObjectStoreDurabilityAsync && durability != types.ObjectStoreDurabilityBeforeAck {
		return fmt.Errorf("installed policy durability is unsupported")
	}
	return nil
}

func validateInitialCluster(cluster quepaxa.Cluster) error {
	if cluster.ConfigID == 0 || len(cluster.Members) == 0 {
		return fmt.Errorf("installed policy initial cluster is incomplete")
	}
	seen := make(map[quepaxa.NodeID]struct{}, len(cluster.Members))
	seenKeys := make(map[quepaxa.PublicKey]struct{}, len(cluster.Members))
	for _, member := range cluster.Members {
		if member.ID == "" || member.PublicKey == (quepaxa.PublicKey{}) {
			return fmt.Errorf("installed policy initial cluster requires unique IDs and public keys")
		}
		if _, ok := seen[member.ID]; ok {
			return fmt.Errorf("installed policy initial cluster contains duplicate member IDs")
		}
		if _, ok := seenKeys[member.PublicKey]; ok {
			return fmt.Errorf("installed policy initial cluster contains duplicate public keys")
		}
		seen[member.ID] = struct{}{}
		seenKeys[member.PublicKey] = struct{}{}
	}
	return nil
}

func decodeInstalledPolicyHash(value string) ([32]byte, error) {
	var hash [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(hash) || hex.EncodeToString(decoded) != value {
		return hash, fmt.Errorf("invalid SHA-256 hex")
	}
	copy(hash[:], decoded)
	return hash, nil
}

func supportsUploadOption(bucket objstore.Bucket, required objstore.ObjectUploadOptionType) bool {
	for _, option := range bucket.SupportedObjectUploadOptions() {
		if option == required {
			return true
		}
	}
	return false
}

func readInstalledPolicyBytes(ctx context.Context, bucket objstore.Bucket, key string) ([]byte, error) {
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, installedPolicyMaxSize+1))
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > installedPolicyMaxSize {
		return nil, fmt.Errorf("installed policy exceeds size limit")
	}
	return data, nil
}

func nilBucket(bucket objstore.Bucket) bool {
	value := reflect.ValueOf(bucket)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func sameInstalledCluster(left, right quepaxa.Cluster) bool {
	if left.ConfigID != right.ConfigID || len(left.Members) != len(right.Members) {
		return false
	}
	for i := range left.Members {
		if left.Members[i] != right.Members[i] {
			return false
		}
	}
	return true
}

func cloneCluster(cluster quepaxa.Cluster) quepaxa.Cluster {
	cluster.Members = append([]quepaxa.Member(nil), cluster.Members...)
	return cluster
}

func cloneInstalledPolicy(record InstalledPolicyRecord) InstalledPolicyRecord {
	record.InitialCluster = cloneCluster(record.InitialCluster)
	return record
}
