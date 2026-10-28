package recovery

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

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const maxImmutableManifestSize = 4 << 20

// ImmutableCheckpointRef identifies an existing checkpoint root. It is a
// structural reference only; callers must still OpenRoot and Verify it.
type ImmutableCheckpointRef struct {
	Index     uint64   `json:"index"`
	RootHash  [32]byte `json:"root_hash"`
	StateHash [32]byte `json:"state_hash"`
}

// ImmutableArchiveBase reuses the consensus checkpoint certificate types. An
// anchor hash is only a reference: it is never trusted from this manifest.
type ImmutableArchiveBase struct {
	Index      quepaxa.Slot            `json:"index"`
	PrefixHash [32]byte                `json:"prefix_hash"`
	Seal       *quepaxa.CheckpointSeal `json:"seal,omitempty"`
	Decision   *quepaxa.DecidedValue   `json:"decision,omitempty"`
	AnchorHash [32]byte                `json:"anchor_hash,omitempty"`
	Checkpoint *ImmutableCheckpointRef `json:"checkpoint,omitempty"`
}

// ImmutableExtentRef identifies an existing extent by its codec content hash
// and ordered object number. It contains no caller-selected object path.
type ImmutableExtentRef struct {
	Object         uint64       `json:"object"`
	Hash           [32]byte     `json:"hash"`
	ConfigID       uint         `json:"config_id"`
	Start          quepaxa.Slot `json:"start"`
	End            quepaxa.Slot `json:"end"`
	StartPrefix    [32]byte     `json:"start_prefix"`
	EndPrefix      [32]byte     `json:"end_prefix"`
	PreviousHash   [32]byte     `json:"previous_hash"`
	PreviousObject uint64       `json:"previous_object"`
}

// ImmutableArchiveManifest is an immutable descriptor, not certified
// authority. Until a caller supplies the trusted manifest hash and independently
// validates its base and replays certificates into an observer, every field is
// untrusted data. This API does not authorize Node activation.
type ImmutableArchiveManifest struct {
	ConfigID            uint     `json:"config_id"`
	InstalledPolicyHash [32]byte `json:"installed_policy_hash"`
	ProfileHash         [32]byte `json:"profile_hash"`
	// WriterIdentityHash is derived from finalized public node, key, and WAL
	// registration bound to InstalledPolicyHash. Recovery does not construct or
	// validate that registration; its producer supplies the finalized digest.
	WriterIdentityHash [32]byte             `json:"writer_identity_hash"`
	Base               ImmutableArchiveBase `json:"base"`
	Tip                quepaxa.Slot         `json:"tip"`
	TipPrefix          [32]byte             `json:"tip_prefix"`
	Extents            []ImmutableExtentRef `json:"extents"`
}

// ImmutableArchiveSelection is trusted caller context used only for lookup
// binding. Its namespace, installed policy, profile, and finalized writer
// registration hashes must come from independently authorized configuration,
// not a manifest or receipt.
type ImmutableArchiveSelection struct {
	InstalledPolicyHash [32]byte
	ProfileHash         [32]byte
	WriterIdentityHash  [32]byte
}

// ImmutableArchive is a structurally verified but uncertified immutable
// descriptor. Its private reference copy prevents callers from substituting an
// extent reference after validation. Certification still requires trusted base
// checks and ReplayCertifiedThrough on an observer Core.
type ImmutableArchive struct {
	bucket    objstore.Bucket
	prefix    string
	selection ImmutableArchiveSelection
	manifest  ImmutableArchiveManifest
	refs      []ImmutableExtentRef
}

// WriteImmutableArchiveManifest publishes encoded extents and then a canonical
// manifest using create-only writes. Conflicts are accepted only after exact
// readback. No mutable selector, claim, or deletion is used. The caller-provided
// namespace and profile binding are not authenticated by this storage layer.
func WriteImmutableArchiveManifest(ctx context.Context, bucket objstore.Bucket, namespace string, manifest ImmutableArchiveManifest, extents []Extent) ([32]byte, error) {
	if err := validateImmutableNamespace(namespace); err != nil {
		return [32]byte{}, err
	}
	if bucket == nil {
		return [32]byte{}, fmt.Errorf("immutable archive bucket is required")
	}
	selection := ImmutableArchiveSelection{InstalledPolicyHash: manifest.InstalledPolicyHash, ProfileHash: manifest.ProfileHash, WriterIdentityHash: manifest.WriterIdentityHash}
	if err := validateImmutableSelection(selection); err != nil {
		return [32]byte{}, err
	}
	if err := validateImmutableManifestIdentity(manifest); err != nil {
		return [32]byte{}, fmt.Errorf("invalid immutable archive identity")
	}
	if err := validateImmutableBase(manifest.Base); err != nil {
		return [32]byte{}, err
	}
	refs, err := prepareImmutableExtents(manifest.ConfigID, manifest.Base, manifest.Tip, manifest.TipPrefix, extents)
	if err != nil {
		return [32]byte{}, err
	}
	manifest.Extents = refs
	if err := validateImmutableManifest(manifest); err != nil {
		return [32]byte{}, err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return [32]byte{}, err
	}
	if len(data) > maxImmutableManifestSize {
		return [32]byte{}, fmt.Errorf("immutable archive manifest exceeds size limit")
	}
	for i, ref := range refs {
		encoded, err := encodeExtent(extents[i])
		if err != nil {
			return [32]byte{}, fmt.Errorf("re-encode immutable archive extent %d: %w", i, err)
		}
		if sha256.Sum256(encoded) != ref.Hash || !immutableRefMatchesExtent(ref, extents[i]) {
			return [32]byte{}, fmt.Errorf("immutable archive extent %d changed after validation", i)
		}
		if err := uploadCreateOnly(ctx, bucket, immutableExtentKey(namespace, selection, ref.Object, ref.Hash), encoded, maxExtentSize); err != nil {
			return [32]byte{}, fmt.Errorf("publish immutable archive extent %d: %w", i, err)
		}
		encoded = nil
	}
	hash := sha256.Sum256(data)
	key := immutableManifestKey(namespace, selection, hash)
	if err := uploadCreateOnly(ctx, bucket, key, data, maxImmutableManifestSize); err != nil {
		return [32]byte{}, fmt.Errorf("publish immutable archive manifest: %w", err)
	}
	return hash, nil
}

// ReadImmutableArchiveManifest reads one exact caller-selected hash. It never
// lists objects or chooses a highest generation, timestamp, or mutable pointer.
func ReadImmutableArchiveManifest(ctx context.Context, bucket objstore.Bucket, namespace string, selection ImmutableArchiveSelection, expectedHash [32]byte) (*ImmutableArchive, error) {
	if err := validateImmutableNamespace(namespace); err != nil {
		return nil, err
	}
	if bucket == nil || expectedHash == ([32]byte{}) {
		return nil, fmt.Errorf("immutable archive bucket and expected hash are required")
	}
	if err := validateImmutableSelection(selection); err != nil {
		return nil, err
	}
	data, err := readImmutableObject(ctx, bucket, immutableManifestKey(namespace, selection, expectedHash), maxImmutableManifestSize)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(data) != expectedHash {
		return nil, fmt.Errorf("immutable archive manifest hash mismatch")
	}
	var manifest ImmutableArchiveManifest
	if err := decodeCanonicalImmutableJSON(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode immutable archive manifest: %w", err)
	}
	if manifest.InstalledPolicyHash != selection.InstalledPolicyHash || manifest.ProfileHash != selection.ProfileHash || manifest.WriterIdentityHash != selection.WriterIdentityHash {
		return nil, fmt.Errorf("immutable archive manifest selection mismatch")
	}
	if err := validateImmutableManifest(manifest); err != nil {
		return nil, err
	}
	view := &ImmutableArchive{bucket: bucket, prefix: namespace, selection: selection, manifest: cloneImmutableManifest(manifest), refs: append([]ImmutableExtentRef(nil), manifest.Extents...)}
	for i := range view.refs {
		if _, err := view.ReadExtent(ctx, i); err != nil {
			return nil, fmt.Errorf("verify immutable archive extent %d: %w", i, err)
		}
	}
	return view, nil
}

// Manifest returns a defensive descriptor copy. It remains untrusted until
// certified base verification and observer replay succeed.
func (a *ImmutableArchive) Manifest() ImmutableArchiveManifest {
	if a == nil {
		return ImmutableArchiveManifest{}
	}
	return cloneImmutableManifest(a.manifest)
}

// ReadExtent reads only a reference privately captured from the selected
// manifest and revalidates content hash, codec fields, and reference equality.
func (a *ImmutableArchive) ReadExtent(ctx context.Context, index int) (Extent, error) {
	if a == nil || a.bucket == nil || index < 0 || index >= len(a.refs) {
		return Extent{}, fmt.Errorf("immutable archive extent reference is unavailable")
	}
	ref := a.refs[index]
	key := immutableExtentKey(a.prefix, a.selection, ref.Object, ref.Hash)
	data, err := readImmutableObject(ctx, a.bucket, key, maxExtentSize)
	if err != nil {
		return Extent{}, err
	}
	if sha256.Sum256(data) != ref.Hash {
		return Extent{}, fmt.Errorf("immutable archive extent hash mismatch")
	}
	extent, err := decodeExtent(data)
	if err != nil {
		return Extent{}, err
	}
	if !immutableRefMatchesExtent(ref, extent) {
		return Extent{}, fmt.Errorf("immutable archive extent reference mismatch")
	}
	prefixes := extentPrefixes(extent)
	if prefixes[len(prefixes)-1] != extent.EndPrefix {
		return Extent{}, fmt.Errorf("immutable archive extent prefix mismatch")
	}
	extent.hash, extent.object = ref.Hash, ref.Object
	extent.prefixes = prefixes
	return extent, nil
}

// prepareImmutableExtents validates and hashes every extent while retaining
// only its compact reference. Bodies are intentionally re-encoded one at a
// time by the publisher after the complete manifest has passed validation.
func prepareImmutableExtents(configID uint, base ImmutableArchiveBase, tip quepaxa.Slot, tipPrefix [32]byte, extents []Extent) ([]ImmutableExtentRef, error) {
	if len(extents) > maxExtentItems {
		return nil, fmt.Errorf("immutable archive has too many extents")
	}
	expectedSlot, expectedPrefix := base.Index+1, base.PrefixHash
	if base.Index == 0 {
		expectedSlot, expectedPrefix = 1, [32]byte{}
	}
	refs := make([]ImmutableExtentRef, 0, len(extents))
	var previousHash [32]byte
	var previousObject uint64
	for i, extent := range extents {
		if extent.ConfigID != configID || extent.Start != expectedSlot || extent.StartPrefix != expectedPrefix || extent.PreviousHash != previousHash || extent.PreviousObject != previousObject {
			return nil, fmt.Errorf("immutable archive extent chain is not contiguous")
		}
		data, err := encodeExtent(extent)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(data)
		object := uint64(i + 1)
		ref := ImmutableExtentRef{Object: object, Hash: hash, ConfigID: configID, Start: extent.Start, End: extent.End, StartPrefix: extent.StartPrefix, EndPrefix: extent.EndPrefix, PreviousHash: extent.PreviousHash, PreviousObject: extent.PreviousObject}
		refs = append(refs, ref)
		data = nil
		expectedSlot, expectedPrefix, previousHash, previousObject = extent.End+1, extent.EndPrefix, hash, object
	}
	if len(extents) == 0 {
		if tip != base.Index || tipPrefix != base.PrefixHash {
			return nil, fmt.Errorf("unsupported immutable archive base form")
		}
	} else if expectedSlot-1 != tip || expectedPrefix != tipPrefix {
		return nil, fmt.Errorf("immutable archive extent chain does not reach declared tip")
	}
	return refs, nil
}

func validateImmutableManifest(m ImmutableArchiveManifest) error {
	if err := validateImmutableManifestIdentity(m); err != nil {
		return err
	}
	if err := validateImmutableBase(m.Base); err != nil {
		return err
	}
	if m.Tip == 0 {
		if len(m.Extents) != 0 || m.Tip != m.Base.Index || m.TipPrefix != m.Base.PrefixHash {
			return fmt.Errorf("invalid empty immutable archive")
		}
		return nil
	}
	expectedSlot := m.Base.Index + 1
	expectedPrefix, previousHash := m.Base.PrefixHash, [32]byte{}
	var previousObject uint64
	for _, ref := range m.Extents {
		if ref.Object != previousObject+1 || ref.Hash == ([32]byte{}) || ref.ConfigID != m.ConfigID || ref.Start == 0 || ref.Start != expectedSlot || ref.End < ref.Start || ref.StartPrefix != expectedPrefix || ref.PreviousHash != previousHash || ref.PreviousObject != previousObject {
			return fmt.Errorf("immutable archive extent references are not contiguous")
		}
		expectedSlot, expectedPrefix, previousHash, previousObject = ref.End+1, ref.EndPrefix, ref.Hash, ref.Object
	}
	if expectedSlot-1 != m.Tip || expectedPrefix != m.TipPrefix {
		return fmt.Errorf("immutable archive references do not reach declared tip")
	}
	return nil
}

func validateImmutableManifestIdentity(m ImmutableArchiveManifest) error {
	if m.ConfigID == 0 || m.InstalledPolicyHash == ([32]byte{}) || m.ProfileHash == ([32]byte{}) || m.WriterIdentityHash == ([32]byte{}) || len(m.Extents) > maxExtentItems || len(m.Extents) != 0 && m.Tip == 0 {
		return fmt.Errorf("invalid immutable archive manifest identity")
	}
	return nil
}

func validateImmutableBase(base ImmutableArchiveBase) error {
	if base.Index == 0 {
		if base.PrefixHash != ([32]byte{}) || base.Seal != nil || base.Decision != nil || base.AnchorHash != ([32]byte{}) || base.Checkpoint != nil {
			return fmt.Errorf("invalid immutable genesis base")
		}
		return nil
	}
	if base.Checkpoint == nil || base.Checkpoint.Index != uint64(base.Index) || base.Checkpoint.RootHash == ([32]byte{}) || base.Checkpoint.StateHash == ([32]byte{}) {
		return fmt.Errorf("immutable checkpoint base lacks exact root reference")
	}
	certified := base.Seal != nil && base.Decision != nil && base.AnchorHash == ([32]byte{})
	anchored := base.Seal == nil && base.Decision == nil && base.AnchorHash != ([32]byte{})
	if !certified && !anchored {
		return fmt.Errorf("unsupported immutable archive base form")
	}
	if certified && (base.Seal.Index != base.Index || base.Seal.PrefixHash != base.PrefixHash || base.Seal.RootHash != base.Checkpoint.RootHash || base.Seal.StateHash != base.Checkpoint.StateHash) {
		return fmt.Errorf("immutable checkpoint seal and root reference differ")
	}
	return nil
}

func immutableRefMatchesExtent(ref ImmutableExtentRef, extent Extent) bool {
	return extent.ConfigID == ref.ConfigID && extent.Start == ref.Start && extent.End == ref.End && extent.StartPrefix == ref.StartPrefix && extent.EndPrefix == ref.EndPrefix && extent.PreviousHash == ref.PreviousHash && extent.PreviousObject == ref.PreviousObject
}

func validateImmutableSelection(s ImmutableArchiveSelection) error {
	if s.InstalledPolicyHash == ([32]byte{}) || s.ProfileHash == ([32]byte{}) || s.WriterIdentityHash == ([32]byte{}) {
		return fmt.Errorf("invalid immutable archive selection")
	}
	return nil
}

func validateImmutableNamespace(namespace string) error {
	if namespace == "" || strings.HasPrefix(namespace, "/") || strings.Contains(namespace, "\\") || path.Clean(namespace) != namespace || namespace == "." || namespace == ".." || strings.HasPrefix(namespace, "../") {
		return fmt.Errorf("invalid immutable archive namespace")
	}
	return nil
}

func immutableScopePath(selection ImmutableArchiveSelection) (string, error) {
	return fmt.Sprintf("profile-%x/writer-%x", selection.ProfileHash, selection.WriterIdentityHash), nil
}

func immutableManifestKey(namespace string, selection ImmutableArchiveSelection, hash [32]byte) string {
	scope, _ := immutableScopePath(selection)
	return path.Join(namespace, scope, "manifest", hex.EncodeToString(hash[:])+".json")
}

func immutableExtentKey(namespace string, selection ImmutableArchiveSelection, object uint64, hash [32]byte) string {
	scope, _ := immutableScopePath(selection)
	return path.Join(namespace, scope, "extent", fmt.Sprintf("%020d-%x.bin", object, hash))
}

func uploadCreateOnly(ctx context.Context, bucket objstore.Bucket, key string, data []byte, limit int) error {
	conditional := false
	for _, option := range bucket.SupportedObjectUploadOptions() {
		conditional = conditional || option == objstore.IfNotExists
	}
	if !conditional {
		return fmt.Errorf("object store does not support create-only uploads")
	}
	err := bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists())
	if err == nil {
		return nil
	}
	if !bucket.IsConditionNotMetErr(err) {
		return err
	}
	stored, readErr := readImmutableObject(ctx, bucket, key, limit)
	if readErr != nil {
		return errors.Join(fmt.Errorf("create-only object conflict: %w", err), readErr)
	}
	if !bytes.Equal(stored, data) {
		return fmt.Errorf("create-only object conflict contains different bytes")
	}
	return nil
}

func readImmutableObject(ctx context.Context, bucket objstore.Bucket, key string, limit int) ([]byte, error) {
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	closeErr := r.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > limit {
		return nil, fmt.Errorf("immutable archive object exceeds size limit")
	}
	return data, nil
}

func decodeCanonicalImmutableJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("immutable archive object has trailing data")
	}
	canonical, err := json.Marshal(target)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, canonical) {
		return fmt.Errorf("immutable archive object is not canonical JSON")
	}
	return nil
}

func cloneImmutableManifest(m ImmutableArchiveManifest) ImmutableArchiveManifest {
	m.Extents = append([]ImmutableExtentRef(nil), m.Extents...)
	if m.Base.Seal != nil {
		seal := *m.Base.Seal
		seal.NextLeaderOrder = append([]quepaxa.NodeID(nil), seal.NextLeaderOrder...)
		seal.FollowingLeaderOrder = append([]quepaxa.NodeID(nil), seal.FollowingLeaderOrder...)
		if seal.Membership != nil {
			membership := cloneImmutableMembership(*seal.Membership)
			seal.Membership = &membership
		}
		m.Base.Seal = &seal
	}
	if m.Base.Decision != nil {
		decision := *m.Base.Decision
		decision.Value = append([]byte(nil), decision.Value...)
		decision.Certificate = append([]byte(nil), decision.Certificate...)
		m.Base.Decision = &decision
	}
	if m.Base.Checkpoint != nil {
		root := *m.Base.Checkpoint
		m.Base.Checkpoint = &root
	}
	return m
}

func cloneImmutableMembership(record quepaxa.MembershipRecord) quepaxa.MembershipRecord {
	copy := quepaxa.MembershipRecord{
		Genesis:     record.Genesis,
		Transitions: make([]quepaxa.ConfigTransition, len(record.Transitions)),
	}
	copy.Genesis.Members = append([]quepaxa.Member(nil), record.Genesis.Members...)
	for i, transition := range record.Transitions {
		copy.Transitions[i] = quepaxa.ConfigTransition{
			Freeze:   cloneImmutableDecision(transition.Freeze),
			Terminal: cloneImmutableDecision(transition.Terminal),
		}
	}
	if record.Abort != nil {
		abort := quepaxa.ConfigTransition{
			Freeze:   cloneImmutableDecision(record.Abort.Freeze),
			Terminal: cloneImmutableDecision(record.Abort.Terminal),
		}
		copy.Abort = &abort
	}
	return copy
}

func cloneImmutableDecision(decision quepaxa.DecidedValue) quepaxa.DecidedValue {
	decision.Value = append([]byte(nil), decision.Value...)
	decision.Certificate = append([]byte(nil), decision.Certificate...)
	return decision
}
