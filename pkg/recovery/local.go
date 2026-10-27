package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"io"
	"path"
	"sync"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// LocalBinding is immutable identity, not a distributed ownership claim.
// Its namespace must have exactly one externally exclusive writer.
type LocalBinding struct {
	Version             uint            `json:"version"`
	ArchiveLocationHash string          `json:"archive_location_hash,omitempty"`
	StoreUUID           string          `json:"store_uuid"`
	ConfigDigest        string          `json:"config_digest"`
	PolicyVersion       uint            `json:"policy_version"`
	Bootstrap           quepaxa.Cluster `json:"bootstrap"`
}

type LocalSnapshot struct {
	Seal         quepaxa.CheckpointSeal `json:"seal"`
	Decision     quepaxa.DecidedValue   `json:"decision"`
	PortableRoot [32]byte               `json:"portable_root"`
	Descriptor   json.RawMessage        `json:"descriptor"`
}

const maxLocalHeadSize = 4 << 20

type localHead struct {
	Version     uint            `json:"version"`
	BindingHash [32]byte        `json:"binding_hash"`
	Tip         quepaxa.Slot    `json:"tip"`
	Prefix      [32]byte        `json:"prefix"`
	Tail        [32]byte        `json:"tail"`
	Origin      *LocalSnapshot  `json:"origin,omitempty"`
	Snapshot    *LocalSnapshot  `json:"snapshot,omitempty"`
	Catalog     []LocalSnapshot `json:"catalog,omitempty"`
}

// LocalArchive reuses the decision extent format without shared-voter leases,
// generation recovery, or any relaxation of consensus certificate validation.
type LocalArchive struct {
	mu       sync.Mutex
	bucket   objstore.Bucket
	prefix   string
	Binding  LocalBinding
	head     localHead
	headHash [32]byte
	version  *objstore.ObjectVersion
	extents  []Extent
	failure  error
}

func OpenLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string) (*LocalArchive, error) {
	return openLocalArchive(ctx, bucket, prefix, nil)
}

// OpenLocalArchiveOwned is restricted to Node's internal filesystem-v2 path.
func OpenLocalArchiveOwned(ctx context.Context, bucket objstore.Bucket, prefix string, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	if owner == nil || !owner.Held() {
		return nil, fmt.Errorf("filesystem-v2 archive open requires its owner")
	}
	a, err := openLocalArchive(ctx, bucket, prefix, owner)
	if err != nil {
		return nil, err
	}
	if a.Binding.Version != 2 || a.Binding.ArchiveLocationHash != owner.LocationHash(prefix) {
		return nil, fmt.Errorf("filesystem-v2 archive location differs from its owner")
	}
	return a, nil
}

func openLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	bucket = checkpoint.LocalBucket(bucket)
	a := &LocalArchive{bucket: bucket, prefix: prefix}
	data, err := a.read(ctx, "BINDING", 64<<10)
	if err != nil {
		return nil, err
	}
	if err := strictLocalJSON(data, &a.Binding); err != nil {
		return nil, err
	}
	if err := validateLocalBinding(a.Binding); err != nil {
		return nil, err
	}
	if a.Binding.Version == 2 && (owner == nil || !owner.Held()) {
		return nil, fmt.Errorf("filesystem-v2 archive requires its owner")
	}
	if a.Binding.Version == 2 {
		if err := a.validateLocalControl(ctx, data); err != nil {
			return nil, err
		}
	}
	if err := a.readLocalHead(ctx, data); err != nil {
		return nil, err
	}
	hash := a.head.Tail
	next, prefixHash := a.head.Tip, a.head.Prefix
	seen := make(map[[32]byte]bool)
	for hash != ([32]byte{}) {
		if seen[hash] {
			return nil, fmt.Errorf("local archive extent cycle")
		}
		seen[hash] = true
		encoded, err := a.read(ctx, fmt.Sprintf("extents/%x", hash), maxExtentSize)
		if err != nil {
			return nil, err
		}
		if sha256.Sum256(encoded) != hash {
			return nil, fmt.Errorf("local archive extent hash mismatch")
		}
		extent, err := decodeExtent(encoded)
		if err != nil {
			return nil, err
		}
		if extent.End != next || extent.EndPrefix != prefixHash || extent.ConfigID != a.Binding.Bootstrap.ConfigID || extent.PreviousHash == ([32]byte{}) && extent.PreviousObject != 0 || extent.PreviousHash != ([32]byte{}) && extent.PreviousObject != 1 {
			return nil, fmt.Errorf("local archive extent chain mismatch")
		}
		a.extents = append(a.extents, extent)
		hash, next, prefixHash = extent.PreviousHash, extent.Start-1, extent.StartPrefix
	}
	for i, j := 0, len(a.extents)-1; i < j; i, j = i+1, j-1 {
		a.extents[i], a.extents[j] = a.extents[j], a.extents[i]
	}
	base, basePrefix := quepaxa.Slot(0), [32]byte{}
	if a.head.Origin != nil {
		base, basePrefix = a.head.Origin.Seal.Index, a.head.Origin.Seal.PrefixHash
	}
	if next != base || prefixHash != basePrefix {
		return nil, fmt.Errorf("local archive does not reach its origin")
	}
	if a.head.Snapshot != nil {
		p, ok := a.prefixAt(a.head.Snapshot.Seal.Index)
		if !ok || p != a.head.Snapshot.Seal.PrefixHash {
			return nil, fmt.Errorf("local snapshot is outside archived history")
		}
		if err := a.validateSnapshot(*a.head.Snapshot, a.head.Origin != nil && a.head.Origin.Seal.RootHash == a.head.Snapshot.Seal.RootHash); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// readLocalHead validates bounded metadata only. Extent reconstruction belongs to Open.
func (a *LocalArchive) readLocalHead(ctx context.Context, data []byte) error {
	attrs, err := a.bucket.Attributes(ctx, a.key("HEAD"))
	if err != nil {
		return fmt.Errorf("local archive head missing: %w", err)
	}
	head, err := a.read(ctx, "HEAD", maxLocalHeadSize)
	if err != nil {
		return err
	}
	after, err := a.bucket.Attributes(ctx, a.key("HEAD"))
	if err != nil || !sameObjectVersion(attrs.Version, after.Version) || attrs.Version == nil {
		return fmt.Errorf("local archive head changed during read")
	}
	if err := strictLocalJSON(head, &a.head); err != nil {
		return err
	}
	if (a.head.Version != 1 && a.head.Version != 2) || a.head.Version == 1 && len(a.head.Catalog) != 0 || a.head.BindingHash != sha256.Sum256(data) {
		return fmt.Errorf("local archive identity mismatch")
	}
	if a.head.Version == 2 {
		if err := a.validateLocalCatalog(); err != nil {
			return err
		}
	}
	a.version = attrs.Version
	a.headHash = sha256.Sum256(head)
	_, _, err = a.localProtocol(ctx)
	return err
}

// CreateLocalArchive uses conditional creation for both genesis objects. A
// partially published genesis is an error on later opens, never empty bootstrap.
func CreateLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, base *LocalSnapshot) (*LocalArchive, error) {
	return createLocalArchive(ctx, bucket, prefix, binding, base, nil)
}

// CreateLocalArchiveOwned is restricted to Node's internal filesystem-v2 path.
func CreateLocalArchiveOwned(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, base *LocalSnapshot, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	if owner == nil || !owner.Held() || binding.ArchiveLocationHash != owner.LocationHash(prefix) {
		return nil, fmt.Errorf("filesystem-v2 archive creation requires its owner")
	}
	genesis, err := PrepareLocalArchiveOwned(ctx, bucket, prefix, binding, owner)
	if err != nil {
		return nil, err
	}
	return CreateLocalArchiveFromPreparedOwned(ctx, genesis, base, owner)
}

// LocalArchiveGenesis is a process-local capability for completing the exact
// BINDING+control pair created by PrepareLocalArchiveOwned.
type LocalArchiveGenesis struct {
	owner       *internalobjstore.LocalFilesystemOwner
	bucket      objstore.Bucket
	prefix      string
	binding     LocalBinding
	bindingData []byte
}

// PrepareLocalArchiveOwned creates BINDING and then the v2 control before any
// checkpoint roots, snapshots, extents, or HEAD are written.
func PrepareLocalArchiveOwned(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchiveGenesis, error) {
	if owner == nil || !owner.Held() || binding.Version != 2 || binding.ArchiveLocationHash != owner.LocationHash(prefix) {
		return nil, fmt.Errorf("filesystem-v2 archive preparation requires its owner")
	}
	bucket = checkpoint.LocalBucket(bucket)
	conditional, match := false, false
	for _, option := range bucket.SupportedObjectUploadOptions() {
		conditional = conditional || option == objstore.IfNotExists
		match = match || option == objstore.IfMatch
	}
	if !conditional || !match {
		return nil, fmt.Errorf("local backup requires conditional object writes")
	}
	if err := validateLocalBinding(binding); err != nil {
		return nil, err
	}
	data, err := json.Marshal(binding)
	if err != nil {
		return nil, err
	}
	a := &LocalArchive{bucket: bucket, prefix: prefix}
	if err := bucket.Upload(ctx, a.key("BINDING"), bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		return nil, err
	}
	bindingHash := sha256.Sum256(data)
	control, err := json.Marshal(localMaintenanceControlV2{Version: 2, LocationHash: binding.ArchiveLocationHash, BindingHash: hex.EncodeToString(bindingHash[:])})
	if err != nil {
		return nil, err
	}
	if err := bucket.Upload(ctx, a.key("local-gc-control"), bytes.NewReader(control), objstore.WithIfNotExists()); err != nil {
		return nil, err
	}
	return &LocalArchiveGenesis{owner: owner, bucket: bucket, prefix: prefix, binding: binding, bindingData: data}, nil
}

// CreateLocalArchiveFromPreparedOwned publishes HEAD only after the caller has
// uploaded and verified any base checkpoint beneath the already-created v2 control.
func CreateLocalArchiveFromPreparedOwned(ctx context.Context, genesis *LocalArchiveGenesis, base *LocalSnapshot, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	if genesis == nil || owner == nil || genesis.owner != owner || !owner.Held() || genesis.binding.Version != 2 || genesis.binding.ArchiveLocationHash != owner.LocationHash(genesis.prefix) {
		return nil, fmt.Errorf("filesystem-v2 archive genesis is invalid")
	}
	a := &LocalArchive{bucket: genesis.bucket, prefix: genesis.prefix, Binding: genesis.binding}
	if base != nil {
		if err := a.validateSnapshot(*base, true); err != nil {
			return nil, err
		}
	}
	binding, err := a.read(ctx, "BINDING", 64<<10)
	if err != nil || !bytes.Equal(binding, genesis.bindingData) {
		return nil, fmt.Errorf("filesystem-v2 binding changed during genesis")
	}
	if err := a.validateLocalControl(ctx, binding); err != nil {
		return nil, err
	}
	a.head = localHead{Version: 1, BindingHash: sha256.Sum256(binding), Origin: base, Snapshot: base}
	if base != nil {
		a.head.Tip, a.head.Prefix = base.Seal.Index, base.Seal.PrefixHash
	}
	if err := a.publish(ctx, a.head); err != nil {
		return nil, err
	}
	return OpenLocalArchiveOwned(ctx, a.bucket, a.prefix, owner)
}

func createLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, base *LocalSnapshot, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	bucket = checkpoint.LocalBucket(bucket)
	a := &LocalArchive{bucket: bucket, prefix: prefix, Binding: binding}
	conditional, match := false, false
	for _, option := range bucket.SupportedObjectUploadOptions() {
		conditional = conditional || option == objstore.IfNotExists
		match = match || option == objstore.IfMatch
	}
	if !conditional || !match {
		return nil, fmt.Errorf("local backup requires conditional object writes")
	}
	if err := validateLocalBinding(binding); err != nil {
		return nil, err
	}
	if binding.Version != 1 || owner != nil {
		return nil, fmt.Errorf("Local archive profile and owner do not match")
	}
	if base != nil {
		if err := a.validateSnapshot(*base, true); err != nil {
			return nil, err
		}
	}
	data, err := json.Marshal(binding)
	if err != nil {
		return nil, err
	}
	if err := bucket.Upload(ctx, a.key("BINDING"), bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		return nil, err
	}
	a.head = localHead{Version: 1, BindingHash: sha256.Sum256(data), Origin: base, Snapshot: base}
	if base != nil {
		a.head.Tip, a.head.Prefix = base.Seal.Index, base.Seal.PrefixHash
	}
	if err := a.publish(ctx, a.head); err != nil {
		return nil, err
	}
	return OpenLocalArchive(ctx, bucket, prefix)
}

// ReadOnly is for startup restore validation while a LocalFilesystemOwner is
// held. It preserves the captured view and rejects every archive mutation.
func (a *LocalArchive) ReadOnly() *LocalArchive {
	return &LocalArchive{
		bucket: &readOnlyLocalArchiveBucket{Bucket: a.bucket}, prefix: a.prefix,
		Binding: a.Binding, head: a.head, headHash: a.headHash, version: a.version,
		extents: append([]Extent(nil), a.extents...), failure: a.failure,
	}
}

type readOnlyLocalArchiveBucket struct{ objstore.Bucket }

func (b *readOnlyLocalArchiveBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	return fmt.Errorf("Local startup archive validation is read-only")
}
func (b *readOnlyLocalArchiveBucket) Delete(context.Context, string) error {
	return fmt.Errorf("Local startup archive validation is read-only")
}

// RecoverMaintenanceControl clears only a v2 held token after the caller has
// completed owner-authorized staged validation and the exclusive owner remains held.
func (a *LocalArchive) RecoverMaintenanceControl(ctx context.Context, owner *internalobjstore.LocalFilesystemOwner) error {
	if a.Binding.Version != 2 || owner == nil || !owner.Held() {
		return fmt.Errorf("v2 Local recovery requires the filesystem owner")
	}
	return a.localManager().RecoverLocalMaintenanceControl(ctx, owner)
}

func (a *LocalArchive) MaintenanceResolved(ctx context.Context) (bool, error) {
	if a.Binding.Version != 2 {
		return true, nil
	}
	return a.localManager().LocalMaintenanceResolved(ctx)
}

type localMaintenanceControlV2 struct {
	Version      uint   `json:"version"`
	Token        string `json:"token"`
	LocationHash string `json:"location_hash"`
	BindingHash  string `json:"binding_hash"`
}

func (a *LocalArchive) validateLocalControl(ctx context.Context, binding []byte) error {
	key := a.key("local-gc-control")
	before, err := a.bucket.Attributes(ctx, key)
	if err != nil {
		return fmt.Errorf("v2 Local maintenance control missing: %w", err)
	}
	data, err := a.read(ctx, "local-gc-control", 4096)
	if err != nil {
		return err
	}
	after, err := a.bucket.Attributes(ctx, key)
	if err != nil || before.Version == nil || !sameObjectVersion(before.Version, after.Version) {
		return fmt.Errorf("v2 Local maintenance control changed during read")
	}
	var control localMaintenanceControlV2
	if err := strictLocalJSON(data, &control); err != nil || control.Version != 2 || control.LocationHash != a.Binding.ArchiveLocationHash || control.BindingHash != fmt.Sprintf("%x", sha256.Sum256(binding)) {
		return fmt.Errorf("invalid v2 Local maintenance control")
	}
	return nil
}

func (a *LocalArchive) Tip() quepaxa.Slot { a.mu.Lock(); defer a.mu.Unlock(); return a.head.Tip }
func (a *LocalArchive) Snapshot() *LocalSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.head.Snapshot
}
func (a *LocalArchive) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.prefixAt(slot)
}
func (a *LocalArchive) prefixAt(slot quepaxa.Slot) ([32]byte, bool) {
	if slot == 0 && a.head.Origin == nil {
		return [32]byte{}, true
	}
	if a.head.Origin != nil && slot == a.head.Origin.Seal.Index {
		return a.head.Origin.Seal.PrefixHash, true
	}
	for _, extent := range a.extents {
		if slot < extent.Start || slot > extent.End {
			continue
		}
		prefix := extent.StartPrefix
		for _, value := range extent.Decisions {
			prefix = quepaxa.AdvancePrefixHash(prefix, value.Slot, value.Hash)
			if value.Slot == slot {
				return prefix, true
			}
		}
	}
	return [32]byte{}, false
}
func (a *LocalArchive) DecisionsFrom(from quepaxa.Slot, limit int) []quepaxa.DecidedValue {
	a.mu.Lock()
	defer a.mu.Unlock()
	var result []quepaxa.DecidedValue
	for _, extent := range a.extents {
		for _, value := range extent.Decisions {
			if value.Slot >= from {
				result = append(result, value)
				if len(result) == limit {
					return result
				}
			}
		}
	}
	return result
}
func (a *LocalArchive) SyncThrough(ctx context.Context, core source, through quepaxa.Slot) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.localManager().WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { return a.syncThrough(ctx, core, through) })
}
func (a *LocalArchive) syncThrough(ctx context.Context, core source, through quepaxa.Slot) error {
	if err := a.checkLocalHead(ctx); err != nil {
		return err
	}
	if a.failure != nil {
		return a.failure
	}
	through = max(through, a.head.Tip)
	prefix, ok := core.PrefixHash(a.head.Tip)
	if a.head.Tip == 0 {
		prefix, ok = [32]byte{}, true
	}
	if !ok || prefix != a.head.Prefix {
		return fmt.Errorf("local backup prefix diverged or proof is unavailable")
	}
	builder := NewManager(a.bucket, a.prefix, a.Binding.Bootstrap.ConfigID)
	for a.head.Tip < through {
		previousObject := uint64(1)
		if a.head.Tail == ([32]byte{}) {
			previousObject = 0
		}
		extent, data, _, err := builder.buildExtent(core, a.head.Tip+1, through, a.head.Tail, previousObject)
		if err != nil {
			return err
		}
		for _, value := range extent.Decisions {
			seal, checkpoint, err := quepaxa.DecodeCheckpointSeal(value.Value)
			if err != nil {
				return err
			}
			if checkpoint {
				snapshot, err := a.ReadSnapshot(ctx, seal.RootHash)
				if err != nil {
					return fmt.Errorf("Local checkpoint artifact must precede its archived seal: %w", err)
				}
				if snapshot.Decision.Slot != value.Slot || snapshot.Decision.Hash != value.Hash || !bytes.Equal(snapshot.Decision.Value, value.Value) || !bytes.Equal(snapshot.Decision.Certificate, value.Certificate) {
					return fmt.Errorf("Local checkpoint artifact does not match archived decision")
				}
			}
		}
		hash := sha256.Sum256(data)
		name := fmt.Sprintf("extents/%x", hash)
		if err := a.bucket.Upload(ctx, a.key(name), bytes.NewReader(data), objstore.WithIfNotExists()); err != nil && !a.bucket.IsConditionNotMetErr(err) {
			return err
		}
		got, err := a.read(ctx, name, maxExtentSize)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, data) {
			return fmt.Errorf("local extent publication readback mismatch")
		}
		head := a.head
		head.Tip, head.Prefix, head.Tail = extent.End, extent.EndPrefix, hash
		if err := a.publish(ctx, head); err != nil {
			return err
		}
		a.extents = append(a.extents, extent)
	}
	return nil
}
func (a *LocalArchive) PublishSnapshot(ctx context.Context, snapshot LocalSnapshot) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.localManager().WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { return a.publishSnapshot(ctx, snapshot) })
}
func (a *LocalArchive) publishSnapshot(ctx context.Context, snapshot LocalSnapshot) error {
	if err := a.checkLocalHead(ctx); err != nil {
		return err
	}
	if a.failure != nil {
		return a.failure
	}
	if err := a.validateSnapshot(snapshot, false); err != nil {
		return err
	}
	if a.head.Version == 2 {
		if _, err := a.localManager().OpenRoot(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot); err != nil {
			return err
		}
	}
	prefix, ok := a.prefixAt(snapshot.Seal.Index)
	if !ok || prefix != snapshot.Seal.PrefixHash || snapshot.Decision.Slot > a.head.Tip {
		return fmt.Errorf("local snapshot is not archived")
	}
	head := a.head
	if head.Version == 1 {
		head.Version = 2
		if head.Snapshot != nil {
			head.Catalog = []LocalSnapshot{*head.Snapshot}
		}
	}
	if head.Version == 2 {
		found := false
		for _, old := range head.Catalog {
			if old.PortableRoot == snapshot.PortableRoot {
				found = true
			}
		}
		if !found {
			head.Catalog = append(append([]LocalSnapshot(nil), head.Catalog...), snapshot)
		}
	}
	head.Origin, head.Snapshot = &snapshot, &snapshot
	head.Tail = [32]byte{}
	// Re-encode only the retained suffix so its chain terminates at the new
	// checkpoint. Old immutable objects are unreachable; v1 does not delete them.
	suffix := localArchiveSuffix{tip: a.head.Tip, prefixes: map[quepaxa.Slot][32]byte{snapshot.Seal.Index: snapshot.Seal.PrefixHash}}
	p := snapshot.Seal.PrefixHash
	for _, extent := range a.extents {
		for _, value := range extent.Decisions {
			if value.Slot > snapshot.Seal.Index {
				suffix.values = append(suffix.values, value)
				p = quepaxa.AdvancePrefixHash(p, value.Slot, value.Hash)
				suffix.prefixes[value.Slot] = p
			}
		}
	}
	builder := NewManager(a.bucket, a.prefix, a.Binding.Bootstrap.ConfigID)
	var refs []Extent
	for from := snapshot.Seal.Index + 1; from <= a.head.Tip; {
		previousObject := uint64(1)
		if head.Tail == ([32]byte{}) {
			previousObject = 0
		}
		extent, data, _, err := builder.buildExtent(suffix, from, a.head.Tip, head.Tail, previousObject)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		name := fmt.Sprintf("extents/%x", hash)
		if err := a.bucket.Upload(ctx, a.key(name), bytes.NewReader(data), objstore.WithIfNotExists()); err != nil && !a.bucket.IsConditionNotMetErr(err) {
			return err
		}
		got, err := a.read(ctx, name, maxExtentSize)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, data) {
			return fmt.Errorf("Local rebase extent readback mismatch")
		}
		head.Tail = hash
		refs = append(refs, extent)
		from = extent.End + 1
	}
	if err := a.publish(ctx, head); err != nil {
		return err
	}
	a.extents = refs
	return nil
}

type localArchiveSuffix struct {
	values   []quepaxa.DecidedValue
	prefixes map[quepaxa.Slot][32]byte
	tip      quepaxa.Slot
}

func (s localArchiveSuffix) Tip() quepaxa.Slot { return s.tip }
func (s localArchiveSuffix) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	p, ok := s.prefixes[slot]
	return p, ok
}
func (s localArchiveSuffix) DecisionsFromBounded(from quepaxa.Slot, limit, bytesLimit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	var result []quepaxa.DecidedValue
	for _, value := range s.values {
		if value.Slot < from {
			continue
		}
		size := len(value.Value) + len(value.Certificate)
		if len(result) >= limit || size > bytesLimit {
			break
		}
		result = append(result, value)
		bytesLimit -= size
	}
	return result, s.tip, nil
}

// The initial transition changes only the protocol, so a retry never needs to
// reconstruct or adopt a partially published rebase.
func localV2Head(head localHead) localHead {
	head.Version = 2
	if head.Snapshot != nil {
		head.Catalog = []LocalSnapshot{*head.Snapshot}
	}
	return head
}

type localUpgradeIntent struct {
	Version           uint                       `json:"version"`
	BindingHash       [32]byte                   `json:"binding_hash"`
	SourceHash        [32]byte                   `json:"source_hash"`
	SourceVersion     []byte                     `json:"source_version"`
	SourceVersionType objstore.ObjectVersionType `json:"source_version_type"`
	TargetHash        [32]byte                   `json:"target_hash"`
}

// A literal 2 is the permanent fence (including archives written by old code).
// Only an exact, uncompleted transition permits a v1 HEAD to reopen.
func (a *LocalArchive) localProtocol(ctx context.Context) (bool, *objstore.ObjectVersion, error) {
	key := a.key("local-gc-protocol")
	before, err := a.bucket.Attributes(ctx, key)
	if a.bucket.IsObjNotFoundErr(err) {
		if a.head.Version == 2 {
			return false, nil, fmt.Errorf("Local GC protocol marker missing")
		}
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	data, err := a.read(ctx, "local-gc-protocol", 4096)
	if err != nil {
		return false, nil, err
	}
	after, err := a.bucket.Attributes(ctx, key)
	if err != nil {
		return false, nil, err
	}
	if !sameObjectVersion(before.Version, after.Version) {
		return false, nil, fmt.Errorf("Local GC protocol changed during read")
	}
	if bytes.Equal(data, []byte("2")) {
		if a.head.Version != 2 {
			return false, nil, fmt.Errorf("Local GC protocol downgrade")
		}
		return false, after.Version, nil
	}
	var intent localUpgradeIntent
	if err := strictLocalJSON(data, &intent); err != nil {
		return false, nil, err
	}
	if intent.Version != 2 || intent.BindingHash != a.head.BindingHash || len(intent.SourceVersion) == 0 || (intent.SourceVersionType != objstore.Generation && intent.SourceVersionType != objstore.ETag) || intent.SourceHash == ([32]byte{}) || intent.TargetHash == ([32]byte{}) {
		return false, nil, fmt.Errorf("invalid Local protocol transition")
	}
	if a.head.Version == 1 {
		target, _ := json.Marshal(localV2Head(a.head))
		if intent.SourceHash != a.headHash || intent.SourceVersionType != a.version.Type || !bytes.Equal(intent.SourceVersion, []byte(a.version.Value)) || intent.TargetHash != sha256.Sum256(target) {
			return false, nil, fmt.Errorf("Local GC protocol downgrade or mismatched transition")
		}
	} else if intent.TargetHash != a.headHash {
		return false, nil, fmt.Errorf("Local protocol transition target mismatch")
	}
	return true, after.Version, nil
}

func (a *LocalArchive) publish(ctx context.Context, head localHead) error {
	// Preserve the no-I/O rejection for an oversized requested publication.
	data, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if len(data) > maxLocalHeadSize {
		return fmt.Errorf("Local HEAD exceeds %d-byte limit", maxLocalHeadSize)
	}
	if a.version != nil {
		pending, markerVersion, err := a.localProtocol(ctx)
		if err != nil {
			return err
		}
		if a.head.Version == 2 && head.Version != 2 {
			return fmt.Errorf("Local GC protocol downgrade")
		}
		if a.head.Version == 1 && (head.Version == 2 || pending) {
			target := localV2Head(a.head)
			encoded, err := json.Marshal(target)
			if err != nil {
				return err
			}
			if len(encoded) > maxLocalHeadSize {
				return fmt.Errorf("Local protocol transition exceeds %d-byte HEAD limit", maxLocalHeadSize)
			}
			if !pending {
				intent, _ := json.Marshal(localUpgradeIntent{Version: 2, BindingHash: a.head.BindingHash, SourceHash: a.headHash, SourceVersion: []byte(a.version.Value), SourceVersionType: a.version.Type, TargetHash: sha256.Sum256(encoded)})
				if err := a.bucket.Upload(ctx, a.key("local-gc-protocol"), bytes.NewReader(intent), objstore.WithIfNotExists()); err != nil {
					return err
				}
				_, markerVersion, err = a.localProtocol(ctx)
				if err != nil {
					return err
				}
			}
			if err := a.publishHead(ctx, target); err != nil {
				return err
			}
			pending = true
			if head.Version == 1 {
				head = localV2Head(head)
			}
		}
		if pending {
			// Seal the intent before acknowledging completion or changing history.
			if err := a.bucket.Upload(ctx, a.key("local-gc-protocol"), bytes.NewReader([]byte("2")), objstore.WithIfMatch(markerVersion)); err != nil {
				return err
			}
		}
	}
	return a.publishHead(ctx, head)
}

func (a *LocalArchive) publishHead(ctx context.Context, head localHead) (resultErr error) {
	attempted := false
	defer func() {
		if resultErr != nil && attempted {
			a.failure = fmt.Errorf("Local HEAD publication uncertain; reopen required: %w", resultErr)
		}
	}()
	data, err := json.Marshal(head)
	if err != nil {
		return err
	}
	// Grace-protected and pinned roots remain queued in the catalog. Refuse
	// backlog growth before I/O rather than publishing an unreadable HEAD.
	if len(data) > maxLocalHeadSize {
		return fmt.Errorf("Local HEAD exceeds %d-byte limit; pending retention catalog must shrink before publication", maxLocalHeadSize)
	}
	option := objstore.WithIfNotExists()
	if a.version != nil {
		option = objstore.WithIfMatch(a.version)
	}
	attempted = true
	if err := a.bucket.Upload(ctx, a.key("HEAD"), bytes.NewReader(data), option); err != nil {
		if checkpoint.LocalBeforeDispatch(err) || a.bucket.IsConditionNotMetErr(err) {
			attempted = false
		}
		return err
	}
	readback, cancel := checkpoint.LocalReadbackContext(ctx)
	defer cancel()
	got, err := a.read(readback, "HEAD", maxLocalHeadSize)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, data) {
		return fmt.Errorf("local head publication readback mismatch")
	}
	attrs, err := a.bucket.Attributes(readback, a.key("HEAD"))
	if err != nil {
		return err
	}
	if attrs.Version == nil {
		return fmt.Errorf("local head version unavailable")
	}
	a.head, a.version = head, attrs.Version
	a.headHash = sha256.Sum256(data)
	return nil
}

func validateLocalBinding(binding LocalBinding) error {
	_, uuidErr := hex.DecodeString(binding.StoreUUID)
	_, digestErr := hex.DecodeString(binding.ConfigDigest)
	location, locationErr := hex.DecodeString(binding.ArchiveLocationHash)
	if (binding.Version != 1 && binding.Version != 2) || binding.Version == 1 && binding.ArchiveLocationHash != "" || binding.Version == 2 && (locationErr != nil || len(location) != 32 || hex.EncodeToString(location) != binding.ArchiveLocationHash) || binding.PolicyVersion != 1 || uuidErr != nil || digestErr != nil || len(binding.StoreUUID) != 32 || len(binding.ConfigDigest) != 64 || binding.Bootstrap.ConfigID == 0 || len(binding.Bootstrap.Members) != 1 || binding.Bootstrap.Members[0].ID == "" {
		return fmt.Errorf("invalid Local archive binding")
	}
	return nil
}

func (a *LocalArchive) validateSnapshot(snapshot LocalSnapshot, origin bool) error {
	encoded, err := quepaxa.EncodeCheckpointSeal(snapshot.Seal)
	if err != nil || !bytes.Equal(encoded, snapshot.Decision.Value) || snapshot.Decision.Hash != sha256.Sum256(encoded) || snapshot.Decision.Slot <= snapshot.Seal.Index || snapshot.PortableRoot == ([32]byte{}) || len(snapshot.Decision.Certificate) == 0 || len(snapshot.Descriptor) == 0 || len(snapshot.Descriptor) > 16<<10 || snapshot.Seal.ConfigID != a.Binding.Bootstrap.ConfigID {
		return fmt.Errorf("invalid Local snapshot authority")
	}
	if origin && a.head.Tip < snapshot.Decision.Slot {
		return nil
	}
	for _, extent := range a.extents {
		for _, value := range extent.Decisions {
			if value.Slot == snapshot.Decision.Slot {
				if value.Hash != snapshot.Decision.Hash || !bytes.Equal(value.Value, snapshot.Decision.Value) || !bytes.Equal(value.Certificate, snapshot.Decision.Certificate) {
					return fmt.Errorf("Local snapshot differs from archived seal decision")
				}
				return nil
			}
		}
	}
	return fmt.Errorf("Local snapshot seal decision is not archived")
}

// StoreLocalSnapshot publishes immutable descriptor-to-portable-root metadata
// before the certified seal can become visible in an archive extent.
func StoreLocalSnapshot(ctx context.Context, bucket objstore.Bucket, prefix string, snapshot LocalSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	key := path.Join(prefix, fmt.Sprintf("snapshots/%x.json", snapshot.Seal.RootHash))
	if err := bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists()); err != nil && !bucket.IsConditionNotMetErr(err) {
		return err
	}
	a := &LocalArchive{bucket: bucket, prefix: prefix}
	got, err := a.read(ctx, fmt.Sprintf("snapshots/%x.json", snapshot.Seal.RootHash), 4<<20)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, data) {
		return fmt.Errorf("Local snapshot publication mismatch")
	}
	return nil
}

func (a *LocalArchive) ReadSnapshot(ctx context.Context, root [32]byte) (LocalSnapshot, error) {
	data, err := a.read(ctx, fmt.Sprintf("snapshots/%x.json", root), 4<<20)
	if err != nil {
		return LocalSnapshot{}, err
	}
	var snapshot LocalSnapshot
	if err := strictLocalJSON(data, &snapshot); err != nil {
		return LocalSnapshot{}, err
	}
	if snapshot.Seal.RootHash != root {
		return LocalSnapshot{}, fmt.Errorf("Local snapshot root mismatch")
	}
	return snapshot, nil
}
func (a *LocalArchive) key(name string) string { return path.Join(a.prefix, name) }
func (a *LocalArchive) read(ctx context.Context, name string, limit int64) ([]byte, error) {
	r, err := a.bucket.Get(ctx, a.key(name))
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	closeErr := r.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("local archive object exceeds size bound")
	}
	return data, nil
}
func strictLocalJSON(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing local archive JSON")
	}
	return nil
}
