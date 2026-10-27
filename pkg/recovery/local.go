package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
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
	ArchiveLocationHash string          `json:"archive_location_hash"`
	StoreUUID           string          `json:"store_uuid"`
	ConfigDigest        string          `json:"config_digest"`
	PolicyVersion       uint            `json:"policy_version"`
	Bootstrap           quepaxa.Cluster `json:"bootstrap"`
}

const localArchiveFormatVersion = types.LocalArchiveFormatVersion

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

type localArchiveMaintenanceControl struct {
	Version      uint   `json:"version"`
	Token        string `json:"token"`
	LocationHash string `json:"location_hash"`
	BindingHash  string `json:"binding_hash"`
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
	readOnly bool
}

func OpenLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string) (*LocalArchive, error) {
	return openLocalArchive(ctx, bucket, prefix, nil)
}

// OpenLocalArchiveOwned opens a filesystem archive while its process-lifetime owner is held.
func OpenLocalArchiveOwned(ctx context.Context, bucket objstore.Bucket, prefix string, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	if owner == nil || !owner.Held() {
		return nil, fmt.Errorf("filesystem Local archive open requires its owner")
	}
	a, err := openLocalArchive(ctx, bucket, prefix, owner)
	if err != nil {
		return nil, err
	}
	if a.Binding.ArchiveLocationHash == "" || a.Binding.ArchiveLocationHash != owner.LocationHash(prefix) {
		return nil, fmt.Errorf("filesystem Local archive location differs from its owner")
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
	if a.Binding.ArchiveLocationHash != "" {
		if owner != nil {
			if !owner.Held() || a.Binding.ArchiveLocationHash != owner.LocationHash(prefix) {
				return nil, fmt.Errorf("filesystem Local archive requires its matching owner")
			}
		} else {
			a.readOnly = true
			a.bucket = &readOnlyLocalArchiveBucket{Bucket: a.bucket}
		}
	}
	if err := a.validateLocalControl(ctx, data); err != nil {
		return nil, err
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
	if a.head.Version != localArchiveFormatVersion || a.head.BindingHash != sha256.Sum256(data) {
		return fmt.Errorf("local archive identity mismatch")
	}
	if err := a.validateLocalCatalog(); err != nil {
		return err
	}
	a.version = attrs.Version
	a.headHash = sha256.Sum256(head)
	return nil
}

// CreateLocalArchive creates a remotely owned archive with conditional genesis writes. A
// partially published genesis is an error on later opens, never empty bootstrap.
func CreateLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, base *LocalSnapshot) (*LocalArchive, error) {
	genesis, err := PrepareLocalArchive(ctx, bucket, prefix, binding)
	if err != nil {
		return nil, err
	}
	return CreateLocalArchiveFromPrepared(ctx, genesis, base)
}

// CreateLocalArchiveOwned creates a filesystem archive while its owner is held.
func CreateLocalArchiveOwned(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, base *LocalSnapshot, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
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

// PrepareLocalArchive creates the current remote archive identity before its data objects.
func PrepareLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding) (*LocalArchiveGenesis, error) {
	return prepareLocalArchive(ctx, bucket, prefix, binding, nil)
}

// PrepareLocalArchiveOwned creates the same identity while a filesystem owner is held.
func PrepareLocalArchiveOwned(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchiveGenesis, error) {
	if owner == nil || !owner.Held() || binding.ArchiveLocationHash != owner.LocationHash(prefix) {
		return nil, fmt.Errorf("filesystem Local archive preparation requires its owner")
	}
	return prepareLocalArchive(ctx, bucket, prefix, binding, owner)
}

func prepareLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchiveGenesis, error) {
	if binding.Version != localArchiveFormatVersion || owner == nil && binding.ArchiveLocationHash != "" || owner != nil && (!owner.Held() || binding.ArchiveLocationHash != owner.LocationHash(prefix)) {
		return nil, fmt.Errorf("Local archive binding and owner do not match")
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
	control, err := json.Marshal(localArchiveMaintenanceControl{Version: localArchiveFormatVersion, LocationHash: binding.ArchiveLocationHash, BindingHash: hex.EncodeToString(bindingHash[:])})
	if err != nil {
		return nil, err
	}
	if err := bucket.Upload(ctx, a.key("local-gc-control"), bytes.NewReader(control), objstore.WithIfNotExists()); err != nil {
		return nil, err
	}
	return &LocalArchiveGenesis{owner: owner, bucket: bucket, prefix: prefix, binding: binding, bindingData: data}, nil
}

// CreateLocalArchiveFromPrepared publishes HEAD after its base data is uploaded and verified.
func CreateLocalArchiveFromPrepared(ctx context.Context, genesis *LocalArchiveGenesis, base *LocalSnapshot) (*LocalArchive, error) {
	if genesis == nil || genesis.owner != nil || genesis.binding.ArchiveLocationHash != "" {
		return nil, fmt.Errorf("remote Local archive genesis is invalid")
	}
	return createLocalArchiveFromPrepared(ctx, genesis, base, nil)
}

// CreateLocalArchiveFromPreparedOwned publishes HEAD only after the caller has
// uploaded and verified any base checkpoint beneath the already-created control.
func CreateLocalArchiveFromPreparedOwned(ctx context.Context, genesis *LocalArchiveGenesis, base *LocalSnapshot, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	if genesis == nil || owner == nil || genesis.owner != owner || !owner.Held() || genesis.binding.Version != localArchiveFormatVersion || genesis.binding.ArchiveLocationHash != owner.LocationHash(genesis.prefix) {
		return nil, fmt.Errorf("filesystem Local archive genesis is invalid")
	}
	return createLocalArchiveFromPrepared(ctx, genesis, base, owner)
}

func createLocalArchiveFromPrepared(ctx context.Context, genesis *LocalArchiveGenesis, base *LocalSnapshot, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error) {
	a := &LocalArchive{bucket: genesis.bucket, prefix: genesis.prefix, Binding: genesis.binding}
	if base != nil {
		if err := a.validateSnapshot(*base, true); err != nil {
			return nil, err
		}
	}
	binding, err := a.read(ctx, "BINDING", 64<<10)
	if err != nil || !bytes.Equal(binding, genesis.bindingData) {
		return nil, fmt.Errorf("Local archive binding changed during genesis")
	}
	if err := a.validateLocalControl(ctx, binding); err != nil {
		return nil, err
	}
	a.head = localHead{Version: localArchiveFormatVersion, BindingHash: sha256.Sum256(binding), Origin: base, Snapshot: base, Catalog: []LocalSnapshot{}}
	if base != nil {
		a.head.Catalog = append(a.head.Catalog, *base)
	}
	if base != nil {
		a.head.Tip, a.head.Prefix = base.Seal.Index, base.Seal.PrefixHash
	}
	if err := a.publish(ctx, a.head); err != nil {
		return nil, err
	}
	if owner != nil {
		return OpenLocalArchiveOwned(ctx, a.bucket, a.prefix, owner)
	}
	return OpenLocalArchive(ctx, a.bucket, a.prefix)
}

// ReadOnly preserves the captured archive view and rejects every mutation.
func (a *LocalArchive) ReadOnly() *LocalArchive {
	return &LocalArchive{
		bucket: &readOnlyLocalArchiveBucket{Bucket: a.bucket}, prefix: a.prefix,
		Binding: a.Binding, head: a.head, headHash: a.headHash, version: a.version,
		extents: append([]Extent(nil), a.extents...), failure: a.failure, readOnly: true,
	}
}

func (a *LocalArchive) requireWritable() error {
	if a.readOnly {
		return fmt.Errorf("Local archive inspection is read-only without filesystem ownership")
	}
	return nil
}

type readOnlyLocalArchiveBucket struct{ objstore.Bucket }

func (b *readOnlyLocalArchiveBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	return fmt.Errorf("Local archive inspection is read-only")
}
func (b *readOnlyLocalArchiveBucket) Delete(context.Context, string) error {
	return fmt.Errorf("Local archive inspection is read-only")
}

// RecoverMaintenanceControl clears a held token only after the caller has
// completed owner-authorized staged validation and the exclusive owner remains held.
func (a *LocalArchive) RecoverMaintenanceControl(ctx context.Context, owner *internalobjstore.LocalFilesystemOwner) error {
	if err := a.requireWritable(); err != nil {
		return err
	}
	if a.Binding.ArchiveLocationHash == "" || owner == nil || !owner.Held() || a.Binding.ArchiveLocationHash != owner.LocationHash(a.prefix) {
		return fmt.Errorf("Local recovery requires the matching filesystem owner")
	}
	return a.localManager().RecoverLocalMaintenanceControl(ctx, owner)
}

func (a *LocalArchive) MaintenanceResolved(ctx context.Context) (bool, error) {
	return a.localManager().LocalMaintenanceResolved(ctx)
}

func (a *LocalArchive) validateLocalControl(ctx context.Context, binding []byte) error {
	key := a.key("local-gc-control")
	before, err := a.bucket.Attributes(ctx, key)
	if err != nil {
		return fmt.Errorf("Local maintenance control missing: %w", err)
	}
	data, err := a.read(ctx, "local-gc-control", 4096)
	if err != nil {
		return err
	}
	after, err := a.bucket.Attributes(ctx, key)
	if err != nil || before.Version == nil || !sameObjectVersion(before.Version, after.Version) {
		return fmt.Errorf("Local maintenance control changed during read")
	}
	var control localArchiveMaintenanceControl
	if err := strictLocalJSON(data, &control); err != nil || control.Version != localArchiveFormatVersion || control.LocationHash != a.Binding.ArchiveLocationHash || control.BindingHash != fmt.Sprintf("%x", sha256.Sum256(binding)) {
		return fmt.Errorf("invalid Local maintenance control")
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
	if err := a.requireWritable(); err != nil {
		return err
	}
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
	if err := a.requireWritable(); err != nil {
		return err
	}
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
	if _, err := a.localManager().OpenRoot(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot); err != nil {
		return err
	}
	prefix, ok := a.prefixAt(snapshot.Seal.Index)
	if !ok || prefix != snapshot.Seal.PrefixHash || snapshot.Decision.Slot > a.head.Tip {
		return fmt.Errorf("local snapshot is not archived")
	}
	head := a.head
	found := false
	for _, old := range head.Catalog {
		if old.PortableRoot == snapshot.PortableRoot {
			found = true
		}
	}
	if !found {
		head.Catalog = append(append([]LocalSnapshot(nil), head.Catalog...), snapshot)
	}
	head.Origin, head.Snapshot = &snapshot, &snapshot
	head.Tail = [32]byte{}
	// Re-encode only the retained suffix so its chain terminates at the new
	// checkpoint. Old immutable objects are unreachable and remain untouched.
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

func (a *LocalArchive) publish(ctx context.Context, head localHead) error {
	// Preserve the no-I/O rejection for an oversized requested publication.
	data, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if len(data) > maxLocalHeadSize {
		return fmt.Errorf("Local HEAD exceeds %d-byte limit", maxLocalHeadSize)
	}
	if head.Version != localArchiveFormatVersion {
		return fmt.Errorf("invalid Local archive format version")
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
	if binding.Version != localArchiveFormatVersion || binding.ArchiveLocationHash != "" && (locationErr != nil || len(location) != 32 || hex.EncodeToString(location) != binding.ArchiveLocationHash) || binding.PolicyVersion != 1 || uuidErr != nil || digestErr != nil || len(binding.StoreUUID) != 32 || len(binding.ConfigDigest) != 64 || binding.Bootstrap.ConfigID == 0 || len(binding.Bootstrap.Members) != 1 || binding.Bootstrap.Members[0].ID == "" {
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
