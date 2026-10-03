package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type countingBucket struct {
	objstore.Bucket
	heads       atomic.Uint64
	markerHeads atomic.Uint64
	gets        atomic.Uint64
	blockGets   atomic.Uint64
	puts        atomic.Uint64
}

type blockingUploadBucket struct {
	objstore.Bucket
	started chan struct{}
}

type failArchiveHeadBucket struct {
	objstore.Bucket
}

func (b *failArchiveHeadBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "archive/head.bin") {
		return errors.New("injected archive head upload failure")
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

type blockingGetBucket struct {
	objstore.Bucket
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
}

type racingHeadBucket struct {
	objstore.Bucket
	once sync.Once
	run  func()
}

type failPostCASBucket struct {
	objstore.Bucket
	armed atomic.Bool
	fail  atomic.Bool
}

type racingRecoveryPinBucket struct {
	objstore.Bucket
	armed atomic.Bool
	run   func()
}

type blockingDeleteBucket struct {
	objstore.Bucket
	name    string
	started chan struct{}
	release chan struct{}
	once    atomic.Bool
}

type blockingHeadUploadBucket struct {
	objstore.Bucket
	armed   atomic.Bool
	started chan struct{}
	release chan struct{}
}

var errArchiveTestCondition = errors.New("injected archive conditional conflict")

type archiveExtentOutcomeBucket struct {
	objstore.Bucket
	armed            atomic.Bool
	uploadErr        error
	persistBeforeErr bool
	cancelOnGet      context.CancelFunc
	uploads          atomic.Uint64
	gets             atomic.Uint64
}

func (b *archiveExtentOutcomeBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.Contains(name, "/archive/blocks/") && b.armed.CompareAndSwap(true, false) {
		b.uploads.Add(1)
		if b.persistBeforeErr {
			if err := b.Bucket.Upload(ctx, name, r, options...); err != nil {
				return err
			}
		}
		if b.uploadErr != nil {
			return b.uploadErr
		}
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func (b *archiveExtentOutcomeBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
	if b.cancelOnGet != nil {
		b.cancelOnGet()
	}
	return b.Bucket.Get(ctx, name)
}

func (b *archiveExtentOutcomeBucket) IsConditionNotMetErr(err error) bool {
	return errors.Is(err, errArchiveTestCondition) || b.Bucket.IsConditionNotMetErr(err)
}

func (b *blockingHeadUploadBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "archive/head.bin") && b.armed.CompareAndSwap(true, false) {
		close(b.started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func (b *blockingDeleteBucket) Delete(ctx context.Context, name string) error {
	if name == b.name && b.once.CompareAndSwap(false, true) {
		close(b.started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *racingRecoveryPinBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	r, err := b.Bucket.Get(ctx, name)
	if err == nil && strings.Contains(name, "/archive/recovery-pins/") && b.armed.CompareAndSwap(true, false) {
		b.run()
	}
	return r, err
}

func (b *failPostCASBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	err := b.Bucket.Upload(ctx, name, r, options...)
	if err == nil && b.armed.Load() && strings.HasSuffix(name, "archive/head.bin") {
		b.fail.Store(true)
	}
	return err
}

func (b *failPostCASBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.fail.CompareAndSwap(true, false) && strings.HasSuffix(name, "archive/head.bin") {
		return objstore.ObjectAttributes{}, errors.New("injected post-CAS attributes failure")
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *racingHeadBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.HasSuffix(name, "archive/head.bin") {
		b.once.Do(b.run)
	}
	return b.Bucket.Get(ctx, name)
}

func (b *blockingGetBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.block.CompareAndSwap(true, false) {
		close(b.started)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Get(ctx, name)
}

func (b *blockingUploadBucket) Upload(ctx context.Context, _ string, _ io.Reader, _ ...objstore.ObjectUploadOption) error {
	select {
	case <-b.started:
	default:
		close(b.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (b *countingBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	b.heads.Add(1)
	if strings.Contains(name, "/archive/gc-candidates/") {
		b.markerHeads.Add(1)
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
	if strings.Contains(name, "/archive/blocks/") {
		b.blockGets.Add(1)
	}
	return b.Bucket.Get(ctx, name)
}

func (b *countingBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	b.puts.Add(1)
	return b.Bucket.Upload(ctx, name, r, options...)
}

func TestSharedArchiveRoundTripUsesBoundedExtents(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxExtentItems+3; i++ {
		if _, _, err := core.Propose(ctx, bytes.Repeat([]byte{byte(i + 1)}, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	bucket := objstore.NewInMemBucket()
	writer := NewManager(bucket, "cluster", 1)
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	for _, extent := range reader.extents {
		if extent.Decisions != nil {
			t.Fatal("archive load retained decision payloads")
		}
	}
	values, tip, err := reader.DecisionsFrom(ctx, 1, int(core.Tip()))
	if err != nil || tip != core.Tip() || len(values) != int(core.Tip()) {
		t.Fatalf("archive tip=%d values=%d err=%v", tip, len(values), err)
	}
	if len(reader.cache) > maxCachedExtents {
		t.Fatalf("archive cache=%d, limit=%d", len(reader.cache), maxCachedExtents)
	}
}

func TestCompactExtentsFlushesAtCountAndByteLimits(t *testing.T) {
	for _, test := range []struct {
		count, valueSize, wantExtents int
	}{{1023, 1, 1}, {1024, 1, 1}, {1025, 1, 2}, {2048, 1, 2}, {1024, 8 << 10, 2}} {
		name := fmt.Sprintf("count_%d_value_%d", test.count, test.valueSize)
		t.Run(name, func(t *testing.T) {
			assertCompactedDecisions(t, test.count, test.valueSize, test.wantExtents)
		})
	}
}

func assertCompactedDecisions(t *testing.T, count, valueSize, wantExtents int) {
	t.Helper()
	manager := NewManager(objstore.NewInMemBucket(), "compact-test", 1)
	defer manager.Close()
	refs := make([]Extent, 0, (count+maxExtentItems-1)/maxExtentItems)
	decisions := make([]quepaxa.DecidedValue, 0, count)
	var prefix [32]byte
	for start := 0; start < count; {
		end := min(start+maxExtentItems/2, count)
		extent := Extent{ConfigID: 1, Start: quepaxa.Slot(start + 1), StartPrefix: prefix}
		for i := start; i < end; i++ {
			value := bytes.Repeat([]byte{byte(i%251 + 1)}, valueSize)
			decision := quepaxa.DecidedValue{Slot: quepaxa.Slot(i + 1), Value: value, Hash: sha256.Sum256(value), Certificate: []byte{byte(i%251 + 1)}}
			extent.Decisions = append(extent.Decisions, decision)
			decisions = append(decisions, decision)
			prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
		}
		extent.End, extent.EndPrefix = quepaxa.Slot(end), prefix
		extent.hash, extent.object = sha256.Sum256([]byte(fmt.Sprint(start))), uint64(len(refs)+1)
		ref := extent
		ref.Decisions = nil
		refs = append(refs, ref)
		manager.cache[extentObject{hash: extent.hash, id: extent.object}] = extent
		start = end
	}
	compacted, err := manager.compactExtents(context.Background(), refs, [32]byte{})
	if err != nil {
		t.Fatalf("compact %d decisions: %v", count, err)
	}
	if len(compacted) != wantExtents {
		t.Fatalf("compacted extents=%d, want %d", len(compacted), wantExtents)
	}
	var got []quepaxa.DecidedValue
	var lastPrefix [32]byte
	var nextSlot quepaxa.Slot = 1
	for _, extent := range compacted {
		if len(extent.Decisions) > maxExtentItems {
			t.Fatalf("extent has %d decisions, limit %d", len(extent.Decisions), maxExtentItems)
		}
		encoded, err := encodeExtent(extent)
		if err != nil || len(encoded) > maxExtentSize {
			t.Fatalf("extent %d-%d encoded bytes=%d err=%v", extent.Start, extent.End, len(encoded), err)
		}
		if extent.StartPrefix != lastPrefix {
			t.Fatalf("extent starts at discontinuous prefix for slot %d", extent.Start)
		}
		if extent.Start != nextSlot || extent.End != extent.Start+quepaxa.Slot(len(extent.Decisions))-1 {
			t.Fatalf("extent has discontinuous slot range %d-%d", extent.Start, extent.End)
		}
		for _, decision := range extent.Decisions {
			got = append(got, decision)
			lastPrefix = quepaxa.AdvancePrefixHash(lastPrefix, decision.Slot, decision.Hash)
		}
		if extent.EndPrefix != lastPrefix {
			t.Fatalf("extent ending at slot %d (%d decisions) has wrong prefix: got %x want %x", extent.End, len(extent.Decisions), extent.EndPrefix, lastPrefix)
		}
		nextSlot = extent.End + 1
	}
	if len(got) != len(decisions) {
		t.Fatalf("compacted decisions=%d, want %d", len(got), len(decisions))
	}
	for i := range decisions {
		if got[i].Slot != decisions[i].Slot || got[i].Hash != decisions[i].Hash || !bytes.Equal(got[i].Value, decisions[i].Value) || !bytes.Equal(got[i].Certificate, decisions[i].Certificate) {
			t.Fatalf("decision %d or certificate changed", i+1)
		}
	}
}

func TestRecoverySnapshotPinsUncompactedArchiveHead(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("uncompacted")); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := manager.BeginRecoverySnapshot(ctx, "cold-start", time.Minute)
	if err != nil {
		t.Fatalf("pin uncompacted archive: %v", err)
	}
	defer snapshot.Close(ctx)
	if _, _, ok := snapshot.RecoveryBase(); ok {
		t.Fatal("uncompacted archive unexpectedly has a checkpoint base")
	}
	values, tip, err := snapshot.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != core.Tip() || len(values) != 1 {
		t.Fatalf("snapshot values=%d tip=%d err=%v", len(values), tip, err)
	}
}

func TestSyncThroughCoalescesAlreadyDecidedSuffix(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{
		NodeID:  "n1",
		Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}},
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	if err := manager.SyncThrough(ctx, core, 1); err != nil {
		t.Fatal(err)
	}
	if tip := manager.Tip(); tip != core.Tip() {
		t.Fatalf("archive tip=%d, want decided tip=%d", tip, core.Tip())
	}
	if puts := bucket.puts.Load(); puts != 2 {
		t.Fatalf("object uploads=%d, want one extent and one head", puts)
	}
}

func TestRecoveryPinGCDoesNotDeleteConcurrentRenewal(t *testing.T) {
	ctx := context.Background()
	bucket := &racingRecoveryPinBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	key := manager.recoveryPinKey("recovery")
	expired := archiveRecoveryPin{OwnerID: "recovery", Token: "token", Base: 1, Tip: 2, TailHash: [32]byte{1}, TailObject: 7, LeaseUntilMS: time.Now().Add(-time.Second).UnixMilli()}
	if err := manager.writeRecoveryPin(ctx, key, expired, objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	stale, err := manager.readRecoveryPin(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	bucket.run = func() {
		renewed := *stale
		renewed.LeaseUntilMS = time.Now().Add(time.Minute).UnixMilli()
		if err := manager.writeRecoveryPin(ctx, key, renewed, objstore.WithIfMatch(stale.version)); err != nil {
			t.Errorf("inject renewal: %v", err)
		}
	}
	bucket.armed.Store(true)
	if generation, err := manager.maxActiveRecoveryGeneration(ctx); err != nil || generation != 7 {
		t.Fatalf("renewed generation is not protected: generation=%d err=%v", generation, err)
	}
	current, err := manager.readRecoveryPin(ctx, key)
	if err != nil || current.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatalf("renewed pin was lost: pin=%+v err=%v", current, err)
	}
}

func TestRecoveryPinGCKeepsGenerationWithoutReadingExtentChain(t *testing.T) {
	ctx := context.Background()
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	pin := archiveRecoveryPin{
		OwnerID:      "recovery",
		Token:        "token",
		Base:         1,
		Tip:          2,
		TailHash:     [32]byte{1},
		TailObject:   7,
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	if err := manager.writeRecoveryPin(ctx, manager.recoveryPinKey(pin.OwnerID), pin, objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	bucket.blockGets.Store(0)
	generation, err := manager.maxActiveRecoveryGeneration(ctx)
	if err != nil || generation != pin.TailObject {
		t.Fatalf("pinned generation=%d err=%v", generation, err)
	}
	if gets := bucket.blockGets.Load(); gets != 0 {
		t.Fatalf("recovery pin GC read %d extent blocks, want 0", gets)
	}
}

func TestExpiredRecoveryPinIsTombstonedOnce(t *testing.T) {
	ctx := context.Background()
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	pin := archiveRecoveryPin{OwnerID: "recovery", Token: "token", Base: 1, Tip: 1, LeaseUntilMS: time.Now().Add(-time.Second).UnixMilli()}
	if err := manager.writeRecoveryPin(ctx, manager.recoveryPinKey(pin.OwnerID), pin, objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	bucket.puts.Store(0)
	if _, err := manager.maxActiveRecoveryGeneration(ctx); err != nil {
		t.Fatal(err)
	}
	if puts := bucket.puts.Load(); puts != 1 {
		t.Fatalf("first expired pin scan PUTs=%d, want 1", puts)
	}
	bucket.puts.Store(0)
	if _, err := manager.maxActiveRecoveryGeneration(ctx); err != nil {
		t.Fatal(err)
	}
	if active, err := manager.hasActiveRecoveryPins(ctx); err != nil || active {
		t.Fatalf("tombstoned pin active=%v err=%v", active, err)
	}
	if puts := bucket.puts.Load(); puts != 0 {
		t.Fatalf("repeated tombstone scan PUTs=%d, want 0", puts)
	}
}

func TestArchiveCloseCancelsAndWaitsForFlush(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("value")); err != nil {
		t.Fatal(err)
	}
	bucket := &blockingUploadBucket{Bucket: objstore.NewInMemBucket(), started: make(chan struct{})}
	manager := NewManager(bucket, "cluster", 1)
	done := make(chan error, 1)
	go func() { done <- manager.SyncThrough(ctx, core, core.Tip()) }()
	<-bucket.started
	manager.Close()
	if err := <-done; !errors.Is(err, context.Canceled) && !errors.Is(err, ErrArchiveClosed) {
		t.Fatalf("flush error=%v", err)
	}
	if err := manager.SyncThrough(ctx, core, core.Tip()); !errors.Is(err, ErrArchiveClosed) {
		t.Fatalf("sync after close error=%v", err)
	}
}

func TestArchiveReadIODoesNotBlockPublication(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &blockingGetBucket{Bucket: objstore.NewInMemBucket(), started: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	for i := 0; i < maxCachedExtents+1; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	bucket.block.Store(true)
	readDone := make(chan error, 1)
	go func() {
		_, _, err := manager.DecisionsFrom(ctx, 1, 1)
		readDone <- err
	}()
	<-bucket.started
	if _, _, err := core.Propose(ctx, []byte("next")); err != nil {
		t.Fatal(err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := manager.SyncThrough(writeCtx, core, core.Tip()); err != nil {
		t.Fatalf("publication waited for archive read: %v", err)
	}
	close(bucket.release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
}

func TestArchiveCleanupIODoesNotBlockPublication(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &blockingGetBucket{Bucket: objstore.NewInMemBucket(), started: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	for i := 0; i < maxCachedExtents+1; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	bucket.block.Store(true)
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- manager.Cleanup(ctx, time.Hour) }()
	<-bucket.started
	if _, _, err := core.Propose(ctx, []byte("next")); err != nil {
		t.Fatal(err)
	}
	writeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := manager.SyncThrough(writeCtx, core, core.Tip()); err != nil {
		t.Fatalf("publication waited for archive cleanup: %v", err)
	}
	close(bucket.release)
	if err := <-cleanupDone; err != nil {
		t.Fatal(err)
	}
}

func TestArchiveExtentSplitUsesEncodedSize(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	for range 72 {
		value := make([]byte, 120<<10)
		if _, err := rng.Read(value); err != nil {
			t.Fatal(err)
		}
		if _, _, err := core.Propose(ctx, value); err != nil {
			t.Fatal(err)
		}
	}
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if len(manager.extents) < 2 {
		t.Fatal("encoded archive payload was not split")
	}
	for _, ref := range manager.extents {
		extent, err := manager.extentForRef(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		data, err := encodeExtent(extent)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > maxExtentSize {
			t.Fatalf("encoded extent size=%d, limit=%d", len(data), maxExtentSize)
		}
	}
	ref := manager.extents[0]
	full, err := manager.extentForRef(ctx, ref)
	if err != nil || len(full.Decisions) < 2 {
		t.Fatalf("cached extent is unavailable: decisions=%d err=%v", len(full.Decisions), err)
	}
	partial := ref
	partial.Start++
	partial.StartPrefix = full.prefixes[1]
	if _, err := manager.extentForRef(ctx, partial); err != nil {
		t.Fatalf("valid cached extent suffix: %v", err)
	}
	partial.StartPrefix[0] ^= 1
	if _, err := manager.extentForRef(ctx, partial); err == nil {
		t.Fatal("cached extent accepted an invalid suffix prefix")
	}
}

func TestArchiveCodecRejectsJSONAndTrailingData(t *testing.T) {
	if _, err := decodeHead([]byte(`{"version":2}`)); err == nil {
		t.Fatal("accepted JSON archive head")
	}
	data, err := encodeHead(archiveHead{ConfigID: 1, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeHead(append(data, 0)); err == nil {
		t.Fatal("accepted trailing archive head data")
	}
}

func TestArchivePayloadAndHeadBoundaries(t *testing.T) {
	value, err := quepaxa.EncodeCheckpointSeal(quepaxa.CheckpointSeal{
		ConfigID: 1, Index: 2, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: [32]byte{3}, NextLeaderOrder: []quepaxa.NodeID{"n1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	decision := quepaxa.DecidedValue{Slot: 3, Hash: sha256.Sum256(value), Value: value, Certificate: make([]byte, maxExtentPayload-len(value))}
	seal, _, err := quepaxa.DecodeCheckpointSeal(value)
	if err != nil {
		t.Fatal(err)
	}
	head := archiveHead{
		ConfigID: 1, Generation: 1, Base: seal.Index, BasePrefix: seal.PrefixHash,
		BaseSeal: &seal, BaseDecision: &decision, Tip: seal.Index,
		LineageAnchor: &archiveAnchorRef{Hash: [32]byte{4}},
	}
	data, err := encodeHead(head)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != maxArchiveHeadSize {
		t.Fatalf("head size=%d, want exact max %d", len(data), maxArchiveHeadSize)
	}
	got, err := decodeHead(data)
	if err != nil || !archiveHeadsEqual(got, head) {
		t.Fatalf("decode max head: equal=%v err=%v", archiveHeadsEqual(got, head), err)
	}
	if err := validateArchivePayload(value, decision.Certificate[:len(decision.Certificate)-1]); err != nil {
		t.Fatalf("payload one byte under budget: %v", err)
	}
	if err := validateArchivePayload(value, append(append([]byte(nil), decision.Certificate...), 0)); err == nil {
		t.Fatal("accepted payload one byte over budget")
	}
	if _, err := encodeBaseDecision(quepaxa.DecidedValue{Value: make([]byte, quepaxa.MaxReplicatedValueBytes+1), Certificate: []byte{1}}); err == nil {
		t.Fatal("accepted value over replicated-value limit")
	}

	extent := Extent{
		ConfigID: 1, Start: 1, End: 1,
		Decisions: []quepaxa.DecidedValue{{Slot: 1, Value: []byte("x"), Hash: sha256.Sum256([]byte("x")), Certificate: make([]byte, maxExtentPayload-1)}},
	}
	extent.EndPrefix = quepaxa.AdvancePrefixHash([32]byte{}, 1, extent.Decisions[0].Hash)
	extentData, err := encodeExtent(extent)
	if err != nil || len(extentData) != maxExtentSize {
		t.Fatalf("exact extent boundary size=%d err=%v", len(extentData), err)
	}
	extent.Decisions[0].Certificate = append(extent.Decisions[0].Certificate, 0)
	if _, err := encodeExtent(extent); err == nil {
		t.Fatal("accepted extent payload one byte over budget")
	}
}

func TestArchiveHeadV2StrictnessAndProofIdentity(t *testing.T) {
	value, err := quepaxa.EncodeCheckpointSeal(quepaxa.CheckpointSeal{
		ConfigID: 1, Index: 1, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: [32]byte{3}, NextLeaderOrder: []quepaxa.NodeID{"n1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	seal, _, err := quepaxa.DecodeCheckpointSeal(value)
	if err != nil {
		t.Fatal(err)
	}
	decision := quepaxa.DecidedValue{Slot: 2, Hash: sha256.Sum256(value), Value: value, Certificate: []byte("proof-a")}
	head := archiveHead{ConfigID: 1, Generation: 1, Base: 1, BasePrefix: seal.PrefixHash, BaseSeal: &seal, BaseDecision: &decision, Tip: 1}
	data, err := encodeHead(head)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeHead(data); err != nil || !archiveHeadsEqual(got, head) {
		t.Fatalf("round trip: equal=%v err=%v", archiveHeadsEqual(got, head), err)
	}
	oldVersion := append([]byte(nil), data...)
	copy(oldVersion[:8], []byte("RHZAHEAD"))
	if _, err := decodeHead(oldVersion); err == nil {
		t.Fatal("accepted old archive-head magic")
	}
	truncated := append([]byte(nil), data[:len(data)-1]...)
	if _, err := decodeHead(truncated); err == nil {
		t.Fatal("accepted truncated archive head")
	}
	badFlags := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(badFlags[12:16], binary.BigEndian.Uint32(badFlags[12:16])|1<<31)
	binary.BigEndian.PutUint32(badFlags[len(badFlags)-archiveCRCSize:], crc32.Checksum(badFlags[:len(badFlags)-archiveCRCSize], archiveCRCTable))
	if _, err := decodeHead(badFlags); err == nil {
		t.Fatal("accepted reserved archive-head flag")
	}
	badCRC := append([]byte(nil), data...)
	badCRC[len(badCRC)-1] ^= 1
	if _, err := decodeHead(badCRC); err == nil {
		t.Fatal("accepted archive head with invalid CRC")
	}
	otherProof := head
	otherDecision := decision
	otherDecision.Certificate = []byte("proof-b")
	otherProof.BaseDecision = &otherDecision
	if archiveHeadsEqual(head, otherProof) || archiveBaseEqual(head, otherProof) {
		t.Fatal("treated different base proof bytes as equal")
	}
	otherBase := head
	otherBase.Generation++
	otherBase.Tip++
	otherBase.TailHash, otherBase.TailObject = [32]byte{5}, 1
	if !archiveBaseEqual(head, otherBase) {
		t.Fatal("base comparison included mutable tip metadata")
	}
}

func TestArchiveTrimRejectsOversizePayloadWithoutAdvancing(t *testing.T) {
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	defer manager.Close()
	before := manager.head
	decision := quepaxa.DecidedValue{Value: []byte("x"), Certificate: make([]byte, maxExtentPayload)}
	err := manager.trimThrough(context.Background(), quepaxa.SealedCheckpoint{}, decision)
	if err == nil {
		t.Fatal("accepted oversized trim decision")
	}
	if !archiveHeadsEqual(before, manager.head) || manager.tip != 0 || len(manager.extents) != 0 {
		t.Fatal("oversized trim advanced archive state")
	}
}

func TestArchiveExtentCodecIsCanonicalAndStrict(t *testing.T) {
	value := []byte("value")
	hash := sha256.Sum256(value)
	endPrefix := quepaxa.AdvancePrefixHash([32]byte{}, 1, hash)
	extent := Extent{ConfigID: 7, Start: 1, End: 1, EndPrefix: endPrefix, Decisions: []quepaxa.DecidedValue{{Slot: 1, Hash: hash, Value: value, Certificate: []byte("certificate")}}}
	first, err := encodeExtent(extent)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeExtent(extent)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("canonical encode mismatch err=%v", err)
	}
	decoded, err := decodeExtent(first)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ConfigID != extent.ConfigID || decoded.Start != 1 || decoded.End != 1 || len(decoded.Decisions) != 1 || decoded.Decisions[0].Hash != hash {
		t.Fatalf("decoded extent=%#v", decoded)
	}
	for _, invalid := range [][]byte{append(append([]byte(nil), first...), 0), func() []byte { b := append([]byte(nil), first...); b[12] = 1; return b }(), func() []byte { b := append([]byte(nil), first...); b[len(b)-1] ^= 1; return b }()} {
		if _, err := decodeExtent(invalid); err == nil {
			t.Fatal("accepted non-canonical archive extent")
		}
	}
}

func TestArchiveCleanupCompactsCardinality(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	for i := 0; i < 8; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	if len(manager.extents) != 8 {
		t.Fatalf("extents before cleanup=%d, want 8", len(manager.extents))
	}
	if err := manager.Cleanup(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if heads := bucket.markerHeads.Load(); heads != 0 {
		t.Fatalf("archive cleanup issued %d per-object marker HEADs", heads)
	}
	count := func(prefix string) int {
		t.Helper()
		total := 0
		if err := bucket.Iter(ctx, prefix, func(string) error { total++; return nil }); err != nil {
			t.Fatal(err)
		}
		return total
	}
	if blocks, markers := count("cluster/archive/blocks"), count("cluster/archive/gc-candidates"); blocks <= 1 || markers == 0 {
		t.Fatalf("first GC pass blocks=%d markers=%d, want retained old blocks and markers", blocks, markers)
	}
	if err := manager.Cleanup(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if blocks := count("cluster/archive/blocks"); blocks != 1 {
		t.Fatalf("second GC pass retained %d blocks, want 1", blocks)
	}
	if err := manager.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if len(manager.extents) != 1 {
		t.Fatalf("extents after cleanup=%d, want 1", len(manager.extents))
	}
}

func TestArchiveCleanupRemovesOrphanMarker(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", 1)
	orphan := manager.gcMarkerKey("cluster/archive/blocks/missing.bin")
	if err := bucket.Upload(ctx, orphan, bytes.NewReader([]byte("cluster/archive/blocks/missing.bin"))); err != nil {
		t.Fatal(err)
	}
	if err := manager.Cleanup(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if exists, err := bucket.Exists(ctx, orphan); err != nil || exists {
		t.Fatalf("orphan marker exists=%t err=%v", exists, err)
	}
}

func TestStaleArchiveCleanupCannotDeleteRepublishedExtent(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("value")); err != nil {
		t.Fatal(err)
	}
	base := objstore.NewInMemBucket()
	builder := NewManager(base, "cluster", 1)
	extent, data, _, err := builder.buildExtent(core, 1, 1, [32]byte{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	name := "cluster/" + extentKey(extent.hash)
	if err := base.Upload(ctx, name, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	bucket := &blockingDeleteBucket{Bucket: base, name: name, started: make(chan struct{}), release: make(chan struct{})}
	gc := NewManager(bucket, "cluster", 1)
	if err := bucket.Upload(ctx, gc.gcMarkerKey(name), bytes.NewReader([]byte(name)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	gcDone := make(chan error, 1)
	go func() { gcDone <- gc.Cleanup(ctx, 0) }()
	<-bucket.started
	stale, err := gc.readGCLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stale.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
	if err := gc.writeGCLock(ctx, *stale, objstore.WithIfMatch(stale.version)); err != nil {
		t.Fatal(err)
	}
	publisher := NewManager(bucket, "cluster", 1)
	successor, err := publisher.acquireGCLock(ctx, "successor", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.SyncThrough(ctx, core, 1); err != nil {
		t.Fatal(err)
	}
	if publisher.head.TailObject == 0 {
		t.Fatal("publisher reused a stale GC candidate physical extent")
	}
	if err := publisher.releaseGCLock(ctx, successor); err != nil {
		t.Fatal(err)
	}
	close(bucket.release)
	if err := <-gcDone; err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := reader.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != 1 || len(values) != 1 || !bytes.Equal(values[0].Value, []byte("value")) {
		t.Fatalf("tip=%d values=%#v err=%v", tip, values, err)
	}
}

func TestBuildExtentPublishedBytesRecoverExactly(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range [][]byte{[]byte("first"), bytes.Repeat([]byte("second"), 512)} {
		if _, _, err := core.Propose(ctx, value); err != nil {
			t.Fatal(err)
		}
	}
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	extent, data, _, err := manager.buildExtent(core, 1, core.Tip(), [32]byte{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(data) != extent.hash {
		t.Fatal("extent hash does not match published bytes")
	}
	if err := manager.uploadExtent(ctx, extent.hash, data, 1); err != nil {
		t.Fatal(err)
	}
	recovered, err := manager.readExtent(ctx, extent.hash, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.Decisions) != len(extent.Decisions) {
		t.Fatalf("recovered decisions=%d, want %d", len(recovered.Decisions), len(extent.Decisions))
	}
	for i := range extent.Decisions {
		if recovered.Decisions[i].Slot != extent.Decisions[i].Slot || !bytes.Equal(recovered.Decisions[i].Value, extent.Decisions[i].Value) || !bytes.Equal(recovered.Decisions[i].Certificate, extent.Decisions[i].Certificate) {
			t.Fatalf("recovered decision %d differs from published bytes", i)
		}
	}
}

func TestArchiveSyncReconcilesExtentAfterWriteThenEPIPE(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("value")); err != nil {
		t.Fatal(err)
	}
	bucket := &archiveExtentOutcomeBucket{
		Bucket:           objstore.NewInMemBucket(),
		uploadErr:        fmt.Errorf("injected lost upload response: %w", syscall.EPIPE),
		persistBeforeErr: true,
	}
	bucket.armed.Store(true)
	writer := NewManager(bucket, "cluster", 1)
	defer writer.Close()
	if err := writer.SyncThrough(ctx, core, 1); err != nil {
		t.Fatalf("sync after the immutable extent was stored: %v", err)
	}
	reader := NewManager(bucket, "cluster", 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("load published extent: %v", err)
	}
	values, tip, err := reader.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != 1 || len(values) != 1 || !bytes.Equal(values[0].Value, []byte("value")) {
		t.Fatalf("tip=%d values=%#v err=%v", tip, values, err)
	}
}

func TestArchiveSyncDoesNotPublishAfterUnverifiedExtentEPIPE(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("value")); err != nil {
		t.Fatal(err)
	}
	bucket := &archiveExtentOutcomeBucket{
		Bucket:    objstore.NewInMemBucket(),
		uploadErr: fmt.Errorf("injected lost upload response: %w", syscall.EPIPE),
	}
	bucket.armed.Store(true)
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	err = manager.SyncThrough(ctx, core, 1)
	if !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("SyncThrough() error = %v, want original EPIPE", err)
	}
	if exists, err := bucket.Exists(ctx, manager.key("archive/head.bin")); err != nil || exists {
		t.Fatalf("archive HEAD exists=%t err=%v after unverified upload", exists, err)
	}
}

func TestUploadExtentReadbackReconcilesOnlyVerifiedConditionalOrEPIPE(t *testing.T) {
	epipe := fmt.Errorf("injected lost upload response: %w", syscall.EPIPE)
	accessDenied := errors.New("injected access denied")
	tests := []struct {
		name           string
		uploadErr      error
		seed           []byte
		seedExact      bool
		persist        bool
		cancelBefore   bool
		deadlineBefore bool
		cancelOnGet    bool
		disableCAS     bool
		wantSuccess    bool
		wantGetCount   uint64
		wantCause      error
		wantCondition  bool
	}{
		{name: "conditional conflict with exact existing extent", seedExact: true, wantSuccess: true, wantGetCount: 1},
		{name: "conditional conflict but missing extent", uploadErr: errArchiveTestCondition, wantGetCount: 1, wantCause: errArchiveTestCondition},
		{name: "conditional conflict with corrupt extent", seed: []byte("corrupt"), wantGetCount: 1, wantCondition: true},
		{name: "EPIPE after exact extent persisted", uploadErr: epipe, persist: true, seed: nil, wantSuccess: true, wantGetCount: 1},
		{name: "EPIPE with missing extent", uploadErr: epipe, wantGetCount: 1, wantCause: syscall.EPIPE},
		{name: "EPIPE with corrupt extent", uploadErr: epipe, seed: []byte("corrupt"), wantGetCount: 1, wantCause: syscall.EPIPE},
		{name: "already canceled context does not read back", uploadErr: epipe, cancelBefore: true, wantCause: syscall.EPIPE},
		{name: "already expired deadline does not read back", uploadErr: epipe, deadlineBefore: true, wantCause: syscall.EPIPE},
		{name: "cancellation during readback preserves upload error", uploadErr: epipe, cancelOnGet: true, wantGetCount: 1, wantCause: syscall.EPIPE},
		{name: "non-CAS bucket does not reconcile", uploadErr: epipe, disableCAS: true, wantCause: syscall.EPIPE},
		{name: "unrelated authorization error does not reconcile", uploadErr: accessDenied, wantCause: accessDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := objstore.NewInMemBucket()
			bucket := &archiveExtentOutcomeBucket{Bucket: base, uploadErr: tt.uploadErr, persistBeforeErr: tt.persist}
			bucket.armed.Store(true)
			manager := NewManager(bucket, "cluster", 1)
			defer manager.Close()
			if tt.disableCAS {
				manager.cas = false
			}
			hash, data := testArchiveExtentBytes(t, manager)
			key := manager.key(extentObjectKey(hash, 1))
			seed := tt.seed
			if tt.seedExact {
				seed = data
			}
			if seed != nil {
				if err := base.Upload(context.Background(), key, bytes.NewReader(seed)); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.cancelBefore {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if tt.deadlineBefore {
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			} else if tt.cancelOnGet {
				ctx, cancel = context.WithCancel(ctx)
				bucket.cancelOnGet = cancel
				defer cancel()
			}
			err := manager.uploadExtent(ctx, hash, data, 1)
			if tt.wantSuccess {
				if err != nil {
					t.Fatalf("uploadExtent() error = %v, want verified readback success", err)
				}
			} else {
				if err == nil {
					t.Fatal("uploadExtent() succeeded without a verified extent")
				}
				if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
					t.Fatalf("uploadExtent() error %v does not preserve cause %v", err, tt.wantCause)
				}
				if tt.wantCondition && !bucket.IsConditionNotMetErr(err) {
					t.Fatalf("uploadExtent() error %v does not preserve conditional conflict", err)
				}
			}
			if got := bucket.gets.Load(); got != tt.wantGetCount {
				t.Fatalf("readback GETs = %d, want %d", got, tt.wantGetCount)
			}
			if got := bucket.uploads.Load(); got != 1 {
				t.Fatalf("extent upload attempts = %d, want 1", got)
			}
		})
	}
}

func testArchiveExtentBytes(t *testing.T, manager *Manager) ([32]byte, []byte) {
	t.Helper()
	value := []byte("immutable extent")
	decision := quepaxa.DecidedValue{Slot: 1, Hash: sha256.Sum256(value), Value: value, Certificate: []byte("certificate")}
	prefixes := [][32]byte{{}, quepaxa.AdvancePrefixHash([32]byte{}, decision.Slot, decision.Hash)}
	extent, data, _, err := manager.buildExtent(archiveBenchmarkSource{decisions: []quepaxa.DecidedValue{decision}, prefixes: prefixes}, 1, 1, [32]byte{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return extent.hash, data
}

type archiveBenchmarkSource struct {
	decisions []quepaxa.DecidedValue
	prefixes  [][32]byte
}

func (s archiveBenchmarkSource) DecisionsFrom(from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	start := int(from - 1)
	end := min(start+limit, len(s.decisions))
	return s.decisions[start:end], quepaxa.Slot(len(s.decisions)), nil
}

func (s archiveBenchmarkSource) DecisionsFromBounded(from quepaxa.Slot, itemLimit, payloadByteLimit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	if itemLimit <= 0 || payloadByteLimit <= 0 || from == 0 || uint64(from) > uint64(len(s.decisions))+1 {
		return nil, quepaxa.Slot(len(s.decisions)), fmt.Errorf("decision limits must be positive")
	}
	start := int(from - 1)
	page := make([]quepaxa.DecidedValue, 0, min(itemLimit, max(0, len(s.decisions)-start)))
	used := 0
	for _, decision := range s.decisions[start:] {
		remaining := payloadByteLimit - used
		if len(decision.Value) > remaining || len(decision.Certificate) > remaining-len(decision.Value) {
			if len(page) == 0 {
				return nil, quepaxa.Slot(len(s.decisions)), fmt.Errorf("decision %d exceeds payload budget", decision.Slot)
			}
			break
		}
		decision.Value = append([]byte(nil), decision.Value...)
		decision.Certificate = append([]byte(nil), decision.Certificate...)
		page = append(page, decision)
		used += len(decision.Value) + len(decision.Certificate)
		if len(page) == itemLimit || decision.Slot == quepaxa.Slot(len(s.decisions)) {
			break
		}
	}
	return page, quepaxa.Slot(len(s.decisions)), nil
}

func (s archiveBenchmarkSource) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	if int(slot) >= len(s.prefixes) {
		return [32]byte{}, false
	}
	return s.prefixes[slot], true
}

func (s archiveBenchmarkSource) Tip() quepaxa.Slot { return quepaxa.Slot(len(s.decisions)) }

func TestBuildExtentRespectsEncodedPayloadAndCountBounds(t *testing.T) {
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	t.Run("exact byte budget and later oversize is a prefix boundary", func(t *testing.T) {
		value := []byte{1}
		certificate := bytes.Repeat([]byte{2}, maxExtentPayload-len(value))
		decisions := []quepaxa.DecidedValue{
			{Slot: 1, Hash: sha256.Sum256(value), Value: value, Certificate: certificate},
			{Slot: 2, Hash: sha256.Sum256([]byte{3}), Value: []byte{3}, Certificate: []byte{4}},
		}
		prefixes := make([][32]byte, len(decisions)+1)
		for i, decision := range decisions {
			prefixes[i+1] = quepaxa.AdvancePrefixHash(prefixes[i], decision.Slot, decision.Hash)
		}
		source := archiveBenchmarkSource{decisions: decisions, prefixes: prefixes}
		extent, data, tip, err := manager.buildExtent(source, 1, 2, [32]byte{}, 0)
		if err != nil || tip != 2 || len(data) != maxExtentSize || len(extent.Decisions) != 1 || extent.End != 1 || extent.EndPrefix != prefixes[1] {
			t.Fatalf("extent=%+v bytes=%d tip=%d err=%v, want exact-size slot 1 prefix", extent, len(data), tip, err)
		}
		if _, err := encodeExtent(extent); err != nil {
			t.Fatalf("exact-budget extent failed encoding: %v", err)
		}
	})
	t.Run("first decision over budget errors", func(t *testing.T) {
		value := []byte{1}
		decision := quepaxa.DecidedValue{Slot: 1, Hash: sha256.Sum256(value), Value: value, Certificate: bytes.Repeat([]byte{2}, maxExtentPayload)}
		prefixes := [][32]byte{{}, quepaxa.AdvancePrefixHash([32]byte{}, 1, decision.Hash)}
		source := archiveBenchmarkSource{decisions: []quepaxa.DecidedValue{decision}, prefixes: prefixes}
		if _, _, _, err := manager.buildExtent(source, 1, 1, [32]byte{}, 0); err == nil || !strings.Contains(err.Error(), "decision 1") {
			t.Fatalf("first oversize error=%v, want slot 1 error", err)
		}
	})
	t.Run("per-decision framing selects and clears the rejected tail", func(t *testing.T) {
		firstValue := []byte{1}
		first := quepaxa.DecidedValue{Slot: 1, Hash: sha256.Sum256(firstValue), Value: firstValue, Certificate: bytes.Repeat([]byte{2}, maxExtentPayload-9)}
		secondValue := []byte{3}
		second := quepaxa.DecidedValue{Slot: 2, Hash: sha256.Sum256(secondValue), Value: secondValue, Certificate: []byte{4}}
		prefixes := [][32]byte{
			{},
			quepaxa.AdvancePrefixHash([32]byte{}, first.Slot, first.Hash),
			quepaxa.AdvancePrefixHash(quepaxa.AdvancePrefixHash([32]byte{}, first.Slot, first.Hash), second.Slot, second.Hash),
		}
		source := archiveBenchmarkSource{decisions: []quepaxa.DecidedValue{first, second}, prefixes: prefixes}
		extent, data, _, err := manager.buildExtent(source, 1, 2, [32]byte{}, 0)
		if err != nil || len(data) != maxExtentSize-8 || len(extent.Decisions) != 1 || cap(extent.Decisions) != 1 || extent.End != 1 || extent.EndPrefix != prefixes[1] {
			t.Fatalf("extent=%+v bytes=%d cap=%d err=%v, want one exact-prefix item with cleared tail", extent, len(data), cap(extent.Decisions), err)
		}
	})
	if _, _, _, err := manager.buildExtent(archiveBenchmarkSource{}, 0, quepaxa.Slot(^uint64(0)), [32]byte{}, 0); err == nil {
		t.Fatal("zero start with maximum range unexpectedly succeeded")
	}
	t.Run("count and through bounds are exact prefixes", func(t *testing.T) {
		decisions := make([]quepaxa.DecidedValue, 1025)
		prefixes := make([][32]byte, len(decisions)+1)
		for i := range decisions {
			slot := quepaxa.Slot(i + 1)
			value := []byte{byte(i)}
			hash := sha256.Sum256(value)
			decisions[i] = quepaxa.DecidedValue{Slot: slot, Hash: hash, Value: value, Certificate: []byte{1}}
			prefixes[i+1] = quepaxa.AdvancePrefixHash(prefixes[i], slot, hash)
		}
		source := archiveBenchmarkSource{decisions: decisions, prefixes: prefixes}
		first, _, _, err := manager.buildExtent(source, 1, 1025, [32]byte{}, 0)
		if err != nil || len(first.Decisions) != 1024 || first.Start != 1 || first.End != 1024 || first.EndPrefix != prefixes[1024] {
			t.Fatalf("first extent=%d decisions %d-%d err=%v", len(first.Decisions), first.Start, first.End, err)
		}
		tail, _, _, err := manager.buildExtent(source, 1025, 1025, first.hash, 1)
		if err != nil || len(tail.Decisions) != 1 || tail.Start != 1025 || tail.End != 1025 || tail.StartPrefix != prefixes[1024] || tail.EndPrefix != prefixes[1025] {
			t.Fatalf("tail extent=%d decisions %d-%d err=%v", len(tail.Decisions), tail.Start, tail.End, err)
		}
		throughBounded, _, _, err := manager.buildExtent(source, 1, 1, [32]byte{}, 0)
		if err != nil || len(throughBounded.Decisions) != 1 || throughBounded.End != 1 || throughBounded.EndPrefix != prefixes[1] {
			t.Fatalf("through-bounded extent=%d end=%d err=%v", len(throughBounded.Decisions), throughBounded.End, err)
		}
	})
}

func TestArchiveSyncStoresMetadataRefsAndCachesOnlyStableTail(t *testing.T) {
	ctx := context.Background()
	decisions := make([]quepaxa.DecidedValue, 2049)
	prefixes := make([][32]byte, len(decisions)+1)
	for i := range decisions {
		slot := quepaxa.Slot(i + 1)
		value := []byte{byte(i)}
		hash := sha256.Sum256(value)
		decisions[i] = quepaxa.DecidedValue{Slot: slot, Hash: hash, Value: value, Certificate: []byte{1}}
		prefixes[i+1] = quepaxa.AdvancePrefixHash(prefixes[i], slot, hash)
	}
	source := archiveBenchmarkSource{decisions: decisions, prefixes: prefixes}
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	defer manager.Close()
	if err := manager.syncNow(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}
	if len(manager.extents) != 3 {
		t.Fatalf("published extents=%d, want 3", len(manager.extents))
	}
	start := quepaxa.Slot(1)
	var previousHash [32]byte
	var previousObject uint64
	for _, ref := range manager.extents {
		if len(ref.Decisions) != 0 || len(ref.prefixes) != 0 {
			t.Fatalf("retained archive reference contains payload: decisions=%d prefixes=%d", len(ref.Decisions), len(ref.prefixes))
		}
		if ref.ConfigID != 1 || ref.object != 1 || ref.Start != start || ref.EndPrefix != prefixes[ref.End] || ref.StartPrefix != prefixes[start-1] || ref.PreviousHash != previousHash || ref.PreviousObject != previousObject {
			t.Fatalf("metadata ref lost chain fields: %+v", ref)
		}
		start, previousHash, previousObject = ref.End+1, ref.hash, ref.object
	}
	if len(manager.cache) != 2 {
		t.Fatalf("cached extents=%d, want only two-tail cache", len(manager.cache))
	}
	for i, ref := range manager.extents {
		_, cached := manager.cache[extentObject{hash: ref.hash, id: ref.object}]
		if cached != (i >= 1) {
			t.Fatalf("extent %d cached=%v, want %v", i, cached, i >= 1)
		}
	}
}

func TestArchiveSyncUploadFailureDoesNotCacheCandidate(t *testing.T) {
	ctx := context.Background()
	value := []byte("decision")
	hash := sha256.Sum256(value)
	source := archiveBenchmarkSource{
		decisions: []quepaxa.DecidedValue{{Slot: 1, Hash: hash, Value: value, Certificate: []byte("certificate")}},
		prefixes:  [][32]byte{{}, quepaxa.AdvancePrefixHash([32]byte{}, 1, hash)},
	}
	bucket := &failArchiveHeadBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	if err := manager.syncNow(ctx, source, 1); err == nil {
		t.Fatal("sync unexpectedly succeeded after injected head upload failure")
	}
	if len(manager.cache) != 0 || len(manager.extents) != 0 || manager.Tip() != 0 {
		t.Fatalf("failed candidate was adopted: cache=%d refs=%d tip=%d", len(manager.cache), len(manager.extents), manager.Tip())
	}
}

func BenchmarkArchiveBeforeAckPublishExtent(b *testing.B) {
	value := bytes.Repeat([]byte("v"), 4<<10)
	for _, count := range []int{1, 32, maxExtentItems} {
		b.Run(fmt.Sprintf("%dx4KiB", count), func(b *testing.B) {
			source := archiveBenchmarkSource{decisions: make([]quepaxa.DecidedValue, count), prefixes: make([][32]byte, count+1)}
			for i := range source.decisions {
				slot := quepaxa.Slot(i + 1)
				hash := sha256.Sum256(value)
				source.decisions[i] = quepaxa.DecidedValue{Slot: slot, Hash: hash, Value: value, Certificate: []byte("certificate")}
				source.prefixes[i+1] = quepaxa.AdvancePrefixHash(source.prefixes[i], slot, hash)
			}
			b.SetBytes(int64(count * len(value)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
				if err := manager.syncNow(context.Background(), source, source.Tip()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestArchiveCleanupIgnoresFutureGenerationBeforeHeadPublish(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("value")); err != nil {
		t.Fatal(err)
	}
	extentBucket := &archiveExtentOutcomeBucket{
		Bucket:           objstore.NewInMemBucket(),
		uploadErr:        fmt.Errorf("injected lost upload response: %w", syscall.EPIPE),
		persistBeforeErr: true,
	}
	extentBucket.armed.Store(true)
	bucket := &blockingHeadUploadBucket{Bucket: extentBucket, started: make(chan struct{}), release: make(chan struct{})}
	bucket.armed.Store(true)
	publisher := NewManager(bucket, "cluster", 1)
	publishDone := make(chan error, 1)
	go func() { publishDone <- publisher.SyncThrough(ctx, core, 1) }()
	<-bucket.started
	var object string
	if err := bucket.Iter(ctx, "cluster/archive/blocks", func(name string) error { object = name; return nil }); err != nil {
		t.Fatal(err)
	}
	if object == "" {
		t.Fatal("publisher did not upload extent before head")
	}
	gc := NewManager(bucket, "cluster", 1)
	if err := bucket.Upload(ctx, gc.gcMarkerKey(object), bytes.NewReader([]byte(object)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if err := gc.Cleanup(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, object); !exists {
		t.Fatal("GC deleted an in-flight future-generation extent")
	}
	close(bucket.release)
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveCASDoesNotRegressOnStaleWriter(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	first := NewManager(bucket, "cluster", 1)
	stale := NewManager(bucket, "cluster", 1)
	if err := first.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stale.Load(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := first.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	if err := stale.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.Tip() != 2 {
		t.Fatalf("archive tip regressed to %d", reader.Tip())
	}
}

func TestArchivePostCASFailureKeepsLocalHeadAndTokenTogether(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &failPostCASBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	if _, _, err := core.Propose(ctx, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := manager.SyncThrough(ctx, core, 1); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	oldHead, oldCAS := manager.head, manager.headCAS
	manager.mu.Unlock()
	if _, _, err := core.Propose(ctx, []byte("second")); err != nil {
		t.Fatal(err)
	}
	bucket.armed.Store(true)
	if err := manager.SyncThrough(ctx, core, 2); err == nil {
		t.Fatal("post-CAS verification failure was ignored")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if !archiveHeadsEqual(manager.head, oldHead) || !sameNullableObjectVersion(manager.headCAS, oldCAS) {
		t.Fatal("post-CAS failure mixed candidate head with stale token")
	}
}

func TestUnchangedArchiveLoadOnlyChecksHead(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("one")); err != nil {
		t.Fatal(err)
	}
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	writer := NewManager(bucket, "cluster", 1)
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	bucket.heads.Store(0)
	bucket.gets.Store(0)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if heads, gets := bucket.heads.Load(), bucket.gets.Load(); heads != 1 || gets != 0 {
		t.Fatalf("unchanged load heads=%d gets=%d, want 1/0", heads, gets)
	}
}

func TestChangedArchiveLoadStopsAtKnownHash(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	writer := NewManager(bucket, "cluster", 1)
	defer writer.Close()
	for i := 0; i < 3; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	reader := NewManager(bucket, "cluster", 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("next")); err != nil {
		t.Fatal(err)
	}
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	bucket.heads.Store(0)
	bucket.gets.Store(0)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if heads, gets := bucket.heads.Load(), bucket.gets.Load(); heads != 3 || gets != 2 {
		t.Fatalf("incremental load heads=%d gets=%d, want 3/2", heads, gets)
	}
}

func TestArchiveLoadRejectsMixedHeadGeneration(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	writer := NewManager(bucket, "cluster", 1)
	defer writer.Close()
	if _, _, err := core.Propose(ctx, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	defer reader.Close()
	if _, _, err := core.Propose(ctx, []byte("two")); err != nil {
		t.Fatal(err)
	}
	racing := &racingHeadBucket{Bucket: bucket, run: func() {
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Errorf("publish raced head: %v", err)
		}
	}}
	reader.bucket = racing
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.Tip() != 2 {
		t.Fatalf("mixed-generation head installed tip %d", reader.Tip())
	}
}

func TestArchivePublishRevalidatesItsTail(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	for i := 0; i < 2; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
		bucket.heads.Store(0)
		bucket.gets.Store(0)
	}
	if heads, gets := bucket.heads.Load(), bucket.gets.Load(); heads != 0 || gets != 0 {
		t.Fatalf("counter reset failed heads=%d gets=%d", heads, gets)
	}
	if _, _, err := core.Propose(ctx, []byte("third")); err != nil {
		t.Fatal(err)
	}
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if heads, gets := bucket.heads.Load(), bucket.gets.Load(); heads != 2 || gets != 1 {
		t.Fatalf("publish heads=%d gets=%d, want 2/1", heads, gets)
	}
}

func TestArchivePublicationCostDoesNotGrowWithHistory(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := &countingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", 1)
	for i := 0; i < 256; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		bucket.puts.Store(0)
		if err := manager.syncNow(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
		if puts := bucket.puts.Load(); puts != 2 {
			t.Fatalf("publication %d used %d PUTs, want 2", i+1, puts)
		}
	}
	foundManifest := false
	if err := bucket.Iter(ctx, "cluster/archive/manifests", func(string) error {
		foundManifest = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if foundManifest {
		t.Fatal("linked archive wrote a full-history manifest")
	}
}

func TestArchiveRejectsGenerationOverflow(t *testing.T) {
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	manager.head.Generation = ^uint64(0)
	if err := manager.syncNow(context.Background(), nil, 1); err == nil || !strings.Contains(err.Error(), "generation exhausted") {
		t.Fatalf("overflow error=%v", err)
	}
}

func TestArchiveTrimRetainsOnlyCheckpointTail(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", 1)
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(3)
	if !ok {
		t.Fatal("missing prefix")
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 3, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: prefix, NextLeaderOrder: []quepaxa.NodeID{"n1"}}
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing checkpoint decision")
	}
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := manager.TrimThrough(ctx, quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: slot}, decision); err != nil {
		t.Fatal(err)
	}
	snapshot, err := manager.BeginRecoverySnapshot(ctx, "crashed-recovery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := manager.acquireGCLock(ctx, "active-gc", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Renew(ctx, time.Minute); err != nil {
		t.Fatalf("renew recovery snapshot during GC: %v", err)
	}
	if err := manager.releaseGCLock(ctx, lock); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(ctx); err != nil {
		t.Fatal(err)
	}
	orphan := archiveRecoveryPin{OwnerID: "crashed-recovery", Token: "orphan", Base: manager.head.Base, Tip: manager.head.Tip, TailHash: manager.head.TailHash, TailObject: manager.head.TailObject, LeaseUntilMS: time.Now().Add(-time.Second).UnixMilli()}
	expired, err := manager.readRecoveryPin(ctx, manager.recoveryPinKey(orphan.OwnerID))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.writeRecoveryPin(ctx, manager.recoveryPinKey(orphan.OwnerID), orphan, objstore.WithIfMatch(expired.version)); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := manager.BeginRecoverySnapshot(ctx, orphan.OwnerID, time.Minute)
	if err != nil {
		t.Fatalf("expired archive recovery pin was not reclaimed: %v", err)
	}
	if err := snapshot.Close(ctx); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("stale snapshot close error=%v, want busy", err)
	}
	current, err := manager.readRecoveryPin(ctx, reclaimed.pinKey)
	if err != nil {
		t.Fatal(err)
	}
	if current.Token != reclaimed.pin.Token || current.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatal("stale snapshot close expired the replacement pin")
	}
	if err := reclaimed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := manager.DecisionsFrom(ctx, 3, 1); !errors.Is(err, quepaxa.ErrCompacted) {
		t.Fatalf("trimmed decision error=%v", err)
	}
	values, tip, err := manager.DecisionsFrom(ctx, 4, 10)
	if err != nil || tip != slot || len(values) != int(slot-3) {
		t.Fatalf("tail tip=%d values=%d err=%v", tip, len(values), err)
	}
	reloaded := NewManager(bucket, "cluster", 1)
	if err := reloaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err = reloaded.DecisionsFrom(ctx, 4, 10)
	if err != nil || len(values) != int(slot-3) {
		t.Fatalf("reloaded tail values=%d err=%v", len(values), err)
	}
	if err := reloaded.Cleanup(ctx, 0); err != nil {
		t.Fatal(err)
	}
	fresh := NewManager(bucket, "cluster", 1)
	if err := fresh.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err = fresh.DecisionsFrom(ctx, 4, 10)
	if err != nil || tip != slot || len(values) != int(slot-3) {
		t.Fatalf("compacted tail tip=%d values=%d err=%v", tip, len(values), err)
	}
}
