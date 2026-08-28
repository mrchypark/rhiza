package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type countingBucket struct {
	objstore.Bucket
	heads atomic.Uint64
	gets  atomic.Uint64
	puts  atomic.Uint64
}

type blockingUploadBucket struct {
	objstore.Bucket
	started chan struct{}
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
	return b.Bucket.Attributes(ctx, name)
}

func (b *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
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
	bucket := objstore.NewInMemBucket()
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
	if heads, gets := bucket.heads.Load(), bucket.gets.Load(); heads != 3 || gets != 2 {
		t.Fatalf("publish heads=%d gets=%d, want 3/2", heads, gets)
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
