package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"

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
	values, tip, err := reader.DecisionsFrom(1, int(core.Tip()))
	if err != nil || tip != core.Tip() || len(values) != int(core.Tip()) {
		t.Fatalf("archive tip=%d values=%d err=%v", tip, len(values), err)
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
	for range 64 {
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
	for _, extent := range manager.extents {
		data, err := json.Marshal(extent)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) > maxExtentSize {
			t.Fatalf("encoded extent size=%d, limit=%d", len(data), maxExtentSize)
		}
	}
}

func TestArchiveRejectsNumberedObject(t *testing.T) {
	if err := decodePersistedJSON([]byte(`{"version":2}`), &archiveHead{}); err == nil {
		t.Fatal("accepted numbered archive object")
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

func TestArchivePublishDoesNotReloadItsTail(t *testing.T) {
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
	if heads, gets := bucket.heads.Load(), bucket.gets.Load(); heads != 1 || gets != 1 {
		t.Fatalf("publish heads=%d gets=%d, want 1/1", heads, gets)
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
	if _, _, err := manager.DecisionsFrom(3, 1); !errors.Is(err, quepaxa.ErrCompacted) {
		t.Fatalf("trimmed decision error=%v", err)
	}
	values, tip, err := manager.DecisionsFrom(4, 10)
	if err != nil || tip != slot || len(values) != int(slot-3) {
		t.Fatalf("tail tip=%d values=%d err=%v", tip, len(values), err)
	}
	reloaded := NewManager(bucket, "cluster", 1)
	if err := reloaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err = reloaded.DecisionsFrom(4, 10)
	if err != nil || len(values) != int(slot-3) {
		t.Fatalf("reloaded tail values=%d err=%v", len(values), err)
	}
}
