package recovery

import (
	"bytes"
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

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
