package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func newSealableArchive(t *testing.T) (context.Context, objstore.Bucket, *quepaxa.Core, *Manager) {
	t.Helper()
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("sealed history")); err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	archive := NewManager(bucket, "cluster", 1)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	return ctx, bucket, core, archive
}

func TestSealStopsPublicationAndKeepsSnapshotReadable(t *testing.T) {
	ctx, bucket, core, _ := newSealableArchive(t)
	if err := Seal(ctx, bucket, "cluster", "recovery-1"); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "cluster", "recovery-1"); err != nil {
		t.Fatalf("same-operation retry: %v", err)
	}
	if err := Seal(ctx, bucket, "cluster", "recovery-2"); err == nil {
		t.Fatal("different operation resealed archive")
	}

	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.BeginRecoverySnapshot(ctx, "fork", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close(ctx)
	values, tip, err := snapshot.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != core.Tip() || len(values) != 1 {
		t.Fatalf("sealed snapshot values=%d tip=%d err=%v", len(values), tip, err)
	}
	if _, _, err := core.Propose(ctx, []byte("late")); err != nil {
		t.Fatal(err)
	}
	if err := reader.SyncThrough(ctx, core, core.Tip()); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("post-seal publish error=%v, want ErrArchiveSealed", err)
	}
	if err := reader.Cleanup(ctx, 0); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("post-seal cleanup error=%v, want ErrArchiveSealed", err)
	}
}

func TestSealFencesStalePublisherHeadCAS(t *testing.T) {
	ctx, base, core, _ := newSealableArchive(t)
	wrapped := &blockingHeadUploadBucket{Bucket: base, started: make(chan struct{}), release: make(chan struct{})}
	stale := NewManager(wrapped, "cluster", 1)
	if err := stale.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("racing late decision")); err != nil {
		t.Fatal(err)
	}
	wrapped.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- stale.SyncThrough(ctx, core, core.Tip()) }()
	select {
	case <-wrapped.started:
	case <-time.After(time.Second):
		t.Fatal("stale publisher did not reach head CAS")
	}
	if err := Seal(ctx, base, "cluster", "recovery-race"); err != nil {
		t.Fatal(err)
	}
	close(wrapped.release)
	if err := <-done; !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("stale publisher error=%v, want ErrArchiveSealed", err)
	}
	reader := NewManager(base, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.Tip() != 1 || !reader.head.Sealed {
		t.Fatalf("sealed head tip=%d sealed=%v", reader.Tip(), reader.head.Sealed)
	}
}
