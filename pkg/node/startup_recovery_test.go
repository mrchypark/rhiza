package node

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	objstore "github.com/thanos-io/objstore"
)

type noCASBucket struct{ objstore.Bucket }

func (noCASBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType { return nil }

func TestStartupRecoveryGuardReloadsFirstArchiveHead(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	stale := recovery.NewManager(bucket, "cluster", 1)
	if err := stale.Load(ctx); err != nil {
		t.Fatal(err)
	}

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
	if _, _, err := core.Propose(ctx, []byte("first archive decision")); err != nil {
		t.Fatal(err)
	}
	writer := recovery.NewManager(bucket, "cluster", 1)
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}

	guard, err := newStartupRecoveryGuard(ctx, stale, "startup-test")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	snapshot := guard.Snapshot()
	if snapshot == nil || snapshot.Tip() != core.Tip() {
		t.Fatalf("snapshot tip=%v, want %d after stale empty load", snapshot, core.Tip())
	}
	values, _, err := snapshot.DecisionsFrom(guard.Context(), 1, 1)
	if err != nil || len(values) != 1 {
		t.Fatalf("snapshot values=%d err=%v", len(values), err)
	}
}

func TestStartupRecoveryGuardSkipsSingleNodeArchiveWithoutCAS(t *testing.T) {
	guard, err := newStartupRecoveryGuard(context.Background(), recovery.NewManager(noCASBucket{objstore.NewInMemBucket()}, "cluster", 1), "startup-test")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	if guard.Snapshot() != nil {
		t.Fatal("non-CAS archive unexpectedly created a shared recovery pin")
	}
}
