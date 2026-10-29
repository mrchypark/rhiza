package node

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestHAAppendOnlyNodeManagers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	n := New(&types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: "append-only-node", NodeID: "n1",
		Members: []quepaxa.Member{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(),
		ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour, ObjStoreGCInterval: time.Millisecond,
		ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
	})
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n.Shutdown(); err != nil {
			t.Error(err)
		}
	}()
	if n.checkpoints == nil || n.archive == nil {
		t.Fatal("HA managers missing")
	}
	if err := n.checkpoints.GarbageCollectFrom(ctx, nil, 1, 1, 0); err == nil {
		t.Fatal("node checkpoint manager allowed GC")
	}
	if err := n.archive.Cleanup(ctx, 0); err == nil {
		t.Fatal("node archive allowed GC")
	}
}
