package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNormalOpenRestoresCertifiedCheckpointBeforeSuffix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dataDir, objectDir := t.TempDir(), t.TempDir()
	config := &types.ExecutionConfig{
		DataDir: dataDir, ClusterID: "normal-checkpoint-restart", NodeID: "n1",
		Members:          []quepaxa.Member{{ID: "n1"}},
		ObjStoreProvider: "filesystem", ObjStoreDir: objectDir,
		ObjStoreDurability:   types.ObjectStoreDurabilityBeforeAck,
		ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour,
	}

	first := New(config)
	if err := first.Open(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Shutdown(); err != nil {
			t.Errorf("shutdown original voter: %v", err)
		}
	})
	if _, err := first.server.Execute(ctx, network.ExecuteRequest{
		RequestID: "normal-checkpoint-schema",
		SQL:       "CREATE TABLE restart_proof (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
	}); err != nil {
		t.Fatalf("create checkpointed table: %v", err)
	}
	if _, err := first.server.Execute(ctx, network.ExecuteRequest{
		RequestID: "normal-checkpoint-base",
		SQL:       "INSERT INTO restart_proof VALUES (1, 'checkpoint-base')",
	}); err != nil {
		t.Fatalf("write checkpointed base: %v", err)
	}
	first.checkpointer.Stop()
	if err := first.checkpointer.CheckpointOnShutdown(ctx, first.material.StateTip()); err != nil {
		t.Fatal(err)
	}
	root := first.checkpoints.Latest()
	seal, sealed, err := first.core.LatestCheckpointSeal()
	if err != nil || !sealed || root == nil || uint64(seal.Index) != root.Index || seal.RootHash != root.RootHash {
		t.Fatalf("certified checkpoint root=%v seal=%+v sealed=%t err=%v", root, seal, sealed, err)
	}
	if floor := first.core.CompactionFloor(); floor != seal.Index {
		t.Fatalf("compacted floor=%d, certified checkpoint=%d", floor, seal.Index)
	}
	if _, err := first.server.Execute(ctx, network.ExecuteRequest{
		RequestID: "normal-checkpoint-suffix",
		SQL:       "INSERT INTO restart_proof VALUES (2, 'certified-suffix')",
	}); err != nil {
		t.Fatalf("write certified suffix: %v", err)
	}
	wantTip, wantIdentity := first.core.Tip(), first.core.WALIdentity()
	if err := first.Shutdown(); err != nil {
		t.Fatalf("shutdown original voter: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		name := filepath.Join(dataDir, "sqlite.db"+suffix)
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove test-owned materializer file %q: %v", suffix, err)
		}
	}

	restarted := New(config)
	if err := restarted.Open(ctx); err != nil {
		t.Fatalf("normal Open failed to restore certified checkpoint and suffix: %v", err)
	}
	defer func() {
		if err := restarted.Shutdown(); err != nil {
			t.Error(err)
		}
	}()
	if !restarted.recoveryPhases.selectedCheckpoint || restarted.recoveryPhases.checkpointIndex != root.Index {
		t.Fatalf("normal Open did not select certified checkpoint %d: %+v", root.Index, restarted.recoveryPhases)
	}
	if restarted.material.Tip() != uint64(wantTip) || restarted.core.Tip() != wantTip || restarted.core.WALIdentity() != wantIdentity {
		t.Fatalf("restarted material/core/identity=%d/%d/%t, want tip=%d", restarted.material.Tip(), restarted.core.Tip(), restarted.core.WALIdentity() == wantIdentity, wantTip)
	}
	rows, err := restarted.server.Query(ctx, network.QueryRequest{SQL: "SELECT id, value FROM restart_proof ORDER BY id", Consistency: "linearizable"})
	if err != nil || len(rows.Rows) != 2 || rows.Rows[0][1] != "checkpoint-base" || rows.Rows[1][1] != "certified-suffix" {
		t.Fatalf("checkpoint plus suffix rows=%v err=%v", rows.Rows, err)
	}
}
