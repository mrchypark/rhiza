package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLocalBackupRestartRecoversOriginalPendingRecorder(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "pending", SQL: "CREATE TABLE recovered_pending (id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	// Exact durable proposal+recorder boundary: no decision or materializer apply.
	appendLocalRecorderFixture(t, wal, value)
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("recover pending Local recorder with archive: %v", err)
	}
	if _, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT * FROM recovered_pending"}); err != nil {
		_ = n.Shutdown()
		t.Fatal(err)
	}
	response, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "pending", SQL: "CREATE TABLE recovered_pending (id INTEGER)"})
	if err != nil || response.Slot != 1 {
		_ = n.Shutdown()
		t.Fatalf("original pending receipt=%+v err=%v", response, err)
	}
	if n.localBackup.Tip() != n.core.Tip() {
		_ = n.Shutdown()
		t.Fatal("recovered pending operation not archived")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(config.DataDir); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	response, err = n.server.Execute(ctx, network.ExecuteRequest{RequestID: "pending", SQL: "CREATE TABLE recovered_pending (id INTEGER)"})
	if err != nil || response.Slot != 1 {
		t.Fatalf("restored original pending receipt=%+v err=%v", response, err)
	}
}

func TestLocalBackupRestartPublishesBeforeResumedCompaction(t *testing.T) {
	for _, firstAttach := range []bool{false, true} {
		for _, sealed := range []bool{false, true} {
			t.Run(fmt.Sprintf("first-attach-%v/sealed-%v", firstAttach, sealed), func(t *testing.T) {
				ctx := context.Background()
				remote := t.TempDir()
				config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), CheckpointInterval: time.Hour}
				if !firstAttach {
					config.ObjStoreProvider, config.ObjStoreDir = "filesystem", remote
				}
				n := New(&config)
				if err := n.Open(ctx); err != nil {
					t.Fatal(err)
				}
				for _, request := range []network.ExecuteRequest{{RequestID: "schema", SQL: "CREATE TABLE resumed_backup (id INTEGER)"}, {RequestID: "row", SQL: "INSERT INTO resumed_backup VALUES (1)"}} {
					if _, err := n.server.Execute(ctx, request); err != nil {
						_ = n.Shutdown()
						t.Fatal(err)
					}
				}
				files, index, cleanup, err := n.material.CheckpointFilesAt(ctx)
				if err != nil {
					_ = n.Shutdown()
					t.Fatal(err)
				}
				prefix, ok := n.core.PrefixHash(quepaxa.Slot(index))
				if !ok {
					cleanup()
					_ = n.Shutdown()
					t.Fatal("checkpoint prefix missing")
				}
				descriptor, err := n.localStore.Publish(ctx, files, index, prefix, n.core.ConfigIDForSlot(quepaxa.Slot(index)))
				cleanup()
				if err != nil {
					_ = n.Shutdown()
					t.Fatal(err)
				}
				seal, err := sealFromLocalDescriptor(n.core, descriptor)
				if err != nil {
					_ = n.Shutdown()
					t.Fatal(err)
				}
				var sealSlot quepaxa.Slot
				if sealed {
					boundary := errors.New("simulated stop after durable seal before publication/compaction")
					sealSlot, err = n.core.ReclaimLocalCheckpoint(ctx, seal, func(context.Context, quepaxa.Slot) error { return boundary })
					if !errors.Is(err, boundary) {
						_ = n.Shutdown()
						t.Fatalf("seal boundary: %v", err)
					}
				} else {
					if err := n.core.PrepareCheckpoint(ctx, seal); err != nil {
						_ = n.Shutdown()
						t.Fatal(err)
					}
				}
				if n.core.CompactionFloor() != 0 {
					_ = n.Shutdown()
					t.Fatal("fixture compacted before restart")
				}
				closeErr := n.Shutdown()
				if sealed && !firstAttach {
					if closeErr == nil || !strings.Contains(closeErr.Error(), "artifact") {
						t.Fatalf("shutdown must preserve unpublished seal: %v", closeErr)
					}
				} else if closeErr != nil {
					t.Fatal(closeErr)
				}
				config.ObjStoreProvider, config.ObjStoreDir = "filesystem", remote
				n = New(&config)
				observedBeforeCompaction := false
				localNodeCheckpointCrashBoundary = func(boundary string) {
					if boundary != "after-local-backup-published-before-compaction" {
						return
					}
					observedBeforeCompaction = true
					if n.core.CompactionFloor() != 0 {
						t.Error("Local compaction preceded verified remote publication")
					}
					snapshot := n.localBackup.Snapshot()
					if snapshot == nil || snapshot.Seal.RootHash != seal.RootHash || n.localBackup.Tip() < snapshot.Decision.Slot {
						t.Error("publication boundary omitted exact remote checkpoint authority")
					}
				}
				t.Cleanup(func() { localNodeCheckpointCrashBoundary = nil })
				if err := n.Open(ctx); err != nil {
					t.Fatalf("resume before backup compaction: %v", err)
				}
				localNodeCheckpointCrashBoundary = nil
				if !observedBeforeCompaction {
					_ = n.Shutdown()
					t.Fatal("resumed reclamation skipped backup publication boundary")
				}
				snapshot := n.localBackup.Snapshot()
				if snapshot == nil || snapshot.Seal.RootHash != seal.RootHash || n.core.CompactionFloor() != seal.Index {
					_ = n.Shutdown()
					t.Fatal("resumed compaction missing published exact snapshot")
				}
				if sealed && snapshot.Decision.Slot != sealSlot {
					_ = n.Shutdown()
					t.Fatal("resume replaced original certified seal")
				}
				if err := n.Shutdown(); err != nil {
					t.Fatal(err)
				}
				if err := os.RemoveAll(config.DataDir); err != nil {
					t.Fatal(err)
				}
				n = New(&config)
				if err := n.Open(ctx); err != nil {
					t.Fatalf("full loss after resumed compaction: %v", err)
				}
				rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id FROM resumed_backup"})
				closeErr = n.Shutdown()
				if err != nil || closeErr != nil || len(rows.Rows) != 1 {
					t.Fatalf("resumed backup rows=%v query=%v close=%v", rows, err, closeErr)
				}
			})
		}
	}
}
