//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestLocalBackupRejectsExistingSnapshotDescriptorMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "backup", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (n INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	if err := n.reclaimLocalCheckpoint(ctx); err != nil {
		t.Fatal(err)
	}
	seal, ok, err := n.core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("checkpoint seal: ok=%v err=%v", ok, err)
	}
	snapshot, err := recovery.ReadLocalSnapshot(ctx, n.bucket, n.localBackupPrefix(), seal.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Descriptor = json.RawMessage(`{}`)
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	key := path.Join(n.localBackupPrefix(), fmt.Sprintf("snapshots/%x.json", seal.RootHash))
	if err := n.bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.uploadLocalBackupSnapshot(ctx, seal.CheckpointSeal, seal.DecisionSlot); err == nil {
		t.Fatal("mismatched immutable descriptor accepted")
	}
}

func TestLocalBackupReclaimAllowsForegroundDuringCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := t.TempDir()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "backup", DataDir: root, ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			_ = n.Shutdown()
		}
	}()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (n INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var once, hit sync.Once
	defer once.Do(func() { close(release) })
	reset := localtesthooks.Set(func(name string) {
		if name == "qlog:after-compaction-build-synced-before-manifest" {
			hit.Do(func() { close(started) })
			<-release
		}
	})
	defer reset()
	done := make(chan error, 1)
	go func() { done <- n.reclaimLocalCheckpoint(ctx) }()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("reclaim before compaction: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	writeCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	_, err := n.server.Execute(writeCtx, network.ExecuteRequest{RequestID: "during", SQL: "INSERT INTO items VALUES (1)"})
	if err != nil {
		t.Errorf("foreground write during compaction: %v", err)
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if err != nil {
		return
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("restore after data loss: %v", err)
	}
	result, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT n FROM items"})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("restore acknowledged row: rows=%v error=%v", result.Rows, err)
	}
}

func TestLocalBackupDrainCannotHoldSnapshotMutex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "backup", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	entered := make(chan struct{})
	var once sync.Once
	n.server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		once.Do(func() { close(entered) })
		return n.syncLocalBackup(ctx)
	})
	n.localSnapshotMu.Lock()
	locked := true
	defer func() {
		if locked {
			n.localSnapshotMu.Unlock()
		}
	}()
	finished := make(chan error, 1)
	go func() {
		_, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "write", SQL: "CREATE TABLE items(n INTEGER)"})
		finished <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	drainCtx, stop := context.WithTimeout(ctx, 250*time.Millisecond)
	defer stop()
	if _, err := n.server.Quiesce(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain while snapshot lock held: %v", err)
	}
	n.localSnapshotMu.Unlock()
	locked = false
	if err := <-finished; err != nil {
		t.Fatalf("foreground completion after unlock: %v", err)
	}
}

type localConcurrentCompactionCrash struct{ Root, Remote, Ready string }

func TestLocalBackupConcurrentCompactionHardKillPreservesACK(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_generation_one=%v", legacy), func(t *testing.T) {
			testLocalBackupConcurrentCompactionHardKillPreservesACK(t, legacy)
		})
	}
}

func testLocalBackupConcurrentCompactionHardKillPreservesACK(t *testing.T, legacy bool) {
	root, remote := t.TempDir(), t.TempDir()
	ready := filepath.Join(t.TempDir(), "ack")
	data, err := json.Marshal(localConcurrentCompactionCrash{Root: root, Remote: remote, Ready: ready})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalBackupConcurrentCompactionHardKillHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_LOCAL_COMPACTION_CONCURRENT_CRASH="+string(data))
	if legacy {
		cmd.Env = append(cmd.Env, "RHIZA_TEST_LEGACY_LOCAL_SNAPSHOT=1")
	}
	startLocalNodeCrashChild(t, cmd, ready)
	waitForLocalNodeCrashBoundary(t, cmd, ready)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "backup", DataDir: root, ObjStoreProvider: "filesystem", ObjStoreDir: remote, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	verify := func() {
		t.Helper()
		n := New(&config)
		if err := n.Open(ctx); err != nil {
			t.Fatalf("open after hard kill: %v", err)
		}
		defer n.Shutdown()
		result, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT n FROM items"})
		if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || fmt.Sprint(result.Rows[0][0]) != "1" {
			t.Fatalf("acknowledged row: rows=%v error=%v", result.Rows, err)
		}
	}
	verify()
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	verify()
}

func TestLocalBackupConcurrentCompactionHardKillHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_COMPACTION_CONCURRENT_CRASH")
	if data == "" {
		return
	}
	var req localConcurrentCompactionCrash
	if err := json.Unmarshal([]byte(data), &req); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "backup", DataDir: req.Root, ObjStoreProvider: "filesystem", ObjStoreDir: req.Remote, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (n INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	localtesthooks.Set(func(name string) {
		if name == "qlog:after-compaction-build-synced-before-manifest" {
			close(started)
			<-release
		}
	})
	done := make(chan error, 1)
	go func() { done <- n.reclaimLocalCheckpoint(ctx) }()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("reclaim before build fence: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if os.Getenv("RHIZA_TEST_LEGACY_LOCAL_SNAPSHOT") == "1" {
		snapshot := n.localBackup.Snapshot()
		if snapshot == nil {
			t.Fatal("no published snapshot")
		}
		manager, err := n.localCheckpointManager(snapshot.Seal.ConfigID)
		if err != nil {
			t.Fatal(err)
		}
		root, err := manager.OpenRoot(ctx, uint64(snapshot.Seal.Index), snapshot.PortableRoot)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range root.Files {
			for _, block := range file.Blocks {
				if block.Generation != 1 {
					t.Fatalf("fixture generation = %d, want 1", block.Generation)
				}
			}
		}
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "during", SQL: "INSERT INTO items VALUES (1)"}); err != nil {
		t.Fatalf("foreground BeforeAck: %v", err)
	}
	if err := os.WriteFile(req.Ready, []byte("ack"), 0600); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	t.Fatal("parent did not kill process after acknowledged write")
}
