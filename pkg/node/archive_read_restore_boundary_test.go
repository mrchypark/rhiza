//go:build rhiza_local_testhooks

package node

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// This single-node in-memory archive case does not install a newer checkpoint.
func TestArchiveRestoreDoesNotRegressConcurrentLinearizableRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	bucket := objstore.NewInMemBucket()
	archive := recovery.NewManager(bucket, "read-restore", 1)
	t.Cleanup(archive.Close)
	checkpoints := checkpoint.NewManager(bucket, "read-restore", t.TempDir(), 1)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(dir, "live.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := network.NewServer(core, material, "cluster", true, nil)
	t.Cleanup(server.Close)
	if _, err := server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := material.CheckpointFilesAt(ctx)
	if err != nil || index != 1 {
		t.Fatalf("checkpoint index=%d err=%v", index, err)
	}
	defer cleanup()
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "n1", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := checkpoints.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	root, err = checkpoints.OpenRoot(ctx, root.Index, root.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(1)
	if !ok {
		t.Fatal("missing prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(ctx context.Context, value quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(value.Index), value.RootHash, value.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	sealSlot, _, err := core.Propose(ctx, encoded)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(sealSlot)
	if !ok {
		t.Fatal("missing seal decision")
	}
	if err := archive.SyncThrough(ctx, core, sealSlot); err != nil {
		t.Fatal(err)
	}
	if err := archive.TrimThrough(ctx, quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: sealSlot}, decision); err != nil {
		t.Fatal(err)
	}
	write, err := server.Execute(ctx, network.ExecuteRequest{RequestID: "write", SQL: "INSERT INTO probe VALUES(1)"})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	n := &Node{config: &types.ExecutionConfig{NodeID: "n1", DataDir: dir}, core: core, material: material, server: server, archive: archive, checkpoints: checkpoints}
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	restore := localtesthooks.Set(func(event string) {
		if event == "network:read:after-barrier" {
			once.Do(func() { close(entered) })
			<-release
		}
	})
	t.Cleanup(restore)
	type result struct {
		response network.QueryResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := server.Query(ctx, network.QueryRequest{SQL: "SELECT count(*) FROM probe", Consistency: "linearizable"})
		done <- result{response, err}
	}()
	select {
	case <-entered:
	case got := <-done:
		t.Fatalf("query exited before barrier gate: %+v", got)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := n.restoreArchiveCatchUp(ctx); err != nil {
		t.Fatalf("actual archive catch-up: %v", err)
	}
	if tip := material.Tip(); tip != uint64(write.Slot) {
		t.Fatalf("restore changed applied tip: got %d want %d", tip, write.Slot)
	}
	releaseGate()
	select {
	case got := <-done:
		if got.err != nil || got.response.AppliedSlot != uint64(write.Slot) || len(got.response.Rows) != 1 || got.response.Rows[0][0] != int64(1) {
			t.Fatalf("stale query: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
