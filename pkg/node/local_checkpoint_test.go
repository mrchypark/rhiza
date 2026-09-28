package node

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type observedDoneContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (ctx *observedDoneContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}

func TestLocalCheckpointPublishAndOpenExact(t *testing.T) {
	dataDir := t.TempDir()
	config := &types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir}
	identity, err := openLocalCheckpointIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := t.TempDir()
	sqlite := filepath.Join(sourceDir, "sqlite.capture")
	graph := filepath.Join(sourceDir, "graph.capture")
	if err := os.WriteFile(sqlite, []byte("sqlite snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(graph, []byte("graph snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	var prefix [32]byte
	prefix[0] = 7
	descriptor, err := identity.Publish(context.Background(), []materializer.CheckpointFile{
		{Role: materializer.CheckpointSQLite, Path: sqlite},
		{Role: materializer.CheckpointGraphData, Path: graph},
	}, 19, prefix, 3)
	if err != nil {
		t.Fatal(err)
	}
	rootBytes, err := hex.DecodeString(descriptor.RootHash)
	if err != nil || len(rootBytes) != 32 {
		t.Fatal(err)
	}
	var root [32]byte
	copy(root[:], rootBytes)
	opened, files, err := identity.OpenExact(root, 19, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if opened.RootHash != descriptor.RootHash || len(files) != 2 {
		t.Fatalf("opened root=%s files=%d", opened.RootHash, len(files))
	}
	expected := quepaxa.LocalCheckpointRoot{RootHash: root, Index: 19, PrefixHash: prefix, ConfigID: 3}
	if _, _, err := identity.OpenExpected(expected); err != nil {
		t.Fatalf("complete WAL authority tuple rejected: %v", err)
	}
	expected.ConfigID++
	if _, _, err := identity.OpenExpected(expected); err == nil {
		t.Fatal("descriptor accepted a mismatched WAL configuration authority")
	}
	expected.ConfigID = 3
	expected.PrefixHash[1] = 1
	if _, _, err := identity.OpenExpected(expected); err == nil {
		t.Fatal("descriptor accepted a mismatched WAL prefix authority")
	}
	reused, err := identity.Publish(context.Background(), []materializer.CheckpointFile{
		{Role: materializer.CheckpointSQLite, Path: sqlite},
		{Role: materializer.CheckpointGraphData, Path: graph},
	}, 19, prefix, 3)
	if err != nil || reused.RootHash != descriptor.RootHash {
		t.Fatalf("identical published checkpoint was not safely reusable: root=%s err=%v", reused.RootHash, err)
	}
}

func TestLocalCheckpointRejectsLegacyDataWithoutPolicy(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "sqlite.db"), []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := openLocalCheckpointIdentity(&types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir})
	if err == nil {
		t.Fatal("legacy Local data without policy was accepted")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "local-store.json")); !os.IsNotExist(err) {
		t.Fatalf("rejected legacy data was changed: policy stat error=%v", err)
	}
}

func TestLocalCheckpointOpenRejectsTamperedFile(t *testing.T) {
	dataDir := t.TempDir()
	identity, err := openLocalCheckpointIdentity(&types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir})
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := t.TempDir()
	sqlite := filepath.Join(sourceDir, "sqlite.capture")
	graph := filepath.Join(sourceDir, "graph.capture")
	_ = os.WriteFile(sqlite, []byte("sqlite"), 0600)
	_ = os.WriteFile(graph, []byte("graph"), 0600)
	var prefix [32]byte
	prefix[0] = 1
	descriptor, err := identity.Publish(context.Background(), []materializer.CheckpointFile{{Role: materializer.CheckpointSQLite, Path: sqlite}, {Role: materializer.CheckpointGraphData, Path: graph}}, 1, prefix)
	if err != nil {
		t.Fatal(err)
	}
	rootBytes, err := hex.DecodeString(descriptor.RootHash)
	if err != nil || len(rootBytes) != 32 {
		t.Fatal(err)
	}
	var root [32]byte
	copy(root[:], rootBytes)
	if err := os.WriteFile(filepath.Join(identity.rootDir, descriptor.RootHash, "sqlite.db"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := identity.OpenExact(root, 1, prefix); err == nil {
		t.Fatal("tampered checkpoint file was accepted")
	}
}

func TestLocalCheckpointCollectValidatesBeforeDeleting(t *testing.T) {
	dataDir := t.TempDir()
	identity, err := openLocalCheckpointIdentity(&types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir})
	if err != nil {
		t.Fatal(err)
	}
	publish := func(slot uint64, suffix string) localCheckpointDescriptor {
		t.Helper()
		dir := t.TempDir()
		sqlite, graph := filepath.Join(dir, "sqlite"), filepath.Join(dir, "graph")
		if err := os.WriteFile(sqlite, []byte("sqlite "+suffix), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(graph, []byte("graph "+suffix), 0600); err != nil {
			t.Fatal(err)
		}
		var prefix [32]byte
		prefix[0] = byte(slot)
		descriptor, err := identity.Publish(context.Background(), []materializer.CheckpointFile{{Role: materializer.CheckpointSQLite, Path: sqlite}, {Role: materializer.CheckpointGraphData, Path: graph}}, slot, prefix)
		if err != nil {
			t.Fatal(err)
		}
		return descriptor
	}
	live, orphan := publish(1, "live"), publish(2, "orphan")
	liveRoot, _ := hex.DecodeString(live.RootHash)
	var root [32]byte
	copy(root[:], liveRoot)
	var prefix [32]byte
	prefix[0] = 1
	roots := []quepaxa.LocalCheckpointRoot{{RootHash: root, Index: 1, PrefixHash: prefix}}
	if err := identity.Collect(roots); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(identity.rootDir, orphan.RootHash)); !os.IsNotExist(err) {
		t.Fatalf("unreferenced root remains after successful collection: %v", err)
	}
	orphan = publish(3, "preserved")
	if err := os.WriteFile(filepath.Join(identity.rootDir, orphan.RootHash, "descriptor.json"), []byte("not canonical"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := identity.Collect(roots); err == nil {
		t.Fatal("malformed descriptor did not abort collection")
	}
	if _, err := os.Stat(filepath.Join(identity.rootDir, live.RootHash)); err != nil {
		t.Fatalf("collection deleted live root before validating all candidates: %v", err)
	}
	if _, err := os.Stat(filepath.Join(identity.rootDir, orphan.RootHash)); err != nil {
		t.Fatalf("collection deleted malformed candidate: %v", err)
	}
}

func TestResumePreparedLocalCheckpointUsesExistingSealPath(t *testing.T) {
	dataDir := t.TempDir()
	config := &types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir}
	identity, err := openLocalCheckpointIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(4 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "node", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "node"}}}, WAL: wal, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(dataDir, "sqlite.db"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if err := material.ConfigureLocalGraphNodePropertyIndexes(nil); err != nil {
		t.Fatal(err)
	}
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{4})
	if _, _, err := core.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	decisions, _, err := core.DecisionsFrom(1, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := material.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := material.CheckpointFilesAt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	prefix, ok := core.PrefixHash(1)
	if !ok || index != 1 {
		t.Fatalf("checkpoint index=%d Core prefix available=%v", index, ok)
	}
	descriptor, err := identity.Publish(context.Background(), files, index, prefix, core.ConfigIDForSlot(1))
	if err != nil {
		t.Fatal(err)
	}
	seal, err := sealFromLocalDescriptor(core, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, requested quepaxa.CheckpointSeal) error {
		_, _, err := identity.OpenExact(requested.RootHash, uint64(requested.Index), requested.PrefixHash)
		return err
	})
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(context.Background(), sealValue); err != nil || slot != 2 {
		t.Fatalf("pre-restart checkpoint seal slot=%d err=%v", slot, err)
	}
	server := network.NewServer(core, material, "cluster", true, nil)
	node := &Node{config: config, core: core, material: material, server: server, wal: wal, localStore: &identity}
	if err := node.resumePreparedLocalCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if floor, root, ok := core.RecoveryRoot(); !ok || floor != 1 || root != seal.RootHash {
		t.Fatalf("resumed floor=(%d,%x,%v), want (1,%x,true)", floor, root, ok, seal.RootHash)
	}
	if core.Tip() != 2 || material.Tip() != 2 {
		t.Fatalf("resumed state Core tip=%d materialized tip=%d, want both 2 without a replacement seal", core.Tip(), material.Tip())
	}
	server.Close()
}

func TestLocalStartupRestoreJournalGuardPreservesWAL(t *testing.T) {
	dataDir := t.TempDir()
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.Append(qlog.Entry{Slot: 1, Type: qlog.EntryProposal, Payload: []byte("preserve")}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	before, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	bytesBefore := wal.Bytes()
	journal := filepath.Join(dataDir, "sqlite.db.restore-state.json")
	if err := os.WriteFile(journal, []byte(`{"phase":"prepared"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := materializer.ValidateCleanRestoreState(filepath.Join(dataDir, "sqlite.db")); err == nil {
		t.Fatal("interrupted Local restore was not rejected before WAL mutation")
	}
	after, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if wal.Bytes() != bytesBefore || len(after) != len(before) || after[0].Hash != before[0].Hash {
		t.Fatalf("read-only restore guard changed WAL: bytes %d=>%d entries %d=>%d", bytesBefore, wal.Bytes(), len(before), len(after))
	}
	if _, err := os.Stat(journal); err != nil {
		t.Fatalf("restore journal was removed instead of preserved: %v", err)
	}
	if _, err := materializer.ValidateLocalGraphNodePropertyIndexes([]types.GraphNodePropertyIndex{{Label: "", Property: "p"}}); err == nil {
		t.Fatal("invalid effective Local graph policy was accepted")
	}
}

func TestReadonlyStartupValidatesNonLatestCheckpointRoot(t *testing.T) {
	dataDir := t.TempDir()
	config := &types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir}
	identity, err := openLocalCheckpointIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(8 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "node", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "node"}}}, WAL: wal, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(dataDir, "sqlite.db"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if err := material.ConfigureLocalGraphNodePropertyIndexes(nil); err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, requested quepaxa.CheckpointSeal) error {
		_, _, err := identity.OpenExact(requested.RootHash, uint64(requested.Index), requested.PrefixHash)
		return err
	})
	var first localCheckpointDescriptor
	for slot := uint64(1); slot <= 2; slot++ {
		value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{byte(slot)})
		if _, _, err := core.Propose(context.Background(), value); err != nil {
			t.Fatal(err)
		}
		decisions, _, err := core.DecisionsFrom(quepaxa.Slot(slot), 8)
		if err != nil {
			t.Fatal(err)
		}
		if err := material.ApplyBatch(context.Background(), decisions); err != nil {
			t.Fatal(err)
		}
		files, index, cleanup, err := material.CheckpointFilesAt(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		prefix, ok := core.PrefixHash(quepaxa.Slot(slot))
		if !ok || index != slot {
			cleanup()
			t.Fatalf("checkpoint index=%d prefix=%v, want %d", index, ok, slot)
		}
		descriptor, publishErr := identity.Publish(context.Background(), files, index, prefix, core.ConfigIDForSlot(quepaxa.Slot(slot)))
		cleanup()
		if publishErr != nil {
			t.Fatal(publishErr)
		}
		if slot == 1 {
			first = descriptor
		}
		seal, err := sealFromLocalDescriptor(core, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(identity.rootDir, first.RootHash, "sqlite.db"), []byte("corrupt historical root"), 0600); err != nil {
		t.Fatal(err)
	}
	bytesBefore := wal.Bytes()
	if err := (&Node{localStore: &identity}).validateLocalRoots(core); err == nil {
		t.Fatal("read-only startup accepted a corrupt non-latest checkpoint root")
	}
	if wal.Bytes() != bytesBefore {
		t.Fatalf("read-only root validation changed WAL bytes: %d=>%d", bytesBefore, wal.Bytes())
	}
}

func TestLocalReclaimQuiesceCancellationIsCleanRefusal(t *testing.T) {
	dataDir := t.TempDir()
	config := &types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir}
	identity, err := openLocalCheckpointIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(dataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(4 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "node", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "node"}}}, WAL: wal, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(dataDir, "sqlite.db"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := network.NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	proposalDone := make(chan error, 1)
	go func() {
		_, err := server.ProposeControl(context.Background(), types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{2}))
		proposalDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not reach durability barrier")
	}
	bytesBefore := wal.Bytes()
	node := &Node{config: config, core: core, material: material, server: server, wal: wal, localStore: &identity}
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := &observedDoneContext{Context: baseCtx, observed: make(chan struct{})}
	maintenanceDone := make(chan error, 1)
	go func() { maintenanceDone <- node.reclaimLocalCheckpoint(ctx) }()
	select {
	case <-ctx.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance did not enter Quiesce")
	}
	cancel()
	if err := <-maintenanceDone; !errors.Is(err, network.ErrLocalMaintenanceRefused) {
		t.Fatalf("quiesce cancellation=%v, want clean maintenance refusal", err)
	}
	if !server.Ready() || wal.Bytes() != bytesBefore {
		t.Fatalf("pre-mutation cancellation poisoned or wrote: ready=%v WAL=%d/%d", server.Ready(), wal.Bytes(), bytesBefore)
	}
	close(release)
	if err := <-proposalDone; err != nil {
		t.Fatal(err)
	}
}
