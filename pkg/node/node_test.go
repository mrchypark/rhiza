package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/ncruces/go-sqlite3/driver"
	objstore "github.com/thanos-io/objstore"
)

func TestCompactedPeerDoesNotOverrideUsableQuorumSuffix(t *testing.T) {
	applied := quepaxa.Slot(10)
	best := &network.DecisionsResponse{Tip: 11}
	if !hasUsablePeerSuffix(applied, 2, 2, best) {
		t.Fatal("usable quorum suffix was rejected")
	}
	best.Tip = applied
	if hasUsablePeerSuffix(applied, 2, 2, best) {
		t.Fatal("stale peer response was accepted as a suffix")
	}
	best.Tip = applied + 1
	if hasUsablePeerSuffix(applied, 2, 3, best) {
		t.Fatal("non-quorum suffix was accepted")
	}
}

func TestOperationSyncPeerPermutationIsDeterministicAndBalanced(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	first := syncSources("n1", members, 0)
	if len(first) != 2 {
		t.Fatal("no sync peer")
	}
	second := syncSources("n1", members, 1)
	again := syncSources("n1", members, 0)
	if !slices.Equal(first, again) || first[0] == second[0] || first[0] == "n1" || second[0] == "n1" || first[1] != second[0] {
		t.Fatalf("sources=%v,%v again=%v", first, second, again)
	}
	for round := uint64(0); round < 32; round++ {
		delay := syncInterval("n1", round)
		if delay < 900*time.Millisecond || delay > 1100*time.Millisecond {
			t.Fatalf("round %d delay=%v", round, delay)
		}
	}
}

func TestTransientCatchUpFailureKeepsReadyNodeServing(t *testing.T) {
	n := &Node{}
	n.ready.Store(true)
	n.observeCatchUp(errors.New("temporary peer timeout"))
	if !n.ready.Load() {
		t.Fatal("transient catch-up failure cleared readiness")
	}
}

func TestRestoreArchiveCatchUpMarksNodeNotReadyBeforeValidation(t *testing.T) {
	n := &Node{}
	n.ready.Store(true)
	if err := n.restoreArchiveCatchUp(context.Background()); err == nil {
		t.Fatal("restore unexpectedly succeeded")
	}
	if n.ready.Load() {
		t.Fatal("restore left node ready before recovery could be validated")
	}
}

func TestCompactedWakeMarksNodeNotReadyAndCoalesces(t *testing.T) {
	n := &Node{catchUpWake: make(chan struct{}, 1)}
	n.ready.Store(true)
	n.wakeCatchUp()
	n.wakeCatchUp()
	if n.ready.Load() {
		t.Fatal("compacted foreground path left node ready")
	}
	select {
	case <-n.catchUpWake:
	default:
		t.Fatal("compacted foreground path did not wake catch-up worker")
	}
	select {
	case <-n.catchUpWake:
		t.Fatal("compacted foreground wake was not coalesced")
	default:
	}
}

func TestArchiveRecoveryRequiredBelowTrimmedBase(t *testing.T) {
	if !archiveRecoveryRequired(148, 149) {
		t.Fatal("lagging local tip did not require checkpoint recovery")
	}
	if archiveRecoveryRequired(149, 149) || archiveRecoveryRequired(150, 149) || archiveRecoveryRequired(0, 0) {
		t.Fatal("retained archive suffix incorrectly required checkpoint recovery")
	}
}

func TestCheckpointGCWaitsForFirstArchiveBase(t *testing.T) {
	floor, ready := advanceArchiveFloor(0, 0, false)
	if ready || floor != 0 {
		t.Fatalf("no-base floor=(%d,%t), want GC blocked", floor, ready)
	}
	floor, ready = advanceArchiveFloor(floor, 7, true)
	if !ready || floor != 7 {
		t.Fatalf("first-base floor=(%d,%t), want (7,true)", floor, ready)
	}
	floor, ready = advanceArchiveFloor(floor, 5, true)
	if !ready || floor != 7 {
		t.Fatalf("stale-base floor=(%d,%t), want monotonic 7", floor, ready)
	}
}

func TestCertifiedCheckpointCompactionIsSingleFlight(t *testing.T) {
	n := &Node{}
	n.compactionMu.Lock()
	done := make(chan error, 1)
	go func() { done <- n.compactCertifiedCheckpoint(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("compaction entered while another transition held the sequence lock: %v", err)
	default:
	}
	n.compactionMu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("compaction did not resume after sequence lock release")
	}
}

func TestCertifiedCheckpointPublicationWaitsForArchiveSync(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	archive := recovery.NewManager(bucket, "cluster", 1)
	defer archive.Close()
	checkpoints := checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("decision")); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "sqlite.db")
	policySnapshot(t, file, "checkpoint")
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := checkpoints.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, 1)
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
	prefix, _ := core.PrefixHash(1)
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	n := &Node{archive: archive, checkpoints: checkpoints, core: core}
	if err := n.publishCertifiedCheckpoint(canceled, root); err == nil {
		t.Fatal("archive sync unexpectedly succeeded")
	}
	if current := checkpoints.Latest(); current != nil {
		t.Fatalf("CURRENT advanced despite failed archive sync: index=%d", current.Index)
	}
	// Another cold-start reader may still pin history. Publication must not
	// attempt compaction or make completion depend on that reader finishing.
	snapshot, err := archive.BeginRecoverySnapshot(ctx, "other-startup", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close(ctx)
	if err := n.publishCertifiedCheckpoint(ctx, root); err != nil {
		t.Fatal(err)
	}
	if checkpoints.Latest() == nil || core.CompactionFloor() != 0 {
		t.Fatal("publication must preserve pinned history")
	}
}

func TestStartupRecoveryPinProtectsSelectedRootFromGC(t *testing.T) {
	ctx := context.Background()
	manager := checkpoint.NewManager(objstore.NewInMemBucket(), "cluster", t.TempDir(), 1)
	create := func(index uint64, contents string) *checkpoint.Checkpoint {
		file := filepath.Join(t.TempDir(), "sqlite.db")
		policySnapshot(t, file, contents)
		claim, err := manager.AcquirePublisherClaim(ctx, "test", index-1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		root, err := manager.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, index)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
		opened, err := manager.OpenRoot(ctx, root.Index, root.RootHash)
		if err != nil {
			t.Fatal(err)
		}
		return opened
	}
	selected := create(7, "selected")
	newer := create(8, "newer")
	if err := manager.PromoteCertifiedCurrent(ctx, newer); err != nil {
		t.Fatal(err)
	}
	guard, err := newStartupRecoveryGuard(ctx, nil, "startup-test")
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	pinned, err := guard.PinRoot(guard.Context(), manager, selected, guard.Owner())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.GarbageCollect(ctx, map[[32]byte]struct{}{}, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.DownloadRootFiles(guard.Context(), pinned.Index, pinned.RootHash, t.TempDir()); err != nil {
		t.Fatalf("GC deleted startup recovery root: %v", err)
	}
}

func TestMultiNodeFilesystemObjectStoreFailsClosed(t *testing.T) {
	for name, configure := range map[string]func(*types.ExecutionConfig){
		"missing": func(*types.ExecutionConfig) {},
		"filesystem": func(config *types.ExecutionConfig) {
			config.ObjStoreProvider = "filesystem"
			config.ObjStoreDir = t.TempDir()
		},
		"implicit filesystem directory": func(config *types.ExecutionConfig) { config.ObjStoreDir = t.TempDir() },
		"S3 without bucket":             func(config *types.ExecutionConfig) { config.ObjStoreProvider = "s3" },
	} {
		t.Run(name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "data")
			config := &types.ExecutionConfig{
				NodeID: "n1", DataDir: dataDir, PeerToken: "peer-token",
				Members: []types.NodeConfig{{ID: "n1", PublicKey: quepaxa.PublicKey{1}}, {ID: "n2", PublicKey: quepaxa.PublicKey{2}}, {ID: "n3", PublicKey: quepaxa.PublicKey{3}}},
			}
			configure(config)
			err := New(config).Open(context.Background())
			if err == nil || (!strings.Contains(err.Error(), "shared object storage") && !strings.Contains(err.Error(), "object-store bucket is required")) {
				t.Fatalf("error=%v, want object-store rejection", err)
			}
			if _, statErr := os.Stat(filepath.Join(dataDir, "qlog")); !os.IsNotExist(statErr) {
				t.Fatalf("invalid config created qlog: %v", statErr)
			}
		})
	}
}

func TestMultiNodePeerIdentityIsRequiredAndDistinctFromAdmin(t *testing.T) {
	for name, config := range map[string]*types.ExecutionConfig{
		"missing peer token": {NodeID: "n1", Members: []types.NodeConfig{{ID: "n1", PublicKey: quepaxa.PublicKey{1}}, {ID: "n2", PublicKey: quepaxa.PublicKey{2}}}},
		"admin token reused": {NodeID: "n1", AdminToken: "shared", PeerToken: "shared",
			Members: []types.NodeConfig{{ID: "n1", PublicKey: quepaxa.PublicKey{1}}, {ID: "n2", PublicKey: quepaxa.PublicKey{2}}}},
		"missing public key": {NodeID: "n1", PeerToken: "voter-1", Members: []types.NodeConfig{{ID: "n1", PublicKey: quepaxa.PublicKey{1}}, {ID: "n2"}}},
	} {
		t.Run(name, func(t *testing.T) {
			config.DataDir = filepath.Join(t.TempDir(), "data")
			if err := New(config).Open(context.Background()); err == nil {
				t.Fatal("invalid peer credentials were accepted")
			}
			if _, err := os.Stat(filepath.Join(config.DataDir, "qlog")); !os.IsNotExist(err) {
				t.Fatalf("invalid credentials created state: %v", err)
			}
		})
	}
}

func TestObjectStoreConfigMatrix(t *testing.T) {
	members := []types.NodeConfig{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	for _, test := range []struct {
		name    string
		members []types.NodeConfig
		apply   func(*types.ExecutionConfig)
		wantErr bool
	}{
		{name: "single filesystem", members: members[:1], apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreDir = "filesystem", t.TempDir()
		}},
		{name: "multi filesystem", members: members, apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreDir = "filesystem", t.TempDir()
		}, wantErr: true},
		{name: "multi implicit directory", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreDir = t.TempDir() }, wantErr: true},
		{name: "multi S3", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreProvider, config.ObjStoreBucket = "s3", "rhiza" }},
		{name: "multi S3 without bucket", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreProvider = "s3" }, wantErr: true},
		{name: "multi GCS", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreProvider, config.ObjStoreBucket = "gcs", "rhiza" }},
		{name: "multi Azure", members: members, apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreBucket, config.ObjStoreAzureStorageAccount = "azure", "rhiza", "account"
		}},
		{name: "multi Azure without account", members: members, apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreBucket = "azure", "rhiza"
		}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := &types.ExecutionConfig{Members: test.members}
			test.apply(config)
			_, err := validateObjectStoreConfig(config)
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestReadAdmissionConfig(t *testing.T) {
	for name, config := range map[string]*types.ExecutionConfig{
		"defaults":                {},
		"configured":              {MaxConcurrentReads: 4, MaxLongPollReads: 1},
		"long poll disabled":      {MaxConcurrentReads: 1},
		"negative total":          {MaxConcurrentReads: -1},
		"negative long poll":      {MaxConcurrentReads: 1, MaxLongPollReads: -1},
		"missing total":           {MaxLongPollReads: 1},
		"long poll exceeds total": {MaxConcurrentReads: 1, MaxLongPollReads: 2},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateReadAdmissionConfig(config)
			valid := name == "defaults" || name == "configured" || name == "long poll disabled"
			if (err == nil) != valid {
				t.Fatalf("error=%v valid=%t", err, valid)
			}
		})
	}
}

func policySnapshot(t testing.TB, path, contents string) {
	t.Helper()
	db, err := driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT); INSERT INTO _rhiza_meta VALUES('sql_execution_policy','` + sqlpolicy.Marker() + `'); CREATE TABLE payload(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO payload VALUES(?)`, contents); err != nil {
		t.Fatal(err)
	}
}

func TestLocalModeHasNoNetwork(t *testing.T) {
	n := New(&types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir()})
	if err := n.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	if n.peer != nil || n.transport != nil || n.catchUp != nil || !n.ready.Load() {
		t.Fatal("local mode created networking or is not ready")
	}
	if err := n.Start(context.Background()); err == nil {
		t.Fatal("local HTTP listener allowed")
	}
}

func TestLocalModeRecoversUndecidedRecorderState(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir()}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "recorded", SQL: "CREATE TABLE recorded(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = n.core.Record(ctx, quepaxa.RecordRequest{Slot: 1, Step: 4, Proposal: quepaxa.Proposal{
		Priority: quepaxa.Priority{1}, ProposerID: "local", Hash: sha256.Sum256(value), Value: value,
	}})
	if err != nil {
		n.Shutdown()
		t.Fatal(err)
	}
	if n.core.Tip() != 0 || n.core.RecorderTip() != 1 {
		n.Shutdown()
		t.Fatal("fixture already decided")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	if n.core.Tip() != 1 {
		t.Fatalf("recovered tip=%d", n.core.Tip())
	}
	if _, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT * FROM recorded", Consistency: "linearizable"}); err != nil {
		t.Fatal(err)
	}
}

func TestLocalLifecycleFailureReopensAndReplaysMultiCommandBatchExactlyOnce(t *testing.T) {
	ctx := context.Background()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir()}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	commands := []types.GraphCommand{
		{RequestID: "reopen-a", Cypher: `CREATE (:ReopenBatch {id: 'A'})`, Events: []types.GraphStreamEvent{{Stream: "reopen-events", Kind: "created", Payload: "A"}}},
		{RequestID: "reopen-b", Cypher: `CREATE (:ReopenBatch {id: 'B'})`, Events: []types.GraphStreamEvent{{Stream: "reopen-events", Kind: "created", Payload: "B"}}},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		_ = n.Shutdown()
		t.Fatal(err)
	}
	if slot, _, err := n.core.Propose(ctx, value); err != nil || slot != 1 {
		_ = n.Shutdown()
		t.Fatalf("seed batch slot=%d error=%v; want slot 1", slot, err)
	}
	if err := n.material.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = n.server.GraphQuery(ctx, network.GraphQueryRequest{Cypher: `MATCH (n:ReopenBatch) RETURN n.id`, Consistency: "linearizable"})
	if err == nil || n.core.Tip() != 1 {
		_ = n.Shutdown()
		t.Fatalf("core tip=%d error=%v; want apply failure for committed multi-command batch", n.core.Tip(), err)
	}
	if n.Ready() {
		_ = n.Shutdown()
		t.Fatal("failed Local lifecycle remained ready")
	}
	if _, err := n.server.GraphExecute(ctx, commands[0]); !errors.Is(err, network.ErrNotReady) {
		_ = n.Shutdown()
		t.Fatalf("same batch command before reopen error=%v, want ErrNotReady", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	if !n.Ready() || n.material.Tip() != 1 || n.core.Tip() != 1 {
		t.Fatalf("reopened readiness=%t core=%d material=%d", n.Ready(), n.core.Tip(), n.material.Tip())
	}
	for _, command := range commands {
		retry, err := n.server.GraphExecute(ctx, command)
		if err != nil {
			t.Fatalf("retry request %q: %v", command.RequestID, err)
		}
		if retry.Slot != 1 {
			t.Fatalf("retry request %q returned slot %d, want original slot 1", command.RequestID, retry.Slot)
		}
	}
	query, err := n.server.GraphQuery(ctx, network.GraphQueryRequest{Cypher: `MATCH (n:ReopenBatch) RETURN n.id`})
	if err != nil || len(query.Rows) != 2 {
		t.Fatalf("graph rows=%v error=%v; want both batch effects exactly once", query.Rows, err)
	}
	rowCounts := map[string]int{}
	for _, row := range query.Rows {
		if len(row) != 1 {
			t.Fatalf("graph row=%v; want one id column", row)
		}
		rowCounts[fmt.Sprint(row[0])]++
	}
	if rowCounts["A"] != 1 || rowCounts["B"] != 1 {
		t.Fatalf("graph row counts=%v; want A and B exactly once", rowCounts)
	}
	stream, err := n.server.GraphStreamRead(ctx, network.GraphStreamReadRequest{Stream: "reopen-events", Limit: 10})
	if err != nil || len(stream.Records) != 2 {
		t.Fatalf("event records=%v error=%v; want two events exactly once", stream.Records, err)
	}
	eventCounts := map[string]int{}
	for _, record := range stream.Records {
		eventCounts[fmt.Sprint(record.Payload)]++
	}
	if eventCounts["A"] != 1 || eventCounts["B"] != 1 {
		t.Fatalf("event counts=%v; want A and B exactly once", eventCounts)
	}
	if n.core.Tip() != 1 || n.material.Tip() != 1 {
		t.Fatalf("retry created another slot: core=%d material=%d", n.core.Tip(), n.material.Tip())
	}
}

func TestLocalModeRejectsConflictingConfiguration(t *testing.T) {
	cases := []types.ExecutionConfig{
		{BindAddr: "127.0.0.1:0"}, {PeerAddr: "127.0.0.1:0"},
		{Members: []quepaxa.Member{{ID: "local"}}}, {Learner: &quepaxa.Member{ID: "learner"}},
		{EnableReconfiguration: true}, {PeerToken: "secret"}, {AdminToken: "secret"},
		{ObjStoreProvider: "filesystem"}, {ObjStoreDir: "archive"}, {ObjStoreEndpoint: "localhost"}, {ObjStoreBucket: "archive"},
	}
	for _, config := range cases {
		config.Local, config.NodeID, config.DataDir = true, "local", filepath.Join(t.TempDir(), "absent")
		n := New(&config)
		if err := n.Open(context.Background()); err == nil {
			n.Shutdown()
			t.Fatalf("accepted conflicting config: %+v", config)
		}
		if _, err := os.Stat(config.DataDir); !os.IsNotExist(err) {
			t.Fatalf("created state for invalid config: %v", err)
		}
	}
}
