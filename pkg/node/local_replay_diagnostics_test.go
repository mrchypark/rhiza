package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestLocalReplayDiagnosticsActualDurabilityAndMaterialization(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	if slot, _, err := core.Propose(context.Background(), value); err != nil || slot != 1 {
		t.Fatalf("certified decision slot=%d error=%v", slot, err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	n := &Node{core: core, material: material, config: &types.ExecutionConfig{NodeID: "n1"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if from, err := n.replayLocalDecisionsFrom(ctx); from != 1 || err != context.Canceled {
		t.Fatalf("common replay changed error identity: from=%d error=%v", from, err)
	}
	err = n.catchUpPeer(ctx, nil, "peer")
	var outer *catchUpDiagnosticError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &outer) || outer.cause != context.Canceled || outer.from != 1 || outer.stage != "local-replay" || outer.replayPhase != "durability" || material.Tip() != 0 {
		t.Fatalf("durability outer=%+v error=%v", outer, err)
	}
	if err := material.Close(); err != nil {
		t.Fatal(err)
	}
	if from, err := n.replayLocalDecisionsFrom(context.Background()); from != 1 || err != sql.ErrConnDone {
		t.Fatalf("common replay changed error identity: from=%d error=%v", from, err)
	}
	err = n.catchUpPeer(context.Background(), nil, "peer")
	if !errors.Is(err, sql.ErrConnDone) || !errors.As(err, &outer) || outer.cause != sql.ErrConnDone || outer.from != 1 || outer.stage != "local-replay" || outer.replayPhase != "materialize" {
		t.Fatalf("materialize outer=%+v error=%v", outer, err)
	}
	if !errors.Is(err, sql.ErrConnDone) {
		t.Fatal("cause identity lost")
	}
	logOperationSyncFailure("private-node", 0, []catchUpAttempt{{source: "private-peer", err: err}}, func(args ...any) {
		line := fmt.Sprint(args...)
		if !strings.Contains(line, "replay_phase=materialize") || !strings.Contains(line, "cause_sql_conn_done=true") || !strings.Contains(line, "apply_site=writer") || strings.Contains(line, "private-node") || strings.Contains(line, "private-peer") || strings.Contains(line, sql.ErrConnDone.Error()) {
			t.Fatalf("unsafe or missing diagnostic: %s", line)
		}
	})
}

func TestLocalReplayDecisionReadFailureReachesCatchUpLog(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})); err != nil || slot != 1 {
		t.Fatalf("initial decision slot=%d error=%v", slot, err)
	}
	bucket := objstore.NewInMemBucket()
	manager := checkpoint.NewManager(bucket, "diagnostic", t.TempDir(), 1)
	path := filepath.Join(t.TempDir(), "checkpoint.sqlite")
	policySnapshot(t, path, "checkpoint")
	claim, err := manager.AcquirePublisherClaim(ctx, "test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: path}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(1)
	if !ok {
		t.Fatal("missing certified prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(1, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "lagging.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	n := &Node{core: core, material: material, config: &types.ExecutionConfig{NodeID: "n1"}}
	err = n.catchUpPeer(ctx, nil, "peer")
	var outer *catchUpDiagnosticError
	if !errors.Is(err, quepaxa.ErrCompacted) || !errors.As(err, &outer) || outer.from != 1 || outer.stage != "local-replay" || outer.replayPhase != "decision-read" || material.Tip() != 0 {
		t.Fatalf("decision read outer=%+v tip=%d error=%v", outer, material.Tip(), err)
	}
	logOperationSyncFailure("node", 0, []catchUpAttempt{{source: "peer", err: err}}, func(args ...any) {
		line := fmt.Sprint(args...)
		if !strings.Contains(line, "error_class=compacted") || !strings.Contains(line, "stage=local-replay from=1") || !strings.Contains(line, "replay_phase=decision-read") {
			t.Fatalf("missing actual decision-read diagnostic: %s", line)
		}
	})
}

func TestLocalReplayDiagnosticsRejectsUntrustedPhaseAndFetchCarryover(t *testing.T) {
	for _, item := range []catchUpDiagnosticError{
		{stage: "local-replay", replayPhase: "secret-data\ninjected-private-record", cause: context.Canceled},
		{stage: "fetch", replayPhase: "materialize", cause: context.Canceled},
		{stage: "apply", replayPhase: "materialize", cause: context.Canceled},
	} {
		logOperationSyncFailure("node", 0, []catchUpAttempt{{source: "peer", err: &item}}, func(args ...any) {
			line := fmt.Sprint(args...)
			if !strings.Contains(line, "replay_phase=unknown") || strings.Contains(line, "secret-data") || strings.Contains(line, "injected-private-record") || strings.ContainsAny(line, "\r\n") || len(line) > 1024 {
				t.Fatalf("unexpected phase: %s", line)
			}
		})
	}
}

func TestLocalReplayPhaseLocalSkipsDurabilityAndFetchDoesNotInheritPhase(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := wal.SetMaxBytes(16 << 20); err != nil {
		t.Fatal(err)
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: cluster, WAL: wal, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})); err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "local.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	n := &Node{core: core, material: material, config: &types.ExecutionConfig{NodeID: "n1"}}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	phase := "unset"
	from, err := n.replayLocalDecisionsToObserved(canceled, 0, &phase)
	if from != 1 || !errors.Is(err, context.Canceled) || phase != "materialize" || material.Tip() != 0 {
		t.Fatalf("Local should skip durability: from=%d phase=%s tip=%d err=%v", from, phase, material.Tip(), err)
	}
	transport := network.NewTransport("cluster", "n1", &cluster, "test-token")
	t.Cleanup(func() { _ = transport.Close() })
	err = n.catchUpPeer(ctx, transport, "absent-peer")
	var outer *catchUpDiagnosticError
	if err == nil || !errors.As(err, &outer) || outer.stage != "fetch" || outer.from != 2 || material.Tip() != 1 {
		t.Fatalf("successful replay then fetch: outer=%+v tip=%d err=%v", outer, material.Tip(), err)
	}
	logOperationSyncFailure("n1", 0, []catchUpAttempt{{source: "absent-peer", err: err}}, func(args ...any) {
		line := fmt.Sprint(args...)
		if !strings.Contains(line, "stage=fetch from=2") || !strings.Contains(line, "replay_phase=unknown") || !strings.Contains(line, "apply_site=unknown") {
			t.Fatalf("fetch inherited replay phase: %s", line)
		}
	})
}

func TestLocalReplayPhaseRestoreSuffixLaterPageFailureCursor(t *testing.T) {
	for _, lastApplied := range []uint64{1, 256} {
		t.Run(fmt.Sprintf("after-%d", lastApplied), func(t *testing.T) {
			testLocalReplayPhaseRestoreSuffixFailureCursor(t, lastApplied)
		})
	}
}

func testLocalReplayPhaseRestoreSuffixFailureCursor(t *testing.T, lastApplied uint64) {
	t.Helper()
	ctx := context.Background()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: cluster, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	for core.Tip() < quepaxa.Slot(lastApplied+1) {
		if _, _, err := core.Propose(ctx, value); err != nil {
			t.Fatalf("proposal at tip %d: %v", core.Tip(), err)
		}
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "suffix.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	n := &Node{core: core, material: material, localRestoreSuffixPending: true, config: &types.ExecutionConfig{NodeID: "n1"}}
	oldHook := localRestoreSuffixCrashBoundary
	defer func() { localRestoreSuffixCrashBoundary = oldHook }()
	var observed []uint64
	localRestoreSuffixCrashBoundary = func(slot uint64) {
		observed = append(observed, slot)
		if slot == lastApplied {
			if err := material.Close(); err != nil {
				t.Errorf("close materializer: %v", err)
			}
		}
	}
	err = n.catchUpPeer(ctx, nil, "peer")
	var outer *catchUpDiagnosticError
	if !errors.Is(err, sql.ErrConnDone) || !errors.As(err, &outer) || outer.cause != sql.ErrConnDone || outer.stage != "local-replay" || outer.replayPhase != "materialize" || outer.from != quepaxa.Slot(lastApplied+1) {
		t.Fatalf("later page failure outer=%+v err=%v", outer, err)
	}
	if uint64(len(observed)) != lastApplied || observed[len(observed)-1] != lastApplied {
		t.Fatalf("suffix apply order count=%d", len(observed))
	}
}
