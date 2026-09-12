package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestAbortPendingAdditionPreservesOldConfigurationAcrossCompactionAndRestart(t *testing.T) {
	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	dir := t.TempDir()
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	wals := make(map[NodeID]*qlog.WAL, len(initial.Members))
	for _, member := range initial.Members {
		wal, err := qlog.Open(filepath.Join(dir, string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		core, err := New(Config{NodeID: member.ID, Cluster: initial, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		wals[member.ID] = wal
		transport.cores[member.ID] = core
	}
	t.Cleanup(func() {
		for _, wal := range wals {
			if wal != nil {
				_ = wal.Close()
			}
		}
	})
	core := transport.cores["a"]
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}
	freeze, err := core.BeginReconfiguration(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, pending := core.PendingReconfiguration(); pending || core.ConfigID() != initial.ConfigID || !sameCluster(core.CurrentCluster(), initial) {
		t.Fatal("abort changed active membership")
	}
	terminal, ok := core.CertifiedValue(freeze + 16)
	if !ok {
		t.Fatal("missing abort terminal")
	}
	control, configured, err := decodeReconfiguration(terminal.Value)
	if err != nil || !configured || !control.Terminal || !control.Abort {
		t.Fatalf("terminal=%+v configured=%v err=%v", control, configured, err)
	}
	history, err := core.MembershipHistory()
	if err != nil || len(history.Transitions) != 0 {
		t.Fatalf("aborted transition entered history: %+v err=%v", history, err)
	}
	abort := ConfigTransition{Freeze: mustCertified(t, core, freeze), Terminal: terminal}
	if verified, err := validateMembershipHistory(initial, MembershipRecord{Genesis: initial, Abort: &abort}); err != nil || verified.ConfigID() != initial.ConfigID {
		t.Fatalf("abort proof validation err=%v config=%d", err, verified.ConfigID())
	}
	tampered := cloneMembershipRecord(MembershipRecord{Genesis: initial, Abort: &abort})
	tampered.Abort.Terminal.Value[0] ^= 1
	if _, err := validateMembershipHistory(initial, tampered); err == nil {
		t.Fatal("tampered abort proof validated")
	}
	if slot, gotTarget, ok := core.LastReconfigurationAbort(); !ok || slot != freeze+16 || !sameCluster(gotTarget, target) {
		t.Fatalf("abort outcome slot=%d target=%+v ok=%v", slot, gotTarget, ok)
	}
	if _, _, err := core.Propose(context.Background(), []byte("after-abort")); err != nil {
		t.Fatal(err)
	}
	index := core.Tip()
	membership, err := core.CheckpointMembership(index)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(index)
	next, following, err := core.CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{ConfigID: initial.ConfigID, Index: index, RootHash: sha256.Sum256([]byte("abort-root")), StateHash: sha256.Sum256([]byte("abort-state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	for _, id := range []NodeID{"a", "b", "c"} {
		member := transport.cores[id]
		member.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		if err := member.PrepareCheckpoint(context.Background(), seal); err != nil {
			t.Fatalf("prepare %s: %v", id, err)
		}
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), encoded); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(index, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	if err := wals["a"].Close(); err != nil {
		t.Fatal(err)
	}
	wals["a"] = nil
	reopened, err := qlog.Open(filepath.Join(dir, "a"))
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := New(Config{NodeID: "a", Cluster: initial, WAL: reopened, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	wals["a"] = reopened
	transport.cores["a"] = restarted
	if restarted.CompactionFloor() != index || restarted.ConfigID() != initial.ConfigID {
		t.Fatalf("restart floor=%d config=%d", restarted.CompactionFloor(), restarted.ConfigID())
	}
	if slot, gotTarget, ok := restarted.LastReconfigurationAbort(); !ok || slot != freeze+16 || !sameCluster(gotTarget, target) {
		t.Fatalf("restarted abort outcome slot=%d target=%+v ok=%v", slot, gotTarget, ok)
	}
}

func TestAbortReconfigurationRejectsRemoval(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	if _, err := core.BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(context.Background()); err == nil {
		t.Fatal("aborted a pending removal")
	}
	if _, pending := core.PendingReconfiguration(); !pending {
		t.Fatal("failed abort cleared pending removal")
	}
}

func TestBeginReconfigurationAtChecksDurableAbortRevision(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	lost := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}
	if _, err := core.BeginReconfiguration(context.Background(), lost); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	revision, _, ok := core.LastReconfigurationAbort()
	if !ok {
		t.Fatal("missing abort revision")
	}
	next := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := core.BeginReconfigurationAt(context.Background(), next, 0); err == nil {
		t.Fatal("stale abort revision started a reconfiguration")
	}
	core.mu.Lock()
	core.durable[revision] = false
	core.mu.Unlock()
	if _, err := core.BeginReconfigurationAt(context.Background(), next, revision); err == nil {
		t.Fatal("undurable abort revision started a reconfiguration")
	}
	core.mu.Lock()
	core.durable[revision] = true
	core.mu.Unlock()
	if _, err := core.BeginReconfigurationAt(context.Background(), next, revision); err != nil {
		t.Fatal(err)
	}
}

func TestGuardedFinishAndAbortRejectStaleReconfigurationRound(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	first := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost-a", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}
	if _, err := core.BeginReconfigurationAt(context.Background(), first, 0); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfigurationAt(context.Background(), first, 0); err != nil {
		t.Fatal(err)
	}
	revision, _, ok := core.LastReconfigurationAbort()
	if !ok {
		t.Fatal("missing first abort revision")
	}
	second := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost-b", WALIdentity: "1111111111111111111111111111111111111111111111111111111111111111"}}}
	if _, err := core.BeginReconfigurationAt(context.Background(), second, revision); err != nil {
		t.Fatal(err)
	}
	tip := core.Tip()
	if err := core.AbortReconfigurationAt(context.Background(), first, 0); err == nil {
		t.Fatal("stale abort certified the new frozen target")
	}
	if err := core.FinishReconfigurationAt(context.Background(), first, 0); err == nil {
		t.Fatal("stale finish certified the new frozen target")
	}
	if core.Tip() != tip {
		t.Fatalf("stale request drained the new round: tip=%d want %d", core.Tip(), tip)
	}
	if pending, ok := core.PendingReconfiguration(); !ok || !sameCluster(pending, second) {
		t.Fatalf("pending target=%+v ok=%v", pending, ok)
	}
	if err := core.AbortReconfigurationAt(context.Background(), second, revision); err != nil {
		t.Fatalf("matching abort round rejected: %v", err)
	}
}

func TestSuccessfulTransitionClearsAbortRevision(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	lost := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}
	if _, err := core.BeginReconfiguration(context.Background(), lost); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	next := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := core.BeginReconfiguration(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := core.LastReconfigurationAbort(); ok {
		t.Fatal("successful configuration transition retained abort revision")
	}
	if got := core.CurrentCluster(); !sameCluster(got, next) {
		t.Fatalf("cluster=%+v want=%+v", got, next)
	}
}

func TestValidateReconfigurationTargetRejectsRetiredIdentity(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	if err := core.ValidateReconfigurationTarget(Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := core.BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := core.ValidateReconfigurationTarget(Cluster{ConfigID: 3, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}); err == nil {
		t.Fatal("retired identity passed journal validation")
	}
}

func TestWALIdentityExposesExistingBootstrapBinding(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	identity := make([]byte, 32)
	identity[0] = 1
	if err := wal.BindIdentity(identity); err != nil {
		t.Fatal(err)
	}
	core, err := New(Config{NodeID: "a", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := core.WALIdentity(), hex.EncodeToString(identity); got != want {
		t.Fatalf("WAL identity=%q want=%q", got, want)
	}
}

func mustCertified(t *testing.T, core *Core, slot Slot) DecidedValue {
	t.Helper()
	value, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatalf("missing certified slot %d", slot)
	}
	return value
}
