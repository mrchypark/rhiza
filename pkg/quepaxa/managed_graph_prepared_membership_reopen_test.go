package quepaxa

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// This verifies actual prepared markers at several membership epochs survive
// a genesis-config reopen after certified ADD and REMOVE decisions.
func TestPreparedCheckpointMarkersReopenAcrossCertifiedMembershipChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	genesis := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	cores := make(map[NodeID]*Core, 4)
	wals := make(map[NodeID]*qlog.WAL, 4)
	walDirs := make(map[NodeID]string, 4)
	closed := make(map[NodeID]bool, 4)
	t.Cleanup(func() {
		for id, wal := range wals {
			if !closed[id] {
				if err := wal.Close(); err != nil {
					t.Errorf("close fixture WAL %s: %v", id, err)
				}
			}
		}
	})
	for _, member := range genesis.Members {
		dir := filepath.Join(root, string(member.ID))
		wal, err := qlog.Open(dir)
		if err != nil {
			t.Fatalf("open initial WAL %s: %v", member.ID, err)
		}
		core, err := New(Config{NodeID: member.ID, Cluster: genesis, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			_ = wal.Close()
			t.Fatalf("open initial Core %s: %v", member.ID, err)
		}
		wals[member.ID], walDirs[member.ID], cores[member.ID] = wal, dir, core
		transport.setCore(member.ID, core)
	}

	learnerDir := filepath.Join(root, "d")
	learnerWAL, err := qlog.Open(learnerDir)
	if err != nil {
		t.Fatal("open learner WAL", err)
	}
	learner, err := NewLearner(Config{NodeID: "d", Cluster: genesis, WAL: learnerWAL, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		_ = learnerWAL.Close()
		t.Fatal("open actual learner", err)
	}
	wals["d"], walDirs["d"], cores["d"] = learnerWAL, learnerDir, learner
	transport.setCore("d", learner)
	for _, core := range cores {
		core.reconfigAdmission = func(ctx context.Context, target Cluster, through Slot, prefix [32]byte) error {
			var added *Member
			for i := range target.Members {
				if target.Members[i].ID == "d" {
					added = &target.Members[i]
					break
				}
			}
			if added == nil || added.WALIdentity != learner.WALIdentity() {
				return errors.New("target lacks the actual learner WAL identity")
			}
			if err := learner.WaitTip(ctx, through); err != nil {
				return err
			}
			if err := learner.EnsureDurableThrough(ctx, through); err != nil {
				return err
			}
			got, ok := learner.PrefixHash(through)
			if !ok || got != prefix {
				return errors.New("actual learner prefix differs from reconfiguration prefix")
			}
			return nil
		}
	}

	prepareAtCurrentCut := func(label string, active []NodeID) CheckpointSeal {
		t.Helper()
		core := cores["a"]
		index := core.Tip()
		membership, err := core.CheckpointMembership(index)
		if err != nil {
			t.Fatalf("read membership at %s cut: %v", label, err)
		}
		prefix, ok := core.PrefixHash(index)
		if !ok {
			t.Fatalf("prefix missing at %s cut", label)
		}
		next, following, err := core.CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatalf("read leader order at %s cut: %v", label, err)
		}
		seal := CheckpointSeal{ConfigID: core.ConfigID(), Index: index, RootHash: sha256.Sum256([]byte("prepared-root:" + label)), StateHash: sha256.Sum256([]byte("prepared-state:" + label)), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
		for _, id := range active {
			voter := cores[id]
			voter.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
			if err := voter.PrepareCheckpoint(ctx, seal); err != nil {
				t.Fatalf("prepare %s marker on voter %s: %v", label, id, err)
			}
		}
		encoded, err := EncodeCheckpointSeal(seal)
		if err != nil {
			t.Fatalf("encode %s seal: %v", label, err)
		}
		if _, _, err := core.Propose(ctx, encoded); err != nil {
			t.Fatalf("certify %s seal: %v", label, err)
		}
		if _, ok := core.CertifiedValue(index + 1); !ok {
			t.Fatalf("%s seal decision was not certified", label)
		}
		return seal
	}
	prepare := func(label string, active []NodeID) CheckpointSeal {
		t.Helper()
		if _, _, err := cores["a"].Propose(ctx, []byte("checkpoint-input:"+label)); err != nil {
			t.Fatalf("certify checkpoint input %s: %v", label, err)
		}
		return prepareAtCurrentCut(label, active)
	}

	beforeAdd := prepare("before-add", []NodeID{"a", "b", "c"})
	abortTarget := Cluster{ConfigID: 2, Members: append(append([]Member(nil), genesis.Members...), Member{ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"})}
	if _, err := cores["a"].BeginReconfiguration(ctx, abortTarget); err != nil {
		t.Fatal("begin actual learner ADD that will be aborted", err)
	}
	if err := cores["a"].AbortReconfiguration(ctx); err != nil {
		t.Fatal("certify actual learner ADD abort", err)
	}
	afterAbort, err := cores["a"].CheckpointMembership(cores["a"].Tip())
	if err != nil || afterAbort.Abort == nil {
		t.Fatalf("actual abort history missing before its marker: abort=%+v err=%v", afterAbort.Abort, err)
	}
	markerAfterAbort := prepareAtCurrentCut("at-abort-terminal", []NodeID{"a", "b", "c"})
	if markerAfterAbort.Index != afterAbort.Abort.Terminal.Slot {
		t.Fatalf("after-abort marker cut=%d, want exact abort terminal=%d", markerAfterAbort.Index, afterAbort.Abort.Terminal.Slot)
	}
	add := Cluster{ConfigID: 2, Members: append(append([]Member(nil), genesis.Members...), Member{ID: "d", WALIdentity: learner.WALIdentity()})}
	if _, err := cores["a"].BeginReconfiguration(ctx, add); err != nil {
		t.Fatal("begin actual learner ADD", err)
	}
	if err := cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatal("certify actual learner ADD", err)
	}
	afterAdd := prepare("after-add", []NodeID{"a", "b", "c", "d"})
	remove := Cluster{ConfigID: 3, Members: []Member{genesis.Members[0], genesis.Members[1], add.Members[3]}}
	if _, err := cores["a"].BeginReconfiguration(ctx, remove); err != nil {
		t.Fatal("begin actual voter REMOVE", err)
	}
	if err := cores["a"].FinishReconfiguration(ctx); err != nil {
		t.Fatal("certify actual voter REMOVE", err)
	}
	afterRemove := prepare("after-remove", []NodeID{"a", "b", "d"})
	if got := cores["a"].CurrentCluster(); got.ConfigID != 3 || len(got.Members) != 3 || got.Members[2].ID != "d" {
		t.Fatalf("actual final membership differs: %+v", got)
	}
	if len(afterRemove.Membership.Transitions) != 2 {
		t.Fatalf("actual membership transition count=%d, want ADD+REMOVE", len(afterRemove.Membership.Transitions))
	}
	pendingRemove := Cluster{ConfigID: 4, Members: []Member{genesis.Members[0], genesis.Members[1]}}
	if _, err := cores["a"].BeginReconfiguration(ctx, pendingRemove); err != nil {
		t.Fatal("begin actual later, intentionally unfinished REMOVE", err)
	}
	if cores["a"].reconfiguration == nil {
		t.Fatal("actual later REMOVE did not remain frozen")
	}

	markers := map[Slot]CheckpointSeal{beforeAdd.Index: beforeAdd, markerAfterAbort.Index: markerAfterAbort, afterAdd.Index: afterAdd, afterRemove.Index: afterRemove}
	entries, err := wals["a"].Read()
	if err != nil {
		t.Fatal("read actual source WAL", err)
	}
	for index, seal := range markers {
		found := false
		for _, entry := range entries {
			if entry.Type == qlog.EntryCheckpointVerified && entry.Slot == uint64(index) && entry.Hash == seal.RootHash {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("actual prepared marker at cut %d was not persisted", index)
		}
	}

	if err := wals["a"].Close(); err != nil {
		t.Fatal("close source WAL for actual reopen", err)
	}
	closed["a"] = true
	reopenedWAL, err := qlog.Open(walDirs["a"])
	if err != nil {
		t.Fatal("reopen source WAL", err)
	}
	defer reopenedWAL.Close()
	reopened, err := New(Config{NodeID: "a", Cluster: genesis, WAL: reopenedWAL, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatalf("Core.New rejected historical prepared markers after certified ADD/REMOVE: %v", err)
	}
	if got := reopened.CurrentCluster(); got.ConfigID != 3 || len(got.Members) != 3 || got.Members[2].ID != "d" {
		t.Fatalf("reopened membership differs: %+v", got)
	}
	if reopened.reconfiguration == nil {
		t.Fatal("reopen incorrectly completed the later REMOVE transition")
	}
	if index, root, ok := reopened.LatestPreparedCheckpoint(); !ok || index != afterRemove.Index || root != afterRemove.RootHash {
		t.Fatalf("reopened latest prepared marker=(%d,%x,%t), want (%d,%x,true)", index, root, ok, afterRemove.Index, afterRemove.RootHash)
	}
	if _, _, err := reopened.Propose(ctx, []byte("must-not-pass-incomplete-transition")); err == nil {
		t.Fatal("reopen admitted an application proposal during the unfinished REMOVE")
	}
	cut := reopened.Tip()
	prefix, ok := reopened.PrefixHash(cut)
	if !ok {
		t.Fatal("pending transition cut lacks the actual prefix")
	}
	next, following, err := reopened.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal("pending transition cut lacks current leader order", err)
	}
	newCandidate := CheckpointSeal{ConfigID: 3, Index: cut, RootHash: sha256.Sum256([]byte("must-not-prepare-during-transition")), StateHash: sha256.Sum256([]byte("unfinished-transition-state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: afterRemove.Membership}
	if err := reopened.PrepareCheckpoint(ctx, newCandidate); err == nil {
		t.Fatal("ordinary new PrepareCheckpoint passed during the unfinished REMOVE")
	}

	bad := []struct {
		name string
		seal CheckpointSeal
	}{
		{name: "membership", seal: afterRemove},
		{name: "config", seal: afterRemove},
		{name: "root", seal: afterRemove},
		{name: "leader-order", seal: afterRemove},
	}
	bad[0].seal.Membership = func() *MembershipRecord {
		copy := cloneMembershipRecord(*afterRemove.Membership)
		copy.Transitions[0].Terminal.Value = append(copy.Transitions[0].Terminal.Value, 0)
		return &copy
	}()
	bad[1].seal.ConfigID++
	bad[2].seal.RootHash = sha256.Sum256([]byte("tampered-root"))
	bad[3].seal.NextLeaderOrder = append([]NodeID(nil), afterRemove.NextLeaderOrder...)
	bad[3].seal.NextLeaderOrder[0] = "tampered-voter"
	for _, candidate := range bad {
		t.Run("reject_tampered_"+candidate.name, func(t *testing.T) {
			if err := reopened.validateRecoveredPreparedCheckpoint(candidate.seal); err == nil {
				t.Fatalf("accepted tampered %s checkpoint", candidate.name)
			}
		})
	}
}
