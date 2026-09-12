package quepaxa

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func checkpointMembershipSource(t *testing.T) (*Core, string, Cluster, CheckpointSeal, DecidedValue) {
	t.Helper()
	initial := Cluster{ConfigID: 1, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	dir := t.TempDir()
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	cores := make(map[NodeID]*Core, len(initial.Members))
	for _, member := range initial.Members {
		wal, err := qlog.Open(filepath.Join(dir, string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		core := newCore(member.ID, &initial, wal, transport)
		core.reconfigEnabled = true
		core.reconfigAdmission = func(context.Context, Cluster, Slot, [32]byte) error { return nil }
		cores[member.ID] = core
		transport.cores[member.ID] = core
	}
	core := cores["a"]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	transport.fail("c")
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 3, Members: []Member{{ID: "a"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("after-two-transitions")); err != nil {
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
	seal := CheckpointSeal{ConfigID: core.ConfigID(), Index: index, RootHash: sha256.Sum256([]byte("membership-root")), StateHash: sha256.Sum256([]byte("membership-state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(core.Tip())
	if !ok {
		t.Fatal("missing checkpoint decision")
	}
	return core, filepath.Join(dir, "a"), initial, seal, decision
}

func TestCheckpointMembershipCompactionRestartsAfterTransitions(t *testing.T) {
	core, walDir, initial, seal, _ := checkpointMembershipSource(t)
	if err := core.CompactThrough(seal.Index, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	if got, err := core.CheckpointMembership(seal.Index); err != nil || !sameMembershipRecord(got, *seal.Membership) {
		t.Fatalf("compacted membership=%+v err=%v", got, err)
	}
	entries, err := core.wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	base, err := decodeConsensusBase(entries[0].Payload)
	if err != nil || base.Membership == nil || !sameMembershipRecord(*base.Membership, *seal.Membership) {
		t.Fatalf("base membership=%+v err=%v", base.Membership, err)
	}
	if core.ConfigID() != 3 || core.ClusterForSlot(seal.Index+1).ConfigID != 3 || len(initial.Members) != 3 {
		t.Fatal("compaction lost current membership")
	}
	if err := core.wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedWAL, err := qlog.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedWAL.Close()
	restarted := newCore("a", &initial, reopenedWAL, nil)
	restarted.reconfigEnabled = true
	if err := restarted.recover(); err != nil {
		t.Fatal(err)
	}
	if restarted.ConfigID() != 3 || restarted.CompactionFloor() != seal.Index {
		t.Fatalf("restart config=%d floor=%d", restarted.ConfigID(), restarted.CompactionFloor())
	}
}

func TestCheckpointMembershipColdObserverRestoreAndRejectsTampering(t *testing.T) {
	source, _, initial, seal, decision := checkpointMembershipSource(t)
	observerWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer observerWAL.Close()
	observer, err := NewObserver(Config{NodeID: "observer", Cluster: initial, WAL: observerWAL, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	observer.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := observer.RestoreCheckpointBase(context.Background(), seal, decision); err != nil {
		t.Fatal(err)
	}
	if observer.IsVoter() || observer.ConfigID() != 3 {
		t.Fatalf("observer voter=%v config=%d", observer.IsVoter(), observer.ConfigID())
	}
	if _, retired := observer.retiredIDs["c"]; !retired {
		t.Fatal("restore lost retired member")
	}
	tampered := seal
	membership := cloneMembershipRecord(*seal.Membership)
	membership.Genesis.Members[0].Token = "tampered"
	tampered.Membership = &membership
	if err := observer.ValidateCheckpointBase(context.Background(), tampered, decision); err == nil {
		t.Fatal("accepted tampered membership history")
	}
	if source.ConfigID() != 3 {
		t.Fatal("source changed during observer restore")
	}
}

func TestCheckpointMembershipRejectsPendingTerminal(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CheckpointMembership(core.Tip()); err == nil {
		t.Fatal("checkpoint membership accepted active freeze")
	}
	pending, ok := core.PendingReconfiguration()
	if !ok || pending.ConfigID != 2 {
		t.Fatalf("pending reconfiguration=%+v ok=%v", pending, ok)
	}
	pending.Members[0].ID = "mutated"
	if fresh, ok := core.PendingReconfiguration(); !ok || fresh.Members[0].ID != "a" {
		t.Fatal("pending reconfiguration exposed core state")
	}
}

func TestCheckpointMembershipProofRespectsValueLimit(t *testing.T) {
	seal := CheckpointSeal{
		ConfigID: 1, Index: 1, RootHash: sha256.Sum256([]byte("root")), StateHash: sha256.Sum256([]byte("state")), PrefixHash: sha256.Sum256([]byte("prefix")), NextLeaderOrder: []NodeID{"a"},
		Membership: &MembershipRecord{Genesis: Cluster{
			ConfigID: 1,
			Members:  []Member{{ID: "a", Token: string(make([]byte, MaxReplicatedValueBytes))}},
		}},
	}
	if _, err := EncodeCheckpointSeal(seal); err == nil {
		t.Fatal("accepted an oversized checkpoint membership proof")
	}
}
