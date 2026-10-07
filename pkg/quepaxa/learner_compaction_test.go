package quepaxa

import (
	"context"
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestPromotedLearnerVotesAfterMembershipBaseRestart(t *testing.T) {
	cores, transport := reconfigCluster(t)
	initial := cores["a"].CurrentCluster()
	learnerDir := t.TempDir()
	learnerWAL, err := qlog.Open(learnerDir)
	if err != nil {
		t.Fatal(err)
	}
	learner, err := NewLearner(Config{NodeID: "d", Cluster: initial, WAL: learnerWAL, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	transport.cores["d"] = learner
	for _, core := range cores {
		core.reconfigAdmission = func(context.Context, Cluster, Slot, [32]byte) error { return nil }
	}
	target := Cluster{ConfigID: 2, Members: append(initial.Members, Member{ID: "d", WALIdentity: learner.WALIdentity()})}
	if _, err := cores["a"].BeginReconfiguration(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err := cores["a"].FinishReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["a"].Propose(context.Background(), []byte("after-promotion")); err != nil {
		t.Fatal(err)
	}
	index := cores["a"].Tip()
	prefix, ok := cores["a"].PrefixHash(index)
	if !ok {
		t.Fatal("missing checkpoint prefix")
	}
	next, following, err := cores["a"].CheckpointLeaderOrders(index)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := cores["a"].CheckpointMembership(index)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for id, core := range cores {
		if err := core.WaitTip(ctx, index); err != nil {
			t.Fatalf("core %s did not reach checkpoint index %d: %v", id, index, err)
		}
		gotPrefix, ok := core.PrefixHash(index)
		if !ok || gotPrefix != prefix {
			t.Fatalf("core %s checkpoint prefix at %d = %x, present=%v; want %x", id, index, gotPrefix, ok, prefix)
		}
		gotNext, gotFollowing, err := core.CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatalf("core %s checkpoint leader orders at %d: %v", id, index, err)
		}
		if !slices.Equal(gotNext, next) || !slices.Equal(gotFollowing, following) {
			t.Fatalf("core %s checkpoint leader orders at %d = %v/%v; want %v/%v", id, index, gotNext, gotFollowing, next, following)
		}
		gotMembership, err := core.CheckpointMembership(index)
		if err != nil {
			t.Fatalf("core %s checkpoint membership at %d: %v", id, index, err)
		}
		if !sameMembershipRecord(gotMembership, membership) {
			t.Fatalf("core %s checkpoint membership at %d does not match seal membership", id, index)
		}
	}
	seal := CheckpointSeal{
		ConfigID: 2, Index: index,
		RootHash: sha256.Sum256([]byte("learner-membership-root")), StateHash: sha256.Sum256([]byte("learner-membership-state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership,
	}
	for _, core := range cores {
		core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
			t.Fatal(err)
		}
	}
	learner.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := learner.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["a"].Propose(context.Background(), encoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := cores["a"].CertifiedValue(index + 1); !ok {
		t.Fatal("missing checkpoint certificate")
	}
	if err := cores["a"].CompactThrough(index, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	if err := learner.CompactThrough(index, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	if err := learnerWAL.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedWAL, err := qlog.Open(learnerDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedWAL.Close()
	restarted, err := NewLearner(Config{NodeID: "d", Cluster: initial, WAL: reopenedWAL, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	transport.setCore("d", restarted)
	if !restarted.IsVoter() || restarted.ConfigID() != target.ConfigID {
		t.Fatalf("restarted learner voter=%v config=%d", restarted.IsVoter(), restarted.ConfigID())
	}
	if _, _, err := restarted.Propose(context.Background(), []byte("learner-write-after-restart")); err != nil {
		t.Fatalf("promoted learner could not write: %v", err)
	}
}
