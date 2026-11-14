package quepaxa

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"
)

func TestHistoricalMembershipUsesCompactedAbortBeforeLaterAbort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	initial := core.CurrentCluster()
	addition := func(id string, digit string) Cluster {
		members := append([]Member(nil), initial.Members...)
		return Cluster{ConfigID: 2, Members: append(members, Member{ID: NodeID(id), WALIdentity: strings.Repeat(digit, 64)})}
	}
	if _, err := core.BeginReconfiguration(ctx, addition("lost1", "0")); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	cut := core.Tip()
	first, err := core.CheckpointMembership(cut)
	if err != nil || first.Abort == nil {
		t.Fatalf("first abort missing: %v", err)
	}
	prefix, ok := core.PrefixHash(cut)
	if !ok {
		t.Fatal("missing cut prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{ConfigID: initial.ConfigID, Index: cut, PrefixHash: prefix, RootHash: sha256.Sum256([]byte("abort base")), StateHash: sha256.Sum256([]byte("abort state")), NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &first}
	for _, peer := range cores {
		peer.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		if err := peer.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
	}
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(cut, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	if _, err := core.BeginReconfiguration(ctx, addition("lost2", "1")); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	core.mu.RLock()
	old, err := core.checkpointMembershipAtLocked(cut)
	current, currentErr := core.checkpointMembershipAtLocked(core.tip)
	core.mu.RUnlock()
	if err != nil || old.Abort == nil || !sameMembershipTransition(*old.Abort, *first.Abort) {
		t.Fatalf("compacted old abort unavailable: %v", err)
	}
	if currentErr != nil || current.Abort == nil || current.Abort.Terminal.Slot <= first.Abort.Terminal.Slot {
		t.Fatalf("later abort unavailable: %v", currentErr)
	}
	if _, err := validateMembershipHistory(initial, old); err != nil {
		t.Fatalf("old retained proof invalid: %v", err)
	}
	if _, err := validateMembershipHistory(initial, current); err != nil {
		t.Fatalf("new retained proof invalid: %v", err)
	}
}
