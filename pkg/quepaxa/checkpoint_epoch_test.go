package quepaxa

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestCheckpointLeaderOrdersAfterMembershipChange(t *testing.T) {
	cores, transport := reconfigCluster(t)
	core := cores["a"]
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := core.BeginReconfiguration(ctx, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	transport.fail("c")
	for core.Tip() < 130 {
		if _, _, err := core.Propose(ctx, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	// Epochs restart at slot 18, not at the bootstrap epoch boundaries.
	for _, index := range []Slot{17, 18, 33, 34, 81, 82, 97, 98, 113, 114} {
		next, following, err := core.CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatalf("index %d: %v", index, err)
		}
		want, err := core.LeaderOrder(index + 1)
		if err != nil || !slices.Equal(next, want) {
			t.Fatalf("index %d next=%v want=%v err=%v", index, next, want, err)
		}
		needsFollowing := index == 82 || index == 98 || index == 114
		if needsFollowing {
			wantFollowing, err := core.LeaderOrder(index + 16)
			if err != nil || !slices.Equal(following, wantFollowing) {
				t.Fatalf("index %d following=%v want=%v err=%v", index, following, wantFollowing, err)
			}
		} else if len(following) != 0 {
			t.Fatalf("index %d unexpected following order %v", index, following)
		}
		if !core.validateCheckpointLeaderOrders(index, next, following) {
			t.Fatalf("index %d rejects its own orders: %v / %v", index, next, following)
		}
		if slices.Contains(next, NodeID("c")) || slices.Contains(following, NodeID("c")) {
			t.Fatalf("index %d retained retired voter", index)
		}
		if core.validateCheckpointLeaderOrders(index, []NodeID{"a", "b", "c"}, following) {
			t.Fatalf("index %d accepted bootstrap voters", index)
		}
	}
}
