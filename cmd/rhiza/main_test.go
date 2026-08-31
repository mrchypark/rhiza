package main

import (
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestReplicaMembersRequirePinnedIdentityOnlyForLearner(t *testing.T) {
	members := []rhiza.Member{{ID: "n1", PeerURL: "quic://n1:9090", Token: "voter-1"}, {ID: "n2"}}
	objectMembers, err := replicaMembers("cluster", members, false)
	if err != nil || len(objectMembers) != 2 || objectMembers[1].ID != "n2" {
		t.Fatalf("object-store members=%+v err=%v", objectMembers, err)
	}
	if _, err := replicaMembers("cluster", members, true); err == nil {
		t.Fatal("learner accepted voter without pinned peer identity inputs")
	}
}
