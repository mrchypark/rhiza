package main

import (
	"encoding/base64"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestReplicaMembersRequirePinnedIdentityOnlyForLearner(t *testing.T) {
	members := []rhiza.Member{{ID: "n1"}, {ID: "n2"}}
	objectMembers, err := objectReplicaMembers(members)
	if err != nil || len(objectMembers) != 2 || objectMembers[1].ID != "n2" {
		t.Fatalf("object-store members=%+v err=%v", objectMembers, err)
	}
	derived, err := rhiza.NewReplicaMember("cluster", rhiza.Member{ID: "n1", PeerURL: "quic://n1:9090", Token: "voter-secret"})
	if err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(derived.PublicKey[:])
	learnerMembers, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`"}]`, members)
	if err != nil || len(learnerMembers) != 1 || learnerMembers[0].PeerURL != "quic://n1:9090" {
		t.Fatalf("learner members=%+v err=%v", learnerMembers, err)
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`","token":"voter-secret"}]`, members); err == nil {
		t.Fatal("learner accepted voter token")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`"}]`, []rhiza.Member{{ID: "n1", Token: "voter-secret"}}); err == nil {
		t.Fatal("learner accepted voter token in cluster members")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`"}] trailing`, members); err == nil {
		t.Fatal("learner accepted trailing replica member input")
	}
}
