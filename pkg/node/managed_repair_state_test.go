package node

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestManagedRepairSnapshotUsesActualRegistration(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, _ := newMembershipOperationNode(t)
	source, _, err := n.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	n.managedStartup = &ManagedStartupConfig{Checker: &membershipAuthorityChecker{}, Identity: ManagedStartupIdentity{PhysicalBinding: source, Member: recovery.PreparedStartupIdentity{NodeID: "a", SourceBinding: source, ClusterUID: "uid", DeploymentUID: "deployment"}}}
	n.opened.Store(true)

	learner := peerMember("cluster", "fresh", "fresh-token")
	before, err := n.ManagedRepairSnapshot(ctx, learner.ID, learner.PublicKey)
	if err != nil || before.LearnerWALIdentity != "" {
		t.Fatalf("unregistered learner identity: snapshot=%+v err=%v", before, err)
	}
	if before.Current.ConfigID != 1 || before.LocalWALIdentity != identities["a"] {
		t.Fatalf("snapshot not bound to actual local voter: %+v", before)
	}

	registered := registerMembershipLearner(t, bucket, learner.ID)
	if registered.PublicKey != learner.PublicKey {
		t.Fatal("fixture learner public key changed")
	}
	after, err := n.ManagedRepairSnapshot(ctx, learner.ID, learner.PublicKey)
	if err != nil || after.LearnerWALIdentity == "" || after.LearnerWALIdentity == identities["a"] {
		t.Fatalf("actual learner registration missing: snapshot=%+v err=%v", after, err)
	}
	if after.Current.ConfigID != before.Current.ConfigID || after.LocalWALIdentity != before.LocalWALIdentity || after.AbortSlot != before.AbortSlot {
		t.Fatalf("read-only snapshot changed current membership evidence: before=%+v after=%+v", before, after)
	}
	wrongKey := learner.PublicKey
	wrongKey[0] ^= 1
	if _, err := n.ManagedRepairSnapshot(ctx, learner.ID, wrongKey); err == nil {
		t.Fatal("registration for a different public key accepted")
	}
	if _, err := n.VerifyManagedRepairCompletion(ctx, ManagedMembershipRequest{Action: ManagedMembershipAdd}); !errors.Is(err, network.ErrInvalidRequest) {
		t.Fatalf("unbound completion request was not rejected as invalid: %v", err)
	}
}
