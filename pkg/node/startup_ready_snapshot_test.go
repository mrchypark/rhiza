package node

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestCatchUpReadinessRequiresQuorumForCurrentMembership(t *testing.T) {
	ctx := context.Background()
	n, identities, _, transport := newMembershipOperationNode(t)
	transport.disable("c")

	firstRound := n.core.CurrentCluster()
	removeC := network.MembershipChange{
		OperationID: "remove-c-before-ready", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "workload-c", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, removeC); err != nil {
		t.Fatalf("remove c: %v", err)
	}

	// A round that started under config 1 must not promote readiness after the
	// terminal changed the active voters to config 2.
	n.observeCatchUpRound(nil, &firstRound)
	if n.ready.Load() || n.Ready() {
		t.Fatal("stale config-1 quorum round promoted the config-2 node to ready")
	}

	currentRound := n.core.CurrentCluster()
	if currentRound.ConfigID != 2 || !n.core.StartupCatchUpRoundEligible(currentRound) {
		t.Fatalf("current config is not eligible for a fresh quorum round: %+v", currentRound)
	}
	n.observeCatchUpRound(nil, &currentRound)
	if !n.Ready() {
		t.Fatal("fresh current-config quorum round did not make the node ready")
	}

	removeB := network.MembershipChange{
		OperationID: "remove-b-after-ready", ClusterID: "cluster", ExpectedConfigID: 2, Remove: "b",
		Fence: &network.MembershipFence{NodeID: "b", WALIdentity: identities["b"], WorkloadUID: "workload-b", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, removeB); err != nil {
		t.Fatalf("remove b: %v", err)
	}
	if n.Ready() {
		t.Fatal("node remained ready after its quorum snapshot became stale")
	}

}

func TestEngineRepairLearnerDoesNotSkipStartupCatchUpWorker(t *testing.T) {
	n := &Node{config: &types.ExecutionConfig{
		EnableReconfiguration: true,
		EngineOwnedRemovals:   true,
		Learner:               &quepaxa.Member{ID: "replacement"},
	}}
	if n.maySkipCatchUp(quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{{ID: "survivor"}}}) {
		t.Fatal("engine-repair learner skipped its catch-up worker for singleton survivor membership")
	}
}
