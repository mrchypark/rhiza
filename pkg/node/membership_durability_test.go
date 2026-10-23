package node

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestMembershipRetryDurablyPublishesCommittedConfiguration(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNode(t)
	transport.disable("c")
	faults := &failFirstArchiveHeadUploadBucket{Bucket: bucket}
	n.archive.Close()
	n.archive = recovery.NewManager(faults, "cluster", 1)
	if err := n.archive.Load(ctx); err != nil {
		t.Fatalf("load archive before mutation: %v", err)
	}
	t.Cleanup(n.archive.Close)

	request := network.MembershipChange{
		OperationID: "durable-remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "workload-c", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, request); err == nil {
		t.Fatal("membership request succeeded despite injected post-commit archive failure")
	}
	if !faults.failed.Load() {
		t.Fatal("membership request failed before the injected archive head upload failure")
	}
	committed := n.core.CurrentCluster()
	if committed.ConfigID != 2 || len(committed.Members) != 2 || containsMember(committed.Members, "c") {
		t.Fatalf("configuration did not commit before persistence failure: %+v", committed)
	}
	through := n.core.Tip()
	if err := n.core.EnsureDurableThrough(ctx, through); err != nil {
		t.Fatalf("committed decision was not durable in the local WAL: %v", err)
	}
	if got := n.archive.Tip(); got >= through {
		t.Fatalf("archive tip=%d unexpectedly covers failed publication target %d", got, through)
	}

	// Conflicting payloads and operation IDs must not adopt the committed
	// configuration or publish a different request as its acknowledgement.
	changedPayload := request
	changedFence := *request.Fence
	changedFence.Evidence = "different-fence"
	changedPayload.Fence = &changedFence
	if err := n.changeMembership(ctx, changedPayload); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("changed payload replay error=%v, want request conflict", err)
	}
	changedOperation := request
	changedOperation.OperationID += "-other"
	if err := n.changeMembership(ctx, changedOperation); !errors.Is(err, network.ErrRequestConflict) {
		t.Fatalf("changed operation replay error=%v, want request conflict", err)
	}
	changedConfig := request
	changedConfig.ExpectedConfigID = 2
	if err := n.changeMembership(ctx, changedConfig); !errors.Is(err, network.ErrInvalidRequest) {
		t.Fatalf("stale removed-member request at next config error=%v, want invalid request", err)
	}
	if got := n.core.Tip(); got != through {
		t.Fatalf("rejected replay changed consensus tip from %d to %d", through, got)
	}
	assertNoMembershipOperation(t, ctx, n, bucket, 2)

	// Only the exact journaled request may retry persistence. It must not
	// commit another config decision or advance the consensus tip.
	if err := n.changeMembership(ctx, request); err != nil {
		t.Fatalf("persist committed configuration on exact replay: %v", err)
	}
	if got := n.core.Tip(); got != through {
		t.Fatalf("exact persistence replay advanced tip from %d to %d", through, got)
	}
	final := n.core.CurrentCluster()
	if final.ConfigID != 2 || len(final.Members) != 2 || containsMember(final.Members, "c") {
		t.Fatalf("exact replay changed committed configuration: %+v", final)
	}
	assertDurableArchivedMembership(t, ctx, n, bucket, through)

	terminal, ok := n.core.CertifiedValue(through)
	if !ok {
		t.Fatalf("committed terminal slot %d is not certified", through)
	}
	reloaded := recovery.NewManager(bucket, "cluster", 1)
	defer reloaded.Close()
	if err := reloaded.Load(ctx); err != nil {
		t.Fatalf("reload persisted membership archive: %v", err)
	}
	values, _, err := reloaded.DecisionsFrom(ctx, through, 1)
	if err != nil || len(values) != 1 || values[0].Hash != terminal.Hash || string(values[0].Value) != string(terminal.Value) {
		t.Fatalf("reloaded terminal differs from committed membership decision: values=%+v err=%v", values, err)
	}
}
