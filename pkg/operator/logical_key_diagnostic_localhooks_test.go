//go:build localhooks

package operator

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestInspectLogicalMutationKeyLocalhooksMapsCanonicalOwnerReadOnly(t *testing.T) {
	e, uid, identity, binding := newLogicalCompactionFixture(t)
	intent := completeStoreIntent(fencing.MutationIntent{
		OperationID:    fencing.MutationRequestID(binding.ProducerUID, 1),
		Kind:           "delete",
		Key:            "diagnostic-key",
		StoreScopeHash: binding.StoreScopeHash,
	})
	_, err := e.changeMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding), binding.ProducerUID,
		func(producer *fencing.MutationProducer) error { return producer.Admit(1, intent) })
	if err != nil {
		t.Fatal(err)
	}

	beforeMissing, err := e.InspectLogicalMutationKeyLocalhooks(t.Context(), uid, identity, 1, intent)
	if err != nil || beforeMissing.Exists {
		t.Fatalf("missing key snapshot=%+v err=%v, want no key record", beforeMissing, err)
	}

	root, err := e.read(t.Context(), uid, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[identity.RunID]
	authority := mutationExecutionAuthority{statefulSetUID: uid, sourceClusterID: identity.SourceClusterID,
		runID: identity.RunID, logicalBinding: run.LogicalBinding}
	owner := authority.mutationOwner(run, binding.ProducerUID, 1, intent)
	if err := e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	before, err := e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := e.InspectLogicalMutationKeyLocalhooks(t.Context(), uid, identity, 1, intent)
	if err != nil {
		t.Fatal(err)
	}
	after, err := e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("read-only inspection mutated key state: before=%+v after=%+v", before, after)
	}
	if !snapshot.Exists || !snapshot.OwnerEntryFound || snapshot.OwnerProducerUID != binding.ProducerUID ||
		snapshot.OwnerSequence != 1 || !snapshot.CurrentTupleMatch || snapshot.OwnerEntryTerminal || snapshot.CurrentEntryTerminal {
		t.Fatalf("owner snapshot=%+v, want exact live current tuple", snapshot)
	}
}
