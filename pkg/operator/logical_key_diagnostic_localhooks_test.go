//go:build localhooks

package operator

import (
	"fmt"
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
	intent.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 4}
	_, err := e.changeMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding), binding.ProducerUID,
		func(producer *fencing.MutationProducer) error { return producer.Admit(1, intent) })
	if err != nil {
		t.Fatal(err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 4}
	if _, err := e.changeMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding), binding.ProducerUID,
		func(producer *fencing.MutationProducer) error {
			if err := producer.Prepare(1, preparation); err != nil {
				return err
			}
			return producer.GrantDispatch(1, "diagnostic-dispatch")
		}); err != nil {
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
	receipt := authority.makeReceipt(run, binding.ProducerUID, 1, intent, preparation, "diagnostic-dispatch",
		mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: preparation.Generation, AllCopiesTerminal: true})
	receipts, err := e.receiptsForLogicalBinding(uid, binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipts.put(receipt); err != nil {
		t.Fatal(err)
	}
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
		snapshot.OwnerSequence != 1 || !snapshot.CurrentTupleMatch || snapshot.OwnerEntryTerminal || snapshot.CurrentEntryTerminal ||
		snapshot.OwnerReceiptStatus != logicalKeyReceiptValid || snapshot.OwnerReceiptOutcome != fencing.MutationCommitted ||
		snapshot.OwnerReceiptDigestMatches {
		t.Fatalf("owner snapshot=%+v, want exact live current tuple", snapshot)
	}
}

func TestInspectLogicalMutationKeyLocalhooksReportsMissingOwnerReceipt(t *testing.T) {
	e, uid, identity, binding := newLogicalCompactionFixture(t)
	intent := completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(binding.ProducerUID, 1),
		Kind: "delete", Key: "missing-owner-receipt", StoreScopeHash: binding.StoreScopeHash})
	if _, err := e.changeMutationProducerForAuthority(t.Context(), logicalMutationAuthority(uid, binding), binding.ProducerUID,
		func(producer *fencing.MutationProducer) error { return producer.Admit(1, intent) }); err != nil {
		t.Fatal(err)
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
	snapshot, err := e.InspectLogicalMutationKeyLocalhooks(t.Context(), uid, identity, 1, intent)
	if err != nil || snapshot.OwnerReceiptStatus != logicalKeyReceiptAbsent || snapshot.OwnerEntryFound || snapshot.OwnerReceiptOutcome != "" ||
		snapshot.OwnerReceiptDigestMatches {
		t.Fatalf("missing receipt snapshot=%+v err=%v, want absent without entry claim", snapshot, err)
	}
}

func TestInspectLogicalMutationKeyLocalhooksDoesNotTraverseArchivedEntry(t *testing.T) {
	e, uid, identity, binding := newLogicalCompactionFixture(t)
	ctx := t.Context()
	intent := completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(binding.ProducerUID, 1),
		Kind: "delete", Key: "archived-owner-diagnostic", StoreScopeHash: binding.StoreScopeHash,
		OwnerStep: fencing.MutationOwnerStep{Owner: "owner", Workflow: "diagnostic", Step: "delete", Attempt: 1}})
	intent.Condition = fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: 4}
	authority := logicalMutationAuthority(uid, binding)
	run := producerRunForLogicalKeyDiagnostic(t, e, uid, identity)
	owner := authority.mutationOwner(run, binding.ProducerUID, 1, intent)
	if err := e.acquireMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: binding.StoreScopeHash, GenerationSet: true, Generation: 4}
	if _, err := e.changeMutationProducerForAuthority(ctx, authority, binding.ProducerUID, func(p *fencing.MutationProducer) error {
		if err := p.Admit(1, intent); err != nil {
			return err
		}
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "archived-diagnostic-dispatch"); err != nil {
			return err
		}
		return p.SealDispatch(1)
	}); err != nil {
		t.Fatal(err)
	}
	receipt := authority.makeReceipt(run, binding.ProducerUID, 1, intent, preparation, "archived-diagnostic-dispatch",
		mutationProviderResult{Outcome: fencing.MutationCommitted, Generation: 4, AllCopiesTerminal: true})
	receipts, err := e.receiptsForLogicalBinding(uid, binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receipts.put(receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := e.changeMutationProducerForAuthority(ctx, authority, binding.ProducerUID, func(p *fencing.MutationProducer) error {
		if err := p.RecordDispatchTerminal(1, receipt.DispatchID, receipt.ReceiptDigest); err != nil {
			return err
		}
		preparationHash, err := p.PreparationHash(1)
		if err != nil {
			return err
		}
		return p.Resolve(1, fencing.MutationResolution{Outcome: receipt.Outcome, PreparationHash: preparationHash,
			ReceiptDigest: receipt.ReceiptDigest, PayloadDigest: receipt.PayloadDigest, AllCopiesTerminal: receipt.AllCopiesTerminal})
	}); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(2); sequence <= 35; sequence++ {
		if err := e.compactLogicalSourceMutationJournal(ctx, uid, binding, sequence); err != nil {
			t.Fatalf("compact before sequence %d: %v", sequence, err)
		}
		intent := completeStoreIntent(fencing.MutationIntent{OperationID: fencing.MutationRequestID(binding.ProducerUID, sequence),
			Kind: "delete", Key: fmt.Sprintf("diagnostic-padding-%d", sequence), StoreScopeHash: binding.StoreScopeHash})
		appendSettledLogicalStoreMutation(t, e, uid, binding, sequence, intent)
	}
	if err := e.compactLogicalSourceMutationJournal(ctx, uid, binding, 36); err != nil {
		t.Fatal(err)
	}
	producer, _, _, err := e.AuthorityState.ReadLogicalProducer(ctx, uid, binding)
	if err != nil || producer.CompactedThrough < 1 {
		t.Fatalf("target tuple was not archived: compacted=%d err=%v", producer.CompactedThrough, err)
	}
	before := logicalKeyDiagnosticCanonicalBytes(t, e, uid, binding, intent)
	snapshot, err := e.InspectLogicalMutationKeyLocalhooks(ctx, uid, identity, 1, intent)
	if err != nil {
		t.Fatal(err)
	}
	after := logicalKeyDiagnosticCanonicalBytes(t, e, uid, binding, intent)
	if before != after {
		t.Fatalf("archived diagnostic changed canonical bytes: before=%s after=%s", before, after)
	}
	if snapshot.OwnerReceiptStatus != logicalKeyReceiptValid || snapshot.OwnerReceiptOutcome != fencing.MutationCommitted ||
		snapshot.OwnerEntryStatus != logicalKeyEntryArchived || snapshot.OwnerEntryFound || !snapshot.OwnerEntryRetired ||
		snapshot.OwnerReceiptDigestMatches {
		t.Fatalf("archived owner snapshot=%+v", snapshot)
	}
}

func logicalKeyDiagnosticCanonicalBytes(t *testing.T, e *FencingExecutor, uid string, binding fencing.LogicalWriterBindingV1, intent fencing.MutationIntent) string {
	t.Helper()
	producerKey, producerIdentity, err := e.AuthorityState.logicalProducerKey(uid, binding)
	if err != nil {
		t.Fatal(err)
	}
	producerBytes, _, err := e.AuthorityState.read(t.Context(), producerKey, producerIdentity)
	if err != nil {
		t.Fatal(err)
	}
	key, keyIdentity := e.mutationKeyIdentity(intent.StoreScopeHash, intent.Key)
	keyBytes, _, err := e.AuthorityState.read(t.Context(), key, keyIdentity)
	if err != nil {
		t.Fatal(err)
	}
	return hashJSON([]string{string(producerBytes), string(keyBytes)})
}

func producerRunForLogicalKeyDiagnostic(t *testing.T, e *FencingExecutor, uid string, identity fencing.Identity) fencing.Run {
	t.Helper()
	state, err := e.read(t.Context(), uid, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := state.Ledger.Runs[identity.RunID]
	if !ok {
		t.Fatal("canonical logical Run is missing")
	}
	return run
}
