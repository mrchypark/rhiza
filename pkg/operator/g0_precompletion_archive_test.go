package operator

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestGenerationZeroGuardedLogicalCommitCannotArchiveBeforeOriginalCompletion(t *testing.T) {
	f := newGenerationZeroReceiptFixture(t)
	payload := []byte(`{"membership":"guarded logical commit, original dispatch unresolved"}`)
	intent := f.admitUpload(t, 1, "membership/g0-precompletion-archive", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "original-unresolved-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return p.Close("close-original-unresolved", hashBytes([]byte("close-original-unresolved")))
	}); err != nil {
		t.Fatalf("prepare unresolved original create dispatch: %v", err)
	}

	// The recovery helper performs its own actual create-only operation against
	// the empty fake provider. The original dispatch remains unresolved in the
	// canonical journal: this does not manufacture or borrow its receipt.
	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 1
	server.objects[pathJoin(f.scope.Prefix, intent.Key)] = heldGCSObject{
		name: pathJoin(f.scope.Prefix, intent.Key), generation: 1, metageneration: 1,
		payload:       append([]byte(nil), payload...),
		metadata:      map[string]string{"rhiza-operation-id": intent.OperationID, "rhiza-payload-sha256": intent.PayloadDigest},
		temporaryHold: true,
	}
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	repair := testEngineRepairRecord(t, f)
	guard, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope)
	if err != nil {
		t.Fatalf("settle guarded logical commit while original dispatch is unresolved: %v", err)
	}

	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	entry := producer.Entries[1]
	if err != nil || uid != f.producer || entry.GenerationZeroGuard == nil || *entry.GenerationZeroGuard != guard ||
		entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.AllCopiesTerminal ||
		entry.CopyCompletion != nil || len(entry.Dispatches) != 1 || entry.Dispatches[guard.DispatchID].ReceiptDigest != "" {
		t.Fatalf("fixture did not preserve guarded logical commit separately from original physical completion: uid=%q entry=%+v err=%v", uid, entry, err)
	}
	if entry.GenerationZeroRights == nil || !generationZeroOperatorRightsTerminal(entry.GenerationZeroRights) {
		t.Fatalf("helper rights are not terminally recorded: %+v", entry.GenerationZeroRights)
	}

	if _, err := producer.PlanMutationCompaction(f.producer, 1); err == nil {
		t.Fatal("compaction plan accepted a G0 logical commit without original dispatch completion")
	}
	retired, err := f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, entry)
	if err != nil || retired {
		t.Fatalf("unresolved original dispatch retired its live barrier: retired=%t err=%v", retired, err)
	}
	state, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || state.Barrier == nil || state.Retirement != nil {
		t.Fatalf("failed archive eligibility did not preserve the live key barrier: state=%+v err=%v", state, err)
	}
	stored, storedUID, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || storedUID != f.producer || stored.CompactedThrough != 0 || stored.ArchiveHead != nil || len(stored.Entries) != 1 ||
		hashJSON(stored.Entries[1]) != hashJSON(entry) {
		t.Fatalf("failed archive attempt changed canonical live journal evidence: producer=%+v uid=%q err=%v", stored, storedUID, err)
	}
}
