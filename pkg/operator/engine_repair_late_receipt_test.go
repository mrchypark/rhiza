package operator

import (
	"bytes"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// A storage-effects proof may be durable while an original G0 dispatch is
// still unresolved. A later exact provider receipt must remain attachable to
// that dispatch without changing the already-persisted proof or letting the
// unresolved entry be archived early.
func TestEngineRepairStorageProofSurvivesLateOriginalG0Receipt(t *testing.T) {
	f := newGenerationZeroReceiptFixture(t)
	payload := []byte(`{"membership":"late original receipt"}`)
	intent := f.admitUpload(t, 1, "membership/g0-late-original", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	run, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	const dispatchID = "late-original-g0-dispatch"
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatalf("acquire original operation key: %v", err)
	}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, dispatchID); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("close unresolved original dispatch: %v", err)
	}

	// Storage closure needs a surviving canonical voter to perform the exact
	// old-cohort admission cut. Reuse the existing closure fixture's enrolled
	// survivor setup rather than weakening that production precondition.
	survivorID := fencing.Identity{SourceClusterID: f.id.SourceClusterID, NodeID: "survivor", PodName: "survivor-0", PodUID: "survivor-pod",
		RunID: "survivor-run", AssignmentID: hashBytes([]byte("survivor-assignment"))}
	survivorRuntime := f.runtime
	survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID = survivorID.NodeID, survivorID.PodName, survivorID.PodUID
	survivorRuntime.RunID, survivorRuntime.AssignmentID, survivorRuntime.ContainerID = survivorID.RunID, survivorID.AssignmentID, "survivor-container"
	if _, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(survivorID, survivorRuntime)
	}); err != nil {
		t.Fatalf("register canonical surviving voter: %v", err)
	}
	if _, err := f.e.reserveMutationProducer(t.Context(), survivorRuntime); err != nil {
		t.Fatalf("reserve surviving voter producer: %v", err)
	}
	survivorStages, err := f.e.sourceStagesFor(t.Context(), survivorRuntime)
	if err != nil {
		t.Fatal(err)
	}
	survivorID.WALIdentity = "survivor-wal"
	configureMutationWorkloadFixture(t, f.e, f.api, survivorID, survivorRuntime)
	if _, err := f.e.bindWALAtScopeWithStage(t.Context(), f.runtime.StatefulSetUID, survivorID, hashJSON(f.scope), survivorStages.incarnation); err != nil {
		t.Fatalf("bind and activate surviving voter producer: %v", err)
	}

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
	originalProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}

	repair := testEngineRepairRecord(t, f)
	if err := f.e.closeEngineRepairOldCohort(t.Context(), repair); err != nil {
		t.Fatalf("record the exact canonical old-member stop: %v", err)
	}
	guard, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope)
	if err != nil {
		t.Fatalf("establish G0 barrier and settle helper rights: %v", err)
	}
	if guard.DispatchID != dispatchID {
		t.Fatalf("helper guard changed the original dispatch identity: got=%q want=%q", guard.DispatchID, dispatchID)
	}
	proof, err := f.e.settleEngineRepairStorageEffects(t.Context(), repair)
	if err != nil || proof.ProofDigest == "" || proof.Validate(repair) != nil {
		t.Fatalf("persist storage-effects proof before original receipt: proof=%+v err=%v", proof, err)
	}
	repairStore := mustEngineRepairStore(t, f.e, repair)
	proofRef, err := repairStore.ReadEngineRepair(t.Context(), repair.Binding.SourceClusterID, repair.Binding.OperationID)
	if err != nil || proofRef.StorageProofDigest != proof.ProofDigest {
		t.Fatalf("canonical repair did not retain the storage proof: record=%+v err=%v", proofRef, err)
	}
	producer, producerUID, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || producerUID != f.producer {
		t.Fatalf("read producer before late original completion: uid=%q err=%v", producerUID, err)
	}
	entry := producer.Entries[1]
	if entry.CopyCompletion != nil || entry.Dispatches[dispatchID].ReceiptDigest != "" {
		t.Fatalf("original dispatch was already terminal before late completion: %+v", entry)
	}
	if _, err := producer.PlanMutationCompaction(f.producer, 1); err == nil {
		t.Fatal("unresolved original dispatch became archiveable after storage proof")
	}
	retired, err := f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, entry)
	if err != nil || retired {
		t.Fatalf("G0 barrier retired before original receipt: retired=%t err=%v", retired, err)
	}

	// The actual original provider request happens only after the storage proof
	// is persisted. It receives a create-only 412 against the exact held object.
	result, err := originalProvider.uploadHeldCreate(t.Context(), intent.Key, intent, preparation, bytes.NewReader(payload))
	if err != nil || result.Outcome != fencing.MutationRejected || !result.ConditionFailed || !result.AllCopiesTerminal ||
		result.Generation != 0 || result.PayloadDigest != "" {
		t.Fatalf("late original request was not an exact terminal create-only 412: result=%+v err=%v", result, err)
	}
	server.mu.Lock()
	if len(server.requests) != 2 || server.requests[1].Status != 412 || server.requests[1].IfGenerationMatch != "0" {
		requests := append([]heldGCSRequest(nil), server.requests...)
		server.mu.Unlock()
		t.Fatalf("late original request did not reach provider with create-only condition: %+v", requests)
	}
	server.mu.Unlock()

	receipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: f.producer,
		Sequence: 1, Intent: intent, Preparation: preparation, DispatchID: dispatchID, Outcome: result.Outcome,
		ConditionFailed: result.ConditionFailed, Generation: result.Generation, Metageneration: result.Metageneration,
		PayloadDigest: result.PayloadDigest, AllCopiesTerminal: result.AllCopiesTerminal}
	receipt.ReceiptDigest = receipt.digest()
	if _, err := f.receipts.put(receipt); err != nil {
		t.Fatalf("persist exact late original receipt: %v", err)
	}
	if producer.Entries[1].GenerationZeroRights == nil {
		t.Fatal("late original receipt lost the terminal helper-right evidence")
	}
	completion := fencing.MutationCopyCompletion{PreparationHash: guard.PreparationHash, ReceiptDigest: receipt.ReceiptDigest,
		GenerationZeroRightsDigest: producer.Entries[1].GenerationZeroRights.Digest()}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.RecordDispatchTerminal(1, dispatchID, receipt.ReceiptDigest); err != nil {
			return err
		}
		return p.ConfirmCopiesTerminal(1, completion)
	}); err != nil {
		t.Fatalf("bind exact late receipt to original dispatch: %v", err)
	}
	producer, producerUID, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || producerUID != f.producer || producer.Entries[1].CopyCompletion == nil ||
		producer.Entries[1].CopyCompletion.ReceiptDigest != receipt.ReceiptDigest {
		t.Fatalf("late original completion was not durably recorded: uid=%q entry=%+v err=%v", producerUID, producer.Entries[1], err)
	}

	replayedProof, err := f.e.settleEngineRepairStorageEffects(t.Context(), repair)
	if err != nil || replayedProof.ProofDigest != proof.ProofDigest {
		t.Fatalf("same persisted storage proof did not revalidate after late receipt: first=%s replay=%s err=%v", proof.ProofDigest, replayedProof.ProofDigest, err)
	}
	currentRepair, err := repairStore.ReadEngineRepair(t.Context(), repair.Binding.SourceClusterID, repair.Binding.OperationID)
	if err != nil || currentRepair.StorageProofDigest != proof.ProofDigest {
		t.Fatalf("late receipt changed or lost canonical storage proof: record=%+v err=%v", currentRepair, err)
	}

	producer, producerUID, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || producerUID != f.producer {
		t.Fatalf("read terminal producer: uid=%q err=%v", producerUID, err)
	}
	entry = producer.Entries[1]
	retired, err = f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, entry)
	if err != nil || !retired {
		t.Fatalf("retire barrier only after exact late receipt: retired=%t err=%v", retired, err)
	}
	if _, err := f.e.commitMutationArchive(t.Context(), f.runtime, storeCompactionPlan(t, f.e, f.runtime, 1)); err != nil {
		t.Fatalf("archive terminal original/helper evidence: %v", err)
	}
	archived, err := f.e.lookupMutation(t.Context(), f.runtime.StatefulSetUID, run.Identity,
		fencing.MutationLookupRequest{ProducerUID: f.producer, Sequence: 1, Intent: intent})
	if err != nil || !archived.Archived || archived.Entry == nil || archived.Entry.CopyCompletion == nil ||
		archived.Entry.CopyCompletion.ReceiptDigest != receipt.ReceiptDigest {
		t.Fatalf("archive omitted exact late original receipt: result=%+v err=%v", archived, err)
	}
	server.mu.Lock()
	postsBeforeReplay := server.posts
	server.mu.Unlock()
	if err := f.e.ExecuteSourceMutation(t.Context(), f.runtime, f.producer, 1, intent, f.scope, originalProvider, f.receipts); err != nil {
		t.Fatalf("archived late original request did not replay from terminal evidence: %v", err)
	}
	server.mu.Lock()
	postsAfterReplay := server.posts
	server.mu.Unlock()
	if postsAfterReplay != postsBeforeReplay {
		t.Fatalf("archived replay repeated provider request: posts=%d->%d", postsBeforeReplay, postsAfterReplay)
	}
}
