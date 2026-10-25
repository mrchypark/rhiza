package operator

import (
	"context"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
)

type guardedActivationStorageVerifier struct {
	e     *FencingExecutor
	proof fencing.StorageEffectsExcluded
	calls int
}

func (v *guardedActivationStorageVerifier) verifyEngineRepairCandidateActive(ctx context.Context, runtime fencing.RuntimeBinding, repair fencing.EngineRepairRecord) error {
	v.calls++
	if repair.StorageProofDigest != v.proof.ProofDigest || repair.CurrentCandidateRuntime() == nil || *repair.CurrentCandidateRuntime() != runtime {
		return fmt.Errorf("activation verifier received a different canonical proof or candidate runtime")
	}
	state, err := v.e.read(ctx, repair.Binding.StatefulSetUID, repair.Binding.SourceClusterID)
	if err != nil {
		return err
	}
	return v.e.verifyEngineRepairStorageSettlements(ctx, repair, state.Ledger, v.proof)
}

// The candidate is advanced through the canonical REMOVE/ADD record and the
// verified activation CAS twice. The original G0 dispatch deliberately has no
// terminal receipt throughout; this checks that activation consumes storage
// closure without upgrading it into physical completion.
func TestEngineRepairActivationPreservesUnknownOriginalG0AcrossFirstAndSecondActivation(t *testing.T) {
	e, api, id, runtime := newMutationStoreFixture(t)
	e.MutationProfile = fencing.MutationHeldExecutionProfile
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	preparePendingArchiveGenesis(t, e, api, id, runtime)
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatalf("reserve original producer before WAL binding: %v", err)
	}
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal-held-profile"
	configureMutationWorkloadFixture(t, e, api, id, runtime)
	if _, err := e.bindWALAtScopeWithStage(t.Context(), runtime.StatefulSetUID, id, hashJSON(scope), stages.incarnation); err != nil {
		t.Fatalf("bind and activate original producer: %v", err)
	}
	run, err := e.enrolledMutationRun(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		t.Fatal(err)
	}
	f := heldProfileFixture{e: e, api: api, id: id, runtime: runtime, producer: run.MutationProducerUID,
		scope: scope, stages: stages, receipts: receipts}

	survivorID := fencing.Identity{SourceClusterID: f.id.SourceClusterID, NodeID: "survivor", PodName: "survivor-0",
		PodUID: "survivor-pod", RunID: "survivor-run", AssignmentID: hashBytes([]byte("survivor-assignment"))}
	survivorRuntime := f.runtime
	survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID = survivorID.NodeID, survivorID.PodName, survivorID.PodUID
	survivorRuntime.RunID, survivorRuntime.AssignmentID, survivorRuntime.ContainerID = survivorID.RunID, survivorID.AssignmentID, "survivor-container"
	if _, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(survivorID, survivorRuntime)
	}); err != nil {
		t.Fatalf("register the authenticated surviving voter: %v", err)
	}
	if _, err := f.e.reserveMutationProducer(t.Context(), survivorRuntime); err != nil {
		t.Fatalf("reserve survivor producer: %v", err)
	}
	survivorStages, err := f.e.sourceStagesFor(t.Context(), survivorRuntime)
	if err != nil {
		t.Fatal(err)
	}
	survivorID.WALIdentity = "survivor-wal"
	configureMutationWorkloadFixture(t, f.e, f.api, survivorID, survivorRuntime)
	if _, err := f.e.bindWALAtScopeWithStage(t.Context(), f.runtime.StatefulSetUID, survivorID, hashJSON(f.scope), survivorStages.incarnation); err != nil {
		t.Fatalf("bind and activate survivor producer: %v", err)
	}

	payload := []byte(`{"activation":"unknown original held create"}`)
	intent := f.admitUpload(t, 1, "membership/activation-g0", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	run, err = f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	owner := hashJSON([]any{f.runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatalf("acquire original G0 key lock: %v", err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(producer *fencing.MutationProducer) error {
		if err := producer.Prepare(1, preparation); err != nil {
			return err
		}
		if err := producer.GrantDispatch(1, "activation-unknown-original-dispatch"); err != nil {
			return err
		}
		return producer.SealDispatch(1)
	}); err != nil {
		t.Fatalf("seal the original producer's unresolved G0 permission: %v", err)
	}
	repair := testEngineRepairRecord(t, f)
	if err := f.e.closeEngineRepairOldCohort(t.Context(), repair); err != nil {
		t.Fatalf("record exact old-member REMOVE stop: %v", err)
	}
	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 0
	server.loseCreate = false
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	guard, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope)
	if err != nil {
		t.Fatalf("install exact G0 guard and close helper rights: %v", err)
	}
	proof, err := f.e.settleEngineRepairStorageEffects(t.Context(), repair)
	if err != nil {
		t.Fatalf("create bounded storage-closure proof before REMOVE/ADD: %v", err)
	}

	store, err := NewEngineRepairStore(f.e.AuthorityState, repair.Binding.StatefulSetUID)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := store.ReadEngineRepair(t.Context(), repair.Binding.SourceClusterID, repair.Binding.OperationID)
	if err != nil || removed.StorageProofDigest != proof.ProofDigest {
		t.Fatalf("read canonical REMOVE storage proof: record=%+v err=%v", removed, err)
	}
	storageClosingDigest := removed.Digest()
	removed.Phase = fencing.RepairRemoved
	removed.RemoveTerminalDigest = hashJSON([]any{"unit REMOVE terminal", *removed.RemovalOperation, proof.ProofDigest})
	removed, err = store.CompareAndSwapEngineRepair(t.Context(), removed.Binding.SourceClusterID, storageClosingDigest, removed)
	if err != nil {
		t.Fatalf("persist canonical REMOVE terminal: %v", err)
	}
	request := network.MembershipChange{OperationID: engineRepairAddOperationID(removed.Binding.OperationID),
		ClusterID: removed.Binding.SourceClusterID, ExpectedConfigID: removed.RemovalOperation.Target.ConfigID,
		ExpectedAbortSlot: 0, Add: &removed.CandidateMember}
	adding := removed
	adding.Phase, adding.AddRequestHash = fencing.RepairAdding, hashJSON(request)
	adding, err = store.CompareAndSwapEngineRepair(t.Context(), removed.Binding.SourceClusterID, removed.Digest(), adding)
	if err != nil {
		t.Fatalf("persist canonical ADD request: %v", err)
	}

	candidate := adding.CurrentCandidateIdentity()
	installActiveRestartPod(t, f.api, candidate, adding)
	promotionHash := hashBytes([]byte("canonical ADD promotion proof"))
	if _, err := f.e.change(t.Context(), adding.Binding.StatefulSetUID, adding.Binding.SourceClusterID, func(ledger *fencing.Ledger) error {
		if err := ledger.Bind(candidate); err != nil {
			return err
		}
		return ledger.PromoteLearner(candidate, request.OperationID, promotionHash)
	}); err != nil {
		t.Fatalf("record candidate ADD promotion in the authority root: %v", err)
	}
	candidateRuntime := *adding.CurrentCandidateRuntime()
	enrollActiveEngineRepairMutationProducer(t, f.e, candidate, candidateRuntime)
	verifier := &guardedActivationStorageVerifier{e: f.e, proof: proof}

	for activation := 1; activation <= 2; activation++ {
		active, err := f.e.confirmEngineRepairActivationWithVerifier(t.Context(), adding.Binding.StatefulSetUID,
			survivorID, adding.Binding.OperationID, verifier)
		if err != nil || active.Phase != fencing.RepairActive {
			t.Fatalf("activation %d did not confirm the exact canonical ADD: phase=%s err=%v", activation, active.Phase, err)
		}
		producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
		if err != nil || uid != f.producer || producer.BarrierComplete() {
			t.Fatalf("activation %d changed the old physical-completion boundary: uid=%q complete=%t err=%v", activation, uid, producer.BarrierComplete(), err)
		}
		entry := producer.Entries[1]
		if entry.GenerationZeroGuard == nil || *entry.GenerationZeroGuard != guard || entry.CopyCompletion != nil || entry.Resolution == nil ||
			entry.Resolution.AllCopiesTerminal || entry.Dispatches[guard.DispatchID].ReceiptDigest != "" {
			t.Fatalf("activation %d fabricated original G0 terminal evidence: %+v", activation, entry)
		}
		state, err := f.e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
		if err != nil || state.Barrier == nil || state.Retirement != nil || !state.Barrier.matchesGuard(guard) {
			t.Fatalf("activation %d removed or changed the original G0 name barrier: state=%+v err=%v", activation, state, err)
		}
		if verifier.calls != activation {
			t.Fatalf("activation %d did not rerun the bounded closure verifier: calls=%d", activation, verifier.calls)
		}
	}
	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || uid != f.producer {
		t.Fatalf("original producer readback failed after activation: uid=%q err=%v", uid, err)
	}
	if _, err := f.receipts.get(mutationExecutionReceiptName(mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: run.Identity.WALIdentity,
		ProducerUID: f.producer, Sequence: 1, Intent: intent, Preparation: preparation, DispatchID: guard.DispatchID})); err == nil {
		t.Fatal("activation created an immutable receipt for the unknown original G0 dispatch")
	}
	if _, err := producer.PlanMutationCompaction(f.producer, 1); err == nil {
		t.Fatal("activation made the original G0 entry eligible for physical archive compaction")
	}
	if retired, err := f.e.retireGenerationZeroBarrier(t.Context(), f.runtime, f.producer, 1, producer.Entries[1]); err != nil || retired {
		t.Fatalf("activation retired the G0 barrier without original terminal evidence: retired=%t err=%v", retired, err)
	}
}
