package operator

import (
	"errors"
	"os"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestEngineRepairStorageClosureProgressesWithUnknownOriginalG0Receipt(t *testing.T) {
	e, api, id, runtime := newMutationStoreFixture(t)
	e.MutationProfile = fencing.MutationHeldExecutionProfile
	preparePendingArchiveGenesis(t, e, api, id, runtime)
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
		t.Fatalf("reserve original producer before WAL binding: %v", err)
	}
	stages, err := e.sourceStagesFor(t.Context(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = "wal-guarded-progress"
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

	survivorID := fencing.Identity{SourceClusterID: id.SourceClusterID, NodeID: "survivor", PodName: "survivor-0", PodUID: "survivor-pod",
		RunID: "survivor-run", AssignmentID: hashBytes([]byte("survivor-assignment"))}
	survivorRuntime := runtime
	survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID = survivorID.NodeID, survivorID.PodName, survivorID.PodUID
	survivorRuntime.RunID, survivorRuntime.AssignmentID, survivorRuntime.ContainerID = survivorID.RunID, survivorID.AssignmentID, "survivor-container"
	if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(survivorID, survivorRuntime)
	}); err != nil {
		t.Fatalf("register canonical survivor: %v", err)
	}
	if _, err := e.reserveMutationProducer(t.Context(), survivorRuntime); err != nil {
		t.Fatalf("reserve canonical survivor producer: %v", err)
	}
	survivorStages, err := e.sourceStagesFor(t.Context(), survivorRuntime)
	if err != nil {
		t.Fatal(err)
	}
	survivorID.WALIdentity = "survivor-wal"
	configureMutationWorkloadFixture(t, e, api, survivorID, survivorRuntime)
	if _, err := e.bindWALAtScopeWithStage(t.Context(), runtime.StatefulSetUID, survivorID, hashJSON(scope), survivorStages.incarnation); err != nil {
		t.Fatalf("bind and activate canonical survivor: %v", err)
	}

	payload := []byte(`{"guard":"keeps original create harmless"}`)
	intent := f.admitUpload(t, 1, "membership/current", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	owner := hashJSON([]any{runtime, run.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := e.acquireMutationKey(t.Context(), intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatalf("acquire the original dispatch's durable key lock: %v", err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	if _, err := e.changeMutationProducer(t.Context(), runtime, func(producer *fencing.MutationProducer) error {
		if err := producer.Prepare(1, preparation); err != nil {
			return err
		}
		if err := producer.GrantDispatch(1, "unknown-original-g0-dispatch"); err != nil {
			return err
		}
		if err := producer.SealDispatch(1); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("prepare original producer with an unresolved create: %v", err)
	}
	repair := testEngineRepairRecord(t, f)
	if err := e.closeEngineRepairOldCohort(t.Context(), repair); err != nil {
		t.Fatalf("record the canonical old-member stop and close its producer: %v", err)
	}

	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 0
	server.loseCreate = false
	server.mu.Unlock()
	e.SourceMutationStorage = client
	guard, err := e.settleEngineRepairZeroGeneration(t.Context(), repair, runtime, f.producer, 1, scope)
	if err != nil {
		t.Fatalf("establish the independently journaled G0 helper guard: %v", err)
	}
	if guard.ObjectGeneration <= 0 || guard.ObjectMetageneration <= 0 || guard.GuardDigest == "" {
		t.Fatalf("helper guard lacks exact object evidence: %+v", guard)
	}

	producer, producerUID, _, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
	if err != nil || producerUID != f.producer {
		t.Fatalf("read guarded producer: uid=%q err=%v", producerUID, err)
	}
	entry := producer.Entries[1]
	if entry.GenerationZeroGuard == nil || *entry.GenerationZeroGuard != guard || entry.GenerationZeroRights == nil ||
		!entry.GenerationZeroRights.Sealed || !entry.GenerationZeroRights.HelperRightsRetired || len(entry.GenerationZeroRights.Rights) != 1 ||
		entry.GenerationZeroRights.Rights[0].TerminalReceiptDigest == "" || entry.CopyCompletion != nil || entry.Resolution == nil ||
		entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.AllCopiesTerminal || len(entry.Dispatches) != 1 ||
		entry.Dispatches["unknown-original-g0-dispatch"].ReceiptDigest != "" || producer.BarrierComplete() {
		t.Fatalf("fixture does not isolate terminal helper rights from the unknown original dispatch: producer=%+v entry=%+v", producer, entry)
	}
	originalReceipt := mutationExecutionReceipt{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: f.producer,
		Sequence: 1, Intent: intent, Preparation: preparation, DispatchID: "unknown-original-g0-dispatch"}
	if _, err := receipts.get(mutationExecutionReceiptName(originalReceipt)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixture unexpectedly has an original dispatch receipt: %v", err)
	}

	if _, err := producer.PlanMutationCompaction(f.producer, 1); err == nil {
		t.Fatal("archive planning accepted an original G0 dispatch without its terminal receipt")
	}
	retired, err := e.retireGenerationZeroBarrier(t.Context(), runtime, f.producer, 1, entry)
	if err != nil || retired {
		t.Fatalf("physical barrier retired without original terminal evidence: retired=%t err=%v", retired, err)
	}
	beforeClosure, err := e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || beforeClosure.Barrier == nil || beforeClosure.Retirement != nil {
		t.Fatalf("unknown original dispatch was not kept behind its live-name barrier: state=%+v err=%v", beforeClosure, err)
	}

	proof, err := e.settleEngineRepairStorageEffects(t.Context(), repair)
	if err != nil {
		t.Fatalf("ordinary source closure remained blocked despite the exact G0 guard and terminal helper rights: %v", err)
	}
	if proof.ProofDigest == "" || proof.Validate(repair) != nil || len(proof.RunSettlements) != 1 {
		t.Fatalf("source closure did not return its exact bounded-effects proof: proof=%+v", proof)
	}
	replayed, err := e.settleEngineRepairStorageEffects(t.Context(), repair)
	if err != nil || replayed.ProofDigest != proof.ProofDigest {
		t.Fatalf("canonical storage-closure proof did not verify idempotently: first=%s replay=%s err=%v", proof.ProofDigest, replayed.ProofDigest, err)
	}

	producer, producerUID, _, err = e.AuthorityState.ReadProducer(t.Context(), runtime)
	if err != nil || producerUID != f.producer {
		t.Fatalf("read producer after source closure: uid=%q err=%v", producerUID, err)
	}
	entry = producer.Entries[1]
	if entry.GenerationZeroGuard == nil || *entry.GenerationZeroGuard != guard || entry.CopyCompletion != nil ||
		entry.Resolution == nil || entry.Resolution.AllCopiesTerminal || entry.Dispatches["unknown-original-g0-dispatch"].ReceiptDigest != "" {
		t.Fatalf("source closure fabricated original physical terminal evidence: %+v", entry)
	}
	if _, err := producer.PlanMutationCompaction(f.producer, 1); err == nil {
		t.Fatal("source closure made the unresolved original G0 entry archiveable")
	}
	retired, err = e.retireGenerationZeroBarrier(t.Context(), runtime, f.producer, 1, entry)
	if err != nil || retired {
		t.Fatalf("source closure retired the live-name barrier without original terminal evidence: retired=%t err=%v", retired, err)
	}
	afterClosure, err := e.readMutationKeyState(t.Context(), intent.StoreScopeHash, intent.Key)
	if err != nil || afterClosure.Barrier == nil || afterClosure.Retirement != nil || *afterClosure.Barrier != *beforeClosure.Barrier {
		t.Fatalf("source closure changed physical G0 protection: before=%+v after=%+v err=%v", beforeClosure, afterClosure, err)
	}
}
