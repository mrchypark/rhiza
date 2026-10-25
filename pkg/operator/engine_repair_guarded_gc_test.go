package operator

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

func makeGuardedG0ClosureFixtureWithActiveSurvivor(t *testing.T) (heldProfileFixture, fencing.EngineRepairRecord, fencing.MutationProducer, fencing.Run, fencing.MutationEntry, heldProfileFixture, fencing.Run) {
	t.Helper()
	f := newGenerationZeroReceiptFixture(t)
	ctx := t.Context()

	// The canonical repair record snapshots all source runs. Enroll and bind
	// the unrelated survivor before creating that record so it is genuine
	// recorded inventory, not a post-record run that the proof must reject.
	survivorID := fencing.Identity{SourceClusterID: f.id.SourceClusterID, NodeID: "survivor", PodName: "survivor-0",
		PodUID: "gc-survivor-pod", RunID: "gc-survivor-run", AssignmentID: hashBytes([]byte("gc-survivor-assignment"))}
	survivorRuntime := f.runtime
	survivorRuntime.NodeID, survivorRuntime.PodName, survivorRuntime.PodUID = survivorID.NodeID, survivorID.PodName, survivorID.PodUID
	survivorRuntime.RunID, survivorRuntime.AssignmentID, survivorRuntime.ContainerID = survivorID.RunID, survivorID.AssignmentID, "gc-survivor-container"
	if _, err := f.e.change(ctx, survivorRuntime.StatefulSetUID, survivorRuntime.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.RegisterEnrolled(survivorID, survivorRuntime)
	}); err != nil {
		t.Fatalf("register the active survivor before canonical repair inventory: %v", err)
	}
	if _, err := f.e.reserveMutationProducer(ctx, survivorRuntime); err != nil {
		t.Fatalf("reserve the active survivor producer: %v", err)
	}
	survivorStages, err := f.e.sourceStagesFor(ctx, survivorRuntime)
	if err != nil {
		t.Fatalf("load active survivor source stages: %v", err)
	}
	survivorID.WALIdentity = "gc-survivor-wal"
	configureMutationWorkloadFixture(t, f.e, f.api, survivorID, survivorRuntime)
	if _, err := f.e.bindWALAtScopeWithStage(ctx, survivorRuntime.StatefulSetUID, survivorID, hashJSON(f.scope), survivorStages.incarnation); err != nil {
		t.Fatalf("bind the active survivor mutation scope: %v", err)
	}
	survivorRun, err := f.e.enrolledMutationRun(ctx, survivorRuntime)
	if err != nil {
		t.Fatalf("read active survivor run: %v", err)
	}
	survivorReceipts, err := f.e.receiptsForRuntime(survivorRuntime)
	if err != nil {
		t.Fatalf("open active survivor receipt store: %v", err)
	}
	survivor := f
	survivor.id, survivor.runtime, survivor.producer = survivorID, survivorRuntime, survivorRun.MutationProducerUID
	survivor.stages, survivor.receipts = survivorStages, survivorReceipts

	payload := []byte(`{"closure":"exact generation-zero binding"}`)
	intent := f.admitUpload(t, 1, "membership/engine-repair-guarded-closure", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, payload)
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash,
		GenerationSet: true, Generation: 0}
	oldRun, err := f.e.enrolledMutationRun(ctx, f.runtime)
	if err != nil {
		t.Fatalf("read original producer run: %v", err)
	}
	owner := hashJSON([]any{f.runtime, oldRun.Identity.WALIdentity, f.producer, uint64(1), intent.OperationID})
	if err := f.e.acquireMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner, true); err != nil {
		t.Fatalf("acquire original G0 key lock: %v", err)
	}
	if _, err := f.e.changeMutationProducer(ctx, f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, preparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "guarded-closure-original-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return p.Close("guarded-closure-old-close", hashBytes([]byte("guarded-closure-old-close")))
	}); err != nil {
		t.Fatalf("close exact original dispatch prefix: %v", err)
	}
	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 0
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	originalProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatalf("create original held provider: %v", err)
	}
	if _, err := originalProvider.uploadHeldCreate(ctx, intent.Key, intent, preparation, bytes.NewReader(payload)); err == nil {
		t.Fatal("original G0 dispatch unexpectedly returned a trusted response")
	}
	server.mu.Lock()
	origin, exists := server.objects[pathJoin(f.scope.Prefix, intent.Key)]
	server.mu.Unlock()
	if !exists || !origin.temporaryHold || !bytes.Equal(origin.payload, payload) {
		t.Fatalf("fixture did not preserve the held G0 origin: exists=%t object=%+v", exists, origin)
	}
	repair := testEngineRepairRecord(t, f)
	if _, err := f.e.settleEngineRepairZeroGeneration(ctx, repair, f.runtime, f.producer, 1, f.scope); err != nil {
		t.Fatalf("establish exact G0 guard and helper rights: %v", err)
	}
	producer, producerUID, _, err := f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil || producerUID != f.producer {
		t.Fatalf("read guarded producer: uid=%q err=%v", producerUID, err)
	}
	entry, ok := producer.Entries[1]
	if !ok || entry.GenerationZeroGuard == nil || entry.GenerationZeroRights == nil || !entry.GenerationZeroRights.Sealed ||
		!entry.GenerationZeroRights.HelperRightsRetired || !generationZeroOperatorRightsTerminal(entry.GenerationZeroRights) ||
		entry.CopyCompletion != nil || entry.Resolution == nil || entry.Resolution.AllCopiesTerminal {
		t.Fatalf("fixture does not separate helper closure from original physical completion: %+v", entry)
	}
	return f, repair, producer, oldRun, entry, survivor, survivorRun
}

func TestEngineRepairUnknownG0GuardDoesNotBlockEligibleDeleteOrNextRound(t *testing.T) {
	f, repair, oldProducer, oldRun, oldEntry, survivor, survivorRun := makeGuardedG0ClosureFixtureWithActiveSurvivor(t)
	guard := *oldEntry.GenerationZeroGuard
	if oldProducer.BarrierComplete() || oldEntry.CopyCompletion != nil || oldEntry.Resolution == nil || oldEntry.Resolution.AllCopiesTerminal ||
		oldEntry.Dispatches[guard.DispatchID].ReceiptDigest != "" {
		t.Fatalf("fixture did not preserve unresolved original G0 completion: producer=%+v entry=%+v", oldProducer, oldEntry)
	}
	ctx := t.Context()
	guardKeyState, err := f.e.readMutationKeyState(ctx, oldEntry.Intent.StoreScopeHash, oldEntry.Intent.Key)
	if err != nil || guardKeyState.Barrier == nil || !guardKeyState.Barrier.matchesGuard(guard) || guardKeyState.Retirement != nil {
		t.Fatalf("unknown original G0 dispatch is not protected by its exact live barrier: state=%+v err=%v", guardKeyState, err)
	}
	guardObject := f.e.SourceMutationStorage.Bucket(f.scope.Bucket).Object(pathJoin(f.scope.Prefix, guard.Key)).Generation(guard.ObjectGeneration)
	guardAttrs, err := guardObject.Attrs(ctx)
	if err != nil || guardAttrs.Generation != guard.ObjectGeneration || guardAttrs.Metageneration != guard.ObjectMetageneration || !guardAttrs.TemporaryHold {
		t.Fatalf("unknown original G0 object is not retained under its exact temporary hold: attrs=%+v err=%v", guardAttrs, err)
	}
	guardReader, err := guardObject.NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	guardPayload, readErr := io.ReadAll(guardReader)
	closeErr := guardReader.Close()
	if err := errors.Join(readErr, closeErr); err != nil || !bytes.Equal(guardPayload, []byte(`{"closure":"exact generation-zero binding"}`)) {
		t.Fatalf("unknown original G0 object bytes changed: payload=%q err=%v", guardPayload, err)
	}

	eligibleKey := "objects/eligible-gc"
	eligibleObject := f.e.SourceMutationStorage.Bucket(f.scope.Bucket).Object(pathJoin(f.scope.Prefix, eligibleKey))
	writer := eligibleObject.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	if _, err := writer.Write([]byte("eligible-generation")); err != nil {
		_ = writer.Close()
		t.Fatalf("seed an independently eligible object generation: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("persist eligible object generation: %v", err)
	}
	eligibleAttrs := writer.Attrs()
	if eligibleAttrs == nil || eligibleAttrs.Generation <= 0 {
		t.Fatalf("fixture object has no concrete generation: %+v", eligibleAttrs)
	}
	eligibleGeneration := eligibleAttrs.Generation
	deleteIntent := survivor.admitDelete(t, 1, eligibleKey, eligibleGeneration)
	provider := heldProfileProvider(t, survivor)
	if err := f.e.ExecuteSourceMutation(ctx, survivor.runtime, survivor.producer, 1, deleteIntent, survivor.scope, provider, survivor.receipts); err != nil {
		t.Fatalf("execute eligible exact-generation source mutation delete: %v", err)
	}
	producer, producerUID, _, err := f.e.AuthorityState.ReadProducer(ctx, survivor.runtime)
	if err != nil || producerUID != survivor.producer {
		t.Fatalf("read successor producer after eligible deletion: uid=%q err=%v", producerUID, err)
	}
	deletedEntry, exists := producer.Entries[1]
	if !exists || deletedEntry.Resolution == nil || deletedEntry.Resolution.Outcome != fencing.MutationCommitted ||
		!deletedEntry.Resolution.AllCopiesTerminal || deletedEntry.Preparation == nil || deletedEntry.Preparation.Generation != eligibleGeneration ||
		!deletedEntry.DispatchClosed || len(deletedEntry.Dispatches) != 1 || deletedEntry.CopyCompletion != nil {
		t.Fatalf("eligible deletion lacks exact terminal journal evidence: entry=%+v", deletedEntry)
	}
	var terminalDispatchID string
	var terminalDispatch fencing.MutationDispatch
	for dispatchID, dispatch := range deletedEntry.Dispatches {
		terminalDispatchID, terminalDispatch = dispatchID, dispatch
	}
	if terminalDispatchID == "" || !mutationDigestValidOperator(terminalDispatch.ReceiptDigest) ||
		terminalDispatch.ReceiptDigest != deletedEntry.Resolution.ReceiptDigest {
		t.Fatalf("eligible deletion lacks its exact closed-dispatch receipt digest: dispatchID=%q dispatch=%+v resolution=%+v",
			terminalDispatchID, terminalDispatch, deletedEntry.Resolution)
	}
	terminalReceipt, receiptErr := survivor.receipts.get(mutationExecutionReceiptName(mutationExecutionReceipt{
		Runtime: survivor.runtime, WALIdentity: survivorRun.Identity.WALIdentity, ProducerUID: survivor.producer,
		Sequence: 1, Intent: deleteIntent, Preparation: *deletedEntry.Preparation, DispatchID: terminalDispatchID,
	}))
	if receiptErr != nil || terminalReceipt.ReceiptDigest != terminalReceipt.digest() ||
		terminalReceipt.Runtime != survivor.runtime || terminalReceipt.WALIdentity != survivorRun.Identity.WALIdentity ||
		terminalReceipt.ProducerUID != survivor.producer || terminalReceipt.Sequence != 1 || terminalReceipt.Intent != deleteIntent ||
		terminalReceipt.Preparation != *deletedEntry.Preparation || terminalReceipt.DispatchID != terminalDispatchID ||
		terminalReceipt.Outcome != fencing.MutationCommitted || !terminalReceipt.AllCopiesTerminal ||
		terminalReceipt.Generation != eligibleGeneration || terminalReceipt.ReceiptDigest != terminalDispatch.ReceiptDigest ||
		terminalReceipt.ReceiptDigest != deletedEntry.Resolution.ReceiptDigest {
		t.Fatalf("eligible deletion terminal receipt does not match the exact journal dispatch: receipt=%+v entry=%+v err=%v",
			terminalReceipt, deletedEntry, receiptErr)
	}
	if _, err := eligibleObject.Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("eligible exact generation was not actually deleted: err=%v", err)
	}
	deleteKeyState, err := f.e.readMutationKeyState(ctx, deleteIntent.StoreScopeHash, eligibleKey)
	if err != nil || deleteKeyState.Owner != "" || deleteKeyState.Barrier != nil || deleteKeyState.PendingGenerationZero != nil {
		t.Fatalf("completed delete did not release its key lock for the next round: state=%+v err=%v", deleteKeyState, err)
	}

	nextPayload := []byte("next mutation round after eligible deletion")
	nextIntent := survivor.admitUpload(t, 2, eligibleKey, fencing.MutationCondition{Mode: fencing.MutationUnconditional}, nextPayload)
	if err := f.e.ExecuteSourceMutation(ctx, survivor.runtime, survivor.producer, 2, nextIntent, survivor.scope, provider, survivor.receipts); err != nil {
		t.Fatalf("next mutation round could not use the released key: %v", err)
	}
	nextAttrs, err := f.e.SourceMutationStorage.Bucket(f.scope.Bucket).Object(pathJoin(f.scope.Prefix, eligibleKey)).Attrs(ctx)
	if err != nil || nextAttrs.Generation <= eligibleGeneration {
		t.Fatalf("next round did not create a newer object generation: attrs=%+v prior=%d err=%v", nextAttrs, eligibleGeneration, err)
	}
	nextReader, err := f.e.SourceMutationStorage.Bucket(f.scope.Bucket).Object(pathJoin(f.scope.Prefix, eligibleKey)).NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nextBytes, readErr := io.ReadAll(nextReader)
	closeErr = nextReader.Close()
	if err := errors.Join(readErr, closeErr); err != nil || !bytes.Equal(nextBytes, nextPayload) {
		t.Fatalf("next round object readback differs: payload=%q err=%v", nextBytes, err)
	}
	nextKeyState, err := f.e.readMutationKeyState(ctx, nextIntent.StoreScopeHash, eligibleKey)
	if err != nil || nextKeyState.Owner != "" {
		t.Fatalf("next round left its key lock held: state=%+v err=%v", nextKeyState, err)
	}

	guardAfter, guardErr := guardObject.Attrs(ctx)
	guardStateAfter, stateErr := f.e.readMutationKeyState(ctx, oldEntry.Intent.StoreScopeHash, oldEntry.Intent.Key)
	oldProducerAfter, oldUID, _, producerErr := f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	oldEntryAfter := oldProducerAfter.Entries[1]
	if err := f.e.verifyGenerationZeroGuard(ctx, f.runtime, oldRun, f.producer, 1, oldEntryAfter); err != nil {
		t.Fatalf("original G0 guard no longer verifies after unrelated deletion and next round: %v", err)
	}
	if guardErr != nil || stateErr != nil || guardStateAfter.Barrier == nil || *guardStateAfter.Barrier != *guardKeyState.Barrier || guardStateAfter.Retirement != nil ||
		producerErr != nil || oldUID != f.producer || oldProducerAfter.BarrierComplete() || oldEntryAfter.GenerationZeroGuard == nil ||
		*oldEntryAfter.GenerationZeroGuard != guard || oldEntryAfter.CopyCompletion != nil || oldEntryAfter.Resolution == nil ||
		oldEntryAfter.Resolution.AllCopiesTerminal || oldEntryAfter.Dispatches[guard.DispatchID].ReceiptDigest != "" ||
		guardAfter.Generation != guard.ObjectGeneration || guardAfter.Metageneration != guard.ObjectMetageneration || !guardAfter.TemporaryHold {
		t.Fatalf("eligible object deletion or later round disturbed unknown original G0 protection: guard=%+v keyState=%+v oldEntry=%+v oldUID=%q producerErr=%v stateErr=%v",
			guardAfter, guardStateAfter, oldEntryAfter, oldUID, errors.Join(guardErr, producerErr), stateErr)
	}
	originalReceipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: oldRun.Identity.WALIdentity, ProducerUID: f.producer,
		Sequence: 1, Intent: oldEntry.Intent, Preparation: *oldEntry.Preparation, DispatchID: guard.DispatchID}
	if _, err := f.receipts.get(mutationExecutionReceiptName(originalReceipt)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("separate eligible delete fabricated the unknown original dispatch receipt: %v", err)
	}
	if err := oldProducerAfter.ValidateGenerationZeroGuard(oldUID, 1, repair, oldEntryAfter); err != nil {
		t.Fatalf("canonical G0 guard no longer validates after eligible deletion: %v", err)
	}
}
