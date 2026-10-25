package operator

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// TestGenerationZeroArchivedSuccessorLookup verifies the archived-successor
// branch through the canonical generation-zero lineage verifier. The origin
// G0 run and its positive-generation successor are distinct enrolled runs;
// the successor's exact runtime, WAL, journal entry, and receipt are retained
// in its immutable archive.
func TestGenerationZeroArchivedSuccessorLookup(t *testing.T) {
	f := newGenerationZeroReceiptFixture(t)
	originPayload := []byte(`{"membership":"archived successor origin"}`)
	originIntent := f.admitUpload(t, 1, "membership/archived-successor", fencing.MutationCondition{Mode: fencing.MutationUnconditional}, originPayload)
	originRun, err := f.e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	originPreparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile,
		StoreScopeHash: originIntent.StoreScopeHash, GenerationSet: true, Generation: 0}
	owner := hashJSON([]any{f.runtime, originRun.Identity.WALIdentity, f.producer, uint64(1), originIntent.OperationID})
	if err := f.e.acquireMutationKey(t.Context(), originIntent.StoreScopeHash, originIntent.Key, owner, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.Prepare(1, originPreparation); err != nil {
			return err
		}
		if err := p.GrantDispatch(1, "archived-origin-dispatch"); err != nil {
			return err
		}
		if err := p.SealDispatch(1); err != nil {
			return err
		}
		return p.Close("close-archived-origin", hashBytes([]byte("close-archived-origin")))
	}); err != nil {
		t.Fatal(err)
	}
	server, client := newHeldGCSServer(t, f)
	server.mu.Lock()
	server.nextGeneration = 0
	server.loseCreate = false
	server.mu.Unlock()
	f.e.SourceMutationStorage = client
	originProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}
	originResult, err := originProvider.uploadHeldCreate(t.Context(), originIntent.Key, originIntent, originPreparation, bytes.NewReader(originPayload))
	if err != nil || originResult.Outcome != fencing.MutationCommitted || !originResult.AllCopiesTerminal || originResult.Generation <= 0 {
		t.Fatalf("origin did not produce an exact terminal create receipt: result=%+v err=%v", originResult, err)
	}
	originReceipt := mutationExecutionReceipt{Runtime: f.runtime, WALIdentity: originRun.Identity.WALIdentity, ProducerUID: f.producer,
		Sequence: 1, Intent: originIntent, Preparation: originPreparation, DispatchID: "archived-origin-dispatch",
		Outcome: originResult.Outcome, ConditionFailed: originResult.ConditionFailed, Generation: originResult.Generation,
		Metageneration: originResult.Metageneration, PayloadDigest: originResult.PayloadDigest, AllCopiesTerminal: originResult.AllCopiesTerminal}
	originReceipt.ReceiptDigest = originReceipt.digest()
	repair := testEngineRepairRecord(t, f)
	guard, err := f.e.settleEngineRepairZeroGeneration(t.Context(), repair, f.runtime, f.producer, 1, f.scope)
	if err != nil {
		t.Fatalf("establish G0 barrier: %v", err)
	}
	if digest, err := f.receipts.put(originReceipt); err != nil || digest != originReceipt.ReceiptDigest {
		t.Fatalf("persist original terminal receipt: digest=%q err=%v", digest, err)
	}
	completion := fencing.MutationCopyCompletion{PreparationHash: guard.PreparationHash, ReceiptDigest: originReceipt.ReceiptDigest}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil || producer.Entries[1].GenerationZeroRights == nil {
		t.Fatalf("read helper-settled origin rights: producer=%+v err=%v", producer, err)
	}
	completion.GenerationZeroRightsDigest = producer.Entries[1].GenerationZeroRights.Digest()
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		if err := p.RecordDispatchTerminal(1, guard.DispatchID, originReceipt.ReceiptDigest); err != nil {
			return err
		}
		return p.ConfirmCopiesTerminal(1, completion)
	}); err != nil {
		t.Fatalf("record original G0 completion: %v", err)
	}
	// A surviving voter is the actual positive-generation successor producer.
	successorID := fencing.Identity{SourceClusterID: f.id.SourceClusterID, NodeID: "successor", PodName: "successor-0",
		PodUID: "successor-pod", RunID: "successor-run", AssignmentID: hashBytes([]byte("successor-assignment"))}
	successorRuntime := f.runtime
	successorRuntime.NodeID, successorRuntime.PodName, successorRuntime.PodUID = successorID.NodeID, successorID.PodName, successorID.PodUID
	successorRuntime.RunID, successorRuntime.AssignmentID, successorRuntime.ContainerID = successorID.RunID, successorID.AssignmentID, "successor-container"
	if _, err := f.e.change(t.Context(), successorRuntime.StatefulSetUID, successorRuntime.SourceClusterID, func(l *fencing.Ledger) error {
		return l.RegisterEnrolled(successorID, successorRuntime)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.reserveMutationProducer(t.Context(), successorRuntime); err != nil {
		t.Fatal(err)
	}
	successorStages, err := f.e.sourceStagesFor(t.Context(), successorRuntime)
	if err != nil {
		t.Fatal(err)
	}
	successorID.WALIdentity = "successor-wal"
	configureMutationWorkloadFixture(t, f.e, f.api, successorID, successorRuntime)
	if _, err := f.e.bindWALAtScopeWithStage(t.Context(), successorRuntime.StatefulSetUID, successorID,
		hashJSON(f.scope), successorStages.incarnation); err != nil {
		t.Fatal(err)
	}
	successorProducer, err := f.e.enrolledMutationRun(t.Context(), successorRuntime)
	if err != nil {
		t.Fatal(err)
	}
	successorReceipts, err := f.e.receiptsForRuntime(successorRuntime)
	if err != nil {
		t.Fatal(err)
	}
	successor := f
	successor.id, successor.runtime, successor.producer = successorID, successorRuntime, successorProducer.MutationProducerUID
	successor.stages, successor.receipts = successorStages, successorReceipts
	successorPayload := []byte(`{"membership":"same-runtime positive generation successor"}`)
	successorIntent := successor.admitUpload(t, 1, originIntent.Key,
		fencing.MutationCondition{Mode: fencing.MutationMatchGeneration, Generation: originResult.Generation}, successorPayload)
	successorProvider, err := newGCSMutationProviderForProfile(client, f.scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.e.ExecuteSourceMutation(t.Context(), successorRuntime, successor.producer, 1, successorIntent,
		f.scope, successorProvider, successorReceipts); err != nil {
		t.Fatalf("execute positive-generation successor: %v", err)
	}
	keyState, err := f.e.readMutationKeyState(t.Context(), originIntent.StoreScopeHash, originIntent.Key)
	if err != nil || keyState.Barrier == nil || keyState.Barrier.LineageRef == "" {
		t.Fatalf("successor did not publish a canonical generation-zero lineage: state=%+v err=%v", keyState, err)
	}
	if _, err := f.e.commitMutationArchive(t.Context(), successorRuntime, storeCompactionPlan(t, f.e, successorRuntime, 1)); err != nil {
		t.Fatalf("archive exact successor journal: %v", err)
	}
	producer, _, _, err = f.e.AuthorityState.ReadProducer(t.Context(), successorRuntime)
	if err != nil || producer.ArchiveHead == nil || producer.CompactedThrough != 1 {
		t.Fatalf("successor archive is not canonical: producer=%+v err=%v", producer, err)
	}
	archivedEntry, err := f.e.lookupGenerationZeroArchivedSuccessor(t.Context(), successorRuntime, producer, 1)
	if err != nil || archivedEntry.Intent != successorIntent {
		t.Fatalf("actual archived-successor lookup failed: entry=%+v err=%v", archivedEntry, err)
	}
	if err := f.e.verifyMutationGenerationZeroLineage(t.Context(), *keyState.Barrier); err != nil {
		t.Fatalf("canonical lineage verifier did not use the archived successor: %v", err)
	}
	if successorProducer.MutationProducerUID != stableMutationProducerUID(successorRuntime) || producer.Runtime != successorRuntime ||
		producer.WALIdentity != successorID.WALIdentity || successorIntent.Condition.Generation != originResult.Generation {
		t.Fatal("fixture did not retain same-runtime positive-generation successor identity")
	}

	// Corrupt only the lookup input copies; canonical stored authority remains
	// unchanged for the remaining receipt and lineage assertions.
	for _, tc := range []struct {
		name   string
		mutate func(*fencing.MutationProducer) uint64
	}{
		{name: "missing head", mutate: func(p *fencing.MutationProducer) uint64 { p.ArchiveHead = nil; return 1 }},
		{name: "bad reference UID", mutate: func(p *fencing.MutationProducer) uint64 { p.ArchiveHead.UID = "wrong-generation"; return 1 }},
		{name: "bad reference digest", mutate: func(p *fencing.MutationProducer) uint64 { p.ArchiveHead.Digest = strings.Repeat("0", 64); return 1 }},
		{name: "noncontiguous head range", mutate: func(p *fencing.MutationProducer) uint64 { p.CompactedThrough++; return 1 }},
		{name: "wrong producer runtime identity", mutate: func(p *fencing.MutationProducer) uint64 { p.WALIdentity = "wrong-wal"; return 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := producer
			if producer.ArchiveHead != nil {
				ref := *producer.ArchiveHead
				bad.ArchiveHead = &ref
			}
			sequence := tc.mutate(&bad)
			if _, err := f.e.lookupGenerationZeroArchivedSuccessor(t.Context(), successorRuntime, bad, sequence); err == nil {
				t.Fatal("archived successor lookup accepted altered canonical archive identity")
			}
		})
	}

	evidence, err := f.e.authorityEvidenceFor(successorRuntime)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := evidence.Read(t.Context(), keyState.Barrier.LineageRef)
	if err != nil {
		t.Fatal(err)
	}
	var lineage mutationGenerationZeroLineageRecord
	decodeErr := json.NewDecoder(reader).Decode(&lineage)
	closeErr := reader.Close()
	if err := errors.Join(decodeErr, closeErr); err != nil {
		t.Fatal(err)
	}
	receipt, err := successorReceipts.get(lineage.ReceiptRef)
	if err != nil {
		t.Fatal(err)
	}
	receipt.ReceiptDigest = strings.Repeat("0", 64)
	if err := f.e.verifyGenerationZeroLineageReceipt(t.Context(), *keyState.Barrier, lineage, receipt); err == nil {
		t.Fatal("archived successor receipt with a changed digest was accepted")
	}
}
