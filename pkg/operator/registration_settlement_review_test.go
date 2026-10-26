package operator

import (
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

type registrationSettlementFixture struct {
	e            *FencingExecutor
	store        *EngineRepairStore
	record       fencing.EngineRepairRecord
	proposal     fencing.EngineRepairRecord
	pending      fencing.PendingEngineRepairRequest
	prior        fencing.Identity
	priorRuntime fencing.RuntimeBinding
	producerKey  string
}

func newRegistrationSettlementFixture(t *testing.T) registrationSettlementFixture {
	t.Helper()
	e, store, record := engineRepairStoreFixture(t)
	root, _, err := e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	old := root.Ledger.Runs[record.Incarnations[0].Identity.RunID]
	prior := old.Identity
	prior.RunID = "run-pre-bind-predecessor"
	prior.WALIdentity = ""
	priorRuntime := *old.Runtime
	priorRuntime.RunID = prior.RunID
	priorRuntime.ContainerID = "predecessor-container"
	if _, err := e.change(t.Context(), priorRuntime.StatefulSetUID, prior.SourceClusterID, func(l *fencing.Ledger) error {
		return l.RegisterEnrolledWithRole(prior, priorRuntime, fencing.StartupRoleVoterActive)
	}); err != nil {
		t.Fatalf("register unbound predecessor: %v", err)
	}
	producer, err := e.reserveMutationProducer(t.Context(), priorRuntime)
	if err != nil || producer.Phase != fencing.MutationReserved || producer.WALIdentity != "" {
		t.Fatalf("reserve zero-work predecessor producer: %+v err=%v", producer, err)
	}
	if _, err := e.change(t.Context(), priorRuntime.StatefulSetUID, prior.SourceClusterID, func(l *fencing.Ledger) error {
		run := l.Runs[prior.RunID]
		run.RegistrationSupersededBy = old.Identity.RunID
		l.Runs[prior.RunID] = run
		return nil
	}); err != nil {
		t.Fatalf("record predecessor registration revocation: %v", err)
	}
	producerKey, _, err := e.AuthorityState.producerKey(priorRuntime)
	if err != nil {
		t.Fatal(err)
	}

	root, _, err = e.AuthorityState.ReadRoot(t.Context(), record.Binding.StatefulSetUID, record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	record.Incarnations, err = fencing.SortedRepairIncarnations(root.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	proposal := pendingEngineRepairProposalForRecord(record)
	pending, err := store.CreateOrReadPendingEngineRepair(t.Context(), proposal)
	if err != nil {
		t.Fatalf("create pending repair: %v", err)
	}
	proposalRecord := record
	proposalRecord.Binding.OperationID = pending.OperationID
	proposalRecord.Binding.RequestHash = pending.RequestDigest
	proposalRecord.CandidateIdentity = pending.CandidateIdentity
	proposalRecord.BootstrapRequestDigest = pending.RequestDigest
	proposalRecord.Incarnations = nil
	if err := proposalRecord.ValidateAdoptionProposal(); err != nil {
		t.Fatalf("construct inventory-free adoption proposal: %v", err)
	}
	return registrationSettlementFixture{e: e, store: store, record: record, proposal: proposalRecord,
		pending: pending, prior: prior, priorRuntime: priorRuntime, producerKey: producerKey}
}

func adoptRegistrationSettlementFixture(t *testing.T, f registrationSettlementFixture) fencing.EngineRepairRecord {
	t.Helper()
	adopted, err := f.store.AdoptPendingEngineRepair(t.Context(), f.pending.PendingID, f.pending.RequestDigest, f.proposal)
	if err != nil {
		t.Fatalf("adopt pending repair with canonical superseded reservation: %v", err)
	}
	if len(adopted.Incarnations) != len(f.record.Incarnations) {
		t.Fatalf("adoption did not snapshot the complete inventory: got=%d want=%d", len(adopted.Incarnations), len(f.record.Incarnations))
	}
	var predecessor fencing.RepairIncarnation
	for _, incarnation := range adopted.Incarnations {
		if incarnation.Identity.RunID == f.prior.RunID {
			predecessor = incarnation
		}
	}
	if !predecessor.SupersededReservation() || predecessor.ProducerUID != stableMutationProducerUID(f.priorRuntime) {
		t.Fatalf("adoption omitted the UID-only superseded reservation: %+v", predecessor)
	}
	root, _, err := f.e.AuthorityState.ReadRoot(t.Context(), adopted.Binding.StatefulSetUID, adopted.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	reference, ok := root.Ledger.EngineRepairs[mustEngineRepairKey(fencing.EngineRepairReference{Binding: adopted.Binding})]
	if !ok || reference.Validate() != nil || reference.RecordDigest != adopted.Digest() {
		t.Fatalf("adoption did not install the exact canonical root reference: %+v", reference)
	}
	if err := adopted.ValidateAgainstLedger(root.Ledger); err != nil {
		t.Fatalf("adopted record does not validate against its root: %v", err)
	}
	return adopted
}

func TestPendingAdoptionCarriesSupersededReservationThroughClosureProof(t *testing.T) {
	f := newRegistrationSettlementFixture(t)
	adopted := adoptRegistrationSettlementFixture(t, f)

	runtime := engineRepairStoreCandidateRuntime(adopted)
	pinned, err := f.store.PinCandidateRuntime(t.Context(), adopted.Binding.SourceClusterID, adopted.Binding.OperationID, adopted.Digest(), runtime)
	if err != nil {
		t.Fatalf("pin trusted candidate runtime: %v", err)
	}
	frozen := freezeEngineRepairRecord(pinned)
	frozen.Phase = fencing.RepairRemoveFrozen
	frozen, err = f.store.CompareAndSwapEngineRepair(t.Context(), pinned.Binding.SourceClusterID, pinned.Digest(), frozen)
	if err != nil {
		t.Fatalf("freeze adopted repair: %v", err)
	}
	closing := frozen
	closing.Phase = fencing.RepairStorageClosing
	closing, err = f.store.CompareAndSwapEngineRepair(t.Context(), frozen.Binding.SourceClusterID, frozen.Digest(), closing)
	if err != nil {
		t.Fatalf("enter storage-closing phase: %v", err)
	}
	// The cohort is zero-work, so the storage client is only needed to enable
	// the real verifier path; no provider request should be issued.
	f.e.SourceMutationStorage = &storage.Client{}
	if err := f.e.closeEngineRepairOldCohort(t.Context(), closing); err != nil {
		t.Fatalf("close old-member cohort including superseded predecessor: %v", err)
	}
	proof, err := f.e.settleEngineRepairStorageEffects(t.Context(), closing)
	if err != nil {
		t.Fatalf("settle adopted superseded reservation: %v", err)
	}
	if err := proof.Validate(closing); err != nil {
		t.Fatalf("validate storage proof: %v", err)
	}
	var settlement fencing.StorageRunSettlement
	for _, item := range proof.RunSettlements {
		if item.RunID == f.prior.RunID {
			settlement = item
		}
	}
	if settlement.RunID == "" || settlement.ProducerUID != stableMutationProducerUID(f.priorRuntime) || settlement.NoProducerDigest != "" {
		t.Fatalf("proof omits the exact producer-backed predecessor settlement: %+v", settlement)
	}
	root, _, err := f.e.AuthorityState.ReadRoot(t.Context(), closing.Binding.StatefulSetUID, closing.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	closed := root.Ledger.Runs[f.prior.RunID]
	if closed.Stop == nil || closed.Stop.Identity != f.prior || closed.Exited || closed.IndependentTerminal != nil {
		t.Fatalf("closure fabricated termination or omitted exact stop: %+v", closed)
	}
	producer, uid, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.priorRuntime)
	if err != nil || uid != settlement.ProducerUID || producer.Phase != fencing.MutationClosed || producer.WALIdentity != "" || producer.LastAdmitted != 0 {
		t.Fatalf("predecessor producer is not the exact zero-work closed reservation: uid=%q producer=%+v err=%v", uid, producer, err)
	}
	readback, err := f.store.ReadEngineRepair(t.Context(), closing.Binding.SourceClusterID, closing.Binding.OperationID)
	if err != nil || readback.StorageProofDigest != proof.ProofDigest || readback.ValidateAgainstLedger(root.Ledger) != nil {
		t.Fatalf("root-pinned proof or adopted inventory did not survive readback: record=%+v err=%v", readback, err)
	}
}

func TestPendingAdoptionRejectsMissingSupersededProducerWithoutRecreatingIt(t *testing.T) {
	f := newRegistrationSettlementFixture(t)
	if err := f.e.AuthorityState.bucket.Delete(t.Context(), f.producerKey); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AdoptPendingEngineRepair(t.Context(), f.pending.PendingID, f.pending.RequestDigest, f.proposal); err == nil {
		t.Fatal("adoption accepted a superseded inventory entry with a deleted producer")
	}
	exists, err := f.e.AuthorityState.bucket.Exists(t.Context(), f.producerKey)
	if err != nil || exists {
		t.Fatalf("failed adoption recreated the missing pinned producer: exists=%t err=%v", exists, err)
	}
	root, _, err := f.e.AuthorityState.ReadRoot(t.Context(), f.record.Binding.StatefulSetUID, f.record.Binding.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Ledger.EngineRepairs) != 0 || len(root.Ledger.PendingEngineRepairs) != 1 {
		t.Fatalf("failed adoption changed canonical repair state: pending=%d repairs=%d", len(root.Ledger.PendingEngineRepairs), len(root.Ledger.EngineRepairs))
	}
}
