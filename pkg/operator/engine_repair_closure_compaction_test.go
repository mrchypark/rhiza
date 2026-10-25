package operator

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestEngineRepairStorageProofSurvivesClosedProducerCompaction(t *testing.T) {
	f := newHeldProfileFixture(t)
	intent := completeStoreIntent(fencing.MutationIntent{
		OperationID:    fencing.MutationRequestID(f.producer, 1),
		StoreScopeHash: hashJSON(f.scope),
		Kind:           "delete",
		Key:            "membership/compaction-proof",
	})
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(producer *fencing.MutationProducer) error {
		return producer.Admit(1, intent)
	}); err != nil {
		t.Fatalf("admit unresolved no-dispatch mutation: %v", err)
	}

	repair := testEngineRepairRecord(t, f)
	if err := f.e.closeEngineRepairOldCohort(t.Context(), repair); err != nil {
		t.Fatalf("close the exact old producer cohort: %v", err)
	}
	_, storageClient := newHeldGCSServer(t, f)
	f.e.SourceMutationStorage = storageClient

	proof, err := f.e.settleEngineRepairStorageEffects(t.Context(), repair)
	if err != nil {
		t.Fatalf("create root-pinned storage-closure proof: %v", err)
	}
	if proof.ProofDigest == "" || proof.Validate(repair) != nil || len(proof.RunSettlements) != 1 {
		t.Fatalf("fixture did not persist the exact producer closure proof: %+v", proof)
	}
	store := mustEngineRepairStore(t, f.e, repair)
	canonical, err := store.ReadEngineRepair(t.Context(), repair.Binding.SourceClusterID, repair.Binding.OperationID)
	if err != nil || canonical.StorageProofDigest != proof.ProofDigest {
		t.Fatalf("proof was not attached to the canonical repair root: digest=%q proof=%q err=%v", canonical.StorageProofDigest, proof.ProofDigest, err)
	}

	before := readFixtureProducer(t, f.e, f.api, f.runtime)
	if before.Phase != fencing.MutationClosed || before.CompactedThrough != 0 || before.ArchiveHead != nil {
		t.Fatalf("fixture producer is not the expected closed, unarchived prefix: %+v", before)
	}
	plan, err := before.PlanMutationCompaction(f.producer, 1)
	if err != nil {
		t.Fatalf("plan compaction of the already-terminal closed prefix: %v", err)
	}
	compacted, err := f.e.commitMutationArchive(t.Context(), f.runtime, plan)
	if err != nil || compacted.Phase != fencing.MutationClosed || compacted.CompactedThrough != 1 || compacted.ArchiveHead == nil {
		t.Fatalf("commit normal closed-producer compaction: compacted=%+v err=%v", compacted, err)
	}

	// This is the important replay boundary: the immutable storage proof was
	// committed before a valid producer CAS advanced its archive cursor/head.
	// Re-reading the same root-pinned proof must validate against the retained
	// exact archive prefix, not require the mutable producer snapshot to be
	// byte-for-byte unchanged.
	replayed, err := f.e.settleEngineRepairStorageEffects(t.Context(), repair)
	if err != nil {
		t.Fatalf("storage proof became unusable after closed-prefix compaction: %v", err)
	}
	if replayed.ProofDigest != proof.ProofDigest || replayed.Digest() != proof.Digest() {
		t.Fatalf("compaction changed the immutable closure proof: first=%+v replay=%+v", proof, replayed)
	}
}
