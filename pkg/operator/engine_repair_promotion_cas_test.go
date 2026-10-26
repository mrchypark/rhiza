package operator

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestEngineRepairPromotionAndProducerCASPinCurrentCandidateRevision(t *testing.T) {
	_, _, record := engineRepairStoreFixture(t)
	runtime := engineRepairStoreCandidateRuntime(record)
	id := record.CandidateIdentity
	id.WALIdentity = record.CandidateMember.WALIdentity
	record.CandidateIdentity = id
	record.CandidateRuntimeBinding = &runtime
	record.CandidateRuntimeHash = fencing.RuntimeBindingDigest(runtime)
	record.Phase = fencing.RepairAdding
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		t.Fatal(err)
	}
	reference := engineRepairRootReference(record)
	proof := learnerPromotionHash{operationID: engineRepairAddOperationID(record.Binding.OperationID), digest: strings.Repeat("d", 64)}
	provenance := engineRepairRunProvenance(record)
	run := fencing.Run{Identity: id, Role: fencing.StartupRoleLearnerPendingRemove, Runtime: &runtime, EngineRepairProvenance: &provenance}
	ledger := &fencing.Ledger{
		Runs:                   map[string]fencing.Run{id.RunID: run},
		RequireRuntimeBinding:  true,
		RequireMutationJournal: true,
		EngineRepairs:          map[string]fencing.EngineRepairReference{key: reference},
	}
	if err := validateEngineRepairCandidateCAS(ledger, record, reference, key, id, runtime, engineRepairPromotionCAS, proof); err != nil {
		t.Fatalf("exact pending candidate promotion CAS rejected: %v", err)
	}

	// A concurrent continuation updates the root reference before this CAS.
	// The old proof must not promote the prior RunID against that new revision.
	changedReference := reference
	changedReference.RecordDigest = strings.Repeat("e", 64)
	ledger.EngineRepairs[key] = changedReference
	if err := validateEngineRepairCandidateCAS(ledger, record, reference, key, id, runtime, engineRepairPromotionCAS, proof); err == nil {
		t.Fatal("promotion accepted a root reference changed by a candidate continuation")
	}
	ledger.EngineRepairs[key] = reference

	run.Role = fencing.StartupRoleVoterActive
	run.Promotion = &fencing.LearnerPromotion{OperationID: proof.operationID, EvidenceHash: proof.digest}
	ledger.Runs[id.RunID] = run
	if err := validateEngineRepairCandidateCAS(ledger, record, reference, key, id, runtime, engineRepairBindCAS, proof); err != nil {
		t.Fatalf("exact promoted candidate bind CAS rejected: %v", err)
	}
	if err := validateEngineRepairCandidateCAS(ledger, record, reference, key, id, runtime, engineRepairProducerPinCAS, proof); err != nil {
		t.Fatalf("exact promoted candidate producer-pin CAS rejected: %v", err)
	}
	staleProof := proof
	staleProof.digest = strings.Repeat("c", 64)
	if err := validateEngineRepairCandidateCAS(ledger, record, reference, key, id, runtime, engineRepairBindCAS, staleProof); err == nil {
		t.Fatal("bind CAS accepted a promotion receipt whose durable ADD evidence changed")
	}

	// A changed runtime or candidate reference is not equivalent, even when the
	// old run still has a valid promotion receipt.
	wrongRuntime := runtime
	wrongRuntime.ContainerID = "replacement-container"
	if err := validateEngineRepairCandidateCAS(ledger, record, reference, key, id, wrongRuntime, engineRepairBindCAS, proof); err == nil {
		t.Fatal("bind CAS accepted a runtime different from the trusted candidate enrollment")
	}
	wrongCandidate := reference
	wrongCandidate.CandidateIdentity.RunID = "new-candidate-run"
	ledger.EngineRepairs[key] = wrongCandidate
	if err := validateEngineRepairCandidateCAS(ledger, record, reference, key, id, runtime, engineRepairProducerPinCAS, proof); err == nil {
		t.Fatal("producer-pin CAS accepted a different current candidate reference")
	}
}
