package fencing

import (
	"strings"
	"testing"
)

func engineRepairRunFixture() (Identity, RuntimeBinding, EngineRepairRunProvenance) {
	candidate := Identity{
		SourceClusterID: "source", NodeID: "logical-candidate", PodName: "rhiza-1",
		PodUID: "pod-candidate", RunID: "run-candidate", WALIdentity: strings.Repeat("a", 64),
		AssignmentID: strings.Repeat("b", 64),
	}
	runtime := enrolledRuntime(candidate, "container-candidate")
	runtime.PodName = candidate.PodName
	provenance := EngineRepairRunProvenance{
		Version: EngineRepairRunProvenanceVersion, OperationID: "repair-op",
		IdentityDigest: strings.Repeat("c", 64), CandidateIdentity: candidate,
		RuntimeBindingDigest: RuntimeBindingDigest(runtime),
	}
	return candidate, runtime, provenance
}

func TestEngineRepairRunProvenancePinsTrustedEnrollmentBeforeWALAndPromotion(t *testing.T) {
	candidate, runtime, provenance := engineRepairRunFixture()
	ledger := Ledger{SourceClusterID: candidate.SourceClusterID, RequireRuntimeBinding: true, RequireMutationJournal: true}
	unbound := candidate
	unbound.WALIdentity = ""

	if err := ledger.RegisterEngineRepairCandidate(unbound, runtime, provenance); err != nil {
		t.Fatalf("register authority-pinned candidate: %v", err)
	}
	if err := ledger.RegisterEngineRepairCandidate(unbound, runtime, provenance); err != nil {
		t.Fatalf("exact registration retry after a lost response: %v", err)
	}
	changedProvenance := provenance
	changedProvenance.OperationID = "other-operation"
	if err := ledger.RegisterEngineRepairCandidate(unbound, runtime, changedProvenance); err == nil {
		t.Fatal("candidate Run provenance was rebound to another operation")
	}
	run := ledger.Runs[candidate.RunID]
	if run.EngineRepairProvenance == nil || *run.EngineRepairProvenance != provenance || run.Identity.WALIdentity != "" {
		t.Fatalf("candidate enrollment lost provenance or bound WAL too early: %+v", run)
	}
	if err := ledger.Bind(unbound); err == nil {
		t.Fatal("empty WAL bind was accepted")
	}
	wrongWAL := candidate
	wrongWAL.WALIdentity = strings.Repeat("d", 64)
	if err := ledger.Bind(wrongWAL); err == nil {
		t.Fatal("WAL different from the authority-pinned candidate was accepted")
	}
	if ledger.Runs[candidate.RunID].Identity.WALIdentity != "" {
		t.Fatal("rejected WAL bind changed the candidate run")
	}
	if err := ledger.Bind(candidate); err != nil {
		t.Fatalf("bind the exact authority-pinned WAL: %v", err)
	}
	if err := ledger.PromoteLearner(candidate, "promotion-op", strings.Repeat("e", 64)); err != nil {
		t.Fatalf("promote exact bound learner: %v", err)
	}
	run = ledger.Runs[candidate.RunID]
	if run.EngineRepairProvenance == nil || *run.EngineRepairProvenance != provenance || runRole(run) != StartupRoleVoterActive {
		t.Fatalf("promotion changed or dropped engine-repair provenance: %+v", run)
	}
}

func TestEngineRepairRunProvenanceRejectsUntrustedOrMismatchedEnrollment(t *testing.T) {
	candidate, runtime, provenance := engineRepairRunFixture()
	unbound := candidate
	unbound.WALIdentity = ""

	for name, mutate := range map[string]func(*Identity, *RuntimeBinding, *EngineRepairRunProvenance){
		"wrong candidate assignment": func(id *Identity, _ *RuntimeBinding, _ *EngineRepairRunProvenance) {
			id.AssignmentID = strings.Repeat("f", 64)
		},
		"caller supplied WAL before authority bind": func(id *Identity, _ *RuntimeBinding, _ *EngineRepairRunProvenance) {
			id.WALIdentity = strings.Repeat("a", 64)
		},
		"wrong runtime container": func(_ *Identity, binding *RuntimeBinding, _ *EngineRepairRunProvenance) {
			binding.ContainerID = "other-container"
		},
		"missing operation": func(_ *Identity, _ *RuntimeBinding, p *EngineRepairRunProvenance) {
			p.OperationID = ""
		},
		"malformed reservation digest": func(_ *Identity, _ *RuntimeBinding, p *EngineRepairRunProvenance) {
			p.IdentityDigest = "not-a-digest"
		},
		"missing expected WAL": func(_ *Identity, _ *RuntimeBinding, p *EngineRepairRunProvenance) {
			p.CandidateIdentity.WALIdentity = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			id, binding, p := unbound, runtime, provenance
			mutate(&id, &binding, &p)
			ledger := Ledger{SourceClusterID: candidate.SourceClusterID, RequireRuntimeBinding: true}
			if err := ledger.RegisterEngineRepairCandidate(id, binding, p); err == nil {
				t.Fatal("mismatched or incomplete engine-repair enrollment was accepted")
			}
			if len(ledger.Runs) != 0 {
				t.Fatalf("rejected enrollment created a Run: %+v", ledger.Runs)
			}
		})
	}
}

func TestGenericRunCannotBeRetroactivelyLabeledEngineRepair(t *testing.T) {
	candidate, runtime, provenance := engineRepairRunFixture()
	unbound := candidate
	unbound.WALIdentity = ""
	ledger := Ledger{SourceClusterID: candidate.SourceClusterID, RequireRuntimeBinding: true}
	if err := ledger.RegisterEnrolledWithRole(unbound, runtime, StartupRoleLearnerPendingRemove); err != nil {
		t.Fatal(err)
	}
	if err := ledger.RegisterEngineRepairCandidate(unbound, runtime, provenance); err == nil {
		t.Fatal("generic Run was retroactively given engine-repair provenance")
	}
	if ledger.Runs[candidate.RunID].EngineRepairProvenance != nil {
		t.Fatal("failed provenance attachment changed the generic Run")
	}
	if err := ledger.RegisterEnrolledWithRole(unbound, runtime, StartupRoleLearnerPendingRemove); err != nil {
		t.Fatalf("ordinary generic enrollment retry changed behavior: %v", err)
	}
}

func TestEngineRepairActiveRestartPreservesExactWALAndProvenance(t *testing.T) {
	record, ledger := repairTestRecord()
	setEngineRepairFixtureCandidateWAL(t, &record, &ledger)
	record.Phase = RepairActive
	record.AddRequestHash = strings.Repeat("d", 64)
	record.StorageProofDigest = strings.Repeat("e", 64)
	record.RemoveTerminalDigest = strings.Repeat("f", 64)

	candidate := record.CandidateIdentity
	installActiveRepairReference(t, &record, &ledger)
	unbound := candidate
	unbound.WALIdentity = ""
	delete(ledger.Runs, candidate.RunID)
	ledger.RequireRuntimeBinding = true
	provenance := EngineRepairRunProvenance{
		Version: EngineRepairRunProvenanceVersion, OperationID: record.Binding.OperationID,
		IdentityDigest: record.IdentityDigest(), CandidateIdentity: candidate,
		RuntimeBindingDigest: record.CandidateRuntimeHash,
	}
	if err := ledger.RegisterEngineRepairCandidate(unbound, *record.CandidateRuntimeBinding, provenance); err != nil {
		t.Fatalf("register candidate with server-pinned provenance: %v", err)
	}
	if err := ledger.Bind(candidate); err != nil {
		t.Fatalf("bind exact candidate WAL: %v", err)
	}
	if err := ledger.PromoteLearner(candidate, engineRepairPromotionOperationID(record.Binding.OperationID), strings.Repeat("a", 64)); err != nil {
		t.Fatalf("promote candidate with canonical evidence: %v", err)
	}
	if err := record.ValidateAgainstLedger(ledger); err != nil {
		t.Fatalf("active repair fixture is invalid: %v", err)
	}

	prior := ledger.Runs[candidate.RunID]
	next := candidate
	next.RunID = "run-candidate-restart"
	runtime := enrolledRuntime(next, "container-candidate-restart")
	runtime.PodName = next.PodName
	if err := ledger.RegisterEngineRepairActiveRestart(record, candidate, next, runtime); err != nil {
		t.Fatalf("register fresh active Run for intact WAL: %v", err)
	}
	if ledger.Runs[candidate.RunID] != prior {
		t.Fatal("active restart rewrote the prior Run history")
	}
	current := ledger.Runs[next.RunID]
	nextProvenance := provenance
	nextProvenance.CandidateIdentity = next
	nextProvenance.RuntimeBindingDigest = RuntimeBindingDigest(runtime)
	if current.Identity != next || current.Runtime == nil || *current.Runtime != runtime || runRole(current) != StartupRoleVoterActive ||
		current.Promotion == nil || *current.Promotion != *prior.Promotion || current.EngineRepairProvenance == nil ||
		*current.EngineRepairProvenance != nextProvenance {
		t.Fatalf("active restart did not preserve exact WAL/promotion/provenance: %+v", current)
	}
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		t.Fatal(err)
	}
	reference := ledger.EngineRepairs[key]
	reference.CandidateIdentity = next
	ledger.EngineRepairs[key] = reference
	if err := record.ValidateAgainstLedger(ledger); err != nil {
		t.Fatalf("canonical Active record rejected its provenance-bound restart history: %v", err)
	}
	if err := ledger.RegisterEngineRepairActiveRestart(record, candidate, next, runtime); err != nil {
		t.Fatalf("exact retry after a lost response was not idempotent: %v", err)
	}
}

func TestEngineRepairActiveRestartRejectsUnboundOrChangedIncarnation(t *testing.T) {
	for name, mutate := range map[string]func(*EngineRepairRecord, *Ledger, *Identity, *RuntimeBinding){
		"wrong WAL": func(_ *EngineRepairRecord, _ *Ledger, next *Identity, _ *RuntimeBinding) {
			next.WALIdentity = "another-wal"
		},
		"wrong physical Pod": func(_ *EngineRepairRecord, _ *Ledger, next *Identity, _ *RuntimeBinding) {
			next.PodUID = "replacement-pod"
		},
		"reused RunID": func(record *EngineRepairRecord, _ *Ledger, next *Identity, _ *RuntimeBinding) {
			next.RunID = record.CandidateIdentity.RunID
		},
		"reused runtime": func(record *EngineRepairRecord, _ *Ledger, _ *Identity, runtime *RuntimeBinding) {
			*runtime = *record.CandidateRuntimeBinding
		},
		"missing provenance": func(record *EngineRepairRecord, ledger *Ledger, _ *Identity, _ *RuntimeBinding) {
			run := ledger.Runs[record.CandidateIdentity.RunID]
			run.EngineRepairProvenance = nil
			ledger.Runs[record.CandidateIdentity.RunID] = run
		},
		"learner is not an active restart source": func(record *EngineRepairRecord, ledger *Ledger, _ *Identity, _ *RuntimeBinding) {
			run := ledger.Runs[record.CandidateIdentity.RunID]
			run.Role = StartupRoleLearnerPendingRemove
			ledger.Runs[record.CandidateIdentity.RunID] = run
		},
		"record is not Active": func(record *EngineRepairRecord, _ *Ledger, _ *Identity, _ *RuntimeBinding) {
			record.Phase = RepairAdding
		},
		"wrong promotion operation": func(record *EngineRepairRecord, ledger *Ledger, _ *Identity, _ *RuntimeBinding) {
			run := ledger.Runs[record.CandidateIdentity.RunID]
			run.Promotion.OperationID = "other-operation"
			ledger.Runs[record.CandidateIdentity.RunID] = run
		},
		"closed root": func(_ *EngineRepairRecord, ledger *Ledger, _ *Identity, _ *RuntimeBinding) {
			ledger.ClosedBy = "dr-operation"
		},
	} {
		t.Run(name, func(t *testing.T) {
			record, ledger := repairTestRecord()
			setEngineRepairFixtureCandidateWAL(t, &record, &ledger)
			record.Phase = RepairActive
			record.AddRequestHash = strings.Repeat("d", 64)
			record.StorageProofDigest = strings.Repeat("e", 64)
			record.RemoveTerminalDigest = strings.Repeat("f", 64)
			candidate := record.CandidateIdentity
			installActiveRepairReference(t, &record, &ledger)
			unbound := candidate
			unbound.WALIdentity = ""
			delete(ledger.Runs, candidate.RunID)
			ledger.RequireRuntimeBinding = true
			provenance := EngineRepairRunProvenance{Version: EngineRepairRunProvenanceVersion,
				OperationID: record.Binding.OperationID, IdentityDigest: record.IdentityDigest(), CandidateIdentity: candidate,
				RuntimeBindingDigest: record.CandidateRuntimeHash}
			if err := ledger.RegisterEngineRepairCandidate(unbound, *record.CandidateRuntimeBinding, provenance); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Bind(candidate); err != nil {
				t.Fatal(err)
			}
			if err := ledger.PromoteLearner(candidate, engineRepairPromotionOperationID(record.Binding.OperationID), strings.Repeat("a", 64)); err != nil {
				t.Fatal(err)
			}
			next := candidate
			next.RunID = "run-candidate-restart"
			runtime := enrolledRuntime(next, "container-candidate-restart")
			runtime.PodName = next.PodName
			mutate(&record, &ledger, &next, &runtime)
			beforeCount := len(ledger.Runs)
			priorBefore := ledger.Runs[candidate.RunID]
			priorNextRun, nextAlreadyExisted := ledger.Runs[next.RunID]
			if err := ledger.RegisterEngineRepairActiveRestart(record, candidate, next, runtime); err == nil {
				t.Fatal("mismatched or closed active restart was accepted")
			}
			afterNextRun, nextExists := ledger.Runs[next.RunID]
			if nextExists != nextAlreadyExisted || nextAlreadyExisted && afterNextRun != priorNextRun ||
				len(ledger.Runs) != beforeCount || ledger.Runs[candidate.RunID] != priorBefore {
				t.Fatal("rejected active restart changed canonical Run history")
			}
		})
	}
}

func setEngineRepairFixtureCandidateWAL(t *testing.T, record *EngineRepairRecord, ledger *Ledger) {
	t.Helper()
	wal := strings.Repeat("d", 64)
	record.CandidateIdentity.WALIdentity = wal
	record.CandidateMember.WALIdentity = wal
	record.CandidateMemberDigest = EngineRepairMemberDigest(record.CandidateMember)
	run := ledger.Runs[record.CandidateIdentity.RunID]
	run.Identity.WALIdentity = wal
	ledger.Runs[record.CandidateIdentity.RunID] = run
	if err := record.Validate(); err != nil {
		t.Fatalf("active-restart fixture must satisfy canonical WAL digest format: %v", err)
	}
}

func installActiveRepairReference(t *testing.T, record *EngineRepairRecord, ledger *Ledger) {
	t.Helper()
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		t.Fatal(err)
	}
	ledger.EngineRepairs = map[string]EngineRepairReference{key: {
		Version: EngineRepairRecordVersion, OperationID: record.Binding.OperationID, Binding: record.Binding,
		CandidateIdentity: record.CurrentCandidateIdentity(), Phase: record.Phase,
		IdentityDigest: record.IdentityDigest(), RecordDigest: record.Digest(), RecordKey: "engine-repairs/canonical.json",
	}}
}
