package operator

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func engineRepairStorageClosureFixture(t *testing.T) (fenceLedger, fencing.EngineRepairRecord, fencing.Identity, fencing.RuntimeBinding, fencing.RuntimeBinding) {
	t.Helper()
	archive := newArchiveGenesisFixture(t)
	commitArchiveGenesisFixture(t, archive)
	e, oldID, oldRuntime := archive.e, archive.id, archive.runtime
	state, err := e.read(t.Context(), oldRuntime.StatefulSetUID, oldRuntime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	oldRun := state.Ledger.Runs[oldID.RunID]
	oldID = oldRun.Identity

	survivorID := fencing.Identity{SourceClusterID: oldID.SourceClusterID, NodeID: "survivor", PodUID: "survivor-pod", RunID: "survivor-run", WALIdentity: "survivor-wal"}
	survivorRuntime := oldRuntime
	survivorRuntime.NodeID, survivorRuntime.PodUID, survivorRuntime.RunID = survivorID.NodeID, survivorID.PodUID, survivorID.RunID
	survivorRuntime.ContainerID = "survivor-container"
	state.Ledger.Runs[survivorID.RunID] = fencing.Run{Identity: survivorID, Role: fencing.StartupRoleVoterActive, Runtime: &survivorRuntime}
	incarnations, err := fencing.SortedRepairIncarnations(state.Ledger)
	if err != nil {
		t.Fatal(err)
	}

	candidateID := fencing.Identity{SourceClusterID: oldID.SourceClusterID, NodeID: "candidate", PodName: "candidate-pod", PodUID: "candidate-pod-uid",
		RunID: "candidate-run", WALIdentity: "candidate-wal", AssignmentID: hashBytes([]byte("candidate-assignment"))}
	candidateRuntime := oldRuntime
	candidateRuntime.NodeID, candidateRuntime.PodName, candidateRuntime.PodUID, candidateRuntime.RunID = candidateID.NodeID, candidateID.PodName, candidateID.PodUID, candidateID.RunID
	candidateRuntime.AssignmentID, candidateRuntime.ContainerID = candidateID.AssignmentID, "candidate-container"
	state.Ledger.Runs[candidateID.RunID] = fencing.Run{Identity: candidateID, Role: fencing.StartupRoleLearnerPendingRemove, Runtime: &candidateRuntime}

	oldMember := quepaxa.Member{ID: quepaxa.NodeID(oldID.NodeID), URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", WALIdentity: oldID.WALIdentity, PublicKey: quepaxa.PublicKey{1}}
	survivor := quepaxa.Member{ID: quepaxa.NodeID(survivorID.NodeID), URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", WALIdentity: survivorID.WALIdentity, PublicKey: quepaxa.PublicKey{2}}
	candidateMember := quepaxa.Member{ID: quepaxa.NodeID(candidateID.NodeID), URL: "http://candidate:8080", PeerURL: "quic://candidate:9090", LogURL: "http://candidate:8081", WALIdentity: candidateID.WALIdentity, PublicKey: quepaxa.PublicKey{3}}
	requestHash := hashBytes([]byte("remove-request"))
	binding := fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
		OperationID: "engine-repair-close", RequestHash: requestHash, SourceClusterID: oldID.SourceClusterID,
		StatefulSetUID: oldRuntime.StatefulSetUID, OldConfigID: 1, ExpectedAbortSlot: 3, OldMemberID: oldID.NodeID,
		OldWALIdentity: oldID.WALIdentity, TargetMemberID: candidateID.NodeID}
	repair := fencing.EngineRepairRecord{Version: fencing.EngineRepairRecordVersion, Binding: binding,
		SourceConfig:          quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, survivor}},
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-auth", UID: "source-auth-uid", AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		Incarnations:          incarnations, CandidateIdentity: candidateID, CandidateMember: candidateMember,
		CandidateCredential:     fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid", PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte("candidate-peer-token"))},
		CandidateRuntimeBinding: &candidateRuntime, CandidateRuntimeHash: fencing.RuntimeBindingDigest(candidateRuntime),
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidateMember),
		RemoveRequestHash:     requestHash, Phase: fencing.RepairCandidatePrepared}
	repair = freezeEngineRepairRecord(repair)
	repair.Phase = fencing.RepairStorageClosing
	if err := repair.ValidateAgainstLedger(state.Ledger); err != nil {
		t.Fatalf("invalid engine repair closure fixture: %v", err)
	}
	return state, repair, survivorID, survivorRuntime, candidateRuntime
}

func TestEngineRepairStorageClosureRequiresSurvivingVoterAndUsesReservedCandidateRuntime(t *testing.T) {
	state, repair, survivorID, survivorRuntime, candidateRuntime := engineRepairStorageClosureFixture(t)
	got, err := engineRepairStorageClosureRuntimes(state, survivorID, survivorRuntime, repair)
	if err != nil || got != candidateRuntime {
		t.Fatalf("survivor closure caller did not resolve exact reserved candidate runtime: got=%+v err=%v", got, err)
	}
}

func TestEngineRepairOldCohortClosureCutsOnlyOldMemberAndClosesOpenProducer(t *testing.T) {
	fixture := newHeldProfileFixture(t)
	repair := testEngineRepairRecord(t, fixture)
	before, err := fixture.e.read(t.Context(), fixture.runtime.StatefulSetUID, fixture.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	open, producerUID, _, err := fixture.e.AuthorityState.ReadProducer(t.Context(), fixture.runtime)
	if err != nil || producerUID != fixture.producer || open.Phase != fencing.MutationOpen {
		t.Fatalf("fixture must start with an open canonical producer: uid=%q phase=%q err=%v", producerUID, open.Phase, err)
	}
	if before.Ledger.ClosedBy != "" || len(before.Ledger.ClosedNodes) != 0 {
		t.Fatalf("fixture unexpectedly starts with a closed root or voter cohort: %+v", before.Ledger)
	}

	if err := fixture.e.closeEngineRepairOldCohort(t.Context(), repair); err != nil {
		t.Fatalf("close exact old-member cohort: %v", err)
	}
	after, err := fixture.e.read(t.Context(), fixture.runtime.StatefulSetUID, fixture.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Ledger.ClosedBy != "" || after.Ledger.ClosedHash != "" || len(after.Ledger.ClosedNodes) != 1 {
		t.Fatalf("old-member closure must not close the generation or another voter: %+v", after.Ledger)
	}
	closed, producerUID, _, err := fixture.e.AuthorityState.ReadProducer(t.Context(), fixture.runtime)
	if err != nil || producerUID != fixture.producer || closed.Phase != fencing.MutationClosed ||
		closed.ClosedThrough != closed.LastAdmitted {
		t.Fatalf("open producer did not read back its exact closed admission prefix: uid=%q producer=%+v err=%v", producerUID, closed, err)
	}
	request := after.Ledger.ClosedNodes[fixture.id.NodeID]
	run := after.Ledger.Runs[fixture.id.RunID]
	if request.OperationID == "" || request.RequestHash == "" || run.Stop == nil ||
		run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != request.RequestHash || run.Stop.Identity != fixture.id ||
		run.Drain != nil || run.Exited || run.IndependentTerminal != nil {
		t.Fatalf("old cohort closure must bind the same slot stop to this exact Run without claiming physical termination: request=%+v run=%+v", request, run)
	}
}

func TestEngineRepairPrebindCandidateAllowanceRequiresExactPendingProvenance(t *testing.T) {
	_, _, record, _ := noProducerEngineRepairFixture(t)
	runtime := engineRepairStoreCandidateRuntime(record)
	want := record.CurrentCandidateIdentity()
	unbound := want
	unbound.WALIdentity = ""
	provenance := engineRepairRunProvenance(record)
	valid := fencing.Run{Identity: unbound, Role: fencing.StartupRoleLearnerPendingRemove, Runtime: &runtime, EngineRepairProvenance: &provenance}
	if !isExactPrebindEngineRepairCandidate(record, valid) {
		t.Fatal("exact pending learner prebind Run was not recognized")
	}
	tests := []struct {
		name   string
		mutate func(*fencing.Run)
	}{
		{name: "missing provenance", mutate: func(run *fencing.Run) { run.EngineRepairProvenance = nil }},
		{name: "wrong operation", mutate: func(run *fencing.Run) {
			copy := *run.EngineRepairProvenance
			copy.OperationID = "other-operation"
			run.EngineRepairProvenance = &copy
		}},
		{name: "wrong reservation digest", mutate: func(run *fencing.Run) {
			copy := *run.EngineRepairProvenance
			copy.IdentityDigest = hashBytes([]byte("other-reservation"))
			run.EngineRepairProvenance = &copy
		}},
		{name: "different bound WAL", mutate: func(run *fencing.Run) { run.Identity.WALIdentity = hashBytes([]byte("different-wal")) }},
		{name: "active candidate", mutate: func(run *fencing.Run) {
			run.Identity.WALIdentity = want.WALIdentity
			run.Role = fencing.StartupRoleVoterActive
			run.Promotion = &fencing.LearnerPromotion{OperationID: engineRepairAddOperationID(record.Binding.OperationID), EvidenceHash: hashBytes([]byte("promotion"))}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := valid
			tc.mutate(&run)
			if isExactPrebindEngineRepairCandidate(record, run) {
				t.Fatal("prebind exception accepted a Run outside the exact pending learner provenance")
			}
		})
	}
}

func TestEngineRepairStorageClosureRejectsCandidateRemovedVoterAndDrainingSource(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fenceLedger, *fencing.EngineRepairRecord, *fencing.Identity, *fencing.RuntimeBinding)
	}{
		{name: "candidate is not the source coordinator", mutate: func(state *fenceLedger, repair *fencing.EngineRepairRecord, caller *fencing.Identity, runtime *fencing.RuntimeBinding) {
			*caller = repair.CandidateIdentity
			*runtime = *state.Ledger.Runs[repair.CandidateIdentity.RunID].Runtime
		}},
		{name: "removed voter is not a survivor", mutate: func(state *fenceLedger, repair *fencing.EngineRepairRecord, caller *fencing.Identity, runtime *fencing.RuntimeBinding) {
			for _, incarnation := range repair.Incarnations {
				if incarnation.Identity.NodeID == repair.Binding.OldMemberID {
					*caller = incarnation.Identity
					*runtime = *state.Ledger.Runs[caller.RunID].Runtime
					return
				}
			}
		}},
		{name: "survivor identity mismatch", mutate: func(_ *fenceLedger, _ *fencing.EngineRepairRecord, caller *fencing.Identity, _ *fencing.RuntimeBinding) {
			caller.AssignmentID = strings.Repeat("a", 64)
		}},
		{name: "draining root", mutate: func(state *fenceLedger, _ *fencing.EngineRepairRecord, _ *fencing.Identity, _ *fencing.RuntimeBinding) {
			state.Ledger.ClosedBy, state.Ledger.ClosedHash = "dr-close", hashBytes([]byte("dr"))
		}},
		{name: "draining run", mutate: func(state *fenceLedger, _ *fencing.EngineRepairRecord, caller *fencing.Identity, _ *fencing.RuntimeBinding) {
			run := state.Ledger.Runs[caller.RunID]
			run.Stop = &fencing.StopRequest{OperationID: "stop-survivor"}
			state.Ledger.Runs[caller.RunID] = run
		}},
		{name: "DR superseded repair", mutate: func(_ *fenceLedger, repair *fencing.EngineRepairRecord, _ *fencing.Identity, _ *fencing.RuntimeBinding) {
			repair.ClosingFrom, repair.Phase = fencing.RepairStorageClosing, fencing.RepairGenerationClosing
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state, repair, caller, callerRuntime, _ := engineRepairStorageClosureFixture(t)
			tc.mutate(&state, &repair, &caller, &callerRuntime)
			if _, err := engineRepairStorageClosureRuntimes(state, caller, callerRuntime, repair); err == nil {
				t.Fatal("storage closure accepted a caller outside the open surviving-voter boundary")
			}
		})
	}
}
