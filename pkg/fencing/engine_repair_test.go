package fencing

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func repairTestBinding() EngineRemovalBinding {
	return EngineRemovalBinding{Version: EngineRemovalBindingVersion, Mode: EngineRemovalMode, OperationID: "repair-op",
		RequestHash: journalTestDigest, SourceClusterID: "source", StatefulSetUID: "statefulset", OldConfigID: 7,
		ExpectedAbortSlot: 2, OldMemberID: "old-member", OldWALIdentity: "old-wal", TargetMemberID: "new-member",
		FreezeSlot: 44, FreezeHash: strings.Repeat("b", 64)}
}

func repairTestRecord() (EngineRepairRecord, Ledger) {
	old := Identity{SourceClusterID: "source", NodeID: "old-member", PodName: "rhiza-1", PodUID: "pod-old", RunID: "run-old", WALIdentity: "old-wal"}
	other := Identity{SourceClusterID: "source", NodeID: "survivor", PodName: "rhiza-0", PodUID: "pod-survivor", RunID: "run-survivor", WALIdentity: "survivor-wal"}
	incarnations := []RepairIncarnation{
		{Identity: old, Role: StartupRoleVoterActive, ProducerUID: "producer-old", MutationScopeHash: journalTestDigest, MutationProfile: MutationHeldExecutionProfile},
		{Identity: other, Role: StartupRoleVoterActive, ProducerUID: "producer-survivor", MutationScopeHash: journalTestDigest, MutationProfile: MutationHeldExecutionProfile},
	}
	oldMember := quepaxa.Member{ID: "old-member", URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", PublicKey: quepaxa.PublicKey{1}, WALIdentity: "old-wal"}
	survivor := quepaxa.Member{ID: "survivor", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", PublicKey: quepaxa.PublicKey{2}, WALIdentity: "survivor-wal"}
	source := quepaxa.Cluster{ConfigID: 7, Members: []quepaxa.Member{oldMember, survivor}}
	freezeHash, _ := hex.DecodeString(strings.Repeat("b", 64))
	var freezeValueHash quepaxa.ValueHash
	copy(freezeValueHash[:], freezeHash)
	removal := &quepaxa.EngineRemoval{Version: 1, Mode: EngineRemovalMode, OperationID: "repair-op", ExpectedConfigID: 7, ExpectedAbortSlot: 2,
		Removed: oldMember, Target: quepaxa.Cluster{ConfigID: 8, Members: []quepaxa.Member{survivor}}, Freeze: 44, FreezeHash: freezeValueHash}
	candidate := Identity{SourceClusterID: "source", NodeID: "new-member", PodName: "rhiza-1", PodUID: "pod-new", RunID: "run-new", WALIdentity: "new-wal", AssignmentID: strings.Repeat("c", 64)}
	candidateRuntime := enrolledRuntime(candidate, "candidate-container")
	candidateRuntime.PodName = candidate.PodName
	member := quepaxa.Member{ID: "new-member", URL: "http://rhiza-1:8080", PeerURL: "quic://rhiza-1:8443", LogURL: "http://rhiza-1:9090", WALIdentity: "new-wal", PublicKey: quepaxa.PublicKey{1}}
	record := EngineRepairRecord{Version: EngineRepairRecordVersion, Binding: repairTestBinding(), SourceConfig: source,
		SourceAdminCredential: EngineRepairAdminCredential{Name: "source-auth", UID: "source-auth-uid", AdminTokenDigest: EngineRepairAdminTokenDigest([]byte("source-admin-token"))},
		RemovalOperation:      removal, Incarnations: incarnations,
		CandidateIdentity: candidate, CandidateMember: member, CandidateCredential: EngineRepairCredential{Name: "rhiza-1-recovery-credentials", UID: "secret-uid", PeerTokenDigest: EngineRepairPeerTokenDigest([]byte("candidate-peer-token"))},
		CandidateRuntimeBinding: &candidateRuntime, CandidateRuntimeHash: RuntimeBindingDigest(candidateRuntime), CandidateMemberDigest: EngineRepairMemberDigest(member),
		RemoveRequestHash: strings.Repeat("f", 64), Phase: RepairStorageClosing}
	ledger := Ledger{SourceClusterID: "source", Runs: map[string]Run{
		old.RunID:   {Identity: old, Role: StartupRoleVoterActive, MutationProducerUID: "producer-old", MutationScopeHash: journalTestDigest, MutationProfile: MutationHeldExecutionProfile},
		other.RunID: {Identity: other, Role: StartupRoleVoterActive, MutationProducerUID: "producer-survivor", MutationScopeHash: journalTestDigest, MutationProfile: MutationHeldExecutionProfile},
		candidate.RunID: {Identity: Identity{SourceClusterID: candidate.SourceClusterID, NodeID: candidate.NodeID, PodName: candidate.PodName,
			PodUID: candidate.PodUID, RunID: candidate.RunID, AssignmentID: candidate.AssignmentID}, Role: StartupRoleLearnerPendingRemove,
			Runtime: &candidateRuntime},
	}}
	return record, ledger
}

func TestPendingEngineRepairPreflightLeavesCanonicalReferencesToAuthority(t *testing.T) {
	bootstrap := Identity{SourceClusterID: "source", NodeID: "old-member", PodName: "rhiza-1", PodUID: "pod-new", RunID: "run-new"}
	candidate := Identity{SourceClusterID: "source", NodeID: "candidate", PodName: bootstrap.PodName, PodUID: bootstrap.PodUID,
		RunID: bootstrap.RunID, WALIdentity: "candidate-wal"}
	proposal := PendingEngineRepairProposal{
		Version: PendingEngineRepairRequestVersion, SourceClusterID: "source", BootstrapIdentity: bootstrap,
		StableOrdinal: 1, OldMemberID: "old-member", OldWALIdentity: "observed-old-wal",
		Outcome: RecoveryBootstrapLostWAL, LocalIdentityHash: journalTestDigest, CandidateIdentity: candidate,
		CandidateMember: quepaxa.Member{ID: "candidate", URL: "http://rhiza-1:8080", PeerURL: "quic://rhiza-1:9090",
			LogURL: "http://rhiza-1:8081", WALIdentity: candidate.WALIdentity, PublicKey: quepaxa.PublicKey{3}},
	}
	if err := proposal.ValidatePreflight(); err != nil {
		t.Fatalf("public physical preflight required authority-owned StatefulSet/source-WAL references: %v", err)
	}
	if err := proposal.Validate(); err == nil {
		t.Fatal("public preflight unexpectedly passed strict authority-enriched validation")
	}
	proposal.SourceOldWALIdentity = "caller-selected-source-wal"
	if err := proposal.ValidatePreflight(); err == nil {
		t.Fatal("public preflight accepted caller-supplied canonical source WAL")
	}
	proposal.SourceOldWALIdentity = ""
	proposal.IncumbentReferenceDigest = strings.Repeat("a", 64)
	if err := proposal.ValidatePreflight(); err == nil {
		t.Fatal("public preflight accepted caller-supplied canonical incumbent reference")
	}
}

func TestPendingEngineRepairCanonicalReceiptAllowsCurrentIncumbentDifferentFromBootstrap(t *testing.T) {
	proposal := PendingEngineRepairProposal{
		Version: PendingEngineRepairRequestVersion, SourceClusterID: "source", StatefulSetUID: "sts-uid",
		BootstrapIdentity: Identity{SourceClusterID: "source", NodeID: "bootstrap-template-id", PodName: "rhiza-1", PodUID: "replacement-pod", RunID: "replacement-run"},
		StableOrdinal:     1, OldMemberID: "recovered-current-member", SourceOldWALIdentity: "recovered-current-wal",
		IncumbentReferenceDigest: strings.Repeat("a", 64), OldWALIdentity: "lost-local-wal",
		Outcome: RecoveryBootstrapLostWAL, LocalIdentityHash: journalTestDigest,
		CandidateIdentity:     Identity{SourceClusterID: "source", NodeID: "next-candidate", PodName: "rhiza-1", PodUID: "replacement-pod", RunID: "replacement-run", WALIdentity: "next-candidate-wal"},
		CandidateMember:       quepaxa.Member{ID: "next-candidate", URL: "http://rhiza-1:8080", PeerURL: "quic://rhiza-1:9090", LogURL: "http://rhiza-1:8081", WALIdentity: "next-candidate-wal", PublicKey: quepaxa.PublicKey{3}},
		CandidateCredential:   EngineRepairCredential{Name: "peer-secret", UID: "peer-secret-uid", PeerTokenDigest: EngineRepairPeerTokenDigest([]byte("peer-token"))},
		SourceAdminCredential: EngineRepairAdminCredential{Name: "admin-secret", UID: "admin-secret-uid", AdminTokenDigest: EngineRepairAdminTokenDigest([]byte("admin-token"))},
	}
	request, err := NewPendingEngineRepairRequest(proposal)
	if err != nil {
		t.Fatalf("canonical incumbent receipt did not permit a later same-ordinal replacement: %v", err)
	}
	if request.BootstrapIdentity.NodeID == request.OldMemberID {
		t.Fatal("test did not distinguish physical bootstrap identity from canonical incumbent")
	}
	request.IncumbentReferenceDigest = strings.Repeat("b", 64)
	if err := request.Validate(); err == nil {
		t.Fatal("request digest did not bind the authority-pinned incumbent reference")
	}
}

func TestEngineRepairRecordBindsExactBoundedRootInventory(t *testing.T) {
	record, ledger := repairTestRecord()
	if err := record.ValidateAgainstLedger(ledger); err != nil {
		t.Fatalf("valid repair record rejected: %v", err)
	}
	for name, mutate := range map[string]func(*EngineRepairRecord){
		"source config":     func(candidate *EngineRepairRecord) { candidate.SourceConfig.ConfigID++ },
		"source Secret UID": func(candidate *EngineRepairRecord) { candidate.SourceAdminCredential.UID = "replacement-secret-uid" },
		"source token digest": func(candidate *EngineRepairRecord) {
			candidate.SourceAdminCredential.AdminTokenDigest = strings.Repeat("e", 64)
		},
	} {
		t.Run("identity binds "+name, func(t *testing.T) {
			candidate := record
			mutate(&candidate)
			if candidate.IdentityDigest() == record.IdentityDigest() {
				t.Fatal("immutable repair identity omitted its original-source credential binding")
			}
		})
	}
	for name, mutate := range map[string]func(*EngineRepairRecord){
		"wrong old WAL in source config": func(candidate *EngineRepairRecord) {
			candidate.SourceConfig.Members[0].WALIdentity = "recreated-old-wal"
		},
		"wrong operation ID":        func(candidate *EngineRepairRecord) { candidate.RemovalOperation.OperationID = "other-operation" },
		"wrong expected abort slot": func(candidate *EngineRepairRecord) { candidate.RemovalOperation.ExpectedAbortSlot++ },
		"wrong freeze hash":         func(candidate *EngineRepairRecord) { candidate.RemovalOperation.FreezeHash[0] ^= 0xff },
		"candidate inserted into removed target": func(candidate *EngineRepairRecord) {
			candidate.RemovalOperation.Target.Members = append(candidate.RemovalOperation.Target.Members, candidate.CandidateMember)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := record
			candidate.SourceConfig.Members = append([]quepaxa.Member(nil), record.SourceConfig.Members...)
			operation := *record.RemovalOperation
			operation.Target.Members = append([]quepaxa.Member(nil), record.RemovalOperation.Target.Members...)
			candidate.RemovalOperation = &operation
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("repair record accepted mismatched source/operation authority")
			}
		})
	}
	roundTrip, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var decoded EngineRepairRecord
	if err := json.Unmarshal(roundTrip, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.ValidateAgainstLedger(ledger); err != nil {
		t.Fatalf("round-tripped repair record rejected: %v", err)
	}
	// The candidate is not an old-generation incarnation, but startup may
	// already have registered its exact unadmitted learner run in the root.
	// That one candidate run is accepted separately from the historical set.
	withCandidate := ledger
	withCandidate.Runs = make(map[string]Run, len(ledger.Runs)+1)
	for runID, run := range ledger.Runs {
		withCandidate.Runs[runID] = run
	}
	unboundCandidate := record.CandidateIdentity
	unboundCandidate.WALIdentity = ""
	withCandidate.Runs[record.CandidateIdentity.RunID] = Run{Identity: unboundCandidate, Role: StartupRoleLearnerPendingRemove, Runtime: record.CandidateRuntimeBinding}
	if err := record.ValidateAgainstLedger(withCandidate); err != nil {
		t.Fatalf("exact pre-grant WAL-unbound candidate run rejected: %v", err)
	}
	boundCandidate := withCandidate
	boundCandidate.Runs = make(map[string]Run, len(withCandidate.Runs))
	for runID, run := range withCandidate.Runs {
		boundCandidate.Runs[runID] = run
	}
	boundRun := boundCandidate.Runs[record.CandidateIdentity.RunID]
	boundRun.Identity = record.CandidateIdentity
	boundCandidate.Runs[record.CandidateIdentity.RunID] = boundRun
	if err := record.ValidateAgainstLedger(boundCandidate); err != nil {
		t.Fatalf("exact post-bind candidate run rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Ledger){
		"candidate run identity mismatch": func(root *Ledger) {
			run := root.Runs[record.CandidateIdentity.RunID]
			run.Identity.PodUID = "different-pod-uid"
			root.Runs[run.Identity.RunID] = run
		},
		"candidate already producer admitted": func(root *Ledger) {
			run := root.Runs[record.CandidateIdentity.RunID]
			run.MutationProducerUID = "producer-already-admitted"
			run.MutationScopeHash = journalTestDigest
			run.MutationProfile = MutationHeldExecutionProfile
			root.Runs[run.Identity.RunID] = run
		},
		"candidate wrong role": func(root *Ledger) {
			run := root.Runs[record.CandidateIdentity.RunID]
			run.Role = StartupRoleVoterActive
			root.Runs[run.Identity.RunID] = run
		},
		"candidate bound to wrong WAL": func(root *Ledger) {
			run := root.Runs[record.CandidateIdentity.RunID]
			run.Identity.WALIdentity = "other-wal"
			root.Runs[run.Identity.RunID] = run
		},
		"unrecorded additional run": func(root *Ledger) {
			id := Identity{SourceClusterID: root.SourceClusterID, NodeID: "unknown", PodUID: "unknown-pod", RunID: "unknown-run", WALIdentity: "unknown-wal"}
			root.Runs[id.RunID] = Run{Identity: id, Role: StartupRoleVoterActive}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidateRoot := withCandidate
			candidateRoot.Runs = make(map[string]Run, len(withCandidate.Runs)+1)
			for runID, run := range withCandidate.Runs {
				candidateRoot.Runs[runID] = run
			}
			mutate(&candidateRoot)
			if err := record.ValidateAgainstLedger(candidateRoot); err == nil {
				t.Fatal("mismatched candidate or unrecorded root run accepted")
			}
		})
	}

	missing := record
	missing.Incarnations = append([]RepairIncarnation(nil), record.Incarnations[:1]...)
	if err := missing.ValidateAgainstLedger(ledger); err == nil {
		t.Fatal("partial incarnation inventory accepted")
	}
	wrongWAL := record
	wrongWAL.Incarnations = append([]RepairIncarnation(nil), record.Incarnations...)
	wrongWAL.Incarnations[0].Identity.WALIdentity = "recreated-wal"
	if err := wrongWAL.Validate(); err == nil {
		t.Fatal("repair record accepted another old-member WAL")
	}
	missingOldWAL := record
	missingOldWAL.Incarnations = append([]RepairIncarnation(nil), record.Incarnations...)
	missingOldWAL.Incarnations[0].Identity.WALIdentity = ""
	if err := missingOldWAL.Validate(); err == nil {
		t.Fatal("repair record accepted an inventory without the exact old WAL identity")
	}
	wrongConfig := record
	wrongConfig.Binding.OldConfigID++
	if wrongConfig.IdentityDigest() == record.IdentityDigest() {
		t.Fatal("repair identity digest omitted the ConfigID binding")
	}
	wrongAbort := record
	wrongAbort.Binding.ExpectedAbortSlot++
	if wrongAbort.IdentityDigest() == record.IdentityDigest() {
		t.Fatal("repair identity digest omitted the abort-slot binding")
	}
	wrongTarget := record
	wrongTarget.CandidateIdentity.NodeID = wrongTarget.Binding.OldMemberID
	if err := wrongTarget.Validate(); err == nil {
		t.Fatal("repair record accepted old identity reuse")
	}
	wrongCandidateEndpoint := record
	wrongCandidateEndpoint.CandidateMember.PeerURL = "quic://attacker:8443"
	if err := wrongCandidateEndpoint.Validate(); err == nil {
		t.Fatal("repair record accepted an endpoint that differs from its member digest")
	}
	wrongCredential := record
	wrongCredential.CandidateCredential.PeerTokenDigest = ""
	if err := wrongCredential.Validate(); err == nil {
		t.Fatal("repair record accepted an unpinned peer credential")
	}
	wrongCandidateWAL := record
	wrongCandidateWAL.CandidateMember.WALIdentity = "other-wal"
	wrongCandidateWAL.CandidateMemberDigest = EngineRepairMemberDigest(wrongCandidateWAL.CandidateMember)
	if err := wrongCandidateWAL.Validate(); err == nil {
		t.Fatal("repair record accepted a member WAL different from the candidate identity")
	}
	missingLogEndpoint := record
	missingLogEndpoint.CandidateMember.LogURL = ""
	missingLogEndpoint.CandidateMemberDigest = EngineRepairMemberDigest(missingLogEndpoint.CandidateMember)
	if err := missingLogEndpoint.Validate(); err == nil {
		t.Fatal("repair record accepted a candidate without its public log endpoint")
	}
	overCapacity := record
	overCapacity.Incarnations = make([]RepairIncarnation, MaxEngineRepairIncarnations+1)
	if err := overCapacity.Validate(); err == nil {
		t.Fatal("over-capacity repair inventory accepted")
	}

	full := record
	full.Incarnations = nil
	ledger.Runs = make(map[string]Run, MaxEngineRepairIncarnations)
	for index := 0; index < MaxEngineRepairIncarnations-1; index++ {
		id := Identity{SourceClusterID: "source", NodeID: fmt.Sprintf("member-%02d", index), PodName: fmt.Sprintf("rhiza-%d", index),
			PodUID: fmt.Sprintf("pod-%02d", index), RunID: fmt.Sprintf("run-%02d", index), WALIdentity: fmt.Sprintf("wal-%02d", index)}
		if index == 0 {
			id.NodeID, id.PodName, id.PodUID, id.RunID, id.WALIdentity = "old-member", "rhiza-1", "pod-old", "run-old", "old-wal"
		}
		incarnation := RepairIncarnation{Identity: id, Role: StartupRoleVoterActive}
		full.Incarnations = append(full.Incarnations, incarnation)
		ledger.Runs[id.RunID] = Run{Identity: id, Role: StartupRoleVoterActive}
	}
	fullCandidateIdentity := full.CandidateIdentity
	fullCandidateIdentity.WALIdentity = ""
	ledger.Runs[fullCandidateIdentity.RunID] = Run{Identity: fullCandidateIdentity, Role: StartupRoleLearnerPendingRemove, Runtime: full.CandidateRuntimeBinding}
	if err := full.ValidateAgainstLedger(ledger); err != nil {
		t.Fatalf("exact %d-run inventory rejected: %v", MaxEngineRepairIncarnations, err)
	}
	tooManyLedgerRuns := ledger
	tooManyLedgerRuns.Runs = make(map[string]Run, MaxEngineRepairIncarnations+1)
	for runID, run := range ledger.Runs {
		tooManyLedgerRuns.Runs[runID] = run
	}
	tooManyID := Identity{SourceClusterID: "source", NodeID: "extra", PodName: "rhiza-extra", PodUID: "pod-extra", RunID: "run-extra", WALIdentity: "wal-extra"}
	tooManyLedgerRuns.Runs[tooManyID.RunID] = Run{Identity: tooManyID, Role: StartupRoleVoterActive}
	tooManyRecord := full
	tooManyRecord.Incarnations = append(append([]RepairIncarnation(nil), full.Incarnations...), RepairIncarnation{Identity: tooManyID, Role: StartupRoleVoterActive})
	if err := tooManyRecord.ValidateAgainstLedger(tooManyLedgerRuns); err == nil {
		t.Fatal("65-run inventory accepted")
	}
}

func TestEngineRepairInventoryAllowsOnlyFreshRuntimeOfRecordedSurvivor(t *testing.T) {
	baseRecord, baseLedger := repairTestRecord()
	for _, phase := range []EngineRepairPhase{RepairCandidatePrepared, RepairStorageClosing, RepairActive} {
		t.Run(string(phase), func(t *testing.T) {
			record := baseRecord
			ledger := cloneRepairLedger(baseLedger)
			switch phase {
			case RepairCandidatePrepared:
				record.Phase = RepairCandidatePrepared
				record.Binding.FreezeSlot = 0
				record.Binding.FreezeHash = ""
				record.RemovalOperation = nil
			case RepairActive:
				record.Phase = RepairActive
				record.AddRequestHash = strings.Repeat("a", 64)
				record.StorageProofDigest = strings.Repeat("b", 64)
				record.RemoveTerminalDigest = strings.Repeat("c", 64)
				candidateRun := ledger.Runs[record.CandidateIdentity.RunID]
				candidateRun.Identity.WALIdentity = record.CandidateIdentity.WALIdentity
				candidateRun.Role = StartupRoleVoterActive
				candidateRun.MutationProducerUID = "candidate-producer"
				candidateRun.MutationScopeHash = journalTestDigest
				candidateRun.MutationProfile = MutationHeldExecutionProfile
				ledger.Runs[record.CandidateIdentity.RunID] = candidateRun
			}
			survivor := ledger.Runs["run-survivor"].Identity
			survivor.RunID = "run-survivor-restarted"
			runtime := enrolledRuntime(survivor, "survivor-restarted-container")
			runtime.PodName = survivor.PodName
			ledger.Runs[survivor.RunID] = Run{Identity: survivor, Role: StartupRoleVoterActive, Runtime: &runtime}
			if err := record.ValidateAgainstLedger(ledger); err != nil {
				t.Fatalf("same-Pod fresh survivor run invalidated phase %s: %v", phase, err)
			}
		})
	}

	record, ledger := baseRecord, cloneRepairLedger(baseLedger)
	survivor := ledger.Runs["run-survivor"].Identity
	survivor.RunID = "run-survivor-restarted"
	runtime := enrolledRuntime(survivor, "survivor-restarted-container")
	runtime.PodName = survivor.PodName
	validRestart := Run{Identity: survivor, Role: StartupRoleVoterActive, Runtime: &runtime}
	for name, mutate := range map[string]func(*Run){
		"changed physical Pod": func(run *Run) { run.Identity.PodUID = "replacement-pod" },
		"unrecorded member":    func(run *Run) { run.Identity.NodeID = "unknown-member" },
		"removed member restart": func(run *Run) {
			run.Identity.NodeID = record.Binding.OldMemberID
			run.Identity.PodName = record.Incarnations[0].Identity.PodName
			run.Identity.PodUID = record.Incarnations[0].Identity.PodUID
			run.Identity.WALIdentity = record.Binding.OldWALIdentity
			run.Runtime = nil
		},
		"candidate extra run": func(run *Run) {
			run.Identity.NodeID = record.CandidateIdentity.NodeID
			run.Identity.PodName = record.CandidateIdentity.PodName
			run.Identity.PodUID = record.CandidateIdentity.PodUID
			run.Identity.WALIdentity = record.CandidateIdentity.WALIdentity
			run.Identity.AssignmentID = record.CandidateIdentity.AssignmentID
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cloneRepairLedger(ledger)
			changed := validRestart
			mutate(&changed)
			candidate.Runs[changed.Identity.RunID] = changed
			if err := record.ValidateAgainstLedger(candidate); err == nil {
				t.Fatal("unbound or unrecorded repair run was accepted")
			}
		})
	}
}

func cloneRepairLedger(ledger Ledger) Ledger {
	copy := ledger
	copy.Runs = make(map[string]Run, len(ledger.Runs))
	for runID, run := range ledger.Runs {
		copy.Runs[runID] = run
	}
	return copy
}

func TestCompletedEngineRepairHistoryDoesNotReserveAnotherCapacitySlot(t *testing.T) {
	record, ledger := repairTestRecord()
	record.Phase = RepairActive
	record.AddRequestHash = strings.Repeat("a", 64)
	record.StorageProofDigest = strings.Repeat("b", 64)
	record.RemoveTerminalDigest = strings.Repeat("c", 64)
	candidateRun := Run{Identity: record.CandidateIdentity, Role: StartupRoleVoterActive, Runtime: record.CandidateRuntimeBinding,
		MutationProducerUID: "producer-candidate", MutationScopeHash: journalTestDigest, MutationProfile: MutationHeldExecutionProfile}
	ledger.Runs[record.CandidateIdentity.RunID] = candidateRun
	if err := record.ValidateAgainstLedger(ledger); err != nil {
		t.Fatalf("completed repair history rejected its exact active candidate run: %v", err)
	}
	reference := EngineRepairReference{Version: EngineRepairRecordVersion, OperationID: record.Binding.OperationID,
		Binding: record.Binding, CandidateIdentity: record.CandidateIdentity, Phase: record.Phase,
		IdentityDigest: record.IdentityDigest(), RecordDigest: record.Digest(), RecordKey: "history/repair.json"}
	ledger.EngineRepairs = map[string]EngineRepairReference{"historical-repair": reference}
	used, err := ledger.EngineRepairCapacityUse()
	if err != nil || used != len(ledger.Runs) {
		t.Fatalf("completed repair consumed capacity beyond its registered runs: used=%d runs=%d err=%v", used, len(ledger.Runs), err)
	}
	wrongRole := ledger
	wrongRun := candidateRun
	wrongRun.Role = StartupRoleLearnerPendingRemove
	wrongRole.Runs[record.CandidateIdentity.RunID] = wrongRun
	if _, err := wrongRole.EngineRepairCapacityUse(); err == nil {
		t.Fatal("completed repair history accepted a candidate that was not active")
	}
}

func TestEngineRepairEvidenceHistoryIsBoundedWithoutLiveRuns(t *testing.T) {
	record, _ := repairTestRecord()
	ledger := Ledger{SourceClusterID: record.Binding.SourceClusterID, Runs: map[string]Run{},
		EngineRepairs: make(map[string]EngineRepairReference, MaxEngineRepairIncarnations+1)}
	for i := 0; i < MaxEngineRepairIncarnations; i++ {
		candidate := record.CandidateIdentity
		candidate.NodeID = fmt.Sprintf("candidate-%d", i)
		candidate.RunID = fmt.Sprintf("candidate-run-%d", i)
		binding := record.Binding
		binding.OperationID = fmt.Sprintf("repair-operation-%d", i)
		binding.TargetMemberID = candidate.NodeID
		binding.RequestHash = hashJSONValue([]any{"request", i})
		ledger.EngineRepairs[fmt.Sprintf("repair-%d", i)] = EngineRepairReference{
			Version: EngineRepairRecordVersion, OperationID: binding.OperationID, Binding: binding,
			CandidateIdentity: candidate, Phase: RepairGenerationClosing, ClosingFrom: RepairCandidatePrepared,
			IdentityDigest: hashJSONValue([]any{"identity", i}), RecordDigest: hashJSONValue([]any{"record", i}),
			RecordKey: fmt.Sprintf("history/%d.json", i),
		}
	}
	if _, err := ledger.EngineRepairCapacityUse(); err != nil {
		t.Fatalf("history at the configured bound rejected: %v", err)
	}
	ledger.EngineRepairs["overflow"] = ledger.EngineRepairs["repair-0"]
	if _, err := ledger.EngineRepairCapacityUse(); err == nil {
		t.Fatal("unbounded closed repair evidence history accepted")
	}
}

func TestEngineRepairIdentitySurvivesFreezeAndGenerationClosing(t *testing.T) {
	record, _ := repairTestRecord()
	record.Phase = RepairCandidatePrepared
	record.Binding.FreezeSlot = 0
	record.Binding.FreezeHash = ""
	removalOperation := record.RemovalOperation
	record.RemovalOperation = nil
	identityDigest := record.IdentityDigest()
	unbound := record
	unbound.CandidateRuntimeBinding = nil
	unbound.CandidateRuntimeHash = ""
	if unbound.IdentityDigest() != identityDigest {
		t.Fatal("trusted runtime finalization changed the immutable request identity")
	}
	changedRequest := unbound
	changedRequest.CandidateIdentity.AssignmentID = strings.Repeat("e", 64)
	if changedRequest.IdentityDigest() == identityDigest {
		t.Fatal("immutable request identity omitted the authority-assigned candidate identity")
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("pre-freeze repair record rejected: %v", err)
	}
	record.RemovalOperation = removalOperation
	record.Binding.FreezeSlot = 44
	record.Binding.FreezeHash = strings.Repeat("b", 64)
	record.Phase = RepairRemoveFrozen
	if got := record.IdentityDigest(); got != identityDigest {
		t.Fatal("freeze completion changed the immutable repair identity digest")
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("frozen repair record rejected: %v", err)
	}
	record.Phase = RepairGenerationClosing
	record.ClosingFrom = RepairRemoveFrozen
	if got := record.IdentityDigest(); got != identityDigest {
		t.Fatal("generation closing changed the immutable repair identity digest")
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("DR closing record lost its in-flight freeze phase: %v", err)
	}
	record.ClosingFrom = RepairRemoved
	if err := record.Validate(); err == nil {
		t.Fatal("DR closing record accepted missing storage/terminal evidence from its prior phase")
	}
	record.StorageProofDigest = strings.Repeat("9", 64)
	record.RemoveTerminalDigest = strings.Repeat("8", 64)
	if err := record.Validate(); err != nil {
		t.Fatalf("DR closing could not preserve a completed REMOVE: %v", err)
	}

	adding := record
	adding.Phase = RepairAdding
	adding.ClosingFrom = ""
	adding.AddRequestHash = strings.Repeat("7", 64)
	if err := adding.Validate(); err != nil {
		t.Fatalf("pending ADD record rejected: %v", err)
	}
	adding.Phase = RepairGenerationClosing
	adding.ClosingFrom = RepairAdding
	if err := adding.Validate(); err != nil {
		t.Fatalf("DR supersede discarded a pending ADD record: %v", err)
	}

	preFreeze, _ := repairTestRecord()
	preFreeze.Phase = RepairCandidatePrepared
	preFreeze.Binding.FreezeSlot = 0
	preFreeze.Binding.FreezeHash = ""
	preFreeze.RemovalOperation = nil
	preFreeze.Phase = RepairGenerationClosing
	preFreeze.ClosingFrom = RepairCandidatePrepared
	if err := preFreeze.Validate(); err != nil {
		t.Fatalf("DR supersede discarded a pre-freeze repair record: %v", err)
	}
}

func TestStorageEffectsExcludedBindsEveryClosedRunSettlement(t *testing.T) {
	record, ledger := repairTestRecord()
	oldHistorical := Identity{SourceClusterID: "source", NodeID: "old-member", PodName: "rhiza-1", PodUID: "pod-old-prior", RunID: "run-old-prior"}
	record.Incarnations = append(record.Incarnations, RepairIncarnation{Identity: oldHistorical, Role: StartupRoleVoterActive})
	ledger.Runs[oldHistorical.RunID] = Run{Identity: oldHistorical, Role: StartupRoleVoterActive}
	proof := StorageEffectsExcluded{Version: StorageEffectsProofVersion, Binding: record.Binding, RepairRecordHash: record.IdentityDigest(),
		RunSettlements: []StorageRunSettlement{
			{RunID: "run-old", PodUID: "pod-old", WALIdentity: "old-wal", ProducerUID: "producer-old", ClosedThrough: 3, SettledThrough: 3,
				ProducerStateHash: strings.Repeat("1", 64), SettlementDigest: strings.Repeat("2", 64)},
			{RunID: "run-old-prior", PodUID: "pod-old-prior", ProducerStateHash: strings.Repeat("3", 64),
				SettlementDigest: strings.Repeat("4", 64), NoProducerDigest: strings.Repeat("5", 64)},
		}}
	proof.ProofDigest = proof.Digest()
	record.StorageProofDigest = proof.ProofDigest
	if err := proof.Validate(record); err != nil {
		t.Fatalf("valid storage-exclusion proof rejected: %v", err)
	}
	if err := record.ValidateAgainstLedger(ledger); err != nil {
		t.Fatalf("valid root-bound record rejected: %v", err)
	}

	mutations := map[string]func(*StorageEffectsExcluded){
		"wrong operation":  func(p *StorageEffectsExcluded) { p.Binding.OperationID = "other" },
		"wrong old config": func(p *StorageEffectsExcluded) { p.Binding.OldConfigID++ },
		"wrong abort slot": func(p *StorageEffectsExcluded) { p.Binding.ExpectedAbortSlot++ },
		"wrong freeze":     func(p *StorageEffectsExcluded) { p.Binding.FreezeHash = strings.Repeat("9", 64) },
		"missing run":      func(p *StorageEffectsExcluded) { p.RunSettlements = p.RunSettlements[:1] },
		"duplicate run":    func(p *StorageEffectsExcluded) { p.RunSettlements[1].RunID = p.RunSettlements[0].RunID },
		"includes survivor": func(p *StorageEffectsExcluded) {
			p.RunSettlements[1] = StorageRunSettlement{RunID: "run-survivor", PodUID: "pod-survivor", WALIdentity: "survivor-wal", ProducerUID: "producer-survivor",
				ProducerStateHash: strings.Repeat("3", 64), SettlementDigest: strings.Repeat("4", 64)}
		},
		"wrong pod uid":              func(p *StorageEffectsExcluded) { p.RunSettlements[0].PodUID = "replacement" },
		"wrong wal":                  func(p *StorageEffectsExcluded) { p.RunSettlements[0].WALIdentity = "new-wal" },
		"not fully settled":          func(p *StorageEffectsExcluded) { p.RunSettlements[0].SettledThrough-- },
		"missing no-producer proof":  func(p *StorageEffectsExcluded) { p.RunSettlements[1].NoProducerDigest = "" },
		"no-producer with admission": func(p *StorageEffectsExcluded) { p.RunSettlements[1].ClosedThrough = 1 },
		"missing state ref":          func(p *StorageEffectsExcluded) { p.RunSettlements[0].ProducerStateHash = "" },
		"wrong repair record":        func(p *StorageEffectsExcluded) { p.RepairRecordHash = strings.Repeat("a", 64) },
		"stale proof digest":         func(p *StorageEffectsExcluded) { p.ProofDigest = strings.Repeat("b", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := proof
			candidate.RunSettlements = append([]StorageRunSettlement(nil), proof.RunSettlements...)
			mutate(&candidate)
			if candidate.ProofDigest == proof.ProofDigest {
				candidate.ProofDigest = candidate.Digest()
			}
			if err := candidate.Validate(record); err == nil {
				t.Fatal("mismatched storage-exclusion proof accepted")
			}
		})
	}
}

func TestMutationGenerationZeroGuardIsDistinctAndKeepsOriginalGrantUnsettled(t *testing.T) {
	repair, _ := repairTestRecord()
	id := repair.Incarnations[0].Identity
	p := newRepairTestProducer(t, id)
	intent := journalUpload("guarded-create")
	prepared := MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 0}
	mustJournal(t, p.Admit(1, intent))
	mustJournal(t, p.Prepare(1, prepared))
	mustJournal(t, p.GrantDispatch(1, "original-dispatch"))
	mustJournal(t, p.SealDispatch(1))
	mustJournal(t, p.Close("repair-op", repair.Binding.RequestHash))
	preparationHash, err := p.PreparationHash(1)
	mustJournal(t, err)
	helperRequest := strings.Repeat("a", 64)
	created, err := p.RecordGenerationZeroPhysicalRight("producer-1", 1, repair, 1, helperRequest)
	mustJournal(t, err)
	if !created {
		t.Fatal("first physical helper attempt was not recorded")
	}
	guard := MutationGenerationZeroGuard{Version: MutationGenerationZeroGuardVersion, RepairBinding: repair.Binding,
		RepairRecordHash: repair.IdentityDigest(), StorageScopeHash: intent.StoreScopeHash, Key: intent.Key, Runtime: p.Runtime,
		WALIdentity: id.WALIdentity, ProducerUID: "producer-1", Sequence: 1, Intent: intent, Preparation: prepared,
		PreparationHash: preparationHash, DispatchID: "original-dispatch", GuardRootIdentity: strings.Repeat("d", 64),
		GuardRef: "mutation-effect-guards/" + strings.Repeat("e", 32), GuardUID: "guard-uid",
		ObjectGeneration: 12, ObjectMetageneration: 3, ObjectSize: intent.PayloadSize, PayloadDigest: intent.PayloadDigest}
	guard.GuardDigest = guard.Digest()
	mustJournal(t, p.RecordGenerationZeroGuard("producer-1", 1, repair, guard))
	if err := p.RecordGenerationZeroPhysicalTerminal(1, 1, helperRequest, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	created, err = p.RecordGenerationZeroPhysicalRight("producer-1", 1, repair, 2, strings.Repeat("f", 64))
	if err != nil || !created {
		t.Fatalf("second physical retry did not receive a distinct right: created=%v err=%v", created, err)
	}
	if err := p.RecordGenerationZeroPhysicalTerminal(1, 2, strings.Repeat("f", 64), strings.Repeat("9", 64)); err != nil {
		t.Fatal(err)
	}
	if err := p.RetireGenerationZeroRights("producer-1", 1, repair); err == nil {
		t.Fatal("unsealed physical-right set was retired")
	}
	mustJournal(t, p.SealGenerationZeroRights("producer-1", 1, repair))
	mustJournal(t, p.RetireGenerationZeroRights("producer-1", 1, repair))
	mustJournal(t, p.RetireGenerationZeroRights("producer-1", 1, repair))
	if created, err := p.RecordGenerationZeroPhysicalRight("producer-1", 1, repair, 3, strings.Repeat("8", 64)); err == nil || created {
		t.Fatal("sealed helper-right set admitted another physical retry")
	}
	entry := p.Entries[1]
	if entry.GenerationZeroGuard == nil || entry.GuardedLogicalResolution != nil || entry.Resolution == nil || !entry.DispatchClosed ||
		entry.Resolution.Outcome != MutationCommitted || entry.Resolution.AllCopiesTerminal ||
		entry.Dispatches["original-dispatch"].ReceiptDigest != "" || p.BarrierComplete() || entry.CopyCompletion != nil ||
		entry.GenerationZeroRights == nil || !entry.GenerationZeroRights.Sealed || !entry.GenerationZeroRights.HelperRightsRetired || !generationZeroRightsTerminal(*entry.GenerationZeroRights) {
		t.Fatal("storage guard reinterpreted or terminalized the original provider grant")
	}
	completion := MutationCopyCompletion{PreparationHash: preparationHash, ReceiptDigest: strings.Repeat("c", 64),
		GenerationZeroRightsDigest: entry.GenerationZeroRights.Digest()}
	rejectJournalUnchanged(t, p, func() error { return p.ConfirmCopiesTerminal(1, completion) })
	mustJournal(t, p.RecordDispatchTerminal(1, "original-dispatch", strings.Repeat("d", 64)))
	mustJournal(t, p.ConfirmCopiesTerminal(1, completion))
	if !p.BarrierComplete() || p.Entries[1].GenerationZeroGuard == nil || p.Entries[1].CopyCompletion == nil {
		t.Fatal("terminal original and helper receipts did not complete the guarded journal entry")
	}
	restarted := reloadProducer(t, p)
	if err := restarted.ValidateGenerationZeroGuard("producer-1", 1, repair, restarted.Entries[1]); err != nil {
		t.Fatalf("round-tripped storage guard rejected: %v", err)
	}
}

func TestMutationGenerationZeroGuardRejectsBindingAndObjectDrift(t *testing.T) {
	makeGuarded := func(t *testing.T) (*MutationProducer, EngineRepairRecord, MutationGenerationZeroGuard) {
		t.Helper()
		repair, _ := repairTestRecord()
		id := repair.Incarnations[0].Identity
		p := newRepairTestProducer(t, id)
		intent := journalUpload("guarded-create")
		prepared := MutationPreparation{Profile: MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 0}
		mustJournal(t, p.Admit(1, intent))
		mustJournal(t, p.Prepare(1, prepared))
		mustJournal(t, p.GrantDispatch(1, "original-dispatch"))
		mustJournal(t, p.SealDispatch(1))
		mustJournal(t, p.Close("repair-op", repair.Binding.RequestHash))
		preparationHash, err := p.PreparationHash(1)
		mustJournal(t, err)
		if created, err := p.RecordGenerationZeroPhysicalRight("producer-1", 1, repair, 1, strings.Repeat("a", 64)); err != nil || !created {
			t.Fatalf("record helper right: created=%v err=%v", created, err)
		}
		guard := MutationGenerationZeroGuard{Version: MutationGenerationZeroGuardVersion, RepairBinding: repair.Binding,
			RepairRecordHash: repair.IdentityDigest(), StorageScopeHash: intent.StoreScopeHash, Key: intent.Key, Runtime: p.Runtime,
			WALIdentity: id.WALIdentity, ProducerUID: "producer-1", Sequence: 1, Intent: intent, Preparation: prepared,
			PreparationHash: preparationHash, DispatchID: "original-dispatch", GuardRootIdentity: strings.Repeat("d", 64),
			GuardRef: "mutation-effect-guards/" + strings.Repeat("e", 32), GuardUID: "guard-uid",
			ObjectGeneration: 12, ObjectMetageneration: 3, ObjectSize: intent.PayloadSize, PayloadDigest: intent.PayloadDigest}
		guard.GuardDigest = guard.Digest()
		mustJournal(t, p.RecordGenerationZeroGuard("producer-1", 1, repair, guard))
		return p, repair, guard
	}

	mutations := map[string]func(*MutationGenerationZeroGuard){
		"wrong repair operation":      func(g *MutationGenerationZeroGuard) { g.RepairBinding.OperationID = "other" },
		"wrong repair record":         func(g *MutationGenerationZeroGuard) { g.RepairRecordHash = strings.Repeat("9", 64) },
		"wrong member":                func(g *MutationGenerationZeroGuard) { g.RepairBinding.OldMemberID = "other" },
		"wrong WAL":                   func(g *MutationGenerationZeroGuard) { g.WALIdentity = "other" },
		"wrong producer":              func(g *MutationGenerationZeroGuard) { g.ProducerUID = "other" },
		"wrong key":                   func(g *MutationGenerationZeroGuard) { g.Key = "other" },
		"wrong dispatch":              func(g *MutationGenerationZeroGuard) { g.DispatchID = "other" },
		"missing guard UID":           func(g *MutationGenerationZeroGuard) { g.GuardUID = "" },
		"wrong guard path":            func(g *MutationGenerationZeroGuard) { g.GuardRef = "../outside" },
		"missing positive generation": func(g *MutationGenerationZeroGuard) { g.ObjectGeneration = 0 },
		"missing metageneration":      func(g *MutationGenerationZeroGuard) { g.ObjectMetageneration = 0 },
		"wrong bytes":                 func(g *MutationGenerationZeroGuard) { g.PayloadDigest = strings.Repeat("f", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			p, repair, original := makeGuarded(t)
			candidate := original
			mutate(&candidate)
			candidate.GuardDigest = candidate.Digest()
			if err := p.ValidateGenerationZeroGuard("producer-1", 1, repair, MutationEntry{Intent: original.Intent, Preparation: &original.Preparation,
				Resolution: p.Entries[1].Resolution, DispatchClosed: true, Dispatches: p.Entries[1].Dispatches, GenerationZeroGuard: &candidate}); err == nil {
				t.Fatal("mismatched generation-zero guard accepted")
			}
		})
	}
}

func newRepairTestProducer(t *testing.T, id Identity) *MutationProducer {
	t.Helper()
	runtime := enrolledRuntime(id, "container-"+id.RunID)
	runtime.PodName = id.PodName
	producer, err := NewMutationProducer(id, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.Activate(id); err != nil {
		t.Fatal(err)
	}
	return producer
}
