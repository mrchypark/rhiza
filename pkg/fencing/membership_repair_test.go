package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipRepairAuthorizationAcceptsEmptyLogURLButRequiresIdentityEndpoints(t *testing.T) {
	old := Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "old-pod", RunID: "old-run", WALIdentity: "old-wal"}
	members := []quepaxa.Member{
		{ID: "rhiza-0", URL: "http://old", PeerURL: "quic://old-peer", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("old"))), WALIdentity: old.WALIdentity},
		{ID: "rhiza-1", URL: "http://other", PeerURL: "quic://other-peer", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("other"))), WALIdentity: "other-wal"},
	}
	makeAuthorization := func(membership []quepaxa.Member) MembershipRepairAuthorization {
		return MembershipRepairAuthorization{
			OperationID: "repair", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64),
			RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger", SourceClusterID: "source",
			MembershipHash: canonicalJSONHash(membership), OldIdentity: old, Membership: quepaxa.Cluster{ConfigID: 1, Members: membership},
		}
	}
	if err := makeAuthorization(members).Validate(); err != nil {
		t.Fatalf("production empty-LogURL membership rejected: %v", err)
	}
	for name, mutate := range map[string]func([]quepaxa.Member) []quepaxa.Member{
		"missing peer endpoint": func(m []quepaxa.Member) []quepaxa.Member { m[1].PeerURL = ""; return m },
		"missing public key":    func(m []quepaxa.Member) []quepaxa.Member { m[1].PublicKey = quepaxa.PublicKey{}; return m },
		"duplicate member ID":   func(m []quepaxa.Member) []quepaxa.Member { m[1].ID = m[0].ID; return m },
	} {
		t.Run(name, func(t *testing.T) {
			bad := append([]quepaxa.Member(nil), members...)
			if err := makeAuthorization(mutate(bad)).Validate(); err == nil {
				t.Fatal("invalid membership accepted")
			}
		})
	}
}

func TestLedgerMembershipRepairRetainsExactRoundsAndBoundSurvivorView(t *testing.T) {
	oldID := Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "old-pod", RunID: "old-run", WALIdentity: "old-wal"}
	oldMember := quepaxa.Member{ID: "rhiza-0", URL: "http://old", PeerURL: "http://old-peer", LogURL: "http://old-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("old-key"))), WALIdentity: "old-wal"}
	otherMember := quepaxa.Member{ID: "other", URL: "http://other", PeerURL: "http://other-peer", LogURL: "http://other-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("other-key"))), WALIdentity: "other-wal"}
	membership := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{oldMember, otherMember}}
	zero := quepaxa.Slot(0)
	authorization := MembershipRepairAuthorization{
		OperationID: "logical-repair", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64),
		RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger", SourceClusterID: "source",
		MembershipHash: canonicalJSONHash(membership.Members), ExpectedAbortSlot: &zero, OldIdentity: oldID, Membership: membership,
	}
	ledger := Ledger{SourceClusterID: "source"}
	registered := oldID
	registered.WALIdentity = ""
	if err := ledger.Register(registered); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Bind(oldID); err != nil {
		t.Fatal(err)
	}
	otherID := Identity{SourceClusterID: "source", NodeID: "other", PodUID: "other-pod", RunID: "other-run", WALIdentity: "other-wal"}
	otherRegistered := otherID
	otherRegistered.WALIdentity = ""
	if err := ledger.Register(otherRegistered); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Bind(otherID); err != nil {
		t.Fatal(err)
	}
	competing := MembershipRepairAuthorization{
		OperationID: "competing-repair", RequestHash: strings.Repeat("9", 64), AuthorizationHash: strings.Repeat("8", 64),
		RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger", SourceClusterID: "source",
		MembershipHash: authorization.MembershipHash, OldIdentity: otherID, Membership: membership,
	}
	if err := ledger.InstallMembershipRepair(authorization); err != nil {
		t.Fatal(err)
	}
	if err := ledger.InstallMembershipRepair(authorization); err != nil {
		t.Fatalf("same authorized repair was not idempotent: %v", err)
	}
	view, err := ledger.ReadMembershipRepair(oldID)
	if err != nil || view.Progress.Stage != MembershipRepairAdmitted || view.Progress.Revision != 1 {
		t.Fatalf("initial repair view=%+v err=%v", view, err)
	}
	unknownStage := view
	unknownStage.Progress.Stage = MembershipRepairStage("future-stage")
	if err := unknownStage.ValidateFor(oldID); err == nil {
		t.Fatal("unknown retained stage was accepted")
	}
	outsider := oldID
	outsider.NodeID = "not-a-voter"
	if _, err := ledger.ReadMembershipRepair(outsider); err == nil {
		t.Fatal("non-voter read repair descriptor")
	}

	nodeID, err := ReplacementNodeID("new-pod")
	if err != nil {
		t.Fatal(err)
	}
	assignment := BootstrapAssignment{
		OperationID: "init-attempt-scope", RequestHash: strings.Repeat("c", 64), AuthorizationHash: strings.Repeat("d", 64),
		Purpose: BootstrapLearner, RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "source",
		PodName: "rhiza-0", PodUID: "new-pod", NodeID: nodeID, CredentialName: "candidate-secret", CredentialUID: "candidate-secret-uid",
		MembershipHash: authorization.MembershipHash, ProfileHash: strings.Repeat("e", 64), LedgerUID: "ledger",
	}
	if err := ledger.AppendMembershipRepairCandidate(MembershipRepairCandidate{Assignment: assignment}); err != nil {
		t.Fatal(err)
	}
	if ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
		t.Fatal("candidate without retained init evidence passed closure completion")
	}
	if err := ledger.CancelMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision); err == nil {
		t.Fatal("repair with an assigned but unbound candidate was cancelled")
	}
	if _, err := ledger.ReadMembershipRepair(oldID); err != nil {
		t.Fatalf("assigned-candidate repair stopped being visible: %v", err)
	}
	if err := ledger.InstallMembershipRepair(competing); err != ErrPending {
		t.Fatalf("second repair replaced an unsettled assigned candidate: %v", err)
	}
	candidateIdentity := Identity{
		SourceClusterID: "source", NodeID: nodeID, PodName: "rhiza-0", PodUID: "new-pod",
		CredentialName: assignment.CredentialName, CredentialUID: assignment.CredentialUID, RunID: "new-run",
	}
	if err := ledger.Register(candidateIdentity); err != nil {
		t.Fatalf("exact preauthorized learner could not register before old-voter closure: %v", err)
	}
	unauthorized := candidateIdentity
	unauthorized.PodUID = "other-new-pod"
	unauthorized.NodeID, _ = ReplacementNodeID(unauthorized.PodUID)
	unauthorized.CredentialName = "other-secret"
	unauthorized.CredentialUID = "other-secret-uid"
	unauthorized.RunID = "unauthorized-run"
	if err := ledger.Register(unauthorized); err == nil {
		t.Fatal("unassigned same-ordinal incarnation registered before old-voter closure")
	}
	newMember := quepaxa.Member{ID: quepaxa.NodeID(nodeID), URL: "http://new", PeerURL: "http://new-peer", LogURL: "http://new-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("new-key"))), WALIdentity: strings.Repeat("5", 64)}
	if err := ledger.BindMembershipRepairCandidate("new-pod", newMember); err != nil {
		t.Fatal(err)
	}
	boundCandidate := candidateIdentity
	boundCandidate.WALIdentity = newMember.WALIdentity
	if err := ledger.Bind(boundCandidate); err != nil {
		t.Fatalf("authorized learner binding failed: %v", err)
	}
	if err := ledger.CancelMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision); err == nil {
		t.Fatal("repair with a bound candidate runtime was cancelled before settlement")
	}
	if _, err := ledger.ReadMembershipRepair(oldID); err != nil {
		t.Fatalf("bound-candidate repair stopped being visible: %v", err)
	}
	if err := ledger.InstallMembershipRepair(competing); err != ErrPending {
		t.Fatalf("second repair replaced an unsettled bound candidate: %v", err)
	}
	if err := ledger.AdvanceMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision, MembershipRepairCandidateBound); err != nil {
		t.Fatal(err)
	}
	if err := ledger.CancelMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision); err == nil {
		t.Fatal("candidate-bound repair was cancelled before old-target closure")
	}
	if _, err := ledger.ReadMembershipRepair(oldID); err != nil {
		t.Fatalf("candidate-bound repair stopped being visible: %v", err)
	}
	if err := ledger.AdvanceMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision, MembershipRepairOldClosed); err != nil {
		t.Fatal(err)
	}

	remove := MembershipRepairPhase{Kind: MembershipRepairRemove, CandidatePodUID: "new-pod", AbortSlot: &zero, Change: network.MembershipChange{
		OperationID: "remove-round", ClusterID: "source", ExpectedConfigID: 1, ExpectedAbortSlot: 0, Remove: "rhiza-0",
		Fence: &network.MembershipFence{NodeID: "rhiza-0", WALIdentity: "old-wal", WorkloadUID: "sts", Confirmed: true, Evidence: "retained-closure"},
	}}
	remove.Hash, err = MembershipRepairPhaseHash(remove)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.AppendMembershipRepairPhase(remove); err != nil {
		t.Fatalf("explicit zero abort slot rejected: %v", err)
	}
	if err := ledger.CancelMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision); err == nil {
		t.Fatal("repair with a retained consensus phase was cancelled")
	}
	missingAbort := remove
	missingAbort.Change.OperationID = "missing-abort-slot"
	missingAbort.AbortSlot = nil
	missingAbort.Hash, _ = MembershipRepairPhaseHash(missingAbort)
	if err := ledger.AppendMembershipRepairPhase(missingAbort); err == nil {
		t.Fatal("missing abort slot was conflated with explicit zero")
	}
	if err := ledger.AuthorizeMembershipPhase(oldID, remove); err != nil {
		t.Fatalf("bound old voter could not authorize its exact retained phase: %v", err)
	}
	forged := remove
	forged.Change.Fence.Evidence = "different-proof"
	if err := ledger.AuthorizeMembershipPhase(oldID, forged); err == nil {
		t.Fatal("different fence receipt reused the authorized REMOVE phase")
	}

	if err := ledger.AdvanceMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision, MembershipRepairRemoved); err != nil {
		t.Fatal(err)
	}
	add := MembershipRepairPhase{Kind: MembershipRepairAdd, CandidatePodUID: "new-pod", AbortSlot: &zero, Change: network.MembershipChange{
		OperationID: "add-round", ClusterID: "source", ExpectedConfigID: 2, ExpectedAbortSlot: 0, Add: &newMember,
	}}
	add.Hash, err = MembershipRepairPhaseHash(add)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.AppendMembershipRepairPhase(add); err != nil {
		t.Fatal(err)
	}
	if err := add.Validate(ledger.MembershipRepair.Authorization, ledger.MembershipRepair.Progress.Candidates[0]); err != nil {
		t.Fatalf("stored ADD phase invalid: %v", err)
	}
	for i, phase := range ledger.MembershipRepair.Progress.Phases {
		phaseHash, hashErr := MembershipRepairPhaseHash(phase)
		if err := phase.Validate(ledger.MembershipRepair.Authorization, ledger.MembershipRepair.Progress.Candidates[0]); err != nil {
			t.Fatalf("stored phase %d invalid: %v hash=%s want=%s hashErr=%v", i, err, phase.Hash, phaseHash, hashErr)
		}
	}
	if _, err := ledger.ReadMembershipRepair(oldID); err != nil {
		t.Fatalf("stored repair view invalid: %v", err)
	}
	if err := ledger.AuthorizeMembershipPhase(oldID, add); err != nil {
		t.Fatalf("bound survivor could not authorize exact ADD phase: %v", err)
	}
	if err := ledger.AdvanceMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision, MembershipRepairAdded); err != nil {
		t.Fatal(err)
	}
	if err := ledger.AdvanceMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision, MembershipRepairComplete); err == nil {
		t.Fatal("membership reached Complete without retained typed runtime completion evidence")
	}
	if ledger.MembershipRepair.Progress.Stage != MembershipRepairAdded {
		t.Fatalf("missing terminal ACK evidence changed stage to %s", ledger.MembershipRepair.Progress.Stage)
	}
	target := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{otherMember, newMember}}
	freezeHash, terminalHash := strings.Repeat("f", 64), strings.Repeat("1", 64)
	phaseResult := MembershipRepairPhaseResult{PhaseHash: add.Hash, JournalOperationID: add.Change.OperationID, JournalRequestHash: canonicalJSONHash(add.Change), Target: target,
		FreezeSlot: 10, FreezeHash: freezeHash, TerminalSlot: 26, TerminalHash: terminalHash, Outcome: "completed"}
	ledger.MembershipRepair.Progress.Results = append(ledger.MembershipRepair.Progress.Results, phaseResult)
	request := network.MembershipRepairCompletionRequest{Version: 1, Action: network.MembershipRepairCompletionAction, Challenge: strings.Repeat("ab", 32), AttemptRevision: 9,
		RepairOperationID: authorization.OperationID, RepairBindingDigest: MembershipRepairAuthorizationHash(authorization), PhaseHash: add.Hash,
		JournalOperationID: add.Change.OperationID, JournalRequestHash: phaseResult.JournalRequestHash, SourceClusterID: "source", SourceConfigID: 2, AbortSlot: &zero,
		FreezeSlot: phaseResult.FreezeSlot, FreezeHash: freezeHash, TerminalSlot: phaseResult.TerminalSlot, TerminalHash: terminalHash, Target: target,
		CandidatePodUID: assignment.PodUID, CandidateMember: newMember, CandidateBindRef: BootstrapAssignmentDigest(assignment),
		ExecutorNodeID: "other", ExecutorPodUID: "other-pod", ExecutorRunID: "other-run", ExecutorWALIdentity: strings.Repeat("4", 64)}
	request.ScopeDigest = MembershipRepairCompletionScopeDigest(request)
	digest, err := network.MembershipRepairCompletionRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	ledger.MembershipRepair.Progress.Completion = &MembershipRepairCompletion{Request: request, Accepted: &network.MembershipRepairCompletionResponse{
		Version: 1, Action: request.Action, Challenge: request.Challenge, AttemptRevision: request.AttemptRevision, ScopeDigest: request.ScopeDigest,
		RequestDigest: digest, ExecutorNodeID: request.ExecutorNodeID, ExecutorPodUID: request.ExecutorPodUID, ExecutorRunID: request.ExecutorRunID,
		ExecutorWALIdentity: request.ExecutorWALIdentity, TerminalSlot: request.TerminalSlot, TerminalHash: request.TerminalHash, Outcome: "completed", MAC: "authority-verified"}}
	if err := network.ValidateMembershipRepairCompletionRequest(request); err != nil {
		t.Fatalf("invalid completion request: %v", err)
	}
	validResponse := *ledger.MembershipRepair.Progress.Completion.Accepted
	for _, test := range []struct {
		name   string
		mutate func(*network.MembershipRepairCompletionRequest)
	}{
		{name: "wrong repair operation", mutate: func(r *network.MembershipRepairCompletionRequest) { r.RepairOperationID = "other-repair" }},
		{name: "wrong authorization binding", mutate: func(r *network.MembershipRepairCompletionRequest) { r.RepairBindingDigest = strings.Repeat("3", 64) }},
		{name: "wrong scope digest", mutate: func(r *network.MembershipRepairCompletionRequest) { r.ScopeDigest = strings.Repeat("2", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			badRequest := request
			test.mutate(&badRequest)
			if test.name != "wrong scope digest" {
				badRequest.ScopeDigest = MembershipRepairCompletionScopeDigest(badRequest)
			}
			badDigest, digestErr := network.MembershipRepairCompletionRequestDigest(badRequest)
			if digestErr != nil {
				t.Fatal(digestErr)
			}
			badResponse := validResponse
			badResponse.ScopeDigest = badRequest.ScopeDigest
			badResponse.RequestDigest = badDigest
			badAttempt := MembershipRepairCompletion{Request: badRequest, Accepted: &badResponse}
			if membershipRepairCompletionAccepted(&MembershipRepairRecord{Authorization: authorization, Progress: MembershipRepairProgress{Completion: &badAttempt, Phases: ledger.MembershipRepair.Progress.Phases, Candidates: ledger.MembershipRepair.Progress.Candidates, Results: ledger.MembershipRepair.Progress.Results}}) {
				t.Fatal("completion accepted with mismatched repair binding or scope")
			}
		})
	}
	if !membershipRepairCompletionAccepted(ledger.MembershipRepair) {
		t.Fatalf("completion evidence failed retained-scope validation: phase=%+v candidate=%+v result=%+v", add, ledger.MembershipRepair.Progress.Candidates[0], phaseResult)
	}
	if err := ledger.AdvanceMembershipRepair(authorization.OperationID, ledger.MembershipRepair.Progress.Revision, MembershipRepairComplete); err != nil {
		t.Fatalf("exact retained typed completion result did not permit Complete: %v", err)
	}
	if _, err := ledger.ReadMembershipRepair(oldID); err != ErrNotFound {
		t.Fatalf("completed repair remained discoverable as active: %v", err)
	}
	if len(ledger.MembershipRepair.Progress.Candidates) != 1 || len(ledger.MembershipRepair.Progress.Phases) != 2 {
		t.Fatalf("repair history changed unexpectedly: %+v", ledger.MembershipRepair.Progress)
	}

	secondMembership := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{otherMember, newMember}}
	second := MembershipRepairAuthorization{
		OperationID: "logical-repair-2", RequestHash: strings.Repeat("f", 64), AuthorizationHash: strings.Repeat("1", 64),
		RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger", SourceClusterID: "source",
		MembershipHash: canonicalJSONHash(secondMembership.Members), OldIdentity: otherID, Membership: secondMembership,
	}
	if err := ledger.InstallMembershipRepair(second); err != nil {
		t.Fatalf("completed history prevented a second repair: %v", err)
	}
	retained, ok := ledger.MembershipRepairHistory[authorization.OperationID]
	if !ok || retained.Progress.Stage != MembershipRepairComplete || !reflect.DeepEqual(retained.Authorization, authorization) {
		t.Fatalf("first operation was not retained after second install: %+v", retained)
	}
	secondView, err := ledger.ReadMembershipRepair(otherID)
	if err != nil || secondView.Authorization.OperationID != second.OperationID {
		t.Fatalf("second active operation view=%+v err=%v", secondView, err)
	}
	if err := ledger.CancelMembershipRepair(second.OperationID, secondView.Progress.Revision); err != nil {
		t.Fatalf("unstarted operation could not be cancelled: %v", err)
	}
	if _, err := ledger.ReadMembershipRepair(otherID); err != ErrNotFound {
		t.Fatalf("cancelled operation remained active: %v", err)
	}
}

func TestMembershipRepairOwnInitStatusBeginAndOutcomeRetainExactCandidate(t *testing.T) {
	old := Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "old-pod", RunID: "old-run", WALIdentity: "old-wal"}
	oldMember := quepaxa.Member{ID: "rhiza-0", URL: "http://old", PeerURL: "quic://old", LogURL: "http://old-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("old"))), WALIdentity: "old-wal"}
	otherMember := quepaxa.Member{ID: "rhiza-1", URL: "http://other", PeerURL: "quic://other", LogURL: "http://other-log", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("other"))), WALIdentity: "other-wal"}
	cluster := quepaxa.Cluster{ConfigID: 7, Members: []quepaxa.Member{oldMember, otherMember}}
	ledger := &Ledger{SourceClusterID: "source"}
	registered := old
	registered.WALIdentity = ""
	if err := ledger.Register(registered); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Bind(old); err != nil {
		t.Fatal(err)
	}
	abort := quepaxa.Slot(0)
	authorization := MembershipRepairAuthorization{OperationID: "repair", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64), RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger", SourceClusterID: "source", MembershipHash: canonicalJSONHash(cluster.Members), ExpectedAbortSlot: &abort, BootstrapMembershipHash: strings.Repeat("c", 64), ProfileHash: strings.Repeat("d", 64), StoreIdentityHash: strings.Repeat("e", 64), Durability: "async", OldIdentity: old, Membership: cluster}
	if err := ledger.InstallMembershipRepair(authorization); err != nil {
		t.Fatal(err)
	}
	if status, err := ledger.BootstrapLearnerStatus("new-pod"); err != nil || status.Assignment != nil {
		t.Fatalf("unassigned Pod status=%+v err=%v", status, err)
	}
	nodeID, _ := ReplacementNodeID("new-pod")
	assignment := BootstrapAssignment{OperationID: "repair-pod-new", RequestHash: authorization.RequestHash, AuthorizationHash: authorization.AuthorizationHash, Purpose: BootstrapLearner, RootUID: authorization.RootUID, StatefulSetUID: authorization.StatefulSetUID, SourceClusterID: authorization.SourceClusterID, PodName: "rhiza-0", PodUID: "new-pod", NodeID: nodeID, CredentialName: "candidate-secret", CredentialUID: "candidate-secret-uid", MembershipHash: authorization.BootstrapMembershipHash, ProfileHash: authorization.ProfileHash, LedgerUID: authorization.LedgerUID, PublicMember: BootstrapPublicMember{ID: nodeID, URL: "http://new", PeerURL: "quic://new", PublicKey: hex.EncodeToString(make([]byte, 32))}, Durability: authorization.Durability, StoreIdentityHash: authorization.StoreIdentityHash}
	if err := ledger.AppendMembershipRepairCandidate(MembershipRepairCandidate{Assignment: assignment}); err != nil {
		t.Fatal(err)
	}
	status, err := ledger.BootstrapLearnerStatus("new-pod")
	if err != nil || status.Assignment == nil || *status.Assignment != assignment || len(status.Attempts) != 0 {
		t.Fatalf("own status=%+v err=%v", status, err)
	}
	scope := InitExecutionScope{PodName: assignment.PodName, PodUID: assignment.PodUID, ContainerName: "rhiza-prepare", ContainerID: "containerd://init", ImageID: "sha256:image", NodeName: "worker", NodeUID: "worker-uid"}
	// Exercise the transition against the fresh ledger snapshot that a CAS
	// retry would supply after closure wins the first write race.
	for _, closeScope := range []string{"generation", "node"} {
		t.Run("closed-"+closeScope+"-retry-snapshot", func(t *testing.T) {
			encoded, err := json.Marshal(ledger)
			if err != nil {
				t.Fatal(err)
			}
			var retry Ledger
			if err := json.Unmarshal(encoded, &retry); err != nil {
				t.Fatal(err)
			}
			if closeScope == "generation" {
				if err := retry.Close("generation-close", "generation-hash"); err != nil {
					t.Fatal(err)
				}
			} else if err := retry.CloseNodes("node-close", "node-hash", []string{assignment.NodeID}); err != nil {
				t.Fatal(err)
			}
			freshScope := scope
			freshScope.ContainerID = "containerd://retry-" + closeScope
			if _, err := retry.BeginMembershipRepairInit(assignment, "retry-"+closeScope, freshScope); !errors.Is(err, ErrClosed) {
				t.Fatalf("fresh CAS retry snapshot admitted init after closure: %v", err)
			}
		})
	}
	attempt, err := ledger.BeginMembershipRepairInit(assignment, "init-nonce", scope)
	if err != nil || attempt.Assignment != assignment || attempt.Scope != scope {
		t.Fatalf("begin=%+v err=%v", attempt, err)
	}
	if _, err := ledger.BeginMembershipRepairInit(assignment, "other-nonce", scope); !errors.Is(err, ErrPending) {
		t.Fatalf("new nonce reused the same init container: %v", err)
	}
	outcome := InitAttemptOutcome{State: InitAttemptMaterialized, LiveCleanup: InitLiveCleanup{Mode: InitModeRestore, BucketDrainSucceeded: true, CleanupSucceeded: true, NamespaceQuiescent: true}}
	if ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
		t.Fatal("unresolved repair init passed closure completion")
	}
	retainedHistory := *ledger.MembershipRepair
	historicalLedger := &Ledger{MembershipRepairHistory: map[string]MembershipRepairRecord{"repair": retainedHistory}}
	if historicalLedger.BootstrapInitComplete([]string{assignment.NodeID}) {
		t.Fatal("unresolved historical repair init was omitted from closure completion")
	}
	if err := ledger.CloseNodes("node-close", "node-hash", []string{assignment.NodeID}); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginMembershipRepairInit(assignment, "late-new-init", InitExecutionScope{PodName: assignment.PodName, PodUID: assignment.PodUID, ContainerName: "rhiza-prepare", ContainerID: "containerd://late", ImageID: "sha256:image", NodeName: "worker", NodeUID: "worker-uid"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("node-closed repair admitted a new init: %v", err)
	}
	for _, closeScope := range []string{"generation", "node"} {
		t.Run("closed-"+closeScope+"-existing-attempt-retry", func(t *testing.T) {
			encoded, err := json.Marshal(ledger)
			if err != nil {
				t.Fatal(err)
			}
			var retry Ledger
			if err := json.Unmarshal(encoded, &retry); err != nil {
				t.Fatal(err)
			}
			if closeScope == "generation" {
				if err := retry.Close("generation-close", "generation-hash"); err != nil {
					t.Fatal(err)
				}
			} else if err := retry.CloseNodes("node-close", "node-hash", []string{assignment.NodeID}); err != nil {
				t.Fatal(err)
			}
			if _, err := retry.BeginMembershipRepairInit(assignment, attempt.AttemptID, scope); !errors.Is(err, ErrClosed) {
				t.Fatalf("existing attempt retry was admitted after %s closure: %v", closeScope, err)
			}
		})
	}
	if err := ledger.RecordMembershipRepairInitOutcome(assignment, attempt.AttemptID, outcome); err != nil {
		t.Fatalf("exact late outcome was not accepted after closure: %v", err)
	}
	if ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
		t.Fatal("repair init outcome without terminal evidence passed closure completion")
	}
	terminal := InitTerminalEvidence{ContainerID: scope.ContainerID, ImageID: scope.ImageID, ExitCode: 0, Reason: "Completed"}
	if err := ledger.RecordMembershipRepairInitTerminal(assignment, attempt.AttemptID, terminal); err != nil {
		t.Fatalf("exact late terminal evidence was not accepted after closure: %v", err)
	}
	if !ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
		t.Fatal("exact outcome and terminal evidence did not settle closed repair init")
	}
	status, err = ledger.BootstrapLearnerStatus("new-pod")
	if err != nil || len(status.Attempts) != 1 || len(status.Attempts[0].Outcomes) != 1 || status.Attempts[0].Outcomes[0] != outcome {
		t.Fatalf("retained learner status=%+v err=%v", status, err)
	}
}
