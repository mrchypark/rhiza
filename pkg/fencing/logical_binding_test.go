package fencing

import (
	"strings"
	"testing"
)

func logicalTestLedger() Ledger {
	return Ledger{SourceClusterID: "source", Profile: LogicalWriterProfileV1, RequireMutationJournal: true, Runs: map[string]Run{}}
}

func logicalTestBinding(role StartupRole) LogicalWriterBindingV1 {
	b := LogicalWriterBindingV1{Version: LogicalWriterVersionV1, Profile: LogicalWriterProfileV1,
		SourceClusterID: "source", NodeID: "node-1", RunID: "run-1", Role: role}
	if role == StartupRoleVoterActive {
		b.ProducerUID = "producer-1"
		b.StoreScopeHash = strings.Repeat("a", 64)
	}
	return b
}

func TestLogicalWriterProfileIsExplicitAndRejectsMixedArtifacts(t *testing.T) {
	id := Identity{SourceClusterID: "source", NodeID: "node-1", RunID: "run-1"}
	b := logicalTestBinding(StartupRoleVoterActive)
	l := logicalTestLedger()
	if err := l.RegisterLogicalWithRole(id, b); err != nil {
		t.Fatal(err)
	}
	if err := l.ValidateProfile(); err != nil {
		t.Fatal(err)
	}

	legacy := Ledger{SourceClusterID: "source", Runs: map[string]Run{"run-1": {Identity: id, LogicalBinding: &b}}}
	if err := legacy.ValidateProfile(); err == nil {
		t.Fatal("legacy root silently selected logical profile")
	}
	legacy.RequireRuntimeBinding = false
	if err := legacy.ValidateProfile(); err == nil {
		t.Fatal("turning off the legacy runtime flag selected the logical profile")
	}
	if err := l.RegisterWithRole(id, StartupRoleVoterActive); err == nil {
		t.Fatal("legacy registration API wrote into the explicit logical profile")
	}
	physical := l
	run := physical.Runs["run-1"]
	run.Runtime = &RuntimeBinding{}
	physical.Runs["run-1"] = run
	if err := physical.ValidateProfile(); err == nil {
		t.Fatal("logical profile accepted mixed physical binding")
	}
	unknown := l
	unknown.Profile = "logical-writer-v2"
	if err := unknown.ValidateProfile(); err == nil {
		t.Fatal("unknown profile accepted")
	}
}

func TestLogicalWALBindIsOneTimeAndClosedGrantCannotBind(t *testing.T) {
	id := Identity{SourceClusterID: "source", NodeID: "node-1", RunID: "run-1"}
	b := logicalTestBinding(StartupRoleVoterActive)
	l := logicalTestLedger()
	if err := l.RegisterLogicalWithRole(id, b); err != nil {
		t.Fatal(err)
	}
	if err := l.ValidateProfile(); err != nil {
		t.Fatalf("registered logical profile is invalid: %v", err)
	}
	if err := l.BindLogicalMutationScope(id, b.StoreScopeHash, MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationStage(id, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = strings.Repeat("c", 64)
	if err := l.BindLogicalWAL(id); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalWAL(id); err != nil {
		t.Fatalf("exact retry failed: %v", err)
	}
	other := id
	other.WALIdentity = strings.Repeat("d", 64)
	if err := l.BindLogicalWAL(other); err == nil {
		t.Fatal("logical Run rebound to a second WAL")
	}

	closed := logicalTestLedger()
	if err := closed.RegisterLogicalWithRole(Identity{SourceClusterID: "source", NodeID: "node-1", RunID: "run-1"}, b); err != nil {
		t.Fatal(err)
	}
	closed.ClosedBy = "close-op"
	id.WALIdentity = strings.Repeat("e", 64)
	if err := closed.BindLogicalWAL(id); err != ErrClosed {
		t.Fatalf("closed generation WAL bind = %v, want ErrClosed", err)
	}
}

func TestLogicalProducerScopeMustBeBoundBeforeWAL(t *testing.T) {
	l := logicalTestLedger()
	id := Identity{SourceClusterID: "source", NodeID: "node-1", RunID: "run-1"}
	b := logicalTestBinding(StartupRoleVoterActive)
	b.ProducerUID, b.StoreScopeHash = "", ""
	if err := l.RegisterLogicalWithRole(id, b); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationProducer(id, "producer-1"); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationScope(id, strings.Repeat("a", 64), MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationStage(id, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = strings.Repeat("c", 64)
	if err := l.BindLogicalWAL(id); err != nil {
		t.Fatal(err)
	}
}

func TestLogicalRestartRequiresVerifiedClosureAndContinuesExactWAL(t *testing.T) {
	l := logicalTestLedger()
	oldID := Identity{SourceClusterID: "source", NodeID: "node-1", PodName: "rhiza-0", PodUID: "same-pod", RunID: "run-1"}
	oldBinding := logicalTestBinding(StartupRoleVoterActive)
	if err := l.RegisterLogicalWithRole(oldID, oldBinding); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationScope(oldID, oldBinding.StoreScopeHash, MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationStage(oldID, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	oldID.WALIdentity = strings.Repeat("c", 64)
	if err := l.BindLogicalWAL(oldID); err != nil {
		t.Fatal(err)
	}

	newID := oldID
	newID.RunID = "run-2"
	newID.WALIdentity = ""
	newBinding := logicalTestBinding(StartupRoleVoterActive)
	newBinding.RunID = newID.RunID
	newBinding.ProducerUID = "producer-2"
	newBinding.WALIdentity = oldID.WALIdentity
	newBinding.Recovery = LogicalRecoveryBindingV1{Version: LogicalRecoveryBindingVersionV1, Kind: LogicalRecoverySameWAL,
		ExpectedWALIdentity: oldID.WALIdentity}
	if err := l.RegisterLogicalWithRole(newID, newBinding); err == nil {
		t.Fatal("same-slot restart inferred closure from the existing run")
	}

	closure := LogicalRunClosureReference{Version: LogicalRunClosureReferenceVersion, Identity: oldID,
		Kind:        LogicalRunClosureBoundWAL,
		ProducerUID: oldBinding.ProducerUID, StoreScopeHash: oldBinding.StoreScopeHash,
		CloseOperationID: "close-run-1", CloseRequestHash: strings.Repeat("d", 64), ProducerStateDigest: strings.Repeat("e", 64),
		EvidenceName: "logical-closure-run-1", EvidenceUID: "closure-uid", EvidenceDigest: strings.Repeat("f", 64)}
	if err := l.RecordLogicalRunClosure(closure); err == nil {
		t.Fatal("logical closure was accepted without a canonical Run Stop")
	}
	run := l.Runs[oldID.RunID]
	run.Stop = &StopRequest{OperationID: closure.CloseOperationID, RequestHash: closure.CloseRequestHash, Identity: oldID}
	l.Runs[oldID.RunID] = run
	for _, test := range []struct {
		name   string
		change func(*LogicalRunClosureReference)
	}{
		{name: "operation ID differs", change: func(ref *LogicalRunClosureReference) { ref.CloseOperationID = "other-operation" }},
		{name: "request hash differs", change: func(ref *LogicalRunClosureReference) { ref.CloseRequestHash = strings.Repeat("a", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mismatched := closure
			test.change(&mismatched)
			if err := l.RecordLogicalRunClosure(mismatched); err == nil {
				t.Fatal("logical closure was accepted for a different Stop operation")
			}
		})
	}
	if err := l.RecordLogicalRunClosure(closure); err != nil {
		t.Fatal(err)
	}
	if err := l.RegisterLogicalWithRole(newID, newBinding); err != nil {
		t.Fatalf("verified same-WAL restart rejected: %v", err)
	}
	if err := l.BindLogicalMutationScope(newID, newBinding.StoreScopeHash, MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationStage(newID, strings.Repeat("1", 64)); err != nil {
		t.Fatal(err)
	}
	wrongWAL := newID
	wrongWAL.WALIdentity = strings.Repeat("a", 64)
	if err := l.BindLogicalWAL(wrongWAL); err == nil {
		t.Fatal("logical restart changed its authority-pinned WAL")
	}
	newID.WALIdentity = oldID.WALIdentity
	if err := l.BindLogicalWAL(newID); err != nil {
		t.Fatalf("logical restart could not bind the exact prior WAL: %v", err)
	}
}

func TestPendingLogicalLearnerNeverGetsProducer(t *testing.T) {
	l := logicalTestLedger()
	id := Identity{SourceClusterID: "source", NodeID: "node-1", PodName: "rhiza-1", PodUID: "pod-1", RunID: "run-1", AssignmentID: strings.Repeat("a", 64)}
	b := logicalTestBinding(StartupRoleLearnerPendingRemove)
	b.AssignmentID = id.AssignmentID
	b.WALIdentity = strings.Repeat("b", 64)
	b.Recovery = LogicalRecoveryBindingV1{Version: LogicalRecoveryBindingVersionV1, Kind: LogicalRecoveryReplacement,
		OperationID: "repair-1", AssignmentID: id.AssignmentID, ExpectedWALIdentity: b.WALIdentity,
		ReplacedNodeID: "old-node", ReplacedWALIdentity: strings.Repeat("c", 64), RemoveOperationID: "remove-1",
		AddOperationID: "add-1", TargetConfigID: 2, TargetMembershipHash: strings.Repeat("d", 64)}
	if err := l.RegisterLogicalWithRole(id, b); err != nil {
		t.Fatal(err)
	}
	id.WALIdentity = b.WALIdentity
	if err := l.BindLogicalWAL(id); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationProducer(id, "producer-1"); err == nil {
		t.Fatal("pending learner received mutation producer")
	}
	if run := l.Runs[id.RunID]; run.MutationProducerUID != "" || run.LogicalBinding.ProducerUID != "" {
		t.Fatal("pending learner producer state changed after rejected bind")
	}
}

func TestLogicalAssignedLearnerBindsOnlyExpectedWALAndPromotesWithTypedEvidence(t *testing.T) {
	l := logicalTestLedger()
	identity := Identity{SourceClusterID: "source", NodeID: "new-node", PodName: "rhiza-1", PodUID: "pod-new", RunID: "run-new", AssignmentID: strings.Repeat("1", 64)}
	expectedWAL := strings.Repeat("2", 64)
	recovery := LogicalRecoveryBindingV1{Version: LogicalRecoveryBindingVersionV1, Kind: LogicalRecoveryReplacement,
		OperationID: "repair-operation", AssignmentID: identity.AssignmentID, ExpectedWALIdentity: expectedWAL,
		ReplacedNodeID: "old-node", ReplacedWALIdentity: strings.Repeat("3", 64), RemoveOperationID: "remove-operation",
		AddOperationID: "add-operation", TargetConfigID: 2, TargetMembershipHash: strings.Repeat("4", 64)}
	pending := LogicalWriterBindingV1{Version: LogicalWriterVersionV1, Profile: LogicalWriterProfileV1,
		SourceClusterID: identity.SourceClusterID, NodeID: identity.NodeID, RunID: identity.RunID,
		AssignmentID: identity.AssignmentID, WALIdentity: expectedWAL, Role: StartupRoleLearnerPendingRemove, Recovery: recovery}
	if err := l.RegisterLogicalWithRole(identity, pending); err != nil {
		t.Fatal(err)
	}
	wrongWAL := identity
	wrongWAL.WALIdentity = strings.Repeat("5", 64)
	if err := l.BindLogicalWAL(wrongWAL); err == nil {
		t.Fatal("assigned learner bound a WAL other than its authority-pinned candidate")
	}
	identity.WALIdentity = expectedWAL
	if err := l.BindLogicalWAL(identity); err != nil {
		t.Fatal(err)
	}
	run := l.Runs[identity.RunID]
	if run.Runtime != nil || run.MutationProducerUID != "" || run.LogicalBinding.Role != StartupRoleLearnerPendingRemove {
		t.Fatalf("pending candidate received writer authority before promotion: %+v", run)
	}

	proof := LogicalPromotionEvidenceV1{Version: LogicalPromotionEvidenceVersionV1, OperationID: recovery.OperationID,
		AddOperationID: recovery.AddOperationID, Identity: identity, AssignmentID: identity.AssignmentID,
		ReplacedNodeID: recovery.ReplacedNodeID, ReplacedWALIdentity: recovery.ReplacedWALIdentity,
		ExpectedWALIdentity: expectedWAL, RemoveIndex: 9, AddIndex: 12, ActivationIndex: 14,
		TargetConfigID: recovery.TargetConfigID, TargetMembershipHash: recovery.TargetMembershipHash,
		RemoveEvidenceHash: strings.Repeat("6", 64), AddEvidenceHash: strings.Repeat("7", 64), ActivationHash: strings.Repeat("8", 64)}
	badProof := proof
	badProof.ActivationIndex = badProof.AddIndex - 1
	active := pending
	active.Role, active.ProducerUID, active.StoreScopeHash = StartupRoleVoterActive, "producer-new", strings.Repeat("9", 64)
	if err := l.PromoteLogicalLearner(identity, badProof, active, MutationExecutionProfile, strings.Repeat("a", 64)); err == nil {
		t.Fatal("logical learner promoted without ordered canonical REMOVE/ADD/activation evidence")
	}
	if err := l.PromoteLogicalLearner(identity, proof, active, MutationExecutionProfile, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	run = l.Runs[identity.RunID]
	if run.Runtime != nil || run.Role != StartupRoleVoterActive || run.Promotion == nil || run.Promotion.OperationID != recovery.AddOperationID ||
		run.LogicalBinding.Recovery.PromotionEvidenceHash != proof.Digest() || run.MutationProducerUID != active.ProducerUID {
		t.Fatalf("logical promotion did not persist exact activation binding: %+v", run)
	}
	if err := l.ValidateProfile(); err != nil {
		t.Fatalf("promoted logical profile is invalid: %v", err)
	}
	if err := l.PromoteLogicalLearner(identity, proof, active, MutationExecutionProfile, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("exact promotion retry: %v", err)
	}
}

func TestLogicalPromotionReceiptMustPreserveAssignedRecovery(t *testing.T) {
	assignment := strings.Repeat("1", 64)
	wal := strings.Repeat("2", 64)
	pending := LogicalWriterBindingV1{Version: LogicalWriterVersionV1, Profile: LogicalWriterProfileV1,
		SourceClusterID: "source", NodeID: "new-node", RunID: "run-new", AssignmentID: assignment,
		WALIdentity: wal, Role: StartupRoleLearnerPendingRemove,
		Recovery: LogicalRecoveryBindingV1{Version: LogicalRecoveryBindingVersionV1, Kind: LogicalRecoveryReplacement,
			OperationID: "repair-operation", AssignmentID: assignment, ExpectedWALIdentity: wal,
			ReplacedNodeID: "old-node", ReplacedWALIdentity: strings.Repeat("3", 64), RemoveOperationID: "remove-operation",
			AddOperationID: "add-operation", TargetConfigID: 2, TargetMembershipHash: strings.Repeat("4", 64)}}
	active := pending
	active.Role = StartupRoleVoterActive
	active.ProducerUID = "producer-new"
	active.StoreScopeHash = strings.Repeat("5", 64)
	active.Recovery.PromotionEvidenceHash = strings.Repeat("6", 64)
	if !logicalPromotionBindingMatches(pending, active) {
		t.Fatal("valid activation receipt did not preserve assigned recovery")
	}

	for name, mutate := range map[string]func(*LogicalWriterBindingV1){
		"assignment":                func(b *LogicalWriterBindingV1) { b.AssignmentID = strings.Repeat("7", 64) },
		"remove operation":          func(b *LogicalWriterBindingV1) { b.Recovery.RemoveOperationID = "other-remove" },
		"target membership":         func(b *LogicalWriterBindingV1) { b.Recovery.TargetMembershipHash = strings.Repeat("8", 64) },
		"missing activation digest": func(b *LogicalWriterBindingV1) { b.Recovery.PromotionEvidenceHash = "" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := active
			mutate(&bad)
			if logicalPromotionBindingMatches(pending, bad) {
				t.Fatal("accepted promotion receipt with changed assignment or recovery proof")
			}
		})
	}
}

func TestLogicalUnboundRegistrationSupersessionRequiresZeroEffectClosure(t *testing.T) {
	l := logicalTestLedger()
	oldID := Identity{SourceClusterID: "source", NodeID: "node-1", PodName: "rhiza-0", PodUID: "pod-old", RunID: "run-old"}
	oldBinding := logicalTestBinding(StartupRoleVoterActive)
	oldBinding.RunID = oldID.RunID
	if err := l.RegisterLogicalWithRole(oldID, oldBinding); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationScope(oldID, oldBinding.StoreScopeHash, MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationStage(oldID, strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	oldRun := l.Runs[oldID.RunID]
	stop := StopRequest{OperationID: "close-unbound", RequestHash: strings.Repeat("c", 64), Identity: oldID}
	oldRun.Stop = &stop
	l.Runs[oldID.RunID] = oldRun
	closure := LogicalRunClosureReference{Version: LogicalRunClosureReferenceVersion, Kind: LogicalRunClosureUnboundNoWork,
		Identity: oldID, ProducerUID: oldBinding.ProducerUID, StoreScopeHash: oldBinding.StoreScopeHash,
		CloseOperationID: stop.OperationID, CloseRequestHash: stop.RequestHash, ProducerStateDigest: strings.Repeat("d", 64),
		EvidenceName: "zero-effect-closure", EvidenceUID: "closure-uid", EvidenceDigest: strings.Repeat("e", 64)}
	bad := closure
	bad.ClosedThrough = 1
	if err := l.RecordLogicalRunClosure(bad); err == nil {
		t.Fatal("unbound supersession accepted closure with admitted effects")
	}
	if err := l.RecordLogicalRunClosure(closure); err != nil {
		t.Fatal(err)
	}
	newID := oldID
	newID.PodUID, newID.RunID = "pod-new", "run-new"
	newBinding := oldBinding
	newBinding.RunID = newID.RunID
	newBinding.ProducerUID = "producer-new"
	if err := l.RegisterLogicalWithRole(newID, newBinding); err != nil {
		t.Fatalf("zero-effect unbound logical registration could not be superseded: %v", err)
	}
	if err := l.BindLogicalMutationScope(newID, newBinding.StoreScopeHash, MutationExecutionProfile); err != nil {
		t.Fatal(err)
	}
	if err := l.BindLogicalMutationStage(newID, strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
}

func TestLogicalBindReceiptHasNoFakeRuntimeAndComparesByValue(t *testing.T) {
	id := Identity{SourceClusterID: "source", NodeID: "node-1", RunID: "run-1", WALIdentity: strings.Repeat("c", 64)}
	b := logicalTestBinding(StartupRoleVoterActive)
	b.WALIdentity = id.WALIdentity
	receipt := BindReceipt{Identity: id, Role: StartupRoleVoterActive, AuthorityProfile: LogicalWriterProfileV1, LogicalBinding: &b,
		Producer: &ProducerBinding{ContractVersion: MutationIntentVersion, Profile: LogicalWriterProfileV1, LogicalBinding: &b,
			ProducerUID: b.ProducerUID, StoreScopeHash: b.StoreScopeHash, ExecutionProfile: MutationExecutionProfile}}
	if err := receipt.Validate(id); err != nil {
		t.Fatal(err)
	}
	clone := receipt.clone()
	if !receipt.SameBinding(clone) {
		t.Fatal("equal logical bindings differ after clone")
	}
	clone.LogicalBinding.RunID = "other"
	if receipt.SameBinding(clone) {
		t.Fatal("logical binding compared by pointer identity or ignored value change")
	}
	bad := receipt
	bad.Producer = &ProducerBinding{ContractVersion: MutationIntentVersion, Profile: LogicalWriterProfileV1,
		LogicalBinding: &b, Runtime: RuntimeBinding{RuntimeID: "fake-runtime"}, ProducerUID: b.ProducerUID,
		StoreScopeHash: b.StoreScopeHash, ExecutionProfile: MutationExecutionProfile}
	if err := bad.Validate(id); err == nil {
		t.Fatal("logical receipt accepted fake physical RuntimeBinding")
	}
	bad = receipt
	bad.Role = ""
	if err := bad.Validate(id); err == nil {
		t.Fatal("logical receipt accepted an omitted authority role")
	}
}

func TestLogicalStartupGrantRequiresExplicitMatchingProfile(t *testing.T) {
	request := testIdentity()
	request.NodeID = "rhiza-0"
	request.PodName = "rhiza-0"
	b := LogicalWriterBindingV1{Version: LogicalWriterVersionV1, Profile: LogicalWriterProfileV1,
		SourceClusterID: request.SourceClusterID, NodeID: request.NodeID, RunID: request.RunID,
		Role: StartupRoleVoterActive, ProducerUID: "producer-1", StoreScopeHash: strings.Repeat("a", 64)}
	grant := StartupGrant{Identity: request, Role: StartupRoleVoterActive, AuthorityProfile: LogicalWriterProfileV1, LogicalBinding: &b}
	if err := grant.Validate(request); err != nil {
		t.Fatal(err)
	}
	walPinned := grant
	pinnedBinding := *grant.LogicalBinding
	pinnedBinding.WALIdentity = strings.Repeat("b", 64)
	walPinned.LogicalBinding = &pinnedBinding
	if err := walPinned.Validate(request); err != nil {
		t.Fatalf("authority-pinned logical candidate WAL rejected: %v", err)
	}
	for name, mutate := range map[string]func(*StartupGrant){
		"missing discriminator":    func(g *StartupGrant) { g.AuthorityProfile = "" },
		"different binding source": func(g *StartupGrant) { g.LogicalBinding.SourceClusterID = "other" },
		"unknown discriminator":    func(g *StartupGrant) { g.AuthorityProfile = "logical-writer-v2" },
		"active without scope":     func(g *StartupGrant) { g.LogicalBinding.StoreScopeHash = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := grant
			copy := *grant.LogicalBinding
			candidate.LogicalBinding = &copy
			mutate(&candidate)
			if err := candidate.Validate(request); err == nil {
				t.Fatal("invalid logical startup grant accepted")
			}
		})
	}
}

func TestLogicalMutationJournalUsesNoRuntimeBinding(t *testing.T) {
	b := logicalTestBinding(StartupRoleVoterActive)
	producer, err := NewLogicalMutationProducer(b)
	if err != nil {
		t.Fatal(err)
	}
	id := Identity{SourceClusterID: b.SourceClusterID, NodeID: b.NodeID, RunID: b.RunID, WALIdentity: strings.Repeat("c", 64)}
	if err := producer.Activate(id); err != nil {
		t.Fatal(err)
	}
	if producer.Runtime != (RuntimeBinding{}) || producer.LogicalBinding == nil || producer.LogicalBinding.WALIdentity != id.WALIdentity {
		t.Fatal("logical producer fabricated runtime evidence or lost WAL binding")
	}
	intent := journalUpload(MutationRequestID(b.ProducerUID, 1))
	if err := producer.Admit(1, intent); err != nil {
		t.Fatal(err)
	}
	if err := producer.Prepare(1, journalPreparation(7)); err != nil {
		t.Fatal(err)
	}
	if _, err := producer.PreparationHash(1); err != nil {
		t.Fatal(err)
	}
	reloaded := reloadProducer(t, producer)
	if reloaded.Runtime != (RuntimeBinding{}) || reloaded.LogicalBinding == nil || reloaded.Profile != LogicalWriterProfileV1 {
		t.Fatal("logical journal did not round-trip its explicit profile")
	}
	bad := *reloaded
	bad.Runtime.RuntimeID = "fabricated"
	if err := bad.Validate(); err == nil {
		t.Fatal("logical journal accepted physical RuntimeBinding")
	}
	pending := b
	pending.Role = StartupRoleLearnerPendingRemove
	pending.ProducerUID, pending.StoreScopeHash = "", ""
	if _, err := NewLogicalMutationProducer(pending); err == nil {
		t.Fatal("pending learner received a mutation journal producer")
	}
}
