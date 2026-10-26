package fencing

import (
	"fmt"
)

const (
	// LogicalWriterProfileV1 is an explicit root/binding profile. It is never
	// inferred by turning off RequireRuntimeBinding on an existing root.
	LogicalWriterProfileV1 = "logical-writer-v1"
	LogicalWriterVersionV1 = 1
	// RuntimeWriterProfileV1 names the existing physical-runtime contract for
	// newly created roots. Empty profile remains the legacy wire/storage form.
	RuntimeWriterProfileV1 = "runtime-bound-v1"
)

type LogicalRecoveryKind string

const (
	LogicalRecoverySameWAL     LogicalRecoveryKind = "same-wal-continuation"
	LogicalRecoveryReplacement LogicalRecoveryKind = "assigned-replacement"
)

// LogicalRecoveryBindingV1 pins an authority-selected same-WAL continuation
// or assigned replacement without asserting physical runtime provenance.
type LogicalRecoveryBindingV1 struct {
	Version               int                 `json:"version"`
	Kind                  LogicalRecoveryKind `json:"kind"`
	OperationID           string              `json:"operationID,omitempty"`
	AssignmentID          string              `json:"assignmentID,omitempty"`
	ExpectedWALIdentity   string              `json:"expectedWALIdentity"`
	ReplacedNodeID        string              `json:"replacedNodeID,omitempty"`
	ReplacedWALIdentity   string              `json:"replacedWALIdentity,omitempty"`
	RemoveOperationID     string              `json:"removeOperationID,omitempty"`
	AddOperationID        string              `json:"addOperationID,omitempty"`
	TargetConfigID        uint                `json:"targetConfigID,omitempty"`
	TargetMembershipHash  string              `json:"targetMembershipHash,omitempty"`
	PromotionEvidenceHash string              `json:"promotionEvidenceHash,omitempty"`
}

const LogicalRecoveryBindingVersionV1 = 1

func (r LogicalRecoveryBindingV1) empty() bool { return r == (LogicalRecoveryBindingV1{}) }

func (r LogicalRecoveryBindingV1) Validate(candidateNodeID, assignmentID string) error {
	if r.empty() {
		return nil
	}
	if r.Version != LogicalRecoveryBindingVersionV1 || !mutationDigestValid(r.ExpectedWALIdentity) {
		return fmt.Errorf("fencing: logical recovery binding is incomplete or unsupported")
	}
	switch r.Kind {
	case LogicalRecoverySameWAL:
		if assignmentID != "" || r.AssignmentID != "" || r.OperationID != "" || r.ReplacedNodeID != "" ||
			r.ReplacedWALIdentity != "" || r.RemoveOperationID != "" || r.AddOperationID != "" ||
			r.TargetConfigID != 0 || r.TargetMembershipHash != "" || r.PromotionEvidenceHash != "" {
			return fmt.Errorf("fencing: same-WAL recovery carries replacement authority")
		}
	case LogicalRecoveryReplacement:
		if !mutationDigestValid(assignmentID) || r.AssignmentID != assignmentID || r.OperationID == "" || len(r.OperationID) > 256 ||
			r.ReplacedNodeID == "" || r.ReplacedNodeID == candidateNodeID || !mutationDigestValid(r.ReplacedWALIdentity) ||
			r.ReplacedWALIdentity == r.ExpectedWALIdentity || r.RemoveOperationID == "" || len(r.RemoveOperationID) > 256 ||
			r.AddOperationID == "" || len(r.AddOperationID) > 256 || r.TargetConfigID == 0 || !mutationDigestValid(r.TargetMembershipHash) {
			return fmt.Errorf("fencing: assigned replacement recovery binding is incomplete")
		}
		if r.PromotionEvidenceHash != "" && !mutationDigestValid(r.PromotionEvidenceHash) {
			return fmt.Errorf("fencing: assigned replacement promotion digest is invalid")
		}
	default:
		return fmt.Errorf("fencing: logical recovery kind is unsupported")
	}
	return nil
}

// LogicalPromotionEvidenceV1 summarizes the canonical REMOVE/ADD/activation
// proof already verified by the authority's storage-backed membership reader.
// It is not self-authenticating; only that trusted consumer may submit it.
type LogicalPromotionEvidenceV1 struct {
	Version              int      `json:"version"`
	OperationID          string   `json:"operationID"`
	AddOperationID       string   `json:"addOperationID"`
	Identity             Identity `json:"identity"`
	AssignmentID         string   `json:"assignmentID"`
	ReplacedNodeID       string   `json:"replacedNodeID"`
	ReplacedWALIdentity  string   `json:"replacedWALIdentity"`
	ExpectedWALIdentity  string   `json:"expectedWALIdentity"`
	RemoveIndex          uint64   `json:"removeIndex"`
	AddIndex             uint64   `json:"addIndex"`
	ActivationIndex      uint64   `json:"activationIndex"`
	TargetConfigID       uint     `json:"targetConfigID"`
	TargetMembershipHash string   `json:"targetMembershipHash"`
	RemoveEvidenceHash   string   `json:"removeEvidenceHash"`
	AddEvidenceHash      string   `json:"addEvidenceHash"`
	ActivationHash       string   `json:"activationHash"`
}

const LogicalPromotionEvidenceVersionV1 = 1

func (p LogicalPromotionEvidenceV1) ValidateAgainst(identity Identity, recovery LogicalRecoveryBindingV1) error {
	if p.Version != LogicalPromotionEvidenceVersionV1 || p.Identity != identity || identity.WALIdentity == "" ||
		recovery.Kind != LogicalRecoveryReplacement || recovery.Validate(identity.NodeID, identity.AssignmentID) != nil ||
		p.OperationID != recovery.OperationID || p.AddOperationID != recovery.AddOperationID ||
		p.AssignmentID != recovery.AssignmentID || p.ReplacedNodeID != recovery.ReplacedNodeID ||
		p.ReplacedWALIdentity != recovery.ReplacedWALIdentity || p.ExpectedWALIdentity != recovery.ExpectedWALIdentity ||
		p.ExpectedWALIdentity != identity.WALIdentity || p.TargetConfigID != recovery.TargetConfigID ||
		p.TargetMembershipHash != recovery.TargetMembershipHash || p.RemoveIndex == 0 || p.AddIndex <= p.RemoveIndex ||
		p.ActivationIndex < p.AddIndex || !mutationDigestValid(p.RemoveEvidenceHash) ||
		!mutationDigestValid(p.AddEvidenceHash) || !mutationDigestValid(p.ActivationHash) {
		return fmt.Errorf("fencing: logical promotion evidence differs from the authority-pinned assignment")
	}
	return nil
}

func (p LogicalPromotionEvidenceV1) Digest() string {
	return hashJSONValue([]any{"rhiza-logical-promotion-evidence-v1", p})
}

// LogicalWriterBindingV1 binds one authority-issued logical writer grant to
// one source generation, voter, WAL run, mutation producer and storage scope.
// WALIdentity is empty only between registration and its one-time WAL bind.
// Pod/runtime/host provenance is deliberately not part of this identity.
type LogicalWriterBindingV1 struct {
	Version         int                      `json:"version"`
	Profile         string                   `json:"profile"`
	SourceClusterID string                   `json:"sourceClusterID"`
	NodeID          string                   `json:"nodeID"`
	RunID           string                   `json:"runID"`
	AssignmentID    string                   `json:"assignmentID,omitempty"`
	WALIdentity     string                   `json:"walIdentity,omitempty"`
	ProducerUID     string                   `json:"producerUID,omitempty"`
	StoreScopeHash  string                   `json:"storeScopeHash,omitempty"`
	Role            StartupRole              `json:"role"`
	Recovery        LogicalRecoveryBindingV1 `json:"recovery,omitempty"`
}

// LogicalRunClosureReference is an authority-persisted pointer to the exact
// verified closure record for one logical Run. Only the trusted storage-backed
// closure consumer may call RecordLogicalRunClosure; a stop request or process
// exit is not a substitute for this reference.
type LogicalRunClosureReference struct {
	Version             int      `json:"version"`
	Kind                string   `json:"kind"`
	Identity            Identity `json:"identity"`
	ProducerUID         string   `json:"producerUID"`
	StoreScopeHash      string   `json:"storeScopeHash"`
	CloseOperationID    string   `json:"closeOperationID"`
	CloseRequestHash    string   `json:"closeRequestHash"`
	ClosedThrough       uint64   `json:"closedThrough"`
	ProducerStateDigest string   `json:"producerStateDigest"`
	EvidenceName        string   `json:"evidenceName"`
	EvidenceUID         string   `json:"evidenceUID"`
	EvidenceDigest      string   `json:"evidenceDigest"`
}

const LogicalRunClosureReferenceVersion = 1

const (
	LogicalRunClosureBoundWAL      = "bound-wal"
	LogicalRunClosureUnboundNoWork = "unbound-no-work"
)

func validateLogicalRunClosure(run Run, ref LogicalRunClosureReference) error {
	if run.LogicalBinding == nil || run.LogicalBinding.Validate() != nil || runRole(run) != StartupRoleVoterActive ||
		run.Stop == nil || run.Stop.Identity != run.Identity || run.Stop.OperationID == "" || !mutationDigestValid(run.Stop.RequestHash) ||
		run.Stop.OperationID != ref.CloseOperationID || run.Stop.RequestHash != ref.CloseRequestHash ||
		ref.Version != LogicalRunClosureReferenceVersion || ref.Identity != run.Identity || ref.ProducerUID == "" ||
		ref.ProducerUID != run.MutationProducerUID || ref.StoreScopeHash != run.MutationScopeHash ||
		!mutationDigestValid(ref.StoreScopeHash) || ref.CloseOperationID == "" || len(ref.CloseOperationID) > 128 ||
		!mutationDigestValid(ref.CloseRequestHash) || !mutationDigestValid(ref.ProducerStateDigest) ||
		ref.EvidenceName == "" || len(ref.EvidenceName) > 512 || ref.EvidenceUID == "" || len(ref.EvidenceUID) > 128 ||
		!mutationDigestValid(ref.EvidenceDigest) || run.MutationProfile == "" || !MutationProfileSupported(run.MutationProfile) {
		return fmt.Errorf("fencing: logical Run closure reference differs from its bound producer")
	}
	switch ref.Kind {
	case LogicalRunClosureBoundWAL:
		if run.Identity.WALIdentity == "" || run.LogicalBinding.ValidateBound() != nil {
			return fmt.Errorf("fencing: bound logical Run closure lacks a WAL-bound producer")
		}
	case LogicalRunClosureUnboundNoWork:
		if run.Identity.WALIdentity != "" || run.LogicalBinding.WALIdentity != "" || ref.ClosedThrough != 0 ||
			run.MutationProducerUID == "" || run.MutationScopeHash == "" || run.MutationProfile == "" ||
			run.MutationStageIncarnation == "" {
			return fmt.Errorf("fencing: unbound logical closure is not a zero-effect scoped registration")
		}
	default:
		return fmt.Errorf("fencing: logical Run closure kind is unsupported")
	}
	return nil
}

// RecordLogicalRunClosure persists the verified immutable closure reference on
// its exact Run. The caller must have loaded and validated the canonical closed
// MutationProducer and the referenced completion evidence from the authoritative
// store under the same source root UID before making this CAS transition.
func (l *Ledger) RecordLogicalRunClosure(ref LogicalRunClosureReference) error {
	if l == nil || l.Profile != LogicalWriterProfileV1 || l.ValidateProfile() != nil {
		return fmt.Errorf("fencing: logical Run closure requires the explicit logical profile")
	}
	run, ok := l.Runs[ref.Identity.RunID]
	if !ok || run.Identity != ref.Identity {
		return fmt.Errorf("fencing: logical Run closure is outside its exact registered Run")
	}
	if err := validateLogicalRunClosure(run, ref); err != nil {
		return err
	}
	if run.LogicalClosure != nil {
		if *run.LogicalClosure == ref {
			return nil
		}
		return fmt.Errorf("fencing: logical Run closure reference is immutable")
	}
	run.LogicalClosure = &ref
	l.Runs[ref.Identity.RunID] = run
	return nil
}

func (b LogicalWriterBindingV1) Validate() error {
	if b.Version != LogicalWriterVersionV1 || b.Profile != LogicalWriterProfileV1 || b.SourceClusterID == "" || b.NodeID == "" || b.RunID == "" || !b.Role.Valid() {
		return fmt.Errorf("fencing: logical writer binding is incomplete or unsupported")
	}
	if b.WALIdentity != "" && !mutationDigestValid(b.WALIdentity) {
		return fmt.Errorf("fencing: logical writer WAL identity is invalid")
	}
	if err := b.Recovery.Validate(b.NodeID, b.AssignmentID); err != nil {
		return err
	}
	if b.AssignmentID != "" && b.Recovery.Kind != LogicalRecoveryReplacement || b.AssignmentID == "" && b.Recovery.Kind == LogicalRecoveryReplacement {
		return fmt.Errorf("fencing: logical assignment and recovery binding differ")
	}
	if b.Recovery.Kind == LogicalRecoverySameWAL && b.Role != StartupRoleVoterActive {
		return fmt.Errorf("fencing: same-WAL recovery requires an active voter role")
	}
	if b.Role == StartupRoleLearnerPendingRemove && (b.AssignmentID == "" || b.Recovery.Kind != LogicalRecoveryReplacement) {
		return fmt.Errorf("fencing: pending logical learner requires an authority-assigned replacement binding")
	}
	if b.Recovery.Kind != "" && b.WALIdentity != "" && b.WALIdentity != b.Recovery.ExpectedWALIdentity {
		return fmt.Errorf("fencing: logical recovery WAL differs from its expected identity")
	}
	if b.Role == StartupRoleLearnerPendingRemove {
		if b.ProducerUID != "" || b.StoreScopeHash != "" {
			return fmt.Errorf("fencing: pending logical learner cannot carry source mutation authority")
		}
		return nil
	}
	if b.ProducerUID == "" && b.StoreScopeHash == "" {
		return nil
	}
	if b.ProducerUID == "" || len(b.ProducerUID) > 128 || (b.StoreScopeHash != "" && !mutationDigestValid(b.StoreScopeHash)) {
		return fmt.Errorf("fencing: active logical writer lacks exact producer scope")
	}
	return nil
}

func (b LogicalWriterBindingV1) ValidateBound() error {
	if err := b.Validate(); err != nil {
		return err
	}
	if b.WALIdentity == "" {
		return fmt.Errorf("fencing: logical writer WAL is not bound")
	}
	if b.Role == StartupRoleVoterActive && (b.ProducerUID == "" || !mutationDigestValid(b.StoreScopeHash)) {
		return fmt.Errorf("fencing: logical active writer lacks producer scope")
	}
	return nil
}

func (b LogicalWriterBindingV1) MatchesIdentity(id Identity) bool {
	return b.SourceClusterID == id.SourceClusterID && b.NodeID == id.NodeID && b.RunID == id.RunID &&
		b.AssignmentID == id.AssignmentID && b.WALIdentity == id.WALIdentity
}

func LogicalWriterBindingDigest(binding LogicalWriterBindingV1) string {
	return hashJSONValue([]any{"rhiza-logical-writer-binding-v1", binding})
}

// ValidateProfile enforces the explicit root discriminator. Missing profile
// is accepted only as the pre-profile legacy format; it never selects the
// logical contract. The new logical profile requires the mutation journal and
// cannot coexist with physical RuntimeBindings or runtime receipts.
func (l Ledger) ValidateProfile() error {
	switch l.Profile {
	case "":
		for _, run := range l.Runs {
			if run.LogicalBinding != nil {
				return fmt.Errorf("fencing: legacy root contains a logical writer Run")
			}
		}
		return nil
	case RuntimeWriterProfileV1:
		if !l.RequireRuntimeBinding {
			return fmt.Errorf("fencing: runtime-bound profile lacks its runtime requirement")
		}
		for _, run := range l.Runs {
			if run.LogicalBinding != nil || run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil {
				return fmt.Errorf("fencing: runtime-bound root contains an invalid physical writer Run")
			}
		}
		return nil
	case LogicalWriterProfileV1:
		if !l.RequireMutationJournal || l.SourceClusterID == "" || len(l.EngineRepairs) != 0 || len(l.PendingEngineRepairs) != 0 {
			return fmt.Errorf("fencing: logical writer root requires a source and mutation journal")
		}
		for _, run := range l.Runs {
			if validateLogicalRunBinding(run) != nil || run.LogicalBinding.SourceClusterID != l.SourceClusterID {
				return fmt.Errorf("fencing: logical writer Run profile or binding differs")
			}
			if run.Drain != nil || run.Exited || run.NeverBoundExit != nil || run.IndependentTerminal != nil || run.InitialRegistration != nil || run.EngineRepairProvenance != nil {
				return fmt.Errorf("fencing: physical terminal or enrollment artifact cannot be reinterpreted as logical evidence")
			}
			if run.RegistrationExpectedWAL != "" || run.RegistrationSupersededBy != "" ||
				(run.MutationScopeHash == "" && run.MutationProfile != "") ||
				(run.MutationProfile != "" && !MutationProfileSupported(run.MutationProfile)) ||
				(run.MutationStageIncarnation != "" && (!mutationDigestValid(run.MutationStageIncarnation) || run.MutationProducerUID == "" || run.MutationScopeHash == "" || run.MutationProfile == "" || runRole(run) != StartupRoleVoterActive)) {
				return fmt.Errorf("fencing: logical Run mutation scope or stage differs")
			}
			if run.LogicalClosure != nil && validateLogicalRunClosure(run, *run.LogicalClosure) != nil {
				return fmt.Errorf("fencing: logical Run closure reference is malformed")
			}
			if run.LogicalBinding.AssignmentID != "" {
				if run.LogicalBinding.Recovery.Kind != LogicalRecoveryReplacement ||
					(runRole(run) == StartupRoleVoterActive && (run.Promotion == nil || run.Promotion.OperationID != run.LogicalBinding.Recovery.AddOperationID || !mutationDigestValid(run.Promotion.EvidenceHash))) ||
					(runRole(run) == StartupRoleLearnerPendingRemove && run.Promotion != nil) {
					return fmt.Errorf("fencing: assigned logical Run lacks its exact promotion evidence")
				}
				if runRole(run) == StartupRoleVoterActive && run.Promotion.EvidenceHash != run.LogicalBinding.Recovery.PromotionEvidenceHash {
					return fmt.Errorf("fencing: logical promotion digest differs from its recovery binding")
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("fencing: unknown root writer profile")
	}
}

// RegisterLogicalWithRole is the explicit logical-profile registration path.
// Role and binding must already have been selected by the authenticated
// authority; caller-supplied annotations are not an authority source.
func (l *Ledger) RegisterLogicalWithRole(identity Identity, binding LogicalWriterBindingV1) error {
	if l == nil || l.Profile != LogicalWriterProfileV1 || !l.RequireMutationJournal || l.ValidateProfile() != nil ||
		identity.SourceClusterID != l.SourceClusterID || identity.NodeID == "" || identity.RunID == "" || identity.WALIdentity != "" ||
		(identity.AssignmentID != "" && !mutationDigestValid(identity.AssignmentID)) || binding.Validate() != nil || binding.AssignmentID != identity.AssignmentID ||
		binding.SourceClusterID != identity.SourceClusterID || binding.NodeID != identity.NodeID || binding.RunID != identity.RunID {
		return fmt.Errorf("fencing: invalid logical writer registration")
	}
	if l.ClosedBy != "" {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	if l.Runs == nil {
		l.Runs = make(map[string]Run)
	}
	if old, exists := l.Runs[identity.RunID]; exists {
		if old.Identity == identity && old.LogicalBinding != nil && *old.LogicalBinding == binding && runRole(old) == binding.Role && old.Stop == nil {
			return nil
		}
		return fmt.Errorf("fencing: logical run ID already registered")
	}
	expectedWAL := ""
	if binding.Recovery.Kind != "" {
		expectedWAL = binding.Recovery.ExpectedWALIdentity
	} else if binding.WALIdentity != "" {
		return fmt.Errorf("fencing: logical WAL cannot be prebound without an authority recovery binding")
	}
	for _, old := range l.Runs {
		if old.Identity.NodeID == identity.NodeID {
			if old.LogicalClosure == nil || validateLogicalRunClosure(old, *old.LogicalClosure) != nil {
				// A process exit, Pod deletion, or stop request never closes this slot.
				return fmt.Errorf("fencing: logical voter slot lacks verified producer closure")
			}
			if old.LogicalClosure.Kind == LogicalRunClosureBoundWAL {
				if expectedWAL != "" && expectedWAL != old.Identity.WALIdentity {
					return fmt.Errorf("fencing: prior logical Runs disagree on WAL continuity")
				}
				expectedWAL = old.Identity.WALIdentity
				sameStaticContinuation := binding.Recovery.Kind == LogicalRecoverySameWAL && binding.Recovery.ExpectedWALIdentity == expectedWAL
				sameAssignedContinuation := binding.Recovery.Kind == LogicalRecoveryReplacement && old.Identity.AssignmentID != "" &&
					binding.AssignmentID == old.Identity.AssignmentID && binding.Recovery == old.LogicalBinding.Recovery &&
					old.Promotion != nil && old.Promotion.OperationID == binding.Recovery.AddOperationID &&
					old.Promotion.EvidenceHash == binding.Recovery.PromotionEvidenceHash
				if !sameStaticContinuation && !sameAssignedContinuation {
					return fmt.Errorf("fencing: logical continuation lacks the exact prior WAL authority")
				}
			} else if old.Identity.WALIdentity != "" || old.LogicalClosure.Kind != LogicalRunClosureUnboundNoWork {
				return fmt.Errorf("fencing: logical unbound supersession differs from zero-effect closure")
			} else if old.Identity.AssignmentID != binding.AssignmentID || old.LogicalBinding.Recovery != binding.Recovery {
				return fmt.Errorf("fencing: unbound logical supersession changed the authority assignment")
			}
			if old.MutationProducerUID == binding.ProducerUID || old.MutationScopeHash != binding.StoreScopeHash {
				return fmt.Errorf("fencing: logical continuation reused a producer or changed its storage scope")
			}
		}
	}
	if binding.WALIdentity != expectedWAL {
		return fmt.Errorf("fencing: logical Run does not match its authority-pinned WAL")
	}
	used, err := l.EngineRepairCapacityUse()
	if err != nil {
		return err
	}
	if used >= MaxEngineRepairIncarnations {
		return fmt.Errorf("fencing: logical Run capacity exceeded")
	}
	run := Run{Identity: identity, Role: binding.Role, LogicalBinding: &binding, LogicalExpectedWAL: expectedWAL,
		MutationProducerUID: binding.ProducerUID, MutationScopeHash: binding.StoreScopeHash}
	if binding.Role == StartupRoleVoterActive && binding.Recovery.Kind == LogicalRecoveryReplacement {
		run.Promotion = &LearnerPromotion{OperationID: binding.Recovery.AddOperationID, EvidenceHash: binding.Recovery.PromotionEvidenceHash}
	}
	l.Runs[identity.RunID] = run
	return nil
}

func (l *Ledger) BindLogicalWAL(identity Identity) error {
	if l == nil || l.Profile != LogicalWriterProfileV1 || l.ValidateProfile() != nil || !mutationDigestValid(identity.WALIdentity) {
		return fmt.Errorf("fencing: invalid logical WAL bind")
	}
	run, ok := l.Runs[identity.RunID]
	if !ok || run.Runtime != nil || run.LogicalBinding == nil || run.LogicalBinding.Validate() != nil ||
		run.LogicalBinding.SourceClusterID != identity.SourceClusterID || run.LogicalBinding.NodeID != identity.NodeID ||
		run.LogicalBinding.RunID != identity.RunID || run.LogicalBinding.AssignmentID != identity.AssignmentID || runRole(run) != run.LogicalBinding.Role {
		return fmt.Errorf("fencing: logical WAL bind is outside its registered Run")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	if run.LogicalExpectedWAL != "" && run.LogicalExpectedWAL != identity.WALIdentity {
		return fmt.Errorf("fencing: logical continuation WAL differs from prior closed Run")
	}
	if run.Identity.WALIdentity != "" {
		if run.Identity != identity || run.LogicalBinding.WALIdentity != identity.WALIdentity {
			return fmt.Errorf("fencing: logical WAL binding is immutable")
		}
		return nil
	}
	previous := identity
	previous.WALIdentity = ""
	if previous != run.Identity || (run.LogicalBinding.WALIdentity != "" && run.LogicalBinding.WALIdentity != identity.WALIdentity) {
		return fmt.Errorf("fencing: logical WAL bind identity differs")
	}
	if runRole(run) == StartupRoleVoterActive && (run.MutationProducerUID == "" || run.MutationScopeHash == "" || run.MutationProfile == "") {
		return fmt.Errorf("fencing: logical active voter lacks producer, scope, or execution profile before WAL binding")
	}
	if runRole(run) == StartupRoleLearnerPendingRemove && run.MutationProducerUID != "" {
		return fmt.Errorf("fencing: pending logical learner cannot bind a mutation producer")
	}
	run.Identity = identity
	binding := *run.LogicalBinding
	binding.WALIdentity = identity.WALIdentity
	run.LogicalBinding = &binding
	l.Runs[identity.RunID] = run
	return nil
}

func (l *Ledger) BindLogicalMutationProducer(identity Identity, producerUID string) error {
	if l == nil || l.Profile != LogicalWriterProfileV1 || l.ValidateProfile() != nil || producerUID == "" || len(producerUID) > 128 {
		return fmt.Errorf("fencing: invalid logical producer binding")
	}
	run, ok := l.Runs[identity.RunID]
	if !ok || run.Runtime != nil || run.LogicalBinding == nil || runRole(run) != StartupRoleVoterActive ||
		identity.SourceClusterID != l.SourceClusterID || identity.NodeID != run.Identity.NodeID || identity.WALIdentity != run.Identity.WALIdentity || identity.WALIdentity != "" ||
		!sameLogicalUnboundIdentity(identity, run.Identity) {
		return fmt.Errorf("fencing: logical producer binding is outside an unbound active Run")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	binding := *run.LogicalBinding
	if binding.ProducerUID != "" && binding.ProducerUID != producerUID {
		return fmt.Errorf("fencing: logical producer UID is immutable")
	}
	binding.ProducerUID = producerUID
	run.LogicalBinding = &binding
	run.MutationProducerUID = producerUID
	l.Runs[identity.RunID] = run
	return nil
}

func (l *Ledger) BindLogicalMutationScope(identity Identity, scopeHash, executionProfile string) error {
	if l == nil || l.Profile != LogicalWriterProfileV1 || l.ValidateProfile() != nil || !mutationDigestValid(scopeHash) || !MutationProfileSupported(executionProfile) {
		return fmt.Errorf("fencing: invalid logical mutation scope")
	}
	run, ok := l.Runs[identity.RunID]
	if !ok || run.Runtime != nil || run.LogicalBinding == nil || runRole(run) != StartupRoleVoterActive ||
		identity.SourceClusterID != l.SourceClusterID || identity.NodeID != run.Identity.NodeID || identity.WALIdentity != run.Identity.WALIdentity || identity.WALIdentity != "" ||
		!sameLogicalUnboundIdentity(identity, run.Identity) || run.MutationProducerUID == "" {
		return fmt.Errorf("fencing: logical mutation scope is outside an unbound active Run")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	binding := *run.LogicalBinding
	if binding.StoreScopeHash != "" && binding.StoreScopeHash != scopeHash || run.MutationScopeHash != "" && run.MutationScopeHash != scopeHash || run.MutationProfile != "" && run.MutationProfile != executionProfile {
		return fmt.Errorf("fencing: logical mutation scope/profile is immutable")
	}
	binding.StoreScopeHash = scopeHash
	run.LogicalBinding = &binding
	run.MutationScopeHash, run.MutationProfile = scopeHash, executionProfile
	l.Runs[identity.RunID] = run
	return nil
}

func (l *Ledger) BindLogicalMutationStage(identity Identity, incarnation string) error {
	if l == nil || l.Profile != LogicalWriterProfileV1 || l.ValidateProfile() != nil || !mutationDigestValid(incarnation) {
		return fmt.Errorf("fencing: invalid logical mutation stage enrollment")
	}
	run, ok := l.Runs[identity.RunID]
	if !ok || run.Runtime != nil || run.LogicalBinding == nil || runRole(run) != StartupRoleVoterActive ||
		identity.SourceClusterID != l.SourceClusterID || identity.NodeID != run.Identity.NodeID || identity.WALIdentity != run.Identity.WALIdentity || identity.WALIdentity != "" ||
		!sameLogicalUnboundIdentity(identity, run.Identity) ||
		run.MutationProducerUID == "" || run.MutationScopeHash == "" || !MutationProfileSupported(run.MutationProfile) {
		return fmt.Errorf("fencing: logical mutation stage is outside a scoped unbound active Run")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	if run.MutationStageIncarnation != "" && run.MutationStageIncarnation != incarnation {
		return fmt.Errorf("fencing: logical mutation stage identity is immutable")
	}
	run.MutationStageIncarnation = incarnation
	l.Runs[identity.RunID] = run
	return nil
}

func sameLogicalUnboundIdentity(left, right Identity) bool {
	return left == right && left.WALIdentity == ""
}

// PromoteLogicalLearner applies the logical-profile pending-to-active
// transition after the authority has verified the canonical REMOVE/ADD and
// activation evidence represented by proof. No RuntimeBinding is fabricated.
func (l *Ledger) PromoteLogicalLearner(identity Identity, proof LogicalPromotionEvidenceV1, activeBinding LogicalWriterBindingV1, executionProfile, stageIncarnation string) error {
	if l == nil || l.Profile != LogicalWriterProfileV1 || l.ValidateProfile() != nil {
		return fmt.Errorf("fencing: logical learner promotion requires the explicit logical profile")
	}
	run, ok := l.Runs[identity.RunID]
	if !ok || run.Identity != identity || run.Runtime != nil || run.LogicalBinding == nil ||
		!mutationDigestValid(identity.AssignmentID) || run.LogicalBinding.ValidateBound() != nil {
		return ErrClosed
	}
	if err := proof.ValidateAgainst(identity, run.LogicalBinding.Recovery); err != nil {
		return err
	}
	evidenceDigest := proof.Digest()
	wantActiveBinding := activeBinding
	wantActiveBinding.Recovery.PromotionEvidenceHash = evidenceDigest
	wantPromotion := LearnerPromotion{OperationID: proof.AddOperationID, EvidenceHash: evidenceDigest}
	if runRole(run) != StartupRoleLearnerPendingRemove || run.Stop != nil || l.ClosedBy != "" {
		if runRole(run) == StartupRoleVoterActive && run.Promotion != nil && *run.Promotion == wantPromotion && *run.LogicalBinding == wantActiveBinding &&
			run.MutationProfile == executionProfile && run.MutationStageIncarnation == stageIncarnation {
			return nil
		}
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	if activeBinding.ValidateBound() != nil || activeBinding.Role != StartupRoleVoterActive ||
		activeBinding.SourceClusterID != identity.SourceClusterID || activeBinding.NodeID != identity.NodeID ||
		activeBinding.RunID != identity.RunID || activeBinding.AssignmentID != identity.AssignmentID ||
		activeBinding.WALIdentity != identity.WALIdentity || activeBinding.Recovery != run.LogicalBinding.Recovery ||
		activeBinding.Recovery.PromotionEvidenceHash != "" ||
		activeBinding.ProducerUID == "" || activeBinding.StoreScopeHash == "" || !MutationProfileSupported(executionProfile) ||
		!mutationDigestValid(stageIncarnation) {
		return fmt.Errorf("fencing: logical learner promotion binding is incomplete")
	}
	if run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.MutationProfile != "" || run.MutationStageIncarnation != "" {
		return fmt.Errorf("fencing: pending logical learner already has mutation authority")
	}
	if run.Promotion != nil {
		return fmt.Errorf("fencing: logical learner promotion is immutable")
	}
	run.Role = StartupRoleVoterActive
	copy := wantActiveBinding
	run.LogicalBinding = &copy
	run.Promotion = &wantPromotion
	run.MutationProducerUID = activeBinding.ProducerUID
	run.MutationScopeHash = activeBinding.StoreScopeHash
	run.MutationProfile = executionProfile
	run.MutationStageIncarnation = stageIncarnation
	l.Runs[identity.RunID] = run
	return nil
}

// validateLogicalRunBinding is shared by root/profile readers and logical
// registration/WAL transitions. It never consults physical runtime fields.
func validateLogicalRunBinding(run Run) error {
	walMatches := run.LogicalBinding != nil && run.LogicalBinding.WALIdentity == run.Identity.WALIdentity
	if run.LogicalExpectedWAL != "" {
		walMatches = run.LogicalBinding != nil && run.LogicalBinding.WALIdentity == run.LogicalExpectedWAL &&
			(run.Identity.WALIdentity == "" || run.Identity.WALIdentity == run.LogicalExpectedWAL)
	}
	if run.Runtime != nil || run.LogicalBinding == nil || run.LogicalBinding.Validate() != nil ||
		run.LogicalBinding.SourceClusterID != run.Identity.SourceClusterID || run.LogicalBinding.NodeID != run.Identity.NodeID ||
		run.LogicalBinding.RunID != run.Identity.RunID || run.LogicalBinding.AssignmentID != run.Identity.AssignmentID || !walMatches ||
		run.LogicalBinding.Role != runRole(run) || run.MutationProducerUID != run.LogicalBinding.ProducerUID ||
		run.MutationScopeHash != run.LogicalBinding.StoreScopeHash {
		return fmt.Errorf("fencing: logical Run binding differs")
	}
	return nil
}
