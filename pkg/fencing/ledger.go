package fencing

import (
	"crypto/sha256"
	"fmt"
)

// Ledger is one source generation's durable CAS value. All mutations must be
// persisted with the resourceVersion read alongside this value. Never replace a
// ledger after a conflict or discard an older incarnation when a Pod disappears.
type Ledger struct {
	SourceClusterID            string                             `json:"sourceClusterID"`
	ClosedBy                   string                             `json:"closedBy,omitempty"`
	ClosedHash                 string                             `json:"closedHash,omitempty"`
	ClosedNodes                map[string]StopRequest             `json:"closedNodes,omitempty"`
	Runs                       map[string]Run                     `json:"runs"`
	RequireRuntimeBinding      bool                               `json:"requireRuntimeBinding,omitempty"`
	RequireMutationJournal     bool                               `json:"requireMutationJournal,omitempty"`
	ArchiveGenesisState        string                             `json:"archiveGenesisState,omitempty"`
	ArchiveScopeHash           string                             `json:"archiveScopeHash,omitempty"`
	ArchiveGenesisRef          *ArchiveGenesisRef                 `json:"archiveGenesisRef,omitempty"`
	EmptyRecoveryAuthorization *ArchiveEmptyRecoveryAuthorization `json:"emptyRecoveryAuthorization,omitempty"`
	// EngineRepairs contains the bounded root-CAS references that authorize
	// immutable engine-repair evidence. Unreferenced evidence objects are inert.
	EngineRepairs map[string]EngineRepairReference `json:"engineRepairs,omitempty"`
	// PendingEngineRepairs are authority-recorded physical requests only. They
	// do not create Runs, roles, grants, or learner permissions.
	PendingEngineRepairs map[string]PendingEngineRepairRequest `json:"pendingEngineRepairs,omitempty"`
}

type Run struct {
	Identity                 Identity           `json:"identity"`
	Role                     StartupRole        `json:"role,omitempty"`
	Stop                     *StopRequest       `json:"stop,omitempty"`
	Drain                    *DrainReceipt      `json:"drain,omitempty"`
	Exited                   bool               `json:"exited,omitempty"`
	NeverBoundExit           *StopRequest       `json:"neverBoundExit,omitempty"`
	Runtime                  *RuntimeBinding    `json:"runtime,omitempty"`
	MutationProducerUID      string             `json:"mutationProducerUID,omitempty"`
	MutationScopeHash        string             `json:"mutationScopeHash,omitempty"`
	MutationProfile          string             `json:"mutationProfile,omitempty"`
	MutationStageIncarnation string             `json:"mutationStageIncarnation,omitempty"`
	IndependentTerminal      *TerminalReference `json:"independentTerminal,omitempty"`
	Promotion                *LearnerPromotion  `json:"promotion,omitempty"`
	// EngineRepairProvenance is authority-written from a canonical repair
	// reservation. It is never accepted from a startup request or grant.
	EngineRepairProvenance *EngineRepairRunProvenance `json:"engineRepairProvenance,omitempty"`
	// InitialRegistration is authority-written only for the one Run admitted
	// from a pristine local voter state. It survives Bind so an active role
	// alone can never be mistaken for first-use authority.
	InitialRegistration *InitialRegistrationProvenance `json:"initialRegistration,omitempty"`
}

const InitialRegistrationProvenanceVersion = 1

// InitialRegistrationProvenance records the authority-verified static
// genesis and store scope for one physical first-registration Run.
type InitialRegistrationProvenance struct {
	Version        int      `json:"version"`
	IdentityDigest string   `json:"identityDigest"`
	MembershipHash string   `json:"membershipHash"`
	StoreScopeHash string   `json:"storeScopeHash"`
	RunIdentity    Identity `json:"runIdentity"`
}

func InitialRegistrationIdentityDigest(identity Identity) string {
	identity.WALIdentity = ""
	return hashJSONValue([]any{"rhiza-initial-voter-registration-v1", identity})
}

func (p InitialRegistrationProvenance) ValidateForIdentity(identity Identity) error {
	want, got := identity, p.RunIdentity
	want.WALIdentity, got.WALIdentity = "", ""
	if p.Version != InitialRegistrationProvenanceVersion || p.IdentityDigest != InitialRegistrationIdentityDigest(identity) ||
		!mutationDigestValid(p.MembershipHash) || !mutationDigestValid(p.StoreScopeHash) || got != want ||
		identity.SourceClusterID == "" || identity.NodeID == "" || identity.PodName != identity.NodeID ||
		identity.PodUID == "" || identity.RunID == "" || identity.AssignmentID != "" {
		return fmt.Errorf("fencing: initial-registration provenance differs from its Run")
	}
	return nil
}

func (p InitialRegistrationProvenance) ValidateForRun(run Run) error {
	if err := p.ValidateForIdentity(run.Identity); err != nil {
		return err
	}
	if runRole(run) != StartupRoleVoterActive || run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil {
		return fmt.Errorf("fencing: initial-registration Run lacks its active runtime enrollment")
	}
	return nil
}

const EngineRepairRunProvenanceVersion = 1

// EngineRepairRunProvenance marks the exact candidate incarnation and trusted
// runtime enrollment admitted by the authority for an engine-repair operation.
// IdentityDigest is the immutable reservation identity; CandidateIdentity and
// RuntimeBindingDigest bind this physical/logical Run and its exact runtime.
// A continuation gets fresh provenance for its own Run rather than rewriting
// this history.
type EngineRepairRunProvenance struct {
	Version              int      `json:"version"`
	OperationID          string   `json:"operationID"`
	IdentityDigest       string   `json:"identityDigest"`
	CandidateIdentity    Identity `json:"candidateIdentity"`
	RuntimeBindingDigest string   `json:"runtimeBindingDigest"`
}

func (p EngineRepairRunProvenance) Validate() error {
	candidate := p.CandidateIdentity
	if p.Version != EngineRepairRunProvenanceVersion || p.OperationID == "" || !mutationDigestValid(p.IdentityDigest) ||
		!mutationDigestValid(p.RuntimeBindingDigest) ||
		candidate.SourceClusterID == "" || candidate.NodeID == "" || candidate.PodName == "" || candidate.PodName == candidate.NodeID ||
		candidate.PodUID == "" || candidate.RunID == "" || !mutationDigestValid(candidate.WALIdentity) || !mutationDigestValid(candidate.AssignmentID) {
		return fmt.Errorf("fencing: invalid engine-repair Run provenance")
	}
	return nil
}

// ValidateForRun ensures the server-recorded provenance still names this
// exact runtime enrollment. WAL may be empty only before the one-time bind;
// after a learner becomes active it must equal the pinned candidate WAL.
func (p EngineRepairRunProvenance) ValidateForRun(run Run) error {
	if err := p.Validate(); err != nil {
		return err
	}
	want := p.CandidateIdentity
	actual := run.Identity
	unbound := actual.WALIdentity == ""
	if !unbound && actual.WALIdentity != want.WALIdentity {
		return fmt.Errorf("fencing: engine-repair Run provenance has a different bound WAL")
	}
	if !engineRepairCandidateRunIdentityMatches(want, actual) || run.Runtime == nil || run.Runtime.Validate(actual) != nil ||
		RuntimeBindingDigest(*run.Runtime) != p.RuntimeBindingDigest ||
		unbound && (runRole(run) != StartupRoleLearnerPendingRemove || run.Promotion != nil || run.MutationProducerUID != "" ||
			run.MutationScopeHash != "" || run.MutationProfile != "" || run.MutationStageIncarnation != "") {
		return fmt.Errorf("fencing: engine-repair Run provenance differs from its exact candidate enrollment")
	}
	if runRole(run) == StartupRoleVoterActive && (run.Promotion == nil || run.Promotion.OperationID == "" || !mutationDigestValid(run.Promotion.EvidenceHash)) {
		return fmt.Errorf("fencing: active engine-repair Run lacks canonical promotion evidence")
	}
	return nil
}

// RegisterEngineRepairCandidate is used only by the authenticated authority
// after resolving the canonical repair reservation and trusted runtime
// enrollment. Client-provided startup identities cannot set this field.
func (l *Ledger) RegisterEngineRepairCandidate(identity Identity, runtime RuntimeBinding, provenance EngineRepairRunProvenance) error {
	if l == nil || provenance.Validate() != nil || identity.WALIdentity != "" ||
		runIdentityMatchesCandidate(identity, provenance.CandidateIdentity) != nil || runtime.Validate(provenance.CandidateIdentity) != nil ||
		RuntimeBindingDigest(runtime) != provenance.RuntimeBindingDigest {
		return fmt.Errorf("fencing: engine-repair candidate enrollment is not the exact authority-pinned incarnation")
	}
	if old, exists := l.Runs[identity.RunID]; exists {
		if old.EngineRepairProvenance == nil || *old.EngineRepairProvenance != provenance {
			return fmt.Errorf("fencing: engine-repair Run provenance is immutable")
		}
	}
	if err := l.RegisterEnrolledWithRole(identity, runtime, StartupRoleLearnerPendingRemove); err != nil {
		return err
	}
	run := l.Runs[identity.RunID]
	copy := provenance
	run.EngineRepairProvenance = &copy
	l.Runs[identity.RunID] = run
	return nil
}

// RegisterEngineRepairActiveRestart enrolls a fresh process incarnation of
// the same already-active engine-repair candidate. The authenticated caller
// must first resolve the current Active repair and exact prior Run; this
// transition itself preserves the operation/candidate provenance and never
// turns a learner or an unmarked generic Run into an active candidate.
func (l *Ledger) RegisterEngineRepairActiveRestart(record EngineRepairRecord, priorIdentity, nextIdentity Identity, runtime RuntimeBinding) error {
	provenance := EngineRepairRunProvenance{
		Version: EngineRepairRunProvenanceVersion, OperationID: record.Binding.OperationID,
		IdentityDigest: record.IdentityDigest(), CandidateIdentity: record.CurrentCandidateIdentity(),
		RuntimeBindingDigest: record.CurrentCandidateRuntimeHash(),
	}
	if l == nil || !engineRepairRecordIsCanonicalActiveReference(record, *l) || record.ValidateAgainstLedger(*l) != nil || provenance.Validate() != nil ||
		!engineRepairCandidateRunIdentityMatches(record.CurrentCandidateIdentity(), priorIdentity) || priorIdentity.WALIdentity == "" || nextIdentity.WALIdentity != priorIdentity.WALIdentity ||
		priorIdentity.RunID == "" || nextIdentity.RunID == "" || nextIdentity.RunID == priorIdentity.RunID ||
		!engineRepairCandidateRunIdentityMatches(provenance.CandidateIdentity, priorIdentity) ||
		!engineRepairCandidateRunIdentityMatches(provenance.CandidateIdentity, nextIdentity) || runtime.Validate(nextIdentity) != nil {
		return fmt.Errorf("fencing: active engine-repair restart differs from its pinned candidate")
	}
	key, _ := record.Binding.EngineRepairGenerationKey()
	if reference := l.EngineRepairs[key]; reference.CandidateIdentity == nextIdentity {
		prior, priorExists := l.Runs[priorIdentity.RunID]
		existing, exists := l.Runs[nextIdentity.RunID]
		nextProvenance := provenance
		nextProvenance.CandidateIdentity = nextIdentity
		nextProvenance.RuntimeBindingDigest = RuntimeBindingDigest(runtime)
		if priorExists && exists && prior.Promotion != nil && existing.Identity == nextIdentity && existing.Runtime != nil && *existing.Runtime == runtime &&
			existing.EngineRepairProvenance != nil && *existing.EngineRepairProvenance == nextProvenance &&
			existing.EngineRepairProvenance.ValidateForRun(existing) == nil && existing.Promotion != nil && *existing.Promotion == *prior.Promotion &&
			engineRepairActiveRestartMatches(record, *l, existing) {
			return nil
		}
		return fmt.Errorf("fencing: active engine-repair restart reference differs from the exact committed continuation")
	}
	prior, exists := l.Runs[priorIdentity.RunID]
	priorProvenance := prior.EngineRepairProvenance
	if !exists || prior.Identity != priorIdentity || priorProvenance == nil || priorProvenance.ValidateForRun(prior) != nil ||
		priorProvenance.OperationID != record.Binding.OperationID || priorProvenance.IdentityDigest != record.IdentityDigest() ||
		priorProvenance.CandidateIdentity != priorIdentity || prior.Runtime == nil ||
		priorProvenance.RuntimeBindingDigest != RuntimeBindingDigest(*prior.Runtime) ||
		runRole(prior) != StartupRoleVoterActive || prior.Runtime == nil || prior.Stop != nil || prior.Drain != nil || prior.Exited || prior.NeverBoundExit != nil ||
		prior.Promotion == nil || prior.Promotion.OperationID != engineRepairPromotionOperationID(record.Binding.OperationID) || !mutationDigestValid(prior.Promotion.EvidenceHash) ||
		prior.EngineRepairProvenance.ValidateForRun(prior) != nil || l.ClosedBy != "" {
		return ErrClosed
	}
	if l.RequireMutationJournal && (prior.MutationProducerUID != engineRepairMutationProducerUID(*prior.Runtime) ||
		!mutationDigestValid(prior.MutationScopeHash) || !MutationProfileSupported(prior.MutationProfile) ||
		!mutationDigestValid(prior.MutationStageIncarnation)) {
		return fmt.Errorf("fencing: active engine-repair restart source lacks its exact mutation journal binding")
	}
	if _, closed := l.ClosedNodes[nextIdentity.NodeID]; closed {
		return ErrClosed
	}
	if l.EngineRepairs[key].CandidateIdentity != priorIdentity {
		return fmt.Errorf("fencing: active engine-repair restart does not advance the current root Run")
	}
	for runID, run := range l.Runs {
		if runID == priorIdentity.RunID || runID == nextIdentity.RunID || run.EngineRepairProvenance == nil ||
			run.EngineRepairProvenance.OperationID != record.Binding.OperationID || run.EngineRepairProvenance.IdentityDigest != record.IdentityDigest() {
			continue
		}
		if run.Runtime == nil || *run.Runtime == runtime {
			return fmt.Errorf("fencing: active engine-repair restart reused a prior runtime enrollment")
		}
	}
	if existing, exists := l.Runs[nextIdentity.RunID]; exists {
		nextProvenance := provenance
		nextProvenance.CandidateIdentity = nextIdentity
		nextProvenance.RuntimeBindingDigest = RuntimeBindingDigest(runtime)
		if existing.Identity == nextIdentity && existing.Runtime != nil && *existing.Runtime == runtime &&
			existing.EngineRepairProvenance != nil && *existing.EngineRepairProvenance == nextProvenance &&
			existing.Promotion != nil && *existing.Promotion == *prior.Promotion && engineRepairActiveRestartMatches(record, *l, existing) {
			return nil
		}
		return fmt.Errorf("fencing: active engine-repair restart RunID already exists with another binding")
	}
	// The generic registration API deliberately accepts only an unbound WAL.
	// Keep that boundary intact: stage the ordinary empty-WAL registration and
	// exact bind on a private ledger copy, then publish the complete Active Run
	// with provenance and promotion evidence as one in-memory transition.
	nextLedger := *l
	nextLedger.Runs = make(map[string]Run, len(l.Runs)+1)
	for runID, run := range l.Runs {
		nextLedger.Runs[runID] = run
	}
	unboundNext := nextIdentity
	unboundNext.WALIdentity = ""
	if err := nextLedger.RegisterEnrolledWithRole(unboundNext, runtime, StartupRoleVoterActive); err != nil {
		return err
	}
	run := nextLedger.Runs[nextIdentity.RunID]
	if nextLedger.RequireMutationJournal {
		run.MutationProducerUID = engineRepairMutationProducerUID(runtime)
		run.MutationScopeHash = prior.MutationScopeHash
		run.MutationProfile = prior.MutationProfile
		run.MutationStageIncarnation = prior.MutationStageIncarnation
		nextLedger.Runs[nextIdentity.RunID] = run
	}
	// Bind the ordinary temporary registration before attaching engine-repair
	// provenance. The generic Bind path intentionally validates any existing
	// provenance against the Run as it exists at bind time, and must not be
	// relaxed to accommodate this transition.
	if err := nextLedger.Bind(nextIdentity); err != nil {
		return err
	}
	run = nextLedger.Runs[nextIdentity.RunID]
	nextProvenance := provenance
	nextProvenance.CandidateIdentity = nextIdentity
	nextProvenance.RuntimeBindingDigest = RuntimeBindingDigest(runtime)
	copy := nextProvenance
	run.EngineRepairProvenance = &copy
	promotion := *prior.Promotion
	run.Promotion = &promotion
	if err := run.EngineRepairProvenance.ValidateForRun(run); err != nil {
		return fmt.Errorf("fencing: active restart provenance does not match its bound Run: %w", err)
	}
	nextLedger.Runs[nextIdentity.RunID] = run
	l.Runs = nextLedger.Runs
	return nil
}

func engineRepairMutationProducerUID(runtime RuntimeBinding) string {
	return "producer-" + hashJSONValue([]any{"rhiza-mutation-producer-v1", runtime})
}

func engineRepairRecordIsCanonicalActiveReference(record EngineRepairRecord, ledger Ledger) bool {
	if record.Phase != RepairActive && !(record.Phase == RepairGenerationClosing && record.ClosingFrom == RepairActive) {
		return false
	}
	key, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		return false
	}
	reference, exists := ledger.EngineRepairs[key]
	if !exists || reference.Validate() != nil || reference.OperationID != record.Binding.OperationID || reference.Binding != record.Binding ||
		!engineRepairCandidateRunIdentityMatches(record.CurrentCandidateIdentity(), reference.CandidateIdentity) ||
		reference.Phase != record.Phase || reference.ClosingFrom != record.ClosingFrom ||
		reference.IdentityDigest != record.IdentityDigest() || reference.RecordDigest != record.Digest() {
		return false
	}
	if reference.CandidateIdentity == record.CurrentCandidateIdentity() {
		return true
	}
	run, ok := ledger.Runs[reference.CandidateIdentity.RunID]
	return ok && engineRepairActiveRestartMatches(record, ledger, run)
}

func engineRepairActiveRestartMatches(record EngineRepairRecord, ledger Ledger, run Run) bool {
	if record.Phase != RepairActive && !(record.Phase == RepairGenerationClosing && record.ClosingFrom == RepairActive) ||
		run.EngineRepairProvenance == nil || run.EngineRepairProvenance.ValidateForRun(run) != nil ||
		run.EngineRepairProvenance.OperationID != record.Binding.OperationID ||
		run.EngineRepairProvenance.IdentityDigest != record.IdentityDigest() ||
		runRole(run) != StartupRoleVoterActive || run.Promotion == nil || run.Promotion.OperationID != engineRepairPromotionOperationID(record.Binding.OperationID) ||
		!mutationDigestValid(run.Promotion.EvidenceHash) || run.Stop != nil || run.Drain != nil || run.Exited || run.NeverBoundExit != nil {
		return false
	}
	identity := run.Identity
	candidate := record.CurrentCandidateIdentity()
	if !engineRepairCandidateRunIdentityMatches(candidate, identity) || identity.WALIdentity != candidate.WALIdentity {
		return false
	}
	currentRun, exists := ledger.Runs[candidate.RunID]
	if !exists || currentRun.EngineRepairProvenance == nil || currentRun.EngineRepairProvenance.ValidateForRun(currentRun) != nil ||
		currentRun.EngineRepairProvenance.OperationID != record.Binding.OperationID ||
		currentRun.EngineRepairProvenance.IdentityDigest != record.IdentityDigest() ||
		currentRun.Promotion == nil || *currentRun.Promotion != *run.Promotion || currentRun.Runtime == nil || run.Runtime == nil ||
		run.Runtime.Validate(identity) != nil || run.Runtime.StatefulSetUID != record.Binding.StatefulSetUID ||
		run.Runtime.AuthorityID != currentRun.Runtime.AuthorityID || run.Runtime.NamespaceUID != currentRun.Runtime.NamespaceUID ||
		run.Runtime.BindingUID != currentRun.Runtime.BindingUID || *run.Runtime == *currentRun.Runtime ||
		(run.MutationProducerUID != "" && (!mutationDigestValid(run.MutationScopeHash) || run.MutationProfile == "")) ||
		(run.MutationProducerUID == "" && (run.MutationScopeHash != "" || run.MutationProfile != "")) {
		return false
	}
	return true
}

func engineRepairCandidateRunIdentityMatches(candidate, run Identity) bool {
	candidate.RunID = run.RunID
	candidate.WALIdentity = run.WALIdentity
	return candidate == run
}

func engineRepairPromotionOperationID(operationID string) string {
	return fmt.Sprintf("engine-repair-add-%x", sha256.Sum256([]byte(operationID)))
}

func runIdentityMatchesCandidate(actual, candidate Identity) error {
	candidate.WALIdentity = ""
	if actual != candidate {
		return fmt.Errorf("fencing: engine-repair run differs from pinned candidate identity")
	}
	return nil
}

// LearnerPromotion binds the role transition to the exact canonical membership
// completion that the authority independently verified. It is not a caller
// supplied capability; only the authenticated fencer writes it.
type LearnerPromotion struct {
	OperationID  string `json:"operationID"`
	EvidenceHash string `json:"evidenceHash"`
}

func (l *Ledger) Register(identity Identity) error {
	return l.RegisterWithRole(identity, StartupRoleVoterActive)
}

// RegisterWithRole is called only by the authenticated authority after it has
// independently resolved a role. Empty roles in historical ledger records are
// interpreted as voter-active for wire/storage compatibility.
func (l *Ledger) RegisterWithRole(identity Identity, role StartupRole) error {
	if identity.SourceClusterID != l.SourceClusterID || identity.NodeID == "" || identity.PodUID == "" || identity.RunID == "" || identity.WALIdentity != "" {
		return fmt.Errorf("fencing: invalid registration")
	}
	assigned := identity.AssignmentID != "" && mutationDigestValid(identity.AssignmentID) && identity.PodName != "" && identity.PodName != identity.NodeID
	if !role.Valid() || (identity.PodName != "" && identity.PodName != identity.NodeID && !assigned) || (identity.AssignmentID != "" && !assigned) {
		return fmt.Errorf("fencing: invalid registration role or physical Pod mapping")
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
		// A caller may resolve an uncertain registration response, but cannot
		// reuse a bound run as authorization for another child spawn.
		if old.Identity == identity && runRole(old) == role && old.Stop == nil {
			return nil
		}
		return fmt.Errorf("fencing: run ID already registered")
	}
	for _, pending := range l.PendingEngineRepairs {
		if pending.SourceClusterID == identity.SourceClusterID && pending.OldMemberID == identity.NodeID {
			return fmt.Errorf("fencing: old-member run admission is reserved by pending engine repair")
		}
	}
	for _, repair := range l.EngineRepairs {
		if repair.Binding.SourceClusterID == identity.SourceClusterID && repair.Binding.OldMemberID == identity.NodeID {
			return fmt.Errorf("fencing: old-member run admission is reserved by engine repair")
		}
	}
	used, err := l.EngineRepairCapacityUse()
	if err != nil {
		return err
	}
	reservedCandidate := false
	for _, repair := range l.EngineRepairs {
		if repairCandidateRunIdentityMatches(repair.CandidateIdentity, identity) {
			reservedCandidate = true
			break
		}
	}
	for _, pending := range l.PendingEngineRepairs {
		if pending.BootstrapIdentity.RunID == identity.RunID {
			return fmt.Errorf("fencing: pending bootstrap run cannot register as a candidate before adoption")
		}
	}
	if !reservedCandidate {
		used++
	}
	if used > MaxEngineRepairIncarnations {
		return fmt.Errorf("fencing: engine repair incarnation capacity exceeded")
	}
	// Same-Pod container restart can continue its intact WAL, not erase history.
	for _, old := range l.Runs {
		if old.Identity.NodeID == identity.NodeID && old.Identity.PodUID != identity.PodUID && (!old.Exited || old.Drain == nil || !old.Drain.Clean()) {
			return fmt.Errorf("fencing: earlier incarnation has no clean termination proof")
		}
	}
	l.Runs[identity.RunID] = Run{Identity: identity, Role: role}
	return nil
}

// EngineRepairCapacityUse counts every registered run and every separately
// reserved pending/repair candidate incarnation exactly once. A candidate
// registration replaces its existing reservation rather than consuming a
// second slot.
func (l Ledger) EngineRepairCapacityUse() (int, error) {
	for _, run := range l.Runs {
		if run.EngineRepairProvenance != nil && run.EngineRepairProvenance.ValidateForRun(run) != nil {
			return 0, fmt.Errorf("fencing: malformed engine-repair Run provenance")
		}
		if run.InitialRegistration != nil && run.InitialRegistration.ValidateForRun(run) != nil {
			return 0, fmt.Errorf("fencing: malformed initial-registration Run provenance")
		}
	}
	// Root references preserve the immutable evidence chain even after a
	// generation is closed. Bound that history separately from live run slots:
	// closed pre-enrollment operations may have no Run to consume capacity.
	if len(l.EngineRepairs)+len(l.PendingEngineRepairs) > MaxEngineRepairIncarnations {
		return 0, fmt.Errorf("fencing: engine repair history capacity exceeded")
	}
	used := len(l.Runs)
	seen := make(map[string]struct{}, len(l.PendingEngineRepairs)+len(l.EngineRepairs))
	for key, pending := range l.PendingEngineRepairs {
		if pending.Validate() != nil || key != pending.SlotKey() {
			return 0, fmt.Errorf("fencing: malformed pending engine repair capacity entry")
		}
		if _, exists := l.Runs[pending.BootstrapIdentity.RunID]; exists {
			return 0, fmt.Errorf("fencing: pending engine repair run is already registered")
		}
		if _, exists := seen[pending.BootstrapIdentity.RunID]; exists {
			return 0, fmt.Errorf("fencing: duplicate engine repair candidate reservation")
		}
		seen[pending.BootstrapIdentity.RunID] = struct{}{}
		used++
	}
	for _, repair := range l.EngineRepairs {
		if repair.Validate() != nil {
			return 0, fmt.Errorf("fencing: malformed engine repair capacity reference")
		}
		id := repair.CandidateIdentity
		if _, exists := seen[id.RunID]; exists {
			return 0, fmt.Errorf("fencing: duplicate engine repair candidate reservation")
		}
		seen[id.RunID] = struct{}{}
		if run, exists := l.Runs[id.RunID]; exists {
			if run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil {
				return 0, fmt.Errorf("fencing: registered repair candidate lacks its trusted runtime binding")
			}
			if repairRequiresActiveCandidateRun(repair.Phase, repair.ClosingFrom) ||
				repairAllowsActiveCandidateRun(repair.Phase, repair.ClosingFrom) && runRole(run) == StartupRoleVoterActive {
				if run.Identity != id || runRole(run) != StartupRoleVoterActive ||
					(run.MutationProducerUID != "" && (!mutationDigestValid(run.MutationScopeHash) || run.MutationProfile == "")) ||
					(run.MutationProducerUID == "" && (run.MutationScopeHash != "" || run.MutationProfile != "")) {
					return 0, fmt.Errorf("fencing: completed repair candidate differs from its active canonical run")
				}
			} else if !repairCandidateRunIdentityMatches(id, run.Identity) || runRole(run) != StartupRoleLearnerPendingRemove ||
				run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.MutationProfile != "" {
				return 0, fmt.Errorf("fencing: registered repair candidate differs from its live reservation")
			}
		} else if !repairIsClosedHistory(repair.Phase) {
			used++
		}
	}
	if used > MaxEngineRepairIncarnations {
		return 0, fmt.Errorf("fencing: engine repair incarnation capacity exceeded")
	}
	return used, nil
}

func repairCandidateRunIdentityMatches(expected, actual Identity) bool {
	if expected == actual {
		return true
	}
	if actual.WALIdentity != "" {
		return false
	}
	expected.WALIdentity = ""
	return expected == actual
}

func repairIsClosedHistory(phase EngineRepairPhase) bool {
	return phase == RepairActive || phase == RepairGenerationClosing
}

func runRole(run Run) StartupRole {
	if run.Role == "" {
		return StartupRoleVoterActive
	}
	return run.Role
}

// StartupRole returns the persisted role, treating pre-role ledger records as
// the historical static-voter profile.
func RunRole(run Run) StartupRole { return runRole(run) }

// PromoteLearner is an idempotent pending-to-active transition. The operator
// authority calls it only after validating the exact persisted REMOVE/ADD
// requests, immutable credentials, committed configuration, and durability
// confirmation. A bound WAL and immutable evidence digest are mandatory.
func (l *Ledger) PromoteLearner(identity Identity, operationID, evidenceHash string) error {
	run, ok := l.Runs[identity.RunID]
	if !ok || identity.WALIdentity == "" || run.Identity != identity || run.Runtime == nil || run.Stop != nil || l.ClosedBy != "" {
		return ErrClosed
	}
	if run.EngineRepairProvenance != nil && run.EngineRepairProvenance.ValidateForRun(run) != nil {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	if operationID == "" || !mutationDigestValid(evidenceHash) || runRole(run) != StartupRoleLearnerPendingRemove {
		if runRole(run) == StartupRoleVoterActive && run.Promotion != nil &&
			run.Promotion.OperationID == operationID && run.Promotion.EvidenceHash == evidenceHash {
			return nil
		}
		return fmt.Errorf("fencing: learner promotion evidence is invalid")
	}
	if run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.MutationProfile != "" || run.MutationStageIncarnation != "" {
		return fmt.Errorf("fencing: pending learner already has source mutation authority")
	}
	run.Role = StartupRoleVoterActive
	run.Promotion = &LearnerPromotion{OperationID: operationID, EvidenceHash: evidenceHash}
	l.Runs[identity.RunID] = run
	return nil
}

func (l *Ledger) Bind(identity Identity) error {
	run, exists := l.Runs[identity.RunID]
	if !exists || identity.WALIdentity == "" {
		return fmt.Errorf("fencing: unknown WAL registration")
	}
	if provenance := run.EngineRepairProvenance; provenance != nil &&
		(provenance.ValidateForRun(run) != nil || identity.WALIdentity != provenance.CandidateIdentity.WALIdentity) {
		return fmt.Errorf("fencing: engine-repair WAL bind differs from the authority-pinned candidate")
	}
	if provenance := run.InitialRegistration; provenance != nil {
		// A bind response can be lost after the CAS commits. Accept only an
		// exact retry of that same binding; the equality check below still
		// rejects any different nonce or identity.
		if provenance.ValidateForRun(run) != nil || run.Stop != nil ||
			runRole(run) != StartupRoleVoterActive {
			return fmt.Errorf("fencing: initial voter WAL bind lacks its authority-issued Run evidence")
		}
		for runID, old := range l.Runs {
			if runID != identity.RunID && old.Identity.NodeID == identity.NodeID && old.Identity.WALIdentity != "" {
				return fmt.Errorf("fencing: initial voter has historical WAL binding")
			}
		}
	}
	old := identity
	old.WALIdentity = run.Identity.WALIdentity
	if old != run.Identity || (run.Identity.WALIdentity != "" && run.Identity.WALIdentity != identity.WALIdentity) {
		return fmt.Errorf("fencing: binding changed")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if l.RequireRuntimeBinding || l.RequireMutationJournal {
		if run.Runtime == nil {
			return fmt.Errorf("fencing: runtime enrollment required before WAL binding")
		}
		if err := run.Runtime.Validate(identity); err != nil {
			return err
		}
	}
	if l.RequireMutationJournal && runRole(run) == StartupRoleVoterActive && run.MutationProducerUID == "" {
		return fmt.Errorf("fencing: immutable mutation producer enrollment required before WAL binding")
	}
	if runRole(run) == StartupRoleLearnerPendingRemove && run.MutationProducerUID != "" {
		return fmt.Errorf("fencing: pending learner cannot bind a mutation producer")
	}
	for _, old := range l.Runs {
		if old.Identity.RunID != identity.RunID && old.Identity.NodeID == identity.NodeID && old.Identity.WALIdentity != "" && old.Identity.WALIdentity != identity.WALIdentity {
			return fmt.Errorf("fencing: restarted voter must retain its historical WAL")
		}
	}
	run.Identity = identity
	l.Runs[identity.RunID] = run
	return nil
}

// CloseNodes fences stable voter slots, including subsequent Pod recreation,
// without stopping surviving voters. Empty nodeIDs is generation-wide.
func (l *Ledger) CloseNodes(operationID, requestHash string, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return l.Close(operationID, requestHash)
	}
	if operationID == "" || requestHash == "" {
		return ErrClosed
	}
	if l.ClosedBy != "" {
		// An earlier pending voter operation may finish after escalation has
		// closed the generation. It cannot reopen or alter its original scope.
		for _, nodeID := range nodeIDs {
			old, ok := l.ClosedNodes[nodeID]
			if !ok || old.OperationID != operationID || old.RequestHash != requestHash {
				return ErrClosed
			}
		}
		return nil
	}
	for _, nodeID := range nodeIDs {
		if nodeID == "" {
			return fmt.Errorf("fencing: empty voter slot")
		}
		if old, ok := l.ClosedNodes[nodeID]; ok && old.OperationID == operationID && old.RequestHash != requestHash {
			return fmt.Errorf("fencing: immutable voter request hash changed")
		}
	}
	if l.ClosedNodes == nil {
		l.ClosedNodes = make(map[string]StopRequest)
	}
	for _, nodeID := range nodeIDs {
		l.ClosedNodes[nodeID] = StopRequest{OperationID: operationID, RequestHash: requestHash}
		for id, run := range l.Runs {
			if run.Identity.NodeID != nodeID {
				continue
			}
			if run.Stop != nil {
				continue
			}
			request := StopRequest{OperationID: operationID, RequestHash: requestHash, Identity: run.Identity}
			run.Stop = &request
			l.Runs[id] = run
		}
	}
	return nil
}

// Close permanently closes generation-wide startup admission before stop
// requests can be delivered. A different operation cannot reopen this authority.
func (l *Ledger) Close(operationID, requestHash string) error {
	if operationID == "" || requestHash == "" {
		return fmt.Errorf("fencing: empty operation")
	}
	if l.ClosedBy != "" && l.ClosedBy != operationID {
		return ErrClosed
	}
	if l.ClosedBy == operationID && l.ClosedHash != requestHash {
		return fmt.Errorf("fencing: immutable request hash changed")
	}
	l.ClosedBy = operationID
	l.ClosedHash = requestHash
	for id, run := range l.Runs {
		// Prior stop requests and their drain/exit evidence remain immutable.
		// Generation-wide closure can include a slot already terminally stopped
		// by an earlier voter fence without issuing a second process command.
		if run.Stop != nil {
			continue
		}
		request := StopRequest{OperationID: operationID, RequestHash: requestHash, Identity: run.Identity}
		run.Stop = &request
		l.Runs[id] = run
	}
	return nil
}

func (l *Ledger) RecordDrain(receipt DrainReceipt) error {
	run, exists := l.Runs[receipt.Request.Identity.RunID]
	if !exists || run.Stop == nil || *run.Stop != receipt.Request {
		return fmt.Errorf("fencing: unmatched drain")
	}
	if run.NeverBoundExit != nil {
		return fmt.Errorf("fencing: never-bound exit cannot become a drain")
	}
	if run.Drain != nil && *run.Drain != receipt {
		return fmt.Errorf("fencing: drain receipt cannot change")
	}
	run.Drain = &receipt
	l.Runs[run.Identity.RunID] = run
	return nil
}

// RecordExit is called only by a supervisor after the child has exited and the
// private process namespace contains no other process. Engine receipts alone
// must never call this method.
func (l *Ledger) RecordExit(request StopRequest) error {
	run, exists := l.Runs[request.Identity.RunID]
	if !exists || run.Identity.WALIdentity == "" || run.Stop == nil || *run.Stop != request || run.Drain == nil || !run.Drain.Clean() {
		return ErrPending
	}
	run.Exited = true
	l.Runs[request.Identity.RunID] = run
	return nil
}

// RecordNeverBoundExit requires a durable closed, never-authorized run and
// separately verified process termination. A lost successful WAL bind response
// cannot use this exception because the WAL identity is already persisted.
func (l *Ledger) RecordNeverBoundExit(request StopRequest) error {
	run, exists := l.Runs[request.Identity.RunID]
	if !exists || run.Identity.WALIdentity != "" || run.Drain != nil || request.Identity != run.Identity || run.Stop == nil || *run.Stop != request {
		return fmt.Errorf("fencing: never-bound exit is not proven")
	}
	if l.ClosedBy == "" {
		if _, closed := l.ClosedNodes[run.Identity.NodeID]; !closed {
			return ErrPending
		}
	}
	run.Exited = true
	copy := request
	run.NeverBoundExit = &copy
	l.Runs[request.Identity.RunID] = run
	return nil
}

// Complete requires exact current Pod inventory in addition to all historic
// runs. A missing registration cannot be inferred safe from Pod health.
func (l *Ledger) Complete(operationID, requestHash string, currentPodUIDs []string) bool {
	return l.CompleteNodes(operationID, requestHash, nil, currentPodUIDs)
}

func (l *Ledger) CompleteNodes(operationID, requestHash string, nodeIDs, currentPodUIDs []string) bool {
	if operationID == "" || requestHash == "" || len(l.Runs) == 0 {
		return false
	}
	wanted := make(map[string]bool)
	for _, id := range nodeIDs {
		closure, ok := l.ClosedNodes[id]
		if !ok || closure.OperationID != operationID || closure.RequestHash != requestHash {
			return false
		}
		wanted[id] = true
	}
	if len(wanted) == 0 && (l.ClosedBy != operationID || l.ClosedHash != requestHash) {
		return false
	}
	registered := make(map[string]bool)
	observed := make(map[string]bool)
	for _, run := range l.Runs {
		if len(wanted) > 0 && !wanted[run.Identity.NodeID] {
			continue
		}
		observed[run.Identity.NodeID] = true
		registered[run.Identity.PodUID] = true
		if run.Stop == nil || run.Stop.Identity != run.Identity || run.Stop.OperationID == "" || run.Stop.RequestHash == "" || !run.Exited {
			return false
		}
		if run.NeverBoundExit != nil {
			if run.Identity.WALIdentity != "" || run.Drain != nil || *run.NeverBoundExit != *run.Stop {
				return false
			}
		} else if run.Identity.WALIdentity == "" || run.Drain == nil || run.Drain.Request != *run.Stop || !run.Drain.Clean() {
			return false
		}
	}
	for id := range wanted {
		if !observed[id] {
			return false
		}
	}
	for _, uid := range currentPodUIDs {
		if !registered[uid] {
			return false
		}
	}
	return true
}
