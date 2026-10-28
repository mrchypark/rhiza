package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Ledger is one source generation's durable CAS value. All mutations must be
// persisted with the resourceVersion read alongside this value. Never replace a
// ledger after a conflict or discard an older incarnation when a Pod disappears.
type Ledger struct {
	SourceClusterID         string                            `json:"sourceClusterID"`
	ClosedBy                string                            `json:"closedBy,omitempty"`
	ClosedHash              string                            `json:"closedHash,omitempty"`
	ClosedNodes             map[string]StopRequest            `json:"closedNodes,omitempty"`
	Runs                    map[string]Run                    `json:"runs"`
	PeerClosure             *PeerClosureReservation           `json:"peerClosure,omitempty"`
	Bootstrap               *BootstrapHistory                 `json:"bootstrap,omitempty"`
	MembershipRepair        *MembershipRepairRecord           `json:"membershipRepair,omitempty"`
	MembershipRepairHistory map[string]MembershipRepairRecord `json:"membershipRepairHistory,omitempty"`
}

type MembershipRepairRecord struct {
	Authorization MembershipRepairAuthorization `json:"authorization"`
	Progress      MembershipRepairProgress      `json:"progress"`
	Preclosure    *MembershipRepairPreclosure   `json:"preclosure,omitempty"`
}

func cloneMembershipRepairPreclosure(preclosure *MembershipRepairPreclosure) *MembershipRepairPreclosure {
	if preclosure == nil {
		return nil
	}
	copy := *preclosure
	return &copy
}

// InstallMembershipRepair retains one immutable same-generation repair scope.
// It is idempotent only for the exact authorization and never replaces prior
// repair history.
func (l *Ledger) InstallMembershipRepair(authorization MembershipRepairAuthorization) error {
	if l == nil || authorization.Validate() != nil || authorization.SourceClusterID != l.SourceClusterID || l.ClosedBy != "" {
		return fmt.Errorf("fencing: invalid membership-repair authorization")
	}
	run, ok := l.Runs[authorization.OldIdentity.RunID]
	if !ok || run.Identity != authorization.OldIdentity || run.Exited || run.Stop != nil || run.Drain != nil || run.Identity.WALIdentity == "" {
		return fmt.Errorf("fencing: old membership-repair runtime is not live and exact")
	}
	if l.Bootstrap != nil && (l.Bootstrap.RootUID != authorization.RootUID || l.Bootstrap.StatefulSetUID != authorization.StatefulSetUID) {
		return fmt.Errorf("fencing: repair differs from retained bootstrap binding")
	}
	if previous, ok := l.MembershipRepairHistory[authorization.OperationID]; ok {
		if reflect.DeepEqual(previous.Authorization, authorization) {
			return nil
		}
		return fmt.Errorf("fencing: repair operation ID was reused")
	}
	if l.MembershipRepair != nil {
		if reflect.DeepEqual(l.MembershipRepair.Authorization, authorization) {
			return nil
		}
		if l.MembershipRepair.Progress.Stage != MembershipRepairComplete && l.MembershipRepair.Progress.Stage != MembershipRepairCancelled {
			return ErrPending
		}
		if l.MembershipRepairHistory == nil {
			l.MembershipRepairHistory = make(map[string]MembershipRepairRecord)
		}
		previous := *l.MembershipRepair
		l.MembershipRepairHistory[previous.Authorization.OperationID] = MembershipRepairRecord{
			Authorization: cloneMembershipRepairAuthorization(previous.Authorization),
			Progress:      cloneMembershipRepairProgress(previous.Progress),
			Preclosure:    cloneMembershipRepairPreclosure(previous.Preclosure),
		}
	}
	l.MembershipRepair = &MembershipRepairRecord{
		Authorization: cloneMembershipRepairAuthorization(authorization),
		Progress:      MembershipRepairProgress{Revision: 1, Stage: MembershipRepairAdmitted},
	}
	return nil
}

// AppendMembershipRepairCandidate records a distinct Pod assignment without
// overwriting the logical repair or the previous candidate incarnation.
func (l *Ledger) AppendMembershipRepairCandidate(candidate MembershipRepairCandidate) error {
	if l == nil || l.MembershipRepair == nil || l.MembershipRepair.Progress.Stage == MembershipRepairComplete || l.MembershipRepair.Progress.Stage == MembershipRepairCancelled || candidate.Assignment.Validate() != nil {
		return fmt.Errorf("fencing: invalid repair candidate")
	}
	record := l.MembershipRepair
	a := record.Authorization
	assignment := candidate.Assignment
	canonical, err := ReplacementNodeID(assignment.PodUID)
	if err != nil || assignment.Purpose != BootstrapLearner || assignment.NodeID != canonical || assignment.OperationID == a.OperationID ||
		assignment.RootUID != a.RootUID || assignment.StatefulSetUID != a.StatefulSetUID || assignment.LedgerUID != a.LedgerUID || assignment.SourceClusterID != a.SourceClusterID ||
		assignment.NodeID == a.OldIdentity.NodeID || assignment.PodUID == a.OldIdentity.PodUID {
		return fmt.Errorf("fencing: repair candidate is not a fresh authorized incarnation")
	}
	if a.BootstrapMembershipHash != "" && (assignment.MembershipHash != a.BootstrapMembershipHash || assignment.ProfileHash != a.ProfileHash || assignment.StoreIdentityHash != a.StoreIdentityHash || assignment.Durability != a.Durability || assignment.PublicMember.ID != assignment.NodeID) {
		return fmt.Errorf("fencing: repair candidate source binding differs")
	}
	if candidate.Member != nil && !validRepairCandidateMember(assignment, *candidate.Member) {
		return fmt.Errorf("fencing: repair candidate member differs from assignment")
	}
	for _, old := range record.Progress.Candidates {
		if old.Assignment.PodUID == assignment.PodUID {
			if reflect.DeepEqual(old, candidate) {
				return nil
			}
			return fmt.Errorf("fencing: repair candidate assignment is immutable")
		}
		if old.Assignment.PodName == assignment.PodName {
			return ErrPending
		}
	}
	if candidate.Member != nil {
		return fmt.Errorf("fencing: candidate must be assigned before runtime binding")
	}
	record.Progress.Candidates = append(record.Progress.Candidates, candidate)
	record.Progress.Revision++
	return nil
}

// BootstrapLearnerStatus returns only the exact active repair assignment for
// this live Pod UID. It is read-only and never selects another slot's candidate.
func (l *Ledger) BootstrapLearnerStatus(podUID string) (BootstrapStatusResult, error) {
	if l == nil || l.MembershipRepair == nil || podUID == "" {
		return BootstrapStatusResult{}, ErrPending
	}
	progress := l.MembershipRepair.Progress
	if progress.Stage != MembershipRepairAdmitted && progress.Stage != MembershipRepairCandidateBound {
		return BootstrapStatusResult{}, ErrClosed
	}
	for i := range progress.Candidates {
		candidate := &progress.Candidates[i]
		if candidate.Assignment.PodUID != podUID {
			continue
		}
		assignment := candidate.Assignment
		result := BootstrapStatusResult{Assignment: &assignment, AssignmentSetHash: BootstrapAssignmentSetHash([]BootstrapAssignment{assignment})}
		for _, attempt := range candidate.Attempts {
			copyAttempt := cloneInitAttempt(attempt)
			result.Attempts = append(result.Attempts, copyAttempt)
		}
		return result, nil
	}
	return BootstrapStatusResult{AssignmentSetHash: BootstrapAssignmentSetHash(nil)}, nil
}

// BeginMembershipRepairInit persists the actual prepare-container execution
// against one exact learner assignment, not the initial bootstrap operation.
func (l *Ledger) BeginMembershipRepairInit(assignment BootstrapAssignment, attemptID string, scope InitExecutionScope) (InitAttempt, error) {
	if l == nil || l.MembershipRepair == nil || assignment.Validate() != nil || assignment.Purpose != BootstrapLearner || attemptID == "" || scope.ValidateFor(assignment) != nil {
		return InitAttempt{}, fmt.Errorf("fencing: invalid learner init begin request")
	}
	record := l.MembershipRepair
	if record.Progress.Stage != MembershipRepairAdmitted {
		return InitAttempt{}, ErrClosed
	}
	var candidate *MembershipRepairCandidate
	for i := range record.Progress.Candidates {
		if record.Progress.Candidates[i].Assignment.PodUID == assignment.PodUID {
			candidate = &record.Progress.Candidates[i]
			break
		}
	}
	if candidate == nil || candidate.Assignment != assignment {
		return InitAttempt{}, ErrPending
	}
	if old, ok := candidate.Attempts[attemptID]; ok {
		if old.Assignment == assignment && old.Scope == scope {
			return cloneInitAttempt(old), nil
		}
		return InitAttempt{}, fmt.Errorf("fencing: learner init attempt ID was reused")
	}
	for _, prior := range l.allInitAttempts() {
		if prior.AttemptID == attemptID {
			return InitAttempt{}, fmt.Errorf("fencing: learner init attempt ID was reused")
		}
	}
	for _, prior := range candidate.Attempts {
		if prior.Scope.ContainerID == scope.ContainerID || len(prior.Outcomes) == 0 || prior.Terminal == nil {
			return InitAttempt{}, ErrPending
		}
	}
	for _, prior := range l.allInitAttempts() {
		if prior.Scope.ContainerID == scope.ContainerID {
			return InitAttempt{}, ErrPending
		}
	}
	if candidate.Attempts == nil {
		candidate.Attempts = make(map[string]InitAttempt)
	}
	attempt := InitAttempt{AttemptID: attemptID, Assignment: assignment, Scope: scope}
	candidate.Attempts[attemptID] = attempt
	record.Progress.Revision++
	return attempt, nil
}

// RecordMembershipRepairInitOutcome retains one immutable result for the
// exact previously observed learner init execution.
func (l *Ledger) RecordMembershipRepairInitOutcome(assignment BootstrapAssignment, attemptID string, outcome InitAttemptOutcome) error {
	if l == nil || l.MembershipRepair == nil || assignment.Validate() != nil || assignment.Purpose != BootstrapLearner || attemptID == "" {
		return fmt.Errorf("fencing: invalid learner init outcome request")
	}
	if err := outcome.ValidateForAssignment(assignment); err != nil {
		return err
	}
	record := l.MembershipRepair
	if record.Progress.Stage != MembershipRepairAdmitted {
		return ErrClosed
	}
	for i := range record.Progress.Candidates {
		candidate := &record.Progress.Candidates[i]
		if candidate.Assignment.PodUID != assignment.PodUID {
			continue
		}
		if candidate.Assignment != assignment {
			return ErrPending
		}
		attempt, ok := candidate.Attempts[attemptID]
		if !ok || attempt.Assignment != assignment {
			return ErrPending
		}
		if len(attempt.Outcomes) != 0 {
			if attempt.Outcomes[len(attempt.Outcomes)-1] == outcome {
				return nil
			}
			return fmt.Errorf("fencing: learner init outcome is immutable")
		}
		attempt.Outcomes = append(attempt.Outcomes, outcome)
		candidate.Attempts[attemptID] = attempt
		record.Progress.Revision++
		return nil
	}
	return ErrPending
}

// RecordMembershipRepairInitTerminal stores authority-observed termination
// for one learner init ContainerID; caller outcomes cannot supply this proof.
func (l *Ledger) RecordMembershipRepairInitTerminal(assignment BootstrapAssignment, attemptID string, terminal InitTerminalEvidence) error {
	if l == nil || l.MembershipRepair == nil || assignment.Validate() != nil || assignment.Purpose != BootstrapLearner || attemptID == "" || terminal.Reason == "" {
		return ErrPending
	}
	for i := range l.MembershipRepair.Progress.Candidates {
		candidate := &l.MembershipRepair.Progress.Candidates[i]
		if candidate.Assignment.PodUID != assignment.PodUID {
			continue
		}
		if candidate.Assignment != assignment {
			return ErrPending
		}
		attempt, ok := candidate.Attempts[attemptID]
		if !ok || attempt.Assignment != assignment || terminal.ContainerID != attempt.Scope.ContainerID || terminal.ImageID != attempt.Scope.ImageID {
			return ErrPending
		}
		if attempt.Terminal != nil {
			if *attempt.Terminal == terminal {
				return nil
			}
			return fmt.Errorf("fencing: learner init terminal evidence changed")
		}
		attempt.Terminal = &terminal
		candidate.Attempts[attemptID] = attempt
		l.MembershipRepair.Progress.Revision++
		return nil
	}
	return ErrPending
}

func (l *Ledger) allInitAttempts() []InitAttempt {
	var attempts []InitAttempt
	if l != nil && l.Bootstrap != nil {
		for _, operation := range l.Bootstrap.Operations {
			for _, attempt := range operation.Attempts {
				attempts = append(attempts, attempt)
			}
		}
	}
	if l != nil && l.MembershipRepair != nil {
		for _, candidate := range l.MembershipRepair.Progress.Candidates {
			for _, attempt := range candidate.Attempts {
				attempts = append(attempts, attempt)
			}
		}
	}
	if l != nil {
		for _, record := range l.MembershipRepairHistory {
			for _, candidate := range record.Progress.Candidates {
				for _, attempt := range candidate.Attempts {
					attempts = append(attempts, attempt)
				}
			}
		}
	}
	return attempts
}

func cloneInitAttempt(attempt InitAttempt) InitAttempt {
	attempt.Outcomes = append([]InitAttemptOutcome(nil), attempt.Outcomes...)
	if attempt.Terminal != nil {
		terminal := *attempt.Terminal
		attempt.Terminal = &terminal
	}
	return attempt
}

func validRepairCandidateMember(assignment BootstrapAssignment, member quepaxa.Member) bool {
	return string(member.ID) == assignment.NodeID && member.WALIdentity != "" && member.PublicKey != (quepaxa.PublicKey{}) && member.URL != "" && member.PeerURL != ""
}

// BindMembershipRepairCandidate records the exact post-Bind public member.
// Rebinding the same Pod UID to another WAL or key is rejected.
func (l *Ledger) BindMembershipRepairCandidate(podUID string, member quepaxa.Member) error {
	if l == nil || l.MembershipRepair == nil || l.MembershipRepair.Progress.Stage == MembershipRepairComplete || l.MembershipRepair.Progress.Stage == MembershipRepairCancelled || podUID == "" {
		return fmt.Errorf("fencing: repair candidate assignment is unavailable")
	}
	for i := range l.MembershipRepair.Progress.Candidates {
		candidate := &l.MembershipRepair.Progress.Candidates[i]
		if candidate.Assignment.PodUID != podUID {
			continue
		}
		if !validRepairCandidateMember(candidate.Assignment, member) {
			return fmt.Errorf("fencing: bound repair member differs from assignment")
		}
		if candidate.Member != nil {
			if *candidate.Member == member {
				return nil
			}
			return fmt.Errorf("fencing: repair candidate WAL and public identity are immutable")
		}
		copy := member
		candidate.Member = &copy
		l.MembershipRepair.Progress.Revision++
		return nil
	}
	return fmt.Errorf("fencing: repair candidate assignment is unavailable")
}

// AppendMembershipRepairPhase appends one exact REMOVE or ADD request. It
// never changes an earlier round or conflates missing abort evidence with 0.
func (l *Ledger) AppendMembershipRepairPhase(phase MembershipRepairPhase) error {
	if l == nil || l.MembershipRepair == nil || l.MembershipRepair.Progress.Stage == MembershipRepairComplete || l.MembershipRepair.Progress.Stage == MembershipRepairCancelled {
		return ErrPending
	}
	record := l.MembershipRepair
	var candidate *MembershipRepairCandidate
	for i := range record.Progress.Candidates {
		if record.Progress.Candidates[i].Assignment.PodUID == phase.CandidatePodUID {
			candidate = &record.Progress.Candidates[i]
			break
		}
	}
	if candidate == nil || phase.Validate(record.Authorization, *candidate) != nil {
		return fmt.Errorf("fencing: invalid repair phase request")
	}
	for _, old := range record.Progress.Phases {
		if old.Change.OperationID == phase.Change.OperationID {
			if reflect.DeepEqual(old, phase) {
				return nil
			}
			return fmt.Errorf("fencing: repair phase operation ID was reused")
		}
	}
	if phase.Kind == MembershipRepairRemove && record.Progress.Stage != MembershipRepairOldClosed {
		return ErrPending
	}
	if phase.Kind == MembershipRepairAdd && record.Progress.Stage != MembershipRepairRemoved {
		return ErrPending
	}
	record.Progress.Phases = append(record.Progress.Phases, cloneMembershipRepairPhase(phase))
	record.Progress.Revision++
	return nil
}

// AdvanceMembershipRepair changes only mutable progress. Authorization,
// candidate assignments, and phase requests remain retained and exact.
func (l *Ledger) AdvanceMembershipRepair(operationID string, expectedRevision uint64, next MembershipRepairStage) error {
	if l == nil || l.MembershipRepair == nil || operationID != l.MembershipRepair.Authorization.OperationID {
		return ErrPending
	}
	progress := &l.MembershipRepair.Progress
	if progress.Revision != expectedRevision || progress.Stage == MembershipRepairComplete || progress.Stage == MembershipRepairCancelled {
		return ErrPending
	}
	valid := false
	switch progress.Stage {
	case MembershipRepairAdmitted:
		valid = next == MembershipRepairCandidateBound && len(progress.Candidates) > 0 && progress.Candidates[len(progress.Candidates)-1].Member != nil
	case MembershipRepairCandidateBound:
		valid = next == MembershipRepairOldClosed
	case MembershipRepairOldClosed:
		valid = next == MembershipRepairRemoved && hasRepairPhase(progress.Phases, MembershipRepairRemove)
	case MembershipRepairRemoved:
		valid = next == MembershipRepairAdded && hasRepairPhase(progress.Phases, MembershipRepairAdd)
	case MembershipRepairAdded:
		valid = next == MembershipRepairComplete && membershipRepairCompletionAccepted(l.MembershipRepair)
	}
	if !valid {
		return ErrPending
	}
	progress.Stage = next
	progress.Revision++
	return nil
}

func membershipRepairCompletionAccepted(record *MembershipRepairRecord) bool {
	if record == nil || record.Progress.Completion == nil || record.Progress.Completion.Accepted == nil {
		return false
	}
	attempt := record.Progress.Completion
	request, response := attempt.Request, *attempt.Accepted
	if record.Authorization.Validate() != nil || network.ValidateMembershipRepairCompletionRequest(request) != nil || response.MAC == "" || response.Outcome != "completed" ||
		request.RepairOperationID != record.Authorization.OperationID || request.RepairBindingDigest != MembershipRepairAuthorizationHash(record.Authorization) ||
		request.ScopeDigest != MembershipRepairCompletionScopeDigest(request) {
		return false
	}
	digest, err := network.MembershipRepairCompletionRequestDigest(request)
	if err != nil || response.Version != request.Version || response.Action != request.Action || response.Challenge != request.Challenge ||
		response.AttemptRevision != request.AttemptRevision || response.ScopeDigest != request.ScopeDigest || response.RequestDigest != digest ||
		response.ExecutorNodeID != request.ExecutorNodeID || response.ExecutorPodUID != request.ExecutorPodUID || response.ExecutorRunID != request.ExecutorRunID ||
		response.ExecutorWALIdentity != request.ExecutorWALIdentity || response.TerminalSlot != request.TerminalSlot || response.TerminalHash != request.TerminalHash {
		return false
	}
	var phase *MembershipRepairPhase
	var candidate *MembershipRepairCandidate
	var result *MembershipRepairPhaseResult
	for i := range record.Progress.Phases {
		if record.Progress.Phases[i].Hash == request.PhaseHash {
			phase = &record.Progress.Phases[i]
		}
	}
	for i := range record.Progress.Candidates {
		if record.Progress.Candidates[i].Assignment.PodUID == request.CandidatePodUID {
			candidate = &record.Progress.Candidates[i]
		}
	}
	for i := range record.Progress.Results {
		if record.Progress.Results[i].PhaseHash == request.PhaseHash {
			result = &record.Progress.Results[i]
		}
	}
	return phase != nil && phase.Kind == MembershipRepairAdd && phase.AbortSlot != nil && candidate != nil && candidate.Member != nil && result != nil &&
		result.Outcome == "completed" && phase.Change.OperationID == request.JournalOperationID && hashJSONMembershipRepairChange(phase.Change) == request.JournalRequestHash &&
		phase.Change.ExpectedConfigID == request.SourceConfigID && phase.Change.ClusterID == request.SourceClusterID && phase.Change.Add != nil &&
		*phase.Change.Add == request.CandidateMember && request.CandidateMember == *candidate.Member && request.CandidateBindRef == BootstrapAssignmentDigest(candidate.Assignment) &&
		request.AbortSlot != nil && *request.AbortSlot == *phase.AbortSlot && request.FreezeSlot == result.FreezeSlot && request.FreezeHash == result.FreezeHash &&
		request.TerminalSlot == result.TerminalSlot && request.TerminalHash == result.TerminalHash && request.JournalOperationID == result.JournalOperationID &&
		request.JournalRequestHash == result.JournalRequestHash && reflect.DeepEqual(request.Target, result.Target)
}

func hashJSONMembershipRepairChange(change network.MembershipChange) string {
	data, _ := json.Marshal(change)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// CancelMembershipRepair cancels only before a candidate or consensus phase is
// retained. Once a candidate exists, its init/runtime effects must be settled
// separately rather than hidden by cancelling the repair.
func (l *Ledger) CancelMembershipRepair(operationID string, expectedRevision uint64) error {
	if l == nil || l.MembershipRepair == nil || operationID != l.MembershipRepair.Authorization.OperationID {
		return ErrPending
	}
	progress := &l.MembershipRepair.Progress
	if progress.Revision != expectedRevision || (progress.Stage != MembershipRepairAdmitted && progress.Stage != MembershipRepairCandidateBound) || len(progress.Candidates) != 0 || len(progress.Phases) != 0 {
		return ErrPending
	}
	progress.Stage = MembershipRepairCancelled
	progress.Revision++
	return nil
}

func hasRepairPhase(phases []MembershipRepairPhase, kind MembershipRepairPhaseKind) bool {
	for _, phase := range phases {
		if phase.Kind == kind {
			return true
		}
	}
	return false
}

// ReadMembershipRepair is an existing-operation read, not discovery proof of
// absence. It permits pending-round survivors but requires exact open runtime
// binding and membership in the authorization's frozen voter set.
func (l *Ledger) ReadMembershipRepair(caller Identity) (MembershipRepairView, error) {
	if l == nil || l.MembershipRepair == nil {
		return MembershipRepairView{}, ErrNotFound
	}
	view := MembershipRepairView{Authorization: cloneMembershipRepairAuthorization(l.MembershipRepair.Authorization), Progress: cloneMembershipRepairProgress(l.MembershipRepair.Progress), Preclosure: cloneMembershipRepairPreclosure(l.MembershipRepair.Preclosure)}
	run, ok := l.Runs[caller.RunID]
	if !ok || run.Identity != caller || run.Exited || run.Stop != nil || run.Drain != nil {
		return MembershipRepairView{}, ErrClosed
	}
	if err := view.ValidateFor(caller); err != nil {
		return MembershipRepairView{}, err
	}
	if view.Progress.Stage == MembershipRepairComplete || view.Progress.Stage == MembershipRepairCancelled {
		return MembershipRepairView{}, ErrNotFound
	}
	return view, nil
}

// AuthorizeMembershipPhase allows only the exact phase retained by the active
// repair and only after the authority has advanced progress to its ready stage.
func (l *Ledger) AuthorizeMembershipPhase(caller Identity, requested MembershipRepairPhase) error {
	view, err := l.ReadMembershipRepair(caller)
	if err != nil {
		return err
	}
	phaseFound := false
	for _, phase := range view.Progress.Phases {
		if reflect.DeepEqual(phase, requested) {
			phaseFound = true
			break
		}
	}
	if !phaseFound {
		return ErrPending
	}
	ready := false
	switch requested.Kind {
	case MembershipRepairRemove:
		ready = view.Progress.Stage == MembershipRepairOldClosed || view.Progress.Stage == MembershipRepairRemoved || view.Progress.Stage == MembershipRepairAdded
	case MembershipRepairAdd:
		ready = view.Progress.Stage == MembershipRepairRemoved || view.Progress.Stage == MembershipRepairAdded
	}
	if !ready {
		return ErrPending
	}
	return nil
}

func cloneMembershipRepairAuthorization(authorization MembershipRepairAuthorization) MembershipRepairAuthorization {
	copy := authorization
	copy.Membership.Members = append([]quepaxa.Member(nil), authorization.Membership.Members...)
	return copy
}

func cloneMembershipRepairPhase(phase MembershipRepairPhase) MembershipRepairPhase {
	copy := phase
	if phase.AbortSlot != nil {
		slot := *phase.AbortSlot
		copy.AbortSlot = &slot
	}
	if phase.Change.Add != nil {
		member := *phase.Change.Add
		copy.Change.Add = &member
	}
	if phase.Change.Fence != nil {
		fence := *phase.Change.Fence
		copy.Change.Fence = &fence
	}
	return copy
}

func cloneMembershipRepairProgress(progress MembershipRepairProgress) MembershipRepairProgress {
	copy := progress
	copy.Candidates = append([]MembershipRepairCandidate(nil), progress.Candidates...)
	for i := range copy.Candidates {
		if progress.Candidates[i].Member != nil {
			member := *progress.Candidates[i].Member
			copy.Candidates[i].Member = &member
		}
		if progress.Candidates[i].Attempts != nil {
			copy.Candidates[i].Attempts = make(map[string]InitAttempt, len(progress.Candidates[i].Attempts))
			for attemptID, attempt := range progress.Candidates[i].Attempts {
				copy.Candidates[i].Attempts[attemptID] = cloneInitAttempt(attempt)
			}
		}
	}
	copy.Phases = append([]MembershipRepairPhase(nil), progress.Phases...)
	for i := range copy.Phases {
		copy.Phases[i] = cloneMembershipRepairPhase(progress.Phases[i])
	}
	copy.Results = append([]MembershipRepairPhaseResult(nil), progress.Results...)
	for i := range copy.Results {
		copy.Results[i].Target.Members = append([]quepaxa.Member(nil), progress.Results[i].Target.Members...)
	}
	if progress.Completion != nil {
		completion := *progress.Completion
		if completion.Request.AbortSlot != nil {
			abort := *completion.Request.AbortSlot
			completion.Request.AbortSlot = &abort
		}
		if progress.Completion.Accepted != nil {
			accepted := *progress.Completion.Accepted
			completion.Accepted = &accepted
		}
		copy.Completion = &completion
	}
	return copy
}

// BootstrapHistory is append-only pre-runtime authorization and init history.
// It is distinct from Runs: init never fabricates a runtime registration/WAL.
type BootstrapHistory struct {
	RootUID        string                        `json:"rootUID"`
	StatefulSetUID string                        `json:"statefulSetUID"`
	Operations     map[string]BootstrapOperation `json:"operations"`
}

type BootstrapOperation struct {
	OperationID       string                         `json:"operationID"`
	RequestHash       string                         `json:"requestHash"`
	AuthorizationHash string                         `json:"authorizationHash"`
	Purpose           BootstrapPurpose               `json:"purpose"`
	RootUID           string                         `json:"rootUID"`
	StatefulSetUID    string                         `json:"statefulSetUID"`
	SourceClusterID   string                         `json:"sourceClusterID"`
	AdmissionClosed   bool                           `json:"admissionClosed,omitempty"`
	Complete          bool                           `json:"complete,omitempty"`
	Assignments       map[string]BootstrapAssignment `json:"assignments"` // keyed by Pod UID
	Attempts          map[string]InitAttempt         `json:"attempts"`    // keyed by per-process nonce
}

// InstallBootstrapOperation is create-only and idempotent for the same
// authorization digest. A later learner/DR operation receives a new key and
// cannot replace initial assignments or attempts.
func (l *Ledger) InstallBootstrapOperation(operation BootstrapOperation) error {
	if l == nil || operation.OperationID == "" || operation.RequestHash == "" || operation.AuthorizationHash == "" ||
		(operation.Purpose != BootstrapInitial && operation.Purpose != BootstrapLearner && operation.Purpose != BootstrapDRSuccessor) ||
		operation.RootUID == "" || operation.StatefulSetUID == "" || operation.SourceClusterID == "" ||
		l.SourceClusterID == "" || operation.SourceClusterID != l.SourceClusterID || l.ClosedBy != "" || operation.Assignments != nil || operation.Attempts != nil || operation.AdmissionClosed || operation.Complete {
		return fmt.Errorf("fencing: invalid bootstrap operation")
	}
	if l.Bootstrap == nil {
		l.Bootstrap = &BootstrapHistory{RootUID: operation.RootUID, StatefulSetUID: operation.StatefulSetUID, Operations: make(map[string]BootstrapOperation)}
	}
	if l.Bootstrap.RootUID != operation.RootUID || l.Bootstrap.StatefulSetUID != operation.StatefulSetUID {
		return fmt.Errorf("fencing: bootstrap generation binding differs")
	}
	if l.Bootstrap.Operations == nil {
		l.Bootstrap.Operations = make(map[string]BootstrapOperation)
	}
	if old, exists := l.Bootstrap.Operations[operation.OperationID]; exists {
		if old.OperationID == operation.OperationID && old.RequestHash == operation.RequestHash && old.AuthorizationHash == operation.AuthorizationHash && old.Purpose == operation.Purpose && old.RootUID == operation.RootUID && old.StatefulSetUID == operation.StatefulSetUID && old.SourceClusterID == operation.SourceClusterID {
			return nil
		}
		return fmt.Errorf("fencing: bootstrap operation authorization changed")
	}
	operation.Assignments = make(map[string]BootstrapAssignment)
	operation.Attempts = make(map[string]InitAttempt)
	l.Bootstrap.Operations[operation.OperationID] = operation
	return nil
}

// AssignBootstrap binds a Pod UID once to an existing retained operation.
func (l *Ledger) AssignBootstrap(assignment BootstrapAssignment) error {
	if l == nil || assignment.Validate() != nil || l.Bootstrap == nil ||
		l.Bootstrap.RootUID != assignment.RootUID || l.Bootstrap.StatefulSetUID != assignment.StatefulSetUID {
		return fmt.Errorf("fencing: invalid bootstrap assignment")
	}
	operation, ok := l.Bootstrap.Operations[assignment.OperationID]
	if !ok || operation.RequestHash != assignment.RequestHash || operation.AuthorizationHash != assignment.AuthorizationHash || operation.Purpose != assignment.Purpose || operation.RootUID != assignment.RootUID || operation.StatefulSetUID != assignment.StatefulSetUID || operation.SourceClusterID != assignment.SourceClusterID || operation.AdmissionClosed || operation.Complete {
		return ErrClosed
	}
	if assignment.SourceClusterID != l.SourceClusterID {
		return fmt.Errorf("fencing: bootstrap assignment source differs")
	}
	if assignment.Purpose == BootstrapDRSuccessor && BootstrapDRAuthorizationHash(assignment) != assignment.AuthorizationHash {
		return fmt.Errorf("fencing: DR approval is not covered by bootstrap authorization")
	}
	if l.ClosedBy != "" {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[assignment.NodeID]; closed {
		return ErrClosed
	}
	if operation.Assignments == nil {
		operation.Assignments = make(map[string]BootstrapAssignment)
	}
	if old, exists := operation.Assignments[assignment.PodUID]; exists {
		if old == assignment {
			return nil
		}
		return fmt.Errorf("fencing: Pod UID bootstrap assignment is immutable")
	}
	for _, priorOperation := range l.Bootstrap.Operations {
		if priorOperation.OperationID == assignment.OperationID && assignment.Purpose == BootstrapDRSuccessor {
			for _, prior := range priorOperation.Assignments {
				if prior.Purpose != BootstrapDRSuccessor || prior.DRApproval != assignment.DRApproval || prior.AuthorizationHash != assignment.AuthorizationHash {
					return fmt.Errorf("fencing: DR approval changed within bootstrap operation")
				}
			}
		}
		if old, exists := priorOperation.Assignments[assignment.PodUID]; exists && old != assignment {
			return fmt.Errorf("fencing: Pod UID already has another bootstrap assignment")
		}
		for _, old := range priorOperation.Assignments {
			if old.PodName == assignment.PodName && old.PodUID != assignment.PodUID && !priorOperation.Complete {
				return ErrPending
			}
		}
	}
	operation.Assignments[assignment.PodUID] = assignment
	l.Bootstrap.Operations[assignment.OperationID] = operation
	return nil
}

// BeginInit records an authority-observed actual init execution before local
// preparation effects. Replaying the same attempt is idempotent; a new nonce
// cannot take over an existing ContainerID or unresolved attempt.
func (l *Ledger) BeginInit(assignment BootstrapAssignment, attemptID string, scope InitExecutionScope) (InitAttempt, error) {
	if l == nil || assignment.Validate() != nil || attemptID == "" || scope.ValidateFor(assignment) != nil || l.Bootstrap == nil {
		return InitAttempt{}, fmt.Errorf("fencing: invalid init begin request")
	}
	operation, ok := l.Bootstrap.Operations[assignment.OperationID]
	if !ok {
		return InitAttempt{}, ErrPending
	}
	if old, exists := operation.Attempts[attemptID]; exists {
		if old.Assignment == assignment && old.Scope == scope {
			return old, nil
		}
		return InitAttempt{}, fmt.Errorf("fencing: init attempt ID was reused")
	}
	if operation.AdmissionClosed || operation.Complete {
		return InitAttempt{}, ErrClosed
	}
	if l.ClosedBy != "" {
		return InitAttempt{}, ErrClosed
	}
	if _, closed := l.ClosedNodes[assignment.NodeID]; closed {
		return InitAttempt{}, ErrClosed
	}
	if operation.Assignments[assignment.PodUID] != assignment {
		return InitAttempt{}, ErrPending
	}
	for _, priorOperation := range l.Bootstrap.Operations {
		for _, old := range priorOperation.Attempts {
			if old.Scope.ContainerID == scope.ContainerID {
				return InitAttempt{}, ErrPending
			}
			if old.Assignment.PodUID == assignment.PodUID && (len(old.Outcomes) == 0 || old.Terminal == nil) {
				return InitAttempt{}, ErrPending
			}
		}
	}
	if operation.Attempts == nil {
		operation.Attempts = make(map[string]InitAttempt)
	}
	attempt := InitAttempt{AttemptID: attemptID, Assignment: assignment, Scope: scope}
	operation.Attempts[attemptID] = attempt
	l.Bootstrap.Operations[assignment.OperationID] = operation
	return attempt, nil
}

// RecordInitOutcome appends one live result. It never settles the assignment;
// terminal observation is a separate authority-controlled ledger mutation.
func (l *Ledger) RecordInitOutcome(assignment BootstrapAssignment, attemptID string, outcome InitAttemptOutcome) error {
	if err := outcome.ValidateForAssignment(assignment); err != nil {
		return err
	}
	operation, ok := l.bootstrapOperation(assignment)
	if !ok {
		return ErrPending
	}
	attempt, ok := operation.Attempts[attemptID]
	if !ok || attempt.Assignment != assignment {
		return ErrPending
	}
	if len(attempt.Outcomes) > 0 {
		if attempt.Outcomes[len(attempt.Outcomes)-1] == outcome {
			return nil
		}
		return fmt.Errorf("fencing: init outcome is immutable")
	}
	attempt.Outcomes = append(attempt.Outcomes, outcome)
	operation.Attempts[attemptID] = attempt
	l.Bootstrap.Operations[assignment.OperationID] = operation
	return nil
}

// RecordInitTerminal stores exact runtime termination observed by the
// authority. It does not infer termination from a caller's outcome or absence.
func (l *Ledger) RecordInitTerminal(assignment BootstrapAssignment, attemptID string, terminal InitTerminalEvidence) error {
	operation, ok := l.bootstrapOperation(assignment)
	if !ok {
		return ErrPending
	}
	attempt, ok := operation.Attempts[attemptID]
	if !ok || attempt.Assignment != assignment || terminal.ContainerID != attempt.Scope.ContainerID || terminal.ImageID != attempt.Scope.ImageID || terminal.Reason == "" {
		return ErrPending
	}
	if attempt.Terminal != nil {
		if *attempt.Terminal == terminal {
			return nil
		}
		return fmt.Errorf("fencing: init terminal evidence changed")
	}
	attempt.Terminal = &terminal
	operation.Attempts[attemptID] = attempt
	l.Bootstrap.Operations[assignment.OperationID] = operation
	return nil
}

func (l *Ledger) bootstrapOperation(assignment BootstrapAssignment) (BootstrapOperation, bool) {
	if l == nil || assignment.Validate() != nil || l.Bootstrap == nil {
		return BootstrapOperation{}, false
	}
	operation, ok := l.Bootstrap.Operations[assignment.OperationID]
	if !ok || operation.Assignments[assignment.PodUID] != assignment {
		return BootstrapOperation{}, false
	}
	return operation, true
}

func (l *Ledger) CloseBootstrapAdmission(operationID, requestHash string) error {
	if l == nil || l.Bootstrap == nil || operationID == "" || requestHash == "" {
		return ErrPending
	}
	operation, ok := l.Bootstrap.Operations[operationID]
	if !ok || operation.RequestHash != requestHash {
		return ErrPending
	}
	operation.AdmissionClosed = true
	l.Bootstrap.Operations[operationID] = operation
	return nil
}

// BootstrapStatus returns one exact Pod assignment and its retained attempts.
// It is a read-only projection and never manufactures a missing record.
func (l *Ledger) BootstrapStatus(operationID, podUID string) (BootstrapStatusResult, error) {
	if l == nil || l.Bootstrap == nil || operationID == "" || podUID == "" {
		return BootstrapStatusResult{}, ErrPending
	}
	operation, ok := l.Bootstrap.Operations[operationID]
	if !ok {
		return BootstrapStatusResult{}, ErrPending
	}
	assignments := make([]BootstrapAssignment, 0, len(operation.Assignments))
	for _, item := range operation.Assignments {
		assignments = append(assignments, item)
	}
	assignment, ok := operation.Assignments[podUID]
	if !ok {
		return BootstrapStatusResult{AssignmentSetHash: BootstrapAssignmentSetHash(assignments), AdmissionClosed: operation.AdmissionClosed, Complete: operation.Complete}, nil
	}
	result := BootstrapStatusResult{Assignment: &assignment, AssignmentSetHash: BootstrapAssignmentSetHash(assignments), AdmissionClosed: operation.AdmissionClosed, Complete: operation.Complete}
	for _, attempt := range operation.Attempts {
		if attempt.Assignment.PodUID != podUID {
			continue
		}
		copyAttempt := attempt
		copyAttempt.Outcomes = append([]InitAttemptOutcome(nil), attempt.Outcomes...)
		if attempt.Terminal != nil {
			terminal := *attempt.Terminal
			copyAttempt.Terminal = &terminal
		}
		result.Attempts = append(result.Attempts, copyAttempt)
	}
	return result, nil
}

// PeerClosureReservation is one CAS-serialized target closure in a source
// membership round. It is durable before an executable RhizaFence is created.
type PeerClosureReservation struct {
	OperationID  string              `json:"operationID"`
	RequestHash  string              `json:"requestHash"`
	ConfigID     uint64              `json:"configID"`
	AbortSlot    uint64              `json:"abortSlot"`
	TargetNodeID string              `json:"targetNodeID"`
	TargetWAL    string              `json:"targetWAL"`
	Voters       []string            `json:"voters"`
	Targets      []PeerClosureTarget `json:"targets"`
}

type Run struct {
	Identity       Identity      `json:"identity"`
	Stop           *StopRequest  `json:"stop,omitempty"`
	Drain          *DrainReceipt `json:"drain,omitempty"`
	Exited         bool          `json:"exited,omitempty"`
	NeverBoundExit *StopRequest  `json:"neverBoundExit,omitempty"`
}

func (l *Ledger) Register(identity Identity) error {
	if identity.SourceClusterID != l.SourceClusterID || identity.WALIdentity != "" || identity.Validate() != nil {
		return fmt.Errorf("fencing: invalid registration")
	}
	if l.Bootstrap != nil && !l.BootstrapInitReady(identity) {
		return ErrPending
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
		if old.Identity == identity && old.Stop == nil {
			return nil
		}
		return fmt.Errorf("fencing: run ID already registered")
	}
	// A Pod UID is one immutable mapping to a consensus identity and Secret.
	// A new UID at the same ordinal is a new identity, but cannot register until
	// every prior incarnation of that ordinal has terminal evidence.
	for _, old := range l.Runs {
		if old.Identity.PodUID == identity.PodUID && old.Identity.RunID != identity.RunID && !runTerminated(old) {
			return fmt.Errorf("fencing: previous run for this Pod UID has no terminal receipt")
		}
		if old.Identity.PodUID == identity.PodUID && (old.Identity.NodeID != identity.NodeID || PodNameForIdentity(old.Identity) != PodNameForIdentity(identity) || old.Identity.CredentialUID != identity.CredentialUID || old.Identity.CredentialName != identity.CredentialName) {
			return fmt.Errorf("fencing: Pod UID is already bound to another identity or credential")
		}
		if PodNameForIdentity(old.Identity) == PodNameForIdentity(identity) && old.Identity.PodUID != identity.PodUID && !runTerminated(old) && !l.authorizesRepairLearner(identity, old.Identity) {
			return fmt.Errorf("fencing: earlier incarnation has no clean termination proof")
		}
	}
	l.Runs[identity.RunID] = Run{Identity: identity}
	return nil
}

func (l *Ledger) authorizesRepairLearner(identity, prior Identity) bool {
	if l == nil || l.MembershipRepair == nil {
		return false
	}
	record := l.MembershipRepair
	if record.Progress.Stage != MembershipRepairAdmitted || prior != record.Authorization.OldIdentity {
		return false
	}
	for _, candidate := range record.Progress.Candidates {
		a := candidate.Assignment
		if candidate.Member == nil && a.Validate() == nil && a.Purpose == BootstrapLearner &&
			a.OperationID != record.Authorization.OperationID && a.RootUID == record.Authorization.RootUID &&
			a.StatefulSetUID == record.Authorization.StatefulSetUID && a.LedgerUID == record.Authorization.LedgerUID &&
			a.SourceClusterID == identity.SourceClusterID && a.PodName == PodNameForIdentity(identity) &&
			a.PodUID == identity.PodUID && a.NodeID == identity.NodeID &&
			a.CredentialName == identity.CredentialName && a.CredentialUID == identity.CredentialUID &&
			a.PodUID != prior.PodUID && a.NodeID != prior.NodeID {
			return true
		}
	}
	return false
}

func (l *Ledger) Bind(identity Identity) error {
	run, exists := l.Runs[identity.RunID]
	if !exists || identity.WALIdentity == "" {
		return fmt.Errorf("fencing: unknown WAL registration")
	}
	if l.Bootstrap != nil && !l.BootstrapInitReady(identity) {
		return ErrPending
	}
	old := identity
	old.WALIdentity = run.Identity.WALIdentity
	if old != run.Identity || (run.Identity.WALIdentity != "" && run.Identity.WALIdentity != identity.WALIdentity) {
		return fmt.Errorf("fencing: binding changed")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
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
	if reservation := l.PeerClosure; reservation != nil {
		if reservation.OperationID != operationID || reservation.RequestHash != requestHash || len(nodeIDs) == 0 {
			return ErrPending
		}
		for _, nodeID := range nodeIDs {
			if nodeID != reservation.TargetNodeID {
				return ErrPending
			}
		}
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

// ReservePeerClosure atomically closes admission for the exact target and
// serializes it against another voter closure in the same source ledger.
func (l *Ledger) ReservePeerClosure(request PeerClosureReservation) error {
	if request.OperationID == "" || request.RequestHash == "" || request.ConfigID == 0 || request.TargetNodeID == "" || request.TargetWAL == "" || len(request.Voters) == 0 || len(request.Targets) == 0 {
		return fmt.Errorf("fencing: incomplete peer-closure reservation")
	}
	voters := make(map[string]bool, len(request.Voters))
	for _, nodeID := range request.Voters {
		if nodeID == "" || voters[nodeID] {
			return fmt.Errorf("fencing: invalid peer-closure voter inventory")
		}
		voters[nodeID] = true
	}
	if !voters[request.TargetNodeID] {
		return fmt.Errorf("fencing: peer-closure target is not a current voter")
	}
	targets := make(map[peerClosureTargetKey]PeerClosureTarget, len(request.Targets))
	requestedWAL := false
	for _, target := range request.Targets {
		key := peerClosureTargetIdentity(target)
		if target.NodeID != request.TargetNodeID || ValidatePodBinding(target.NodeID, target.Pod, target.PodUID) != nil || (target.WALIdentity != "" && target.WALIdentity != request.TargetWAL) {
			return fmt.Errorf("fencing: invalid peer-closure target inventory")
		}
		if old, exists := targets[key]; exists {
			if old != target {
				return fmt.Errorf("fencing: conflicting peer-closure runtime inventory")
			}
			return fmt.Errorf("fencing: duplicate peer-closure target inventory")
		}
		if target.WALIdentity == request.TargetWAL {
			requestedWAL = true
		}
		targets[key] = target
	}
	if !requestedWAL {
		return fmt.Errorf("fencing: requested target WAL is absent from retained history")
	}
	// CloseNodes and Register share this CAS. The inventory must cover every run
	// now, so a concurrent registration cannot escape the eventual fence.
	ledgerTargets := make(map[peerClosureTargetKey]bool)
	for _, run := range l.Runs {
		if run.Identity.NodeID != request.TargetNodeID {
			continue
		}
		key := peerClosureTargetIdentity(PeerClosureTarget{NodeID: run.Identity.NodeID, Pod: PodNameForIdentity(run.Identity), PodUID: run.Identity.PodUID, WALIdentity: run.Identity.WALIdentity})
		if _, ok := targets[key]; !ok {
			return fmt.Errorf("fencing: peer-closure target inventory changed")
		}
		ledgerTargets[key] = true
	}
	if len(ledgerTargets) != len(targets) {
		return fmt.Errorf("fencing: peer-closure target inventory has unregistered incarnations")
	}

	if old := l.PeerClosure; old != nil {
		if samePeerClosureReservation(*old, request) {
			return l.CloseNodes(request.OperationID, request.RequestHash, []string{request.TargetNodeID})
		}
		if request.ConfigID <= old.ConfigID || voters[old.TargetNodeID] || !l.peerClosureTargetComplete(*old) {
			return ErrPending
		}
	}
	for nodeID, old := range l.ClosedNodes {
		if nodeID == request.TargetNodeID {
			if old.OperationID != request.OperationID || old.RequestHash != request.RequestHash {
				return ErrPending
			}
			continue
		}
		if voters[nodeID] || !l.closedNodeComplete(nodeID) {
			return ErrPending
		}
	}
	if requestTarget, exists := l.ClosedNodes[request.TargetNodeID]; exists && (requestTarget.OperationID != request.OperationID || requestTarget.RequestHash != request.RequestHash) {
		return ErrPending
	}
	// A successful source-generation close permanently supersedes peer closures.
	if l.ClosedBy != "" {
		return ErrClosed
	}
	previous := l.PeerClosure
	l.PeerClosure = nil
	if err := l.CloseNodes(request.OperationID, request.RequestHash, []string{request.TargetNodeID}); err != nil {
		l.PeerClosure = previous
		return err
	}
	copy := request
	copy.Voters = append([]string(nil), request.Voters...)
	copy.Targets = append([]PeerClosureTarget(nil), request.Targets...)
	l.PeerClosure = &copy
	return nil
}

func samePeerClosureReservation(a, b PeerClosureReservation) bool {
	if a.OperationID != b.OperationID || a.RequestHash != b.RequestHash || a.ConfigID != b.ConfigID || a.AbortSlot != b.AbortSlot || a.TargetNodeID != b.TargetNodeID || a.TargetWAL != b.TargetWAL || len(a.Voters) != len(b.Voters) || len(a.Targets) != len(b.Targets) {
		return false
	}
	for i := range a.Voters {
		if a.Voters[i] != b.Voters[i] {
			return false
		}
	}
	for i := range a.Targets {
		if a.Targets[i] != b.Targets[i] {
			return false
		}
	}
	return true
}

type peerClosureTargetKey struct {
	NodeID, Pod, PodUID, WALIdentity string
}

func peerClosureTargetIdentity(target PeerClosureTarget) peerClosureTargetKey {
	return peerClosureTargetKey{NodeID: target.NodeID, Pod: target.Pod, PodUID: target.PodUID, WALIdentity: target.WALIdentity}
}

func (l *Ledger) peerClosureTargetComplete(reservation PeerClosureReservation) bool {
	seen := make(map[peerClosureTargetKey]bool, len(reservation.Targets))
	for _, target := range reservation.Targets {
		seen[peerClosureTargetIdentity(target)] = false
	}
	for _, run := range l.Runs {
		if run.Identity.NodeID != reservation.TargetNodeID {
			continue
		}
		target := PeerClosureTarget{NodeID: run.Identity.NodeID, Pod: PodNameForIdentity(run.Identity), PodUID: run.Identity.PodUID, WALIdentity: run.Identity.WALIdentity}
		key := peerClosureTargetIdentity(target)
		if _, ok := seen[key]; !ok || !runTerminated(run) {
			return false
		}
		seen[key] = true
	}
	for _, covered := range seen {
		if !covered {
			return false
		}
	}
	return true
}

func (l *Ledger) closedNodeComplete(nodeID string) bool {
	seen := false
	for _, run := range l.Runs {
		if run.Identity.NodeID == nodeID {
			seen = true
			if !runTerminated(run) {
				return false
			}
		}
	}
	return seen
}

func runTerminated(run Run) bool {
	if !run.Exited || run.Stop == nil {
		return false
	}
	if run.Identity.WALIdentity == "" {
		return run.NeverBoundExit != nil && *run.NeverBoundExit == *run.Stop && run.Drain == nil
	}
	return run.Drain != nil && run.Drain.Request == *run.Stop && run.Drain.Clean()
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
	if !l.bootstrapInitComplete(wanted) {
		return false
	}
	for _, uid := range currentPodUIDs {
		if !registered[uid] {
			return false
		}
	}
	return true
}

// bootstrapInitComplete makes every retained assignment part of the same
// completion boundary as runtime Runs. A successful outcome is live Node
// evidence only; it must also have exact authority-observed terminal evidence.
func (l *Ledger) bootstrapInitComplete(wanted map[string]bool) bool {
	if l.Bootstrap == nil {
		return true
	}
	for _, operation := range l.Bootstrap.Operations {
		for _, assignment := range operation.Assignments {
			if len(wanted) != 0 && !wanted[assignment.NodeID] {
				continue
			}
			resolved := false
			materialized := false
			for _, attempt := range operation.Attempts {
				if attempt.Assignment.PodUID != assignment.PodUID {
					continue
				}
				if attempt.Assignment != assignment || len(attempt.Outcomes) != 1 || attempt.Terminal == nil ||
					attempt.Terminal.ContainerID != attempt.Scope.ContainerID || attempt.Terminal.ImageID != attempt.Scope.ImageID || attempt.Terminal.Reason == "" {
					return false
				}
				if attempt.Outcomes[0].ValidateForAssignment(assignment) != nil {
					return false
				}
				resolved = true
				if attempt.Outcomes[0].State == InitAttemptMaterialized && attempt.Terminal.ExitCode == 0 && attempt.Terminal.Reason == "Completed" {
					materialized = true
				}
			}
			if !resolved || !materialized {
				return false
			}
		}
	}
	return true
}

// BootstrapInitComplete reports whether retained init history is eligible for
// the supplied closure scope. It is a prerequisite for cooperative and
// independent fencing; external runtime proof cannot waive unresolved init.
func (l *Ledger) BootstrapInitComplete(nodeIDs []string) bool {
	wanted := make(map[string]bool, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID == "" || wanted[nodeID] {
			return false
		}
		wanted[nodeID] = true
	}
	return l.bootstrapInitComplete(wanted)
}

// BootstrapInitReady gates runtime Register/Bind only after a ledger has
// entered the bootstrap protocol. Legacy ledgers retain their old behavior;
// once bootstrap history exists, every new Pod UID needs its own assignment
// and completed init attempt before runtime admission.
func (l *Ledger) BootstrapInitReady(identity Identity) bool {
	if l == nil || l.Bootstrap == nil {
		return true
	}
	var matched *BootstrapAssignment
	for _, operation := range l.Bootstrap.Operations {
		for _, assignment := range operation.Assignments {
			if assignment.PodUID != identity.PodUID {
				continue
			}
			if matched != nil || assignment.SourceClusterID != identity.SourceClusterID || assignment.NodeID != identity.NodeID || assignment.PodName != PodNameForIdentity(identity) ||
				(identity.CredentialName != "" && assignment.CredentialName != identity.CredentialName) ||
				(identity.CredentialUID != "" && assignment.CredentialUID != identity.CredentialUID) {
				return false
			}
			copy := assignment
			matched = &copy
		}
	}
	if matched == nil {
		var retained *MembershipRepairCandidate
		consider := func(record *MembershipRepairRecord) bool {
			if record == nil || record.Progress.Stage == MembershipRepairCancelled {
				return true
			}
			for i := range record.Progress.Candidates {
				candidate := &record.Progress.Candidates[i]
				if candidate.Assignment.PodUID != identity.PodUID {
					continue
				}
				if retained != nil && !reflect.DeepEqual(*retained, *candidate) {
					return false
				}
				retained = candidate
			}
			return true
		}
		if !consider(l.MembershipRepair) {
			return false
		}
		for _, record := range l.MembershipRepairHistory {
			copy := record
			if !consider(&copy) {
				return false
			}
		}
		if retained == nil {
			return false
		}
		candidate := retained
		{
			a := candidate.Assignment
			if a.PodUID != identity.PodUID {
				return false
			}
			if a.Validate() != nil || a.Purpose != BootstrapLearner || a.SourceClusterID != identity.SourceClusterID || a.NodeID != identity.NodeID || a.PodName != PodNameForIdentity(identity) ||
				a.CredentialName != identity.CredentialName || a.CredentialUID != identity.CredentialUID {
				return false
			}
			resolved, materialized := false, false
			for _, attempt := range candidate.Attempts {
				if attempt.Assignment != a || len(attempt.Outcomes) != 1 || attempt.Terminal == nil || attempt.Terminal.ContainerID != attempt.Scope.ContainerID || attempt.Terminal.ImageID != attempt.Scope.ImageID || attempt.Terminal.Reason == "" || attempt.Outcomes[0].ValidateForAssignment(a) != nil {
					return false
				}
				resolved = true
				if attempt.Outcomes[0].State == InitAttemptMaterialized && attempt.Terminal.ExitCode == 0 && attempt.Terminal.Reason == "Completed" {
					materialized = true
				}
			}
			return resolved && materialized
		}
		return false
	}
	return l.bootstrapInitComplete(map[string]bool{matched.NodeID: true})
}
