package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// AuthorityMessage is sent only over authenticated TLS. The server authenticates
// the bound service account token independently of the claimed identity.
type AuthorityMessage struct {
	Action              string                                `json:"action"`
	Identity            Identity                              `json:"identity"`
	Drain               *DrainReceipt                         `json:"drain,omitempty"`
	Request             *StopRequest                          `json:"request,omitempty"`
	PeerClosure         *PeerClosureRequest                   `json:"peerClosure,omitempty"`
	BootstrapStatus     *BootstrapStatusRequest               `json:"bootstrapStatus,omitempty"`
	RuntimeAdmission    *BootstrapStatusRequest               `json:"runtimeAdmission,omitempty"`
	BeginInit           *BeginInitRequest                     `json:"beginInit,omitempty"`
	InitOutcome         *RecordInitOutcomeRequest             `json:"initOutcome,omitempty"`
	Completion          *BootstrapCompletion                  `json:"bootstrapCompletion,omitempty"`
	RepairStatus        *MembershipRepairStatusRequest        `json:"membershipRepairStatus,omitempty"`
	RepairPhase         *MembershipRepairAuthorizationRequest `json:"membershipRepairPhase,omitempty"`
	RepairReserve       *MembershipRepairReserveRequest       `json:"membershipRepairReserve,omitempty"`
	RepairQualification *MembershipRepairQualificationRequest `json:"membershipRepairQualification,omitempty"`
	RepairPhasePrepare  *MembershipRepairPhasePrepareRequest  `json:"membershipRepairPhasePrepare,omitempty"`
	RepairPhaseObserve  *MembershipRepairPhaseObserveRequest  `json:"membershipRepairPhaseObserve,omitempty"`
	RepairPhaseComplete *MembershipRepairPhaseObserveRequest  `json:"membershipRepairPhaseComplete,omitempty"`
}

// ValidateAuthorityMessageShape enforces the action-to-payload allowlist at
// both authority trust boundaries. Payload contents remain validated by the
// action-specific handlers.
func ValidateAuthorityMessageShape(message AuthorityMessage) error {
	allowed := ""
	switch message.Action {
	case "register", "bind", "stop":
	case "drain":
		allowed = "Drain"
	case "exit", "never-bound-exit":
		allowed = "Request"
	case "peer-closure-request", "peer-closure-status":
		allowed = "PeerClosure"
	case "bootstrap-status":
		allowed = "BootstrapStatus"
	case "bootstrap-runtime-admission":
		allowed = "RuntimeAdmission"
	case "bootstrap-complete":
		allowed = "Completion"
	case "init-begin":
		allowed = "BeginInit"
	case "init-outcome":
		allowed = "InitOutcome"
	case "membership-repair-status":
		allowed = "RepairStatus"
	case "membership-repair-reserve":
		allowed = "RepairReserve"
	case "membership-repair-authorize":
		allowed = "RepairPhase"
	case "membership-repair-qualify":
		allowed = "RepairQualification"
	case "membership-repair-phase-prepare":
		allowed = "RepairPhasePrepare"
	case "membership-repair-phase-observe":
		allowed = "RepairPhaseObserve"
	case "membership-repair-phase-complete":
		allowed = "RepairPhaseComplete"
	default:
		return fmt.Errorf("fencing: unknown authority action %q", message.Action)
	}
	payloads := []struct {
		name    string
		present bool
	}{
		{"Drain", message.Drain != nil},
		{"Request", message.Request != nil},
		{"PeerClosure", message.PeerClosure != nil},
		{"BootstrapStatus", message.BootstrapStatus != nil},
		{"RuntimeAdmission", message.RuntimeAdmission != nil},
		{"BeginInit", message.BeginInit != nil},
		{"InitOutcome", message.InitOutcome != nil},
		{"Completion", message.Completion != nil},
		{"RepairStatus", message.RepairStatus != nil},
		{"RepairPhase", message.RepairPhase != nil},
		{"RepairReserve", message.RepairReserve != nil},
		{"RepairQualification", message.RepairQualification != nil},
		{"RepairPhasePrepare", message.RepairPhasePrepare != nil},
		{"RepairPhaseObserve", message.RepairPhaseObserve != nil},
		{"RepairPhaseComplete", message.RepairPhaseComplete != nil},
	}
	for _, payload := range payloads {
		if payload.present != (payload.name == allowed) {
			return fmt.Errorf("fencing: authority action %q has an unexpected or missing %s payload", message.Action, payload.name)
		}
	}
	return nil
}

type AuthorityClient struct {
	URL       string
	TokenFile string
	Client    *http.Client
}

// InitAdmission is an unforgeable-in-package capability for one live init
// attempt returned by the authenticated authority. Its fields are deliberately
// private so callers cannot manufacture a DR self-member restore grant from a
// copied assignment or recovery-anchor receipt.
type InitAdmission struct {
	attempt InitAttempt
}

func (admission InitAdmission) Assignment() BootstrapAssignment { return admission.attempt.Assignment }
func (admission InitAdmission) Attempt() InitAttempt            { return admission.attempt }
func (admission InitAdmission) Scope() InitExecutionScope       { return admission.attempt.Scope }

func (admission InitAdmission) ValidateFor(assignment BootstrapAssignment) error {
	attempt := admission.attempt
	if attempt.AttemptID == "" || attempt.Assignment != assignment || attempt.Scope.ValidateFor(assignment) != nil || len(attempt.Outcomes) != 0 || attempt.Terminal != nil {
		return fmt.Errorf("fencing: init admission is absent, stale, or differs from assignment")
	}
	return nil
}

var _ PeerClosureAuthority = (*AuthorityClient)(nil)

func (a *AuthorityClient) call(ctx context.Context, message AuthorityMessage, result any) error {
	return a.callURL(ctx, a.URL, message, result)
}

func (a *AuthorityClient) callPeerClosure(ctx context.Context, endpointPath string, message AuthorityMessage, result any) error {
	u, err := url.Parse(a.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("fencing: HTTPS authority required")
	}
	if !strings.HasSuffix(u.Path, "/v1/supervisor") {
		return fmt.Errorf("fencing: configured authority URL must end in /v1/supervisor")
	}
	u.Path = strings.TrimSuffix(u.Path, "/v1/supervisor") + endpointPath
	u.RawPath = ""
	return a.callURL(ctx, u.String(), message, result)
}

func (a *AuthorityClient) callURL(ctx context.Context, endpoint string, message AuthorityMessage, result any) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("fencing: HTTPS authority required")
	}
	token, err := os.ReadFile(a.TokenFile)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(token)) == 0 {
		return fmt.Errorf("fencing: empty authority token")
	}
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	if a.Client == nil {
		return fmt.Errorf("fencing: bounded TLS client required")
	}
	// Never forward a projected token to a redirect destination.
	client := *a.Client
	if client.Timeout <= 0 || client.Timeout > 30*time.Second {
		client.Timeout = 30 * time.Second
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		return ErrPending
	}
	if resp.StatusCode == http.StatusGone {
		return ErrClosed
	}
	if resp.StatusCode == http.StatusNotFound && message.Action == "peer-closure-status" {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			return ErrUnavailable
		}
		return fmt.Errorf("fencing: authority HTTP %d", resp.StatusCode)
	}
	if result == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(data) > 65536 {
		return fmt.Errorf("fencing: authority response too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing authority response")
	}
	return nil
}

func (a *AuthorityClient) Register(ctx context.Context, id Identity) (Registration, error) {
	var registration Registration
	err := a.call(ctx, AuthorityMessage{Action: "register", Identity: id}, &registration)
	if err != nil {
		return Registration{}, err
	}
	if err := registration.ValidateFor(id); err != nil {
		return Registration{}, err
	}
	return registration, nil
}
func (a *AuthorityClient) Bind(ctx context.Context, id Identity) error {
	return a.call(ctx, AuthorityMessage{Action: "bind", Identity: id}, nil)
}
func (a *AuthorityClient) Stop(ctx context.Context, id Identity) (StopRequest, error) {
	var request StopRequest
	err := a.call(ctx, AuthorityMessage{Action: "stop", Identity: id}, &request)
	return request, err
}
func (a *AuthorityClient) Drain(ctx context.Context, r DrainReceipt) error {
	return a.call(ctx, AuthorityMessage{Action: "drain", Identity: r.Request.Identity, Drain: &r}, nil)
}
func (a *AuthorityClient) Exit(ctx context.Context, r StopRequest) error {
	return a.call(ctx, AuthorityMessage{Action: "exit", Identity: r.Identity, Request: &r}, nil)
}

func (a *AuthorityClient) NeverBoundExit(ctx context.Context, r StopRequest) error {
	return a.call(ctx, AuthorityMessage{Action: "never-bound-exit", Identity: r.Identity, Request: &r}, nil)
}

// BootstrapStatus is a noncreating query. A missing assignment is represented
// by a nil Assignment in the result; it is never converted into a registration.
func (a *AuthorityClient) BootstrapStatus(ctx context.Context, operationID string, candidate Identity) (BootstrapStatusResult, error) {
	if candidate.SourceClusterID == "" || candidate.PodUID == "" || candidate.NodeID == "" || candidate.PodName == "" || ValidatePodBinding(candidate.NodeID, candidate.PodName, candidate.PodUID) != nil {
		return BootstrapStatusResult{}, fmt.Errorf("fencing: incomplete bootstrap status query")
	}
	request := BootstrapStatusRequest{OperationID: operationID}
	var result BootstrapStatusResult
	if err := a.call(ctx, AuthorityMessage{Action: "bootstrap-status", Identity: candidate, BootstrapStatus: &request}, &result); err != nil {
		return BootstrapStatusResult{}, err
	}
	if err := validateBootstrapStatusResult(operationID, candidate, result); err != nil {
		return BootstrapStatusResult{}, err
	}
	return result, nil
}

// BootstrapRuntimeAdmission is a fresh pre-bind read. It returns only after
// the authority verifies the retained assignment, successful materialization,
// observed init termination, and current DR anchor binding.
func (a *AuthorityClient) BootstrapRuntimeAdmission(ctx context.Context, operationID string, candidate Identity) (BootstrapRuntimeAdmissionResult, error) {
	if operationID == "" || candidate.SourceClusterID == "" || candidate.PodUID == "" || candidate.PodName == "" || candidate.NodeID == "" || ValidatePodBinding(candidate.NodeID, candidate.PodName, candidate.PodUID) != nil {
		return BootstrapRuntimeAdmissionResult{}, fmt.Errorf("fencing: incomplete runtime admission query")
	}
	request := BootstrapStatusRequest{OperationID: operationID}
	var result BootstrapRuntimeAdmissionResult
	message := AuthorityMessage{Action: "bootstrap-runtime-admission", Identity: candidate, RuntimeAdmission: &request}
	if err := a.call(ctx, message, &result); err != nil {
		return BootstrapRuntimeAdmissionResult{}, err
	}
	if err := result.Assignment.Validate(); err != nil || result.Assignment.OperationID != operationID || result.Assignment.SourceClusterID != candidate.SourceClusterID || result.Assignment.PodName != candidate.PodName || result.Assignment.PodUID != candidate.PodUID || result.Assignment.Purpose != BootstrapDRSuccessor {
		return BootstrapRuntimeAdmissionResult{}, fmt.Errorf("fencing: runtime admission assignment differs from candidate")
	}
	return result, nil
}

func validateBootstrapStatusResult(operationID string, candidate Identity, result BootstrapStatusResult) error {
	if !validBootstrapHash(result.AssignmentSetHash) {
		return fmt.Errorf("fencing: bootstrap status assignment-set hash is invalid")
	}
	if result.Assignment != nil {
		assignment := *result.Assignment
		if err := assignment.Validate(); err != nil || assignment.SourceClusterID != candidate.SourceClusterID || assignment.PodUID != candidate.PodUID || assignment.PodName != candidate.PodName || !validBootstrapHash(result.AssignmentSetHash) {
			return fmt.Errorf("fencing: bootstrap status assignment differs from candidate")
		}
		if operationID == "" {
			want, _ := ReplacementNodeID(candidate.PodUID)
			if assignment.Purpose != BootstrapLearner || assignment.OperationID == "" || assignment.NodeID != want || assignment.PublicMember.ID != want || assignment.ProfileHash == "" || assignment.MembershipHash == "" || assignment.Durability == "" || assignment.StoreIdentityHash == "" || result.AdmissionClosed || result.Complete {
				return fmt.Errorf("fencing: own-init status is not an exact active learner assignment")
			}
		} else if assignment.OperationID != operationID || assignment.NodeID != candidate.NodeID {
			return fmt.Errorf("fencing: bootstrap status operation or identity differs")
		}
		for _, attempt := range result.Attempts {
			if attempt.AttemptID == "" || attempt.Assignment != assignment || attempt.Scope.ValidateFor(assignment) != nil {
				return fmt.Errorf("fencing: bootstrap status contains an invalid init attempt")
			}
			for _, outcome := range attempt.Outcomes {
				if err := outcome.ValidateForAssignment(assignment); err != nil {
					return fmt.Errorf("fencing: bootstrap status contains an invalid init outcome: %w", err)
				}
			}
			if attempt.Terminal != nil && (attempt.Terminal.ContainerID != attempt.Scope.ContainerID || attempt.Terminal.ImageID != attempt.Scope.ImageID || attempt.Terminal.Reason == "") {
				return fmt.Errorf("fencing: bootstrap status contains mismatched terminal evidence")
			}
		}
	} else {
		if operationID != "" || result.AssignmentSetHash != BootstrapAssignmentSetHash(nil) || len(result.Attempts) != 0 || result.AdmissionClosed || result.Complete {
			return fmt.Errorf("fencing: unassigned bootstrap status is not an exact empty own-init lookup")
		}
	}
	return nil
}

// ReserveMembershipRepair starts or idempotently resumes one source-authorized
// preparation reservation. It never starts peer closure or consensus changes.
func (a *AuthorityClient) ReserveMembershipRepair(ctx context.Context, caller Identity, request MembershipRepairReserveRequest) (MembershipRepairReserveResult, error) {
	closure := PeerClosureRequest{ExpectedConfigID: request.ExpectedConfigID, ExpectedAbortSlot: request.ExpectedAbortSlot, TargetNodeID: request.TargetNodeID, TargetWALIdentity: request.TargetWALIdentity}
	if err := closure.Validate(caller); err != nil {
		return MembershipRepairReserveResult{}, err
	}
	var result MembershipRepairReserveResult
	message := AuthorityMessage{Action: "membership-repair-reserve", Identity: caller, RepairReserve: &request}
	if err := a.call(ctx, message, &result); err != nil {
		return MembershipRepairReserveResult{}, err
	}
	if result.Repair == nil || result.Repair.ValidateFor(caller) != nil {
		return MembershipRepairReserveResult{}, fmt.Errorf("fencing: repair reservation response is not bound to caller")
	}
	return result, nil
}

func (a *AuthorityClient) QualifyMembershipRepair(ctx context.Context, caller Identity, request MembershipRepairQualificationRequest) (MembershipRepairQualificationResult, error) {
	if caller.Validate() != nil || caller.WALIdentity == "" || request.OperationID == "" || request.CandidatePodUID == "" {
		return MembershipRepairQualificationResult{}, fmt.Errorf("fencing: invalid membership-repair qualification request")
	}
	var result MembershipRepairQualificationResult
	message := AuthorityMessage{Action: "membership-repair-qualify", Identity: caller, RepairQualification: &request}
	if err := a.call(ctx, message, &result); err != nil {
		return MembershipRepairQualificationResult{}, err
	}
	if result.OperationID != request.OperationID || result.Preclosure == nil {
		return MembershipRepairQualificationResult{}, fmt.Errorf("fencing: qualification response differs from request")
	}
	return result, nil
}

func (a *AuthorityClient) PrepareMembershipRepairPhase(ctx context.Context, caller Identity, request MembershipRepairPhasePrepareRequest) (MembershipRepairPhasePrepareResult, error) {
	if caller.Validate() != nil || caller.WALIdentity == "" || request.OperationID == "" || (request.Kind != MembershipRepairRemove && request.Kind != MembershipRepairAdd) {
		return MembershipRepairPhasePrepareResult{}, fmt.Errorf("fencing: invalid membership-repair phase preparation request")
	}
	var result MembershipRepairPhasePrepareResult
	message := AuthorityMessage{Action: "membership-repair-phase-prepare", Identity: caller, RepairPhasePrepare: &request}
	if err := a.call(ctx, message, &result); err != nil {
		return MembershipRepairPhasePrepareResult{}, err
	}
	if result.Phase.Change.ClusterID != caller.SourceClusterID || result.Phase.Kind != request.Kind || result.Phase.Hash == "" {
		return MembershipRepairPhasePrepareResult{}, fmt.Errorf("fencing: retained phase response differs from request")
	}
	return result, nil
}

func (a *AuthorityClient) ObserveMembershipRepairPhase(ctx context.Context, caller Identity, request MembershipRepairPhaseObserveRequest) (MembershipRepairPhaseObserveResult, error) {
	if caller.Validate() != nil || caller.WALIdentity == "" || request.OperationID == "" || !validBootstrapHash(request.PhaseHash) ||
		request.JournalOperationID == "" || !validBootstrapHash(request.JournalRequestHash) || request.Target.ConfigID == 0 || request.TerminalSlot == 0 {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: invalid membership-repair phase observation")
	}
	var result MembershipRepairPhaseObserveResult
	message := AuthorityMessage{Action: "membership-repair-phase-observe", Identity: caller, RepairPhaseObserve: &request}
	if err := a.call(ctx, message, &result); err != nil {
		return MembershipRepairPhaseObserveResult{}, err
	}
	if result.OperationID != request.OperationID || result.PhaseHash != request.PhaseHash {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: phase observation response differs from request")
	}
	return result, nil
}

func (a *AuthorityClient) CompleteMembershipRepairPhase(ctx context.Context, caller Identity, request MembershipRepairPhaseObserveRequest) (MembershipRepairPhaseObserveResult, error) {
	if caller.Validate() != nil || caller.WALIdentity == "" || request.OperationID == "" || !validBootstrapHash(request.PhaseHash) || request.JournalOperationID == "" ||
		!validBootstrapHash(request.JournalRequestHash) || request.Target.ConfigID == 0 || request.TerminalSlot == 0 {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: invalid membership-repair phase completion")
	}
	var result MembershipRepairPhaseObserveResult
	message := AuthorityMessage{Action: "membership-repair-phase-complete", Identity: caller, RepairPhaseComplete: &request}
	if err := a.call(ctx, message, &result); err != nil {
		return MembershipRepairPhaseObserveResult{}, err
	}
	if result.OperationID != request.OperationID || result.PhaseHash != request.PhaseHash || !result.Complete {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: phase completion response differs from request")
	}
	return result, nil
}

// BeginInit durably reserves one actual process nonce before local preparation.
// Runtime scope in the response is authority-observed, not supplied by Node.
func (a *AuthorityClient) BeginInit(ctx context.Context, assignment BootstrapAssignment, attemptID string) (InitAttempt, error) {
	if err := assignment.Validate(); err != nil || attemptID == "" {
		return InitAttempt{}, fmt.Errorf("fencing: invalid init begin request")
	}
	request := BeginInitRequest{Assignment: assignment, AttemptID: attemptID}
	var attempt InitAttempt
	identity := bootstrapCallerIdentity(assignment)
	if err := a.call(ctx, AuthorityMessage{Action: "init-begin", Identity: identity, BeginInit: &request}, &attempt); err != nil {
		return InitAttempt{}, err
	}
	if attempt.AttemptID != attemptID || attempt.Assignment != assignment || attempt.Scope.ValidateFor(assignment) != nil || len(attempt.Outcomes) != 0 || attempt.Terminal != nil {
		return InitAttempt{}, fmt.Errorf("fencing: init begin response differs from request")
	}
	return attempt, nil
}

// BeginInitAdmission returns an opaque admission capability only after the
// authenticated authority confirms the exact assignment and live execution
// scope for this process.
func (a *AuthorityClient) BeginInitAdmission(ctx context.Context, assignment BootstrapAssignment, attemptID string) (InitAdmission, error) {
	attempt, err := a.BeginInit(ctx, assignment, attemptID)
	if err != nil {
		return InitAdmission{}, err
	}
	return InitAdmission{attempt: attempt}, nil
}

// RecordInitOutcome records only the live preparation result. It does not
// assert process termination, owner settlement, or durable archive proof.
func (a *AuthorityClient) RecordInitOutcome(ctx context.Context, assignment BootstrapAssignment, attemptID string, outcome InitAttemptOutcome) error {
	if err := assignment.Validate(); err != nil || attemptID == "" || outcome.ValidateForAssignment(assignment) != nil {
		return fmt.Errorf("fencing: invalid init outcome request")
	}
	request := RecordInitOutcomeRequest{Assignment: assignment, AttemptID: attemptID, Outcome: outcome}
	return a.call(ctx, AuthorityMessage{Action: "init-outcome", Identity: bootstrapCallerIdentity(assignment), InitOutcome: &request}, nil)
}

// CompleteBootstrap asks the authority to independently verify the certified
// barrier reference. The strings are lookup/binding values, never proof by
// themselves; the authority must read the existing certified archive state.
func (a *AuthorityClient) CompleteBootstrap(ctx context.Context, runtime Identity, completion BootstrapCompletion) error {
	if runtime.Validate() != nil || runtime.WALIdentity == "" || completion.OperationID == "" || completion.AuthorizationHash == "" || !validBootstrapHash(completion.AssignmentSetHash) {
		return fmt.Errorf("fencing: incomplete bootstrap completion request")
	}
	if _, err := ParseBootstrapBarrierSlot(completion.CertifiedBarrierRef); err != nil {
		return err
	}
	return a.call(ctx, AuthorityMessage{Action: "bootstrap-complete", Identity: runtime, Completion: &completion}, nil)
}

func (a *AuthorityClient) ReadMembershipRepair(ctx context.Context, runtime Identity) (MembershipRepairStatusResult, error) {
	return a.ReadMembershipRepairForTarget(ctx, runtime, "")
}

// ReadMembershipRepairForTarget optionally asks for a source-authenticated
// observation of one stable StatefulSet slot. The authority resolves the
// retained old identity and current replacement UID; this request is read-only.
func (a *AuthorityClient) ReadMembershipRepairForTarget(ctx context.Context, runtime Identity, targetPodName string) (MembershipRepairStatusResult, error) {
	if runtime.Validate() != nil || runtime.WALIdentity == "" {
		return MembershipRepairStatusResult{}, fmt.Errorf("fencing: membership-repair discovery requires a bound runtime")
	}
	var result MembershipRepairStatusResult
	message := AuthorityMessage{Action: "membership-repair-status", Identity: runtime, RepairStatus: &MembershipRepairStatusRequest{TargetPodName: targetPodName}}
	if err := a.call(ctx, message, &result); err != nil {
		return MembershipRepairStatusResult{}, err
	}
	if result.Repair != nil {
		if err := result.Repair.ValidateFor(runtime); err != nil {
			return MembershipRepairStatusResult{}, err
		}
	}
	if err := validateMembershipRepairSourceObservation(targetPodName, result); err != nil {
		return MembershipRepairStatusResult{}, err
	}
	return result, nil
}

func (a *AuthorityClient) AuthorizeMembershipPhase(ctx context.Context, runtime Identity, phase MembershipRepairPhase) error {
	if runtime.Validate() != nil || runtime.WALIdentity == "" || phase.Change.ClusterID != runtime.SourceClusterID || phase.AbortSlot == nil || phase.Change.ExpectedAbortSlot != *phase.AbortSlot || !validBootstrapHash(phase.Hash) {
		return fmt.Errorf("fencing: invalid membership-repair phase authorization request")
	}
	request := MembershipRepairAuthorizationRequest{Phase: phase}
	message := AuthorityMessage{Action: "membership-repair-authorize", Identity: runtime, RepairPhase: &request}
	return a.call(ctx, message, nil)
}

func bootstrapCallerIdentity(assignment BootstrapAssignment) Identity {
	return Identity{SourceClusterID: assignment.SourceClusterID, NodeID: assignment.NodeID, PodName: assignment.PodName, PodUID: assignment.PodUID, CredentialName: assignment.CredentialName, CredentialUID: assignment.CredentialUID}
}

func (a *AuthorityClient) RequestPeerClosure(ctx context.Context, caller Identity, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	if err := closure.Validate(caller); err != nil {
		return PeerClosureReceipt{}, err
	}
	var receipt PeerClosureReceipt
	err := a.callPeerClosure(ctx, "/v1/peer-closure/request", AuthorityMessage{Action: "peer-closure-request", Identity: caller, PeerClosure: &closure}, &receipt)
	if err != nil {
		return PeerClosureReceipt{}, err
	}
	if err := validatePeerClosureReceipt(caller, closure, receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	return receipt, nil
}

func (a *AuthorityClient) ReadPeerClosure(ctx context.Context, caller Identity, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	if err := closure.Validate(caller); err != nil {
		return PeerClosureReceipt{}, err
	}
	var receipt PeerClosureReceipt
	err := a.callPeerClosure(ctx, "/v1/peer-closure/status", AuthorityMessage{Action: "peer-closure-status", Identity: caller, PeerClosure: &closure}, &receipt)
	if err != nil {
		return PeerClosureReceipt{}, err
	}
	if err := validatePeerClosureReceipt(caller, closure, receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	return receipt, nil
}

func validatePeerClosureReceipt(caller Identity, request PeerClosureRequest, receipt PeerClosureReceipt) error {
	if err := request.Validate(caller); err != nil {
		return err
	}
	if receipt.ExpectedAbortSlot == nil || receipt.OperationID == "" || receipt.SourceClusterID != caller.SourceClusterID || receipt.BindingUID == "" || receipt.StatefulSetUID == "" || receipt.RequestHash == "" || receipt.ProofID == "" ||
		receipt.ExpectedConfigID != request.ExpectedConfigID || *receipt.ExpectedAbortSlot != *request.ExpectedAbortSlot || receipt.TargetNodeID != request.TargetNodeID || receipt.TargetWALIdentity != request.TargetWALIdentity ||
		!receipt.ProcessesTerminated || !receipt.RecreationBlocked || !receipt.StorageQuiesced || len(receipt.Targets) == 0 {
		return fmt.Errorf("fencing: peer-closure receipt does not match request")
	}
	covered := false
	requestedWAL := false
	for _, target := range receipt.Targets {
		if target.NodeID != request.TargetNodeID || ValidatePodBinding(target.NodeID, target.Pod, target.PodUID) != nil || (target.WALIdentity != "" && target.WALIdentity != request.TargetWALIdentity) {
			return fmt.Errorf("fencing: peer-closure receipt target differs")
		}
		if target.WALIdentity == request.TargetWALIdentity {
			requestedWAL = true
		}
		covered = true
	}
	if !covered || !requestedWAL {
		return fmt.Errorf("fencing: peer-closure receipt omitted target")
	}
	return nil
}
