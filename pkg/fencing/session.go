package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// Session authenticates one application child to its local supervisor. The
// supervisor, not the application, reserves the durable incarnation before spawn.
type Session struct {
	client       *http.Client
	ticket       string
	mu           sync.Mutex
	identity     Identity
	registration Registration
}

// Connect must complete before opening a node or performing storage mutations.
func Connect(ctx context.Context, socketPath, ticket, sourceClusterID, nodeID string, podNames ...string) (*Session, error) {
	if socketPath == "" || ticket == "" || sourceClusterID == "" || nodeID == "" {
		return nil, fmt.Errorf("fencing: complete startup configuration is required")
	}
	if len(podNames) > 1 {
		return nil, fmt.Errorf("fencing: at most one Pod name may be supplied")
	}
	candidate := Identity{SourceClusterID: sourceClusterID, NodeID: nodeID}
	if len(podNames) == 1 {
		candidate.PodName = podNames[0]
	}
	s := &Session{ticket: ticket, client: &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	var registration Registration
	if err := s.call(ctx, "/v1/startup", candidate, &registration); err != nil {
		return nil, err
	}
	if err := registration.validateLocalStartup(candidate); err != nil {
		return nil, fmt.Errorf("fencing: invalid startup registration: %w", err)
	}
	s.identity = registration.Identity
	s.registration = registration
	return s, nil
}

func (s *Session) Identity() Identity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.identity
}

func (s *Session) Registration() Registration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registration
}

// BindWAL binds once, after opening the WAL but before starting any writer.
func (s *Session) BindWAL(ctx context.Context, walIdentity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if walIdentity == "" || (s.identity.WALIdentity != "" && s.identity.WALIdentity != walIdentity) {
		return fmt.Errorf("fencing: WAL identity cannot change")
	}
	bound := s.identity
	bound.WALIdentity = walIdentity
	if err := s.call(ctx, "/v1/bind", bound, nil); err != nil {
		return err
	}
	s.identity = bound
	return nil
}

// WaitStop waits for an immutable terminal request. A pending response is not
// authorization to restart or reopen the node.
func (s *Session) WaitStop(ctx context.Context) (StopRequest, error) {
	identity := s.Identity()
	var request StopRequest
	if err := s.call(ctx, "/v1/stop", identity, &request); err != nil {
		return StopRequest{}, err
	}
	if request.Identity != identity || request.OperationID == "" || request.RequestHash == "" {
		return StopRequest{}, fmt.Errorf("fencing: stop request identity mismatch")
	}
	return request, nil
}

func (s *Session) AcknowledgeDrain(ctx context.Context, receipt DrainReceipt) error {
	if receipt.Request.Identity != s.Identity() || receipt.Request.OperationID == "" || receipt.Request.RequestHash == "" {
		return fmt.Errorf("fencing: drain identity mismatch")
	}
	return s.call(ctx, "/v1/drained", receipt, nil)
}

// RequestPeerClosure asks the bound supervisor to submit one exact voter-slot
// closure. The local session identity remains the caller; target identity is a
// separate field and cannot be substituted for the caller.
func (s *Session) RequestPeerClosure(ctx context.Context, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	return s.peerClosureCall(ctx, "/v1/peer-closure/request", "peer-closure-request", closure)
}

// ReadPeerClosure reads only an already persisted exact operation. A missing
// record is reported as ErrNotFound and must never trigger request creation.
func (s *Session) ReadPeerClosure(ctx context.Context, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	return s.peerClosureCall(ctx, "/v1/peer-closure/status", "peer-closure-status", closure)
}

// BootstrapStatus reads retained init evidence through this bound runtime
// session. It is distinct from the init container's unbound direct authority
// query and cannot run before the main WAL has been bound.
func (s *Session) BootstrapStatus(ctx context.Context, operationID string) (BootstrapStatusResult, error) {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || operationID == "" {
		return BootstrapStatusResult{}, fmt.Errorf("fencing: bootstrap status requires a bound runtime identity")
	}
	request := BootstrapStatusRequest{OperationID: operationID}
	message := AuthorityMessage{Action: "bootstrap-status", Identity: identity, BootstrapStatus: &request}
	var result BootstrapStatusResult
	if err := s.call(ctx, "/v1/bootstrap-status", message, &result); err != nil {
		return BootstrapStatusResult{}, err
	}
	if err := validateBootstrapStatusResult(operationID, identity, result); err != nil {
		return BootstrapStatusResult{}, err
	}
	return result, nil
}

// BootstrapRuntimeAdmission performs the fresh DR assignment/readiness check
// before the application creates or binds its local WAL. The supervisor
// authenticates this session identity and forwards it to the authority; this
// method deliberately does not register a new run or mutate local identity.
func (s *Session) BootstrapRuntimeAdmission(ctx context.Context, operationID string) (BootstrapRuntimeAdmissionResult, error) {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity != "" || operationID == "" {
		return BootstrapRuntimeAdmissionResult{}, fmt.Errorf("fencing: runtime admission requires an unbound runtime identity")
	}
	request := BootstrapStatusRequest{OperationID: operationID}
	message := AuthorityMessage{Action: "bootstrap-runtime-admission", Identity: identity, RuntimeAdmission: &request}
	var result BootstrapRuntimeAdmissionResult
	if err := s.call(ctx, "/v1/bootstrap-runtime-admission", message, &result); err != nil {
		return BootstrapRuntimeAdmissionResult{}, err
	}
	if err := result.Assignment.Validate(); err != nil || result.Assignment.OperationID != operationID || result.Assignment.SourceClusterID != identity.SourceClusterID || result.Assignment.NodeID != identity.NodeID || result.Assignment.PodName != identity.PodName || result.Assignment.PodUID != identity.PodUID || result.Assignment.Purpose != BootstrapDRSuccessor {
		return BootstrapRuntimeAdmissionResult{}, fmt.Errorf("fencing: runtime admission assignment differs from session identity")
	}
	return result, nil
}

// CompleteBootstrap forwards the existing certified-barrier lookup reference
// over the runtime's supervisor channel. The authority must independently
// verify archive contents; the slot string is not evidence.
func (s *Session) CompleteBootstrap(ctx context.Context, completion BootstrapCompletion) error {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || completion.OperationID == "" || completion.AuthorizationHash == "" || !validBootstrapHash(completion.AssignmentSetHash) {
		return fmt.Errorf("fencing: bootstrap completion requires a bound runtime identity")
	}
	if _, err := ParseBootstrapBarrierSlot(completion.CertifiedBarrierRef); err != nil {
		return err
	}
	message := AuthorityMessage{Action: "bootstrap-complete", Identity: identity, Completion: &completion}
	return s.call(ctx, "/v1/bootstrap-complete", message, nil)
}

// ReadMembershipRepair performs read-only discovery for this exact bound
// voter. An empty Repair is a successful authoritative no-active-repair result;
// transport or authority errors remain errors and must not be treated as none.
func (s *Session) ReadMembershipRepair(ctx context.Context) (MembershipRepairStatusResult, error) {
	return s.ReadMembershipRepairForTarget(ctx, "")
}

// ReadMembershipRepairForTarget requests an optional read-only observation of
// one stable slot. The authority, not the caller, supplies the old WAL identity
// and live replacement Pod UID.
func (s *Session) ReadMembershipRepairForTarget(ctx context.Context, targetPodName string) (MembershipRepairStatusResult, error) {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" {
		return MembershipRepairStatusResult{}, fmt.Errorf("fencing: membership-repair discovery requires a bound voter")
	}
	message := AuthorityMessage{Action: "membership-repair-status", Identity: identity, RepairStatus: &MembershipRepairStatusRequest{TargetPodName: targetPodName}}
	var result MembershipRepairStatusResult
	if err := s.call(ctx, "/v1/membership-repair/status", message, &result); err != nil {
		return MembershipRepairStatusResult{}, err
	}
	if result.Repair != nil {
		if err := result.Repair.ValidateFor(identity); err != nil {
			return MembershipRepairStatusResult{}, err
		}
	}
	if err := validateMembershipRepairSourceObservation(targetPodName, result); err != nil {
		return MembershipRepairStatusResult{}, err
	}
	return result, nil
}

// ReserveMembershipRepair is the existing source voter's explicit admission
// mutation. It preserves the requested config/abort/old identity and does not
// initiate peer closure.
func (s *Session) ReserveMembershipRepair(ctx context.Context, request MembershipRepairReserveRequest) (MembershipRepairReserveResult, error) {
	identity := s.Identity()
	closure := PeerClosureRequest{ExpectedConfigID: request.ExpectedConfigID, ExpectedAbortSlot: request.ExpectedAbortSlot, TargetNodeID: request.TargetNodeID, TargetWALIdentity: request.TargetWALIdentity}
	if err := closure.Validate(identity); err != nil {
		return MembershipRepairReserveResult{}, err
	}
	message := AuthorityMessage{Action: "membership-repair-reserve", Identity: identity, RepairReserve: &request}
	var result MembershipRepairReserveResult
	if err := s.call(ctx, "/v1/membership-repair/reserve", message, &result); err != nil {
		return MembershipRepairReserveResult{}, err
	}
	if result.Repair == nil || result.Repair.ValidateFor(identity) != nil {
		return MembershipRepairReserveResult{}, fmt.Errorf("fencing: repair reservation response differs from source voter")
	}
	return result, nil
}

// AuthorizeMembershipPhase asks the authority to recheck the currently
// retained exact phase before Node journals or executes the membership change.
func (s *Session) AuthorizeMembershipPhase(ctx context.Context, phase MembershipRepairPhase) error {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || phase.Change.ClusterID != identity.SourceClusterID || phase.AbortSlot == nil || phase.Change.ExpectedAbortSlot != *phase.AbortSlot || !validBootstrapHash(phase.Hash) {
		return fmt.Errorf("fencing: invalid membership-repair phase request")
	}
	message := AuthorityMessage{Action: "membership-repair-authorize", Identity: identity, RepairPhase: &MembershipRepairAuthorizationRequest{Phase: phase}}
	return s.call(ctx, "/v1/membership-repair/authorize", message, nil)
}

func (s *Session) QualifyMembershipRepair(ctx context.Context, request MembershipRepairQualificationRequest) (MembershipRepairQualificationResult, error) {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || request.OperationID == "" || request.CandidatePodUID == "" {
		return MembershipRepairQualificationResult{}, fmt.Errorf("fencing: invalid membership-repair qualification request")
	}
	message := AuthorityMessage{Action: "membership-repair-qualify", Identity: identity, RepairQualification: &request}
	var result MembershipRepairQualificationResult
	if err := s.call(ctx, "/v1/membership-repair/qualify", message, &result); err != nil {
		return MembershipRepairQualificationResult{}, err
	}
	if result.OperationID != request.OperationID || result.Preclosure == nil {
		return MembershipRepairQualificationResult{}, fmt.Errorf("fencing: qualification response differs from request")
	}
	return result, nil
}

func (s *Session) PrepareMembershipRepairPhase(ctx context.Context, request MembershipRepairPhasePrepareRequest) (MembershipRepairPhasePrepareResult, error) {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || request.OperationID == "" || (request.Kind != MembershipRepairRemove && request.Kind != MembershipRepairAdd) {
		return MembershipRepairPhasePrepareResult{}, fmt.Errorf("fencing: invalid membership-repair phase preparation request")
	}
	message := AuthorityMessage{Action: "membership-repair-phase-prepare", Identity: identity, RepairPhasePrepare: &request}
	var result MembershipRepairPhasePrepareResult
	if err := s.call(ctx, "/v1/membership-repair/phase/prepare", message, &result); err != nil {
		return MembershipRepairPhasePrepareResult{}, err
	}
	if result.Phase.Change.ClusterID != identity.SourceClusterID || result.Phase.Kind != request.Kind || result.Phase.Hash == "" {
		return MembershipRepairPhasePrepareResult{}, fmt.Errorf("fencing: phase preparation response differs from request")
	}
	return result, nil
}

func (s *Session) ObserveMembershipRepairPhase(ctx context.Context, request MembershipRepairPhaseObserveRequest) (MembershipRepairPhaseObserveResult, error) {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || request.OperationID == "" || !validBootstrapHash(request.PhaseHash) ||
		request.JournalOperationID == "" || !validBootstrapHash(request.JournalRequestHash) || request.Target.ConfigID == 0 || request.TerminalSlot == 0 {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: invalid membership-repair phase observation")
	}
	message := AuthorityMessage{Action: "membership-repair-phase-observe", Identity: identity, RepairPhaseObserve: &request}
	var result MembershipRepairPhaseObserveResult
	if err := s.call(ctx, "/v1/membership-repair/phase/observe", message, &result); err != nil {
		return MembershipRepairPhaseObserveResult{}, err
	}
	if result.OperationID != request.OperationID || result.PhaseHash != request.PhaseHash {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: phase observation response differs from request")
	}
	return result, nil
}

func (s *Session) CompleteMembershipRepairPhase(ctx context.Context, request MembershipRepairPhaseObserveRequest) (MembershipRepairPhaseObserveResult, error) {
	identity := s.Identity()
	if identity.Validate() != nil || identity.WALIdentity == "" || request.OperationID == "" || !validBootstrapHash(request.PhaseHash) || request.JournalOperationID == "" ||
		!validBootstrapHash(request.JournalRequestHash) || request.Target.ConfigID == 0 || request.TerminalSlot == 0 {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: invalid membership-repair phase completion")
	}
	message := AuthorityMessage{Action: "membership-repair-phase-complete", Identity: identity, RepairPhaseComplete: &request}
	var result MembershipRepairPhaseObserveResult
	if err := s.call(ctx, "/v1/membership-repair/phase/complete", message, &result); err != nil {
		return MembershipRepairPhaseObserveResult{}, err
	}
	if result.OperationID != request.OperationID || result.PhaseHash != request.PhaseHash || !result.Complete {
		return MembershipRepairPhaseObserveResult{}, fmt.Errorf("fencing: phase completion response differs from request")
	}
	return result, nil
}

func (s *Session) peerClosureCall(ctx context.Context, path, action string, closure PeerClosureRequest) (PeerClosureReceipt, error) {
	caller := s.Identity()
	if err := closure.Validate(caller); err != nil {
		return PeerClosureReceipt{}, err
	}
	message := AuthorityMessage{Action: action, Identity: caller, PeerClosure: &closure}
	var receipt PeerClosureReceipt
	if err := s.call(ctx, path, message, &receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	if err := validatePeerClosureReceipt(caller, closure, receipt); err != nil {
		return PeerClosureReceipt{}, err
	}
	return receipt, nil
}

func (s *Session) call(ctx context.Context, path string, value, out any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://supervisor"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.ticket)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusAccepted:
		return ErrPending
	case http.StatusGone:
		return ErrClosed
	case http.StatusNotFound:
		if path == "/v1/peer-closure/status" {
			return ErrNotFound
		}
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			return ErrUnavailable
		}
		return fmt.Errorf("fencing: supervisor returned HTTP %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(data) > 65536 {
		return fmt.Errorf("fencing: supervisor response too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing supervisor response")
	}
	return nil
}
