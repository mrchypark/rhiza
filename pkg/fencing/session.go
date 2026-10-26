package fencing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Session authenticates one application child to its local supervisor. The
// supervisor, not the application, reserves the durable incarnation before spawn.
type Session struct {
	client              *http.Client
	ticket              string
	mu                  sync.Mutex
	identity            Identity
	role                StartupRole
	authorityProfile    string
	logicalBinding      *LogicalWriterBindingV1
	initialRegistration *InitialRegistrationProvenance
	startupEnvironment  map[string]string
	expectedWALIdentity string
	replacedNodeID      string
	replacedWALIdentity string
	binding             *BindReceipt
}

// Connect must complete before opening a node or performing storage mutations.
func Connect(ctx context.Context, socketPath, ticket, sourceClusterID, nodeID string) (*Session, error) {
	if socketPath == "" || ticket == "" || sourceClusterID == "" || nodeID == "" {
		return nil, fmt.Errorf("fencing: complete startup configuration is required")
	}
	s := &Session{ticket: ticket, client: &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	physical := os.Getenv("RHIZA_POD_NAME")
	if physical == "" {
		physical = nodeID
	}
	request := Identity{SourceClusterID: sourceClusterID, NodeID: physical, PodName: physical}
	var grant StartupGrant
	if err := s.call(ctx, "/v1/startup", request, &grant); err != nil {
		return nil, err
	}
	if err := grant.Validate(Identity{SourceClusterID: sourceClusterID, NodeID: physical, PodName: request.PodName, PodUID: grant.Identity.PodUID, RunID: grant.Identity.RunID}); err != nil {
		return nil, fmt.Errorf("fencing: invalid startup grant: %w", err)
	}
	if grant.Identity.NodeID != nodeID && grant.Environment["RHIZA_NODE_ID"] != grant.Identity.NodeID {
		return nil, fmt.Errorf("fencing: startup grant logical identity differs from child configuration")
	}
	s.identity, s.role = grant.Identity, grant.Role
	s.authorityProfile = grant.AuthorityProfile
	if grant.LogicalBinding != nil {
		binding := *grant.LogicalBinding
		s.logicalBinding = &binding
	}
	if grant.InitialRegistration != nil {
		copy := *grant.InitialRegistration
		s.initialRegistration = &copy
	}
	s.startupEnvironment = make(map[string]string, len(grant.Environment))
	for key, value := range grant.Environment {
		s.startupEnvironment[key] = value
	}
	s.expectedWALIdentity, s.replacedNodeID, s.replacedWALIdentity = grant.ExpectedWALIdentity, grant.ReplacedNodeID, grant.ReplacedWALIdentity
	return s, nil
}

// StartupEnvironment returns the authority-pinned child configuration from
// the validated startup grant. The caller receives an independent copy.
func (s *Session) StartupEnvironment() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := make(map[string]string, len(s.startupEnvironment))
	for key, value := range s.startupEnvironment {
		copy[key] = value
	}
	return copy
}

func (s *Session) Identity() Identity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.identity
}

func (s *Session) StartupRole() StartupRole {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.role
}

// InitialRegistration returns the authority-issued first-use metadata for
// this exact Run. It is absent for ordinary active voters and repair learners.
func (s *Session) InitialRegistration() (InitialRegistrationProvenance, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initialRegistration == nil {
		return InitialRegistrationProvenance{}, false
	}
	return *s.initialRegistration, true
}

func (s *Session) ExpectedWALIdentity() (string, string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expectedWALIdentity, s.replacedNodeID, s.replacedWALIdentity
}

// BindWAL binds once, after opening the WAL but before starting any writer.
func (s *Session) BindWAL(ctx context.Context, walIdentity string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if walIdentity == "" || (s.identity.WALIdentity != "" && s.identity.WALIdentity != walIdentity) ||
		(s.expectedWALIdentity != "" && s.expectedWALIdentity != walIdentity) {
		return fmt.Errorf("fencing: WAL identity cannot change")
	}
	bound := s.identity
	bound.WALIdentity = walIdentity
	var receipt BindReceipt
	if err := s.call(ctx, "/v1/bind", bound, &receipt); err != nil {
		return err
	}
	if err := receipt.Validate(bound); err != nil {
		return err
	}
	if receipt.AuthorityProfile != s.authorityProfile {
		return fmt.Errorf("fencing: bind receipt profile differs from startup grant")
	}
	if receipt.AuthorityProfile == LogicalWriterProfileV1 {
		if s.logicalBinding == nil || receipt.LogicalBinding == nil {
			return fmt.Errorf("fencing: logical bind receipt has no startup binding")
		}
		want, got := *s.logicalBinding, *receipt.LogicalBinding
		want.WALIdentity = got.WALIdentity
		if want != got {
			return fmt.Errorf("fencing: logical bind receipt differs from startup grant")
		}
	}
	startupRole := s.role
	if startupRole == "" && s.identity.AssignmentID == "" {
		// Package-local legacy sessions used by static-voter integrations may
		// predate the startup-grant field. Assigned identities never inherit
		// this default; their grant and bind receipt must both carry a role.
		startupRole = StartupRoleVoterActive
	}
	receiptRole, roleOK := receipt.normalizedRole()
	if startupRole == "" || !roleOK || receiptRole != startupRole {
		return fmt.Errorf("fencing: bind role differs from startup grant")
	}
	if s.binding != nil && !s.binding.SameBinding(receipt) {
		return fmt.Errorf("fencing: source producer binding changed")
	}
	copy := receipt.clone()
	s.binding = &copy
	s.role = startupRole
	s.identity = bound
	return nil
}

// RegisterLearnerIdentity requests only the exact immutable per-node identity
// object. The authority rejects any other key or payload for a pending learner.
func (s *Session) RegisterLearnerIdentity(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleLearnerPendingRemove || s.identity.WALIdentity == "" || key == "" || len(data) == 0 || len(data) > 64<<10 {
		return fmt.Errorf("fencing: pending learner identity registration is unavailable")
	}
	request := struct {
		Identity Identity                    `json:"identity"`
		Object   LearnerIdentityRegistration `json:"object"`
	}{s.identity, LearnerIdentityRegistration{Key: key, Data: append([]byte(nil), data...)}}
	return s.call(ctx, "/v1/learner-identity", request, nil)
}

// PromoteLearner changes the local role only after the authenticated authority
// returns an active receipt for this exact bound WAL. The server independently
// verifies the durable remove/add journal and its post-commit barrier.
func (s *Session) PromoteLearner(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleLearnerPendingRemove || s.identity.WALIdentity == "" {
		return fmt.Errorf("fencing: learner promotion is unavailable")
	}
	var receipt BindReceipt
	if err := s.call(ctx, "/v1/promote-learner", s.identity, &receipt); err != nil {
		return err
	}
	if err := receipt.Validate(s.identity); err != nil || receipt.Role != StartupRoleVoterActive {
		return fmt.Errorf("fencing: learner promotion receipt is invalid")
	}
	if receipt.AuthorityProfile != s.authorityProfile {
		return fmt.Errorf("fencing: learner promotion profile differs from startup grant")
	}
	if receipt.AuthorityProfile == LogicalWriterProfileV1 {
		if s.logicalBinding == nil || receipt.LogicalBinding == nil || !logicalPromotionBindingMatches(*s.logicalBinding, *receipt.LogicalBinding) {
			return fmt.Errorf("fencing: logical learner promotion binding differs from startup grant")
		}
	}
	s.role = StartupRoleVoterActive
	copy := receipt.clone()
	s.binding = &copy
	if receipt.LogicalBinding != nil {
		binding := *receipt.LogicalBinding
		s.logicalBinding = &binding
	}
	return nil
}

func logicalPromotionBindingMatches(pending, active LogicalWriterBindingV1) bool {
	if pending.ValidateBound() != nil || active.ValidateBound() != nil ||
		pending.Role != StartupRoleLearnerPendingRemove || active.Role != StartupRoleVoterActive ||
		pending.SourceClusterID != active.SourceClusterID || pending.NodeID != active.NodeID ||
		pending.RunID != active.RunID || pending.AssignmentID != active.AssignmentID ||
		pending.WALIdentity != active.WALIdentity || pending.ProducerUID != "" || pending.StoreScopeHash != "" ||
		active.ProducerUID == "" || active.StoreScopeHash == "" ||
		!mutationDigestValid(active.Recovery.PromotionEvidenceHash) {
		return false
	}
	wantRecovery := pending.Recovery
	gotRecovery := active.Recovery
	wantRecovery.PromotionEvidenceHash = gotRecovery.PromotionEvidenceHash
	return wantRecovery == gotRecovery
}

// MutationBinding returns an immutable copy of the validated journal binding.
// Presence is not a dispatch right or proof that a source retry file is durable.
func (s *Session) MutationBinding() (ProducerBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.binding == nil || s.binding.Producer == nil {
		return ProducerBinding{}, false
	}
	copy := s.binding.clone()
	return *copy.Producer, true
}

// CurrentEngineRepair reads the canonical repair only from an authenticated
// active-voter session bound to a WAL.
func (s *Session) CurrentEngineRepair(ctx context.Context) (EngineRepairRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.identity.WALIdentity == "" || s.binding == nil {
		return EngineRepairRecord{}, false, fmt.Errorf("fencing: engine repair requires an active bound voter")
	}
	request := EngineRepairRequest{Identity: s.identity}
	var response EngineRepairResponse
	if err := s.call(ctx, "/v1/engine-repair/current", request, &response); err != nil {
		return EngineRepairRecord{}, false, err
	}
	if !response.Found {
		if response.Record != nil || response.Proof != nil {
			return EngineRepairRecord{}, false, fmt.Errorf("fencing: malformed empty engine repair response")
		}
		return EngineRepairRecord{}, false, nil
	}
	if response.Record == nil || response.Proof != nil || response.Record.Validate() != nil || response.Record.Binding.SourceClusterID != s.identity.SourceClusterID {
		return EngineRepairRecord{}, false, fmt.Errorf("fencing: invalid engine repair response")
	}
	return *response.Record, true, nil
}

// CompareAndSwapEngineRepair proposes only a digest-bound next revision; the
// authority independently reloads and validates the canonical reservation.
func (s *Session) CompareAndSwapEngineRepair(ctx context.Context, expectedDigest string, next EngineRepairRecord) (EngineRepairRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.identity.WALIdentity == "" || s.binding == nil || expectedDigest == "" ||
		next.Validate() != nil || next.Binding.SourceClusterID != s.identity.SourceClusterID {
		return EngineRepairRecord{}, fmt.Errorf("fencing: engine repair CAS is unavailable")
	}
	request := EngineRepairRequest{Identity: s.identity, OperationID: next.Binding.OperationID, ExpectedDigest: expectedDigest, Record: &next}
	var response EngineRepairResponse
	if err := s.call(ctx, "/v1/engine-repair/cas", request, &response); err != nil {
		return EngineRepairRecord{}, err
	}
	if !response.Found || response.Record == nil || response.Proof != nil || response.Record.Validate() != nil ||
		response.Record.IdentityDigest() != next.IdentityDigest() || response.Record.Binding.OperationID != next.Binding.OperationID {
		return EngineRepairRecord{}, fmt.Errorf("fencing: invalid engine repair CAS response")
	}
	return *response.Record, nil
}

// CloseEngineRepairStorageEffects requests authority-derived exclusion proof
// for one canonical operation, never caller-supplied proof bytes.
func (s *Session) CloseEngineRepairStorageEffects(ctx context.Context, operationID string) (StorageEffectsExcluded, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.identity.WALIdentity == "" || s.binding == nil || operationID == "" {
		return StorageEffectsExcluded{}, fmt.Errorf("fencing: engine repair storage closure is unavailable")
	}
	request := EngineRepairRequest{Identity: s.identity, OperationID: operationID}
	var response EngineRepairResponse
	if err := s.call(ctx, "/v1/engine-repair/storage-effects", request, &response); err != nil {
		return StorageEffectsExcluded{}, err
	}
	if response.Found || response.Record != nil || response.Proof == nil || response.Proof.Binding.SourceClusterID != s.identity.SourceClusterID || response.Proof.Binding.OperationID != operationID {
		return StorageEffectsExcluded{}, fmt.Errorf("fencing: invalid engine repair storage-closure response")
	}
	return *response.Proof, nil
}

// VerifyEngineRemovalStorageEffects is a read-only terminal admission check.
// It binds the engine's exact REMOVE terminal to authority-canonical proof;
// unlike CloseEngineRepairStorageEffects it never settles or closes anything.
func (s *Session) VerifyEngineRemovalStorageEffects(ctx context.Context, operation quepaxa.EngineRemoval) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.identity.WALIdentity == "" || s.binding == nil || operation.OperationID == "" ||
		operation.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) {
		return fmt.Errorf("fencing: engine removal storage-effects verification is unavailable")
	}
	request := EngineRepairRequest{Identity: s.identity, OperationID: operation.OperationID, RemovalOperation: &operation}
	var response EngineRepairResponse
	if err := s.call(ctx, "/v1/engine-repair/storage-effects-verify", request, &response); err != nil {
		return err
	}
	if response.Found || response.Record != nil || response.Proof != nil || len(response.PendingRequests) != 0 {
		return fmt.Errorf("fencing: malformed engine removal storage-effects verification response")
	}
	return nil
}

// ConfirmEngineRepair asks an authenticated surviving voter to confirm the
// exact candidate ADD after the node has durably applied it. The authority
// independently validates the candidate and persists the Active transition.
func (s *Session) ConfirmEngineRepair(ctx context.Context, operationID string) (EngineRepairRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.identity.WALIdentity == "" || s.binding == nil || operationID == "" {
		return EngineRepairRecord{}, fmt.Errorf("fencing: engine repair confirmation is unavailable")
	}
	request := EngineRepairRequest{Identity: s.identity, OperationID: operationID}
	var response EngineRepairResponse
	if err := s.call(ctx, "/v1/engine-repair/confirm", request, &response); err != nil {
		return EngineRepairRecord{}, err
	}
	if !response.Found || response.Record == nil || response.Proof != nil || response.Record.Validate() != nil ||
		response.Record.Binding.SourceClusterID != s.identity.SourceClusterID || response.Record.Binding.OperationID != operationID || response.Record.Phase != RepairActive {
		return EngineRepairRecord{}, fmt.Errorf("fencing: invalid engine repair confirmation response")
	}
	return *response.Record, nil
}

// ListPendingEngineRepairs exposes only the canonical requests visible to an
// authenticated active surviving voter; pending requests never grant roles.
func (s *Session) ListPendingEngineRepairs(ctx context.Context) ([]PendingEngineRepairRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.identity.WALIdentity == "" || s.binding == nil {
		return nil, fmt.Errorf("fencing: pending engine repair lookup requires an active bound voter")
	}
	request := EngineRepairRequest{Identity: s.identity}
	var response EngineRepairResponse
	if err := s.call(ctx, "/v1/engine-repair/pending", request, &response); err != nil {
		return nil, err
	}
	if response.Found || response.Record != nil || response.Proof != nil {
		return nil, fmt.Errorf("fencing: malformed pending engine repair response")
	}
	for _, pending := range response.PendingRequests {
		if pending.Validate() != nil || pending.SourceClusterID != s.identity.SourceClusterID {
			return nil, fmt.Errorf("fencing: invalid pending engine repair response")
		}
	}
	return append([]PendingEngineRepairRequest(nil), response.PendingRequests...), nil
}

// AdoptPendingEngineRepair asks the authority to atomically replace one
// exact pending request with its canonical CandidatePrepared reservation.
func (s *Session) AdoptPendingEngineRepair(ctx context.Context, pendingID, pendingDigest string, record EngineRepairRecord) (EngineRepairRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.role != StartupRoleVoterActive || s.identity.WALIdentity == "" || s.binding == nil || pendingID == "" || !mutationDigestValid(pendingDigest) ||
		record.ValidateAdoptionProposal() != nil || record.Binding.SourceClusterID != s.identity.SourceClusterID || record.BootstrapRequestDigest != pendingDigest {
		return EngineRepairRecord{}, fmt.Errorf("fencing: pending engine repair adoption is unavailable")
	}
	request := EngineRepairRequest{Identity: s.identity, PendingID: pendingID, PendingDigest: pendingDigest, Record: &record}
	var response EngineRepairResponse
	if err := s.call(ctx, "/v1/engine-repair/adopt", request, &response); err != nil {
		return EngineRepairRecord{}, err
	}
	if !response.Found || response.Record == nil || response.Proof != nil || len(response.PendingRequests) != 0 ||
		response.Record.Validate() != nil || response.Record.Binding.SourceClusterID != s.identity.SourceClusterID || response.Record.BootstrapRequestDigest != pendingDigest {
		return EngineRepairRecord{}, fmt.Errorf("fencing: invalid pending engine repair adoption response")
	}
	return *response.Record, nil
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
