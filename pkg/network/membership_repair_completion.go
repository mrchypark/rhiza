package network

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const MembershipRepairCompletionAction = "complete"

// MembershipRepairCompletionRequest is the authority-challenged exact ADD
// completion scope. The runtime still verifies the journal and certified
// transition before it attempts terminal delivery.
type MembershipRepairCompletionRequest struct {
	Version             uint            `json:"version"`
	Action              string          `json:"action"`
	Challenge           string          `json:"challenge"`
	AttemptRevision     uint64          `json:"attempt_revision"`
	ScopeDigest         string          `json:"scope_digest"`
	RepairOperationID   string          `json:"repair_operation_id"`
	RepairBindingDigest string          `json:"repair_binding_digest"`
	PhaseHash           string          `json:"phase_hash"`
	JournalOperationID  string          `json:"journal_operation_id"`
	JournalRequestHash  string          `json:"journal_request_hash"`
	SourceClusterID     string          `json:"source_cluster_id"`
	SourceConfigID      uint            `json:"source_config_id"`
	AbortSlot           *quepaxa.Slot   `json:"abort_slot"`
	FreezeSlot          uint64          `json:"freeze_slot"`
	FreezeHash          string          `json:"freeze_hash"`
	TerminalSlot        uint64          `json:"terminal_slot"`
	TerminalHash        string          `json:"terminal_hash"`
	Target              quepaxa.Cluster `json:"target"`
	CandidatePodUID     string          `json:"candidate_pod_uid"`
	CandidateMember     quepaxa.Member  `json:"candidate_member"`
	CandidateBindRef    string          `json:"candidate_bind_ref"`
	ExecutorNodeID      string          `json:"executor_node_id"`
	ExecutorPodUID      string          `json:"executor_pod_uid"`
	ExecutorRunID       string          `json:"executor_run_id"`
	ExecutorWALIdentity string          `json:"executor_wal_identity"`
}

// MembershipRepairCompletionResponse is emitted only after the exact terminal
// decision was completed and durably archived by the authenticated runtime.
type MembershipRepairCompletionResponse struct {
	Version             uint   `json:"version"`
	Action              string `json:"action"`
	Challenge           string `json:"challenge"`
	AttemptRevision     uint64 `json:"attempt_revision"`
	ScopeDigest         string `json:"scope_digest"`
	RequestDigest       string `json:"request_digest"`
	ExecutorNodeID      string `json:"executor_node_id"`
	ExecutorPodUID      string `json:"executor_pod_uid"`
	ExecutorRunID       string `json:"executor_run_id"`
	ExecutorWALIdentity string `json:"executor_wal_identity"`
	TerminalSlot        uint64 `json:"terminal_slot"`
	TerminalHash        string `json:"terminal_hash"`
	Outcome             string `json:"outcome"`
	MAC                 string `json:"mac"`
}

type MembershipRepairCompletionFunc func(context.Context, MembershipRepairCompletionRequest) (MembershipRepairCompletionResponse, error)

// SetMembershipRepairCompletion installs the exact terminal-completion action
// before the HTTP server is exposed.
func (s *Server) SetMembershipRepairCompletion(complete MembershipRepairCompletionFunc) {
	s.membershipRepairCompletion = complete
}

func StrictDecodeMembershipRepairCompletionRequest(data []byte) (MembershipRepairCompletionRequest, error) {
	var request MembershipRepairCompletionRequest
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return request, fmt.Errorf("invalid membership repair completion: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return request, fmt.Errorf("invalid membership repair completion: %w", err)
	}
	if err := requireExactJSONFields(fields, []string{
		"version", "action", "challenge", "attempt_revision", "scope_digest", "repair_operation_id", "repair_binding_digest", "phase_hash",
		"journal_operation_id", "journal_request_hash", "source_cluster_id", "source_config_id", "abort_slot", "freeze_slot", "freeze_hash",
		"terminal_slot", "terminal_hash", "target", "candidate_pod_uid", "candidate_member", "candidate_bind_ref", "executor_node_id",
		"executor_pod_uid", "executor_run_id", "executor_wal_identity",
	}); err != nil {
		return request, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, fmt.Errorf("invalid membership repair completion: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return request, fmt.Errorf("invalid membership repair completion: trailing JSON data")
	}
	if err := ValidateMembershipRepairCompletionRequest(request); err != nil {
		return request, err
	}
	return request, nil
}

func ValidateMembershipRepairCompletionRequest(request MembershipRepairCompletionRequest) error {
	challenge, challengeErr := hex.DecodeString(request.Challenge)
	if request.Version != 1 || request.Action != MembershipRepairCompletionAction || challengeErr != nil || len(challenge) != 32 || hex.EncodeToString(challenge) != request.Challenge ||
		request.AttemptRevision == 0 || !validProbeDigest(request.ScopeDigest) || request.RepairOperationID == "" || !validProbeDigest(request.RepairBindingDigest) ||
		!validProbeDigest(request.PhaseHash) || request.JournalOperationID == "" || !validProbeDigest(request.JournalRequestHash) || request.SourceClusterID == "" ||
		request.SourceConfigID == 0 || request.SourceConfigID == ^uint(0) || request.AbortSlot == nil || request.FreezeSlot == 0 || !validProbeDigest(request.FreezeHash) || request.TerminalSlot <= request.FreezeSlot ||
		!validProbeDigest(request.TerminalHash) || request.Target.ConfigID != request.SourceConfigID+1 || len(request.Target.Members) < 2 ||
		request.CandidatePodUID == "" || request.CandidateMember.ID == "" || request.CandidateMember.PeerURL == "" || request.CandidateMember.PublicKey == (quepaxa.PublicKey{}) ||
		!validProbeDigest(request.CandidateMember.WALIdentity) || !validProbeDigest(request.CandidateBindRef) || request.ExecutorNodeID == "" || request.ExecutorPodUID == "" ||
		request.ExecutorRunID == "" || !validProbeDigest(request.ExecutorWALIdentity) {
		return fmt.Errorf("invalid membership repair completion scope")
	}
	if !memberInTarget(request.Target, request.CandidateMember) || request.CandidateMember.ID == quepaxa.NodeID(request.ExecutorNodeID) {
		return fmt.Errorf("membership repair completion target does not bind candidate and distinct executor")
	}
	seen := make(map[quepaxa.NodeID]struct{}, len(request.Target.Members))
	for _, member := range request.Target.Members {
		if member.ID == "" || member.PublicKey == (quepaxa.PublicKey{}) || member.WALIdentity == "" {
			return fmt.Errorf("membership repair completion target has an incomplete member")
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return fmt.Errorf("membership repair completion target has duplicate members")
		}
		seen[member.ID] = struct{}{}
	}
	return nil
}

func memberInTarget(target quepaxa.Cluster, candidate quepaxa.Member) bool {
	for _, member := range target.Members {
		if member.ID == candidate.ID {
			return member == candidate
		}
	}
	return false
}

func MembershipRepairCompletionRequestDigest(request MembershipRepairCompletionRequest) (string, error) {
	data, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func StrictDecodeMembershipRepairCompletionResponse(data []byte, request MembershipRepairCompletionRequest, token string) (MembershipRepairCompletionResponse, error) {
	var response MembershipRepairCompletionResponse
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return response, fmt.Errorf("invalid membership repair completion response: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return response, fmt.Errorf("invalid membership repair completion response: %w", err)
	}
	if err := requireExactJSONFields(fields, []string{"version", "action", "challenge", "attempt_revision", "scope_digest", "request_digest", "executor_node_id", "executor_pod_uid", "executor_run_id", "executor_wal_identity", "terminal_slot", "terminal_hash", "outcome", "mac"}); err != nil {
		return response, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return response, fmt.Errorf("invalid membership repair completion response: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return response, fmt.Errorf("invalid membership repair completion response: trailing JSON data")
	}
	if err := validateMembershipRepairCompletionResponse(request, response); err != nil {
		return response, err
	}
	got, err := hex.DecodeString(response.MAC)
	if err != nil {
		return response, ErrRecoveryProbeMAC
	}
	want, _ := hex.DecodeString(membershipRepairCompletionMAC(response, token))
	if token == "" || !hmac.Equal(got, want) {
		return response, ErrRecoveryProbeMAC
	}
	return response, nil
}

func validateMembershipRepairCompletionResponse(request MembershipRepairCompletionRequest, response MembershipRepairCompletionResponse) error {
	if err := ValidateMembershipRepairCompletionRequest(request); err != nil {
		return err
	}
	digest, err := MembershipRepairCompletionRequestDigest(request)
	if err != nil {
		return err
	}
	if response.Version != request.Version || response.Action != request.Action || response.Challenge != request.Challenge || response.AttemptRevision != request.AttemptRevision ||
		response.ScopeDigest != request.ScopeDigest || response.RequestDigest != digest || response.ExecutorNodeID != request.ExecutorNodeID || response.ExecutorPodUID != request.ExecutorPodUID ||
		response.ExecutorRunID != request.ExecutorRunID || response.ExecutorWALIdentity != request.ExecutorWALIdentity || response.TerminalSlot != request.TerminalSlot ||
		response.TerminalHash != request.TerminalHash || response.Outcome != "completed" {
		return ErrRecoveryProbeIdentity
	}
	return nil
}

func membershipRepairCompletionMAC(response MembershipRepairCompletionResponse, token string) string {
	response.MAC = ""
	data, _ := json.Marshal(response)
	return recoveryProbeMACBytes(data, token)
}
