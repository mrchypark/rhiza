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
	"mime"
	"net/http"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const RecoveryProbeProtocolVersion uint = 1

var (
	ErrRecoveryProbeSchema   = errors.New("invalid recovery probe schema")
	ErrRecoveryProbeVersion  = errors.New("unsupported recovery probe version")
	ErrRecoveryProbeIdentity = errors.New("recovery probe challenge or scope mismatch")
	ErrRecoveryProbeMAC      = errors.New("invalid recovery probe MAC")
)

// RecoveryProbe binds a fresh challenge to the real ReadIndex-backed status.
// It is not a fencing proof and a timeout is not evidence of process death.
type RecoveryProbe struct {
	Version  uint                `json:"version"`
	Excluded string              `json:"excluded"`
	Nonce    string              `json:"nonce"`
	Status   VoterRecoveryStatus `json:"status"`
	MAC      string              `json:"mac"`
}

// MembershipRepairProbeRequest is a fresh authority-scoped request for one
// current voter to verify an exact candidate prefix before old-member
// closure. AbortSlot is a pointer so an explicit zero is distinct from absent.
type MembershipRepairProbeRequest struct {
	Version             uint           `json:"version"`
	Challenge           string         `json:"challenge"`
	ScopeDigest         string         `json:"scope_digest"`
	SourceConfigID      uint           `json:"source_config_id"`
	AbortSlot           *quepaxa.Slot  `json:"abort_slot"`
	ExcludedOldMemberID string         `json:"excluded_old_member_id"`
	CandidateMember     quepaxa.Member `json:"candidate_member"`
	CandidateBindRef    string         `json:"candidate_bind_ref"`
}

// MembershipRepairProbeResponse reports the exact observations made by one
// authenticated current voter. CandidatePrefix is populated only after the
// runtime's real VerifyLearner call accepts the exact member/WAL/bind ref.
// It is not a caller-supplied Verified flag.
type MembershipRepairProbeResponse struct {
	Version                uint             `json:"version"`
	Challenge              string           `json:"challenge"`
	ScopeDigest            string           `json:"scope_digest"`
	VerifierNodeID         string           `json:"verifier_node_id"`
	VerifierWALIdentity    string           `json:"verifier_wal_identity"`
	Ready                  bool             `json:"ready"`
	Voting                 bool             `json:"voting"`
	Pending                bool             `json:"pending"`
	SourceConfigID         uint             `json:"source_config_id"`
	SourceMembers          []quepaxa.Member `json:"source_members"`
	AbortSlot              *quepaxa.Slot    `json:"abort_slot"`
	ExcludedOldMemberID    string           `json:"excluded_old_member_id"`
	ReadIndexSlot          uint64           `json:"read_index_slot"`
	BarrierSlot            uint64           `json:"barrier_slot"`
	BarrierNonce           string           `json:"barrier_nonce"`
	PrefixHash             string           `json:"prefix_hash"`
	CandidateMember        quepaxa.Member   `json:"candidate_member"`
	CandidateBindRef       string           `json:"candidate_bind_ref"`
	CandidatePrefixThrough uint64           `json:"candidate_prefix_through"`
	CandidatePrefixHash    string           `json:"candidate_prefix_hash"`
	MAC                    string           `json:"mac"`
}

type MembershipRepairProbeFunc func(context.Context, MembershipRepairProbeRequest) (MembershipRepairProbeResponse, error)

// SetMembershipRepairProbe installs the authenticated preclosure verifier
// before the HTTP server is exposed.
func (s *Server) SetMembershipRepairProbe(probe MembershipRepairProbeFunc) {
	s.membershipRepairProbe = probe
}

// MembershipRepairBarrierNonce deterministically binds the barrier value to
// the authority-issued challenge.
func MembershipRepairBarrierNonce(challenge string) [quepaxa.ReadBarrierNonceSize]byte {
	hash := sha256.Sum256([]byte("rhiza-membership-repair-barrier-v1\x00" + challenge))
	var nonce [quepaxa.ReadBarrierNonceSize]byte
	copy(nonce[:], hash[:len(nonce)])
	return nonce
}

type legacyRecoveryStatus struct {
	NodeID       string `json:"node_id"`
	ClusterID    string `json:"cluster_id"`
	Durability   string `json:"durability"`
	Ready        bool   `json:"ready"`
	Quorum       bool   `json:"quorum"`
	CertifiedTip uint64 `json:"certified_tip"`
	AppliedTip   uint64 `json:"applied_tip"`
	ArchiveTip   uint64 `json:"archive_tip"`
}

type legacyRecoveryProbe struct {
	Excluded string               `json:"excluded,omitempty"`
	Nonce    string               `json:"nonce"`
	Status   legacyRecoveryStatus `json:"status"`
	MAC      string               `json:"mac"`
}

func (p RecoveryProbe) legacy() legacyRecoveryProbe {
	return legacyRecoveryProbe{
		Excluded: p.Excluded,
		Nonce:    p.Nonce,
		Status: legacyRecoveryStatus{
			NodeID: p.Status.NodeID, ClusterID: p.Status.ClusterID, Durability: p.Status.Durability,
			Ready: p.Status.Ready, Quorum: p.Status.Quorum, CertifiedTip: p.Status.CertifiedTip,
			AppliedTip: p.Status.AppliedTip, ArchiveTip: p.Status.ArchiveTip,
		},
		MAC: p.MAC,
	}
}

func recoveryProbeMAC(p RecoveryProbe, token string) string {
	p.MAC = ""
	data, _ := json.Marshal(p)
	return recoveryProbeMACBytes(data, token)
}

func legacyRecoveryProbeMAC(p RecoveryProbe, token string) string {
	legacy := p.legacy()
	legacy.MAC = ""
	data, _ := json.Marshal(legacy)
	return recoveryProbeMACBytes(data, token)
}

func recoveryProbeMACBytes(data []byte, token string) string {
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyRecoveryProbe verifies the pre-versioned wire format for compatibility
// with existing callers. New clients should use StrictDecodeRecoveryProbe.
func VerifyRecoveryProbe(p RecoveryProbe, nonce, token string) bool {
	if token == "" || p.Nonce != nonce {
		return false
	}
	got, err := hex.DecodeString(p.MAC)
	if err != nil {
		return false
	}
	want, _ := hex.DecodeString(legacyRecoveryProbeMAC(p, token))
	return hmac.Equal(got, want)
}

func verifyVersionedRecoveryProbe(p RecoveryProbe, token string) bool {
	if token == "" || p.Version != RecoveryProbeProtocolVersion {
		return false
	}
	got, err := hex.DecodeString(p.MAC)
	if err != nil {
		return false
	}
	want, _ := hex.DecodeString(recoveryProbeMAC(p, token))
	return hmac.Equal(got, want)
}

func validRecoveryObservation(status VoterRecoveryStatus) bool {
	localVoter := false
	for _, voter := range status.Voters {
		if voter == status.NodeID {
			localVoter = true
			break
		}
	}
	if status.QuorumKnown {
		return status.Quorum && !status.QuorumUnavailable && status.Voting && localVoter && !status.Pending && status.QuorumConfigID == status.ConfigID
	}
	if status.QuorumUnavailable {
		return !status.Quorum && status.Voting && localVoter && !status.Pending && status.QuorumConfigID == 0 && status.ReadIndexSlot == 0
	}
	return !status.Quorum && status.QuorumConfigID == 0 && status.ReadIndexSlot == 0
}

func validVersionedRecoveryProbe(probe RecoveryProbe) bool {
	return probe.Version == RecoveryProbeProtocolVersion &&
		!(probe.Excluded != "" && probe.Excluded == probe.Status.NodeID) &&
		validRecoveryObservation(probe.Status)
}

func strictDecodeRecoveryProbe(data []byte) (RecoveryProbe, error) {
	var probe RecoveryProbe
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return probe, fmt.Errorf("%w: %v", ErrRecoveryProbeSchema, err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return probe, fmt.Errorf("%w: %v", ErrRecoveryProbeSchema, err)
	}
	if err := requireExactJSONFields(top, []string{"version", "excluded", "nonce", "status", "mac"}); err != nil {
		return probe, err
	}
	var status map[string]json.RawMessage
	if err := json.Unmarshal(top["status"], &status); err != nil {
		return probe, fmt.Errorf("%w: status: %v", ErrRecoveryProbeSchema, err)
	}
	if err := requireExactJSONFields(status, []string{
		"node_id", "cluster_id", "durability", "ready", "config_id", "voters", "wal_identity",
		"voting", "pending", "abort_slot", "quorum", "quorum_known", "quorum_unavailable",
		"quorum_config_id", "read_index_slot", "certified_tip", "applied_tip", "archive_tip",
	}); err != nil {
		return probe, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&probe); err != nil {
		return RecoveryProbe{}, fmt.Errorf("%w: %v", ErrRecoveryProbeSchema, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return RecoveryProbe{}, fmt.Errorf("%w: trailing JSON data", ErrRecoveryProbeSchema)
	}
	if probe.Version != RecoveryProbeProtocolVersion {
		return RecoveryProbe{}, fmt.Errorf("%w: %d", ErrRecoveryProbeVersion, probe.Version)
	}
	if !validVersionedRecoveryProbe(probe) {
		return RecoveryProbe{}, fmt.Errorf("%w: inconsistent quorum state", ErrRecoveryProbeSchema)
	}
	return probe, nil
}

func requireExactJSONFields(fields map[string]json.RawMessage, required []string) error {
	if len(fields) != len(required) {
		return fmt.Errorf("%w: field count", ErrRecoveryProbeSchema)
	}
	for _, key := range required {
		raw, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("%w: missing or null field %q", ErrRecoveryProbeSchema, key)
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON data")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("array is not closed")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	return nil
}

// StrictDecodeRecoveryProbe validates the versioned wire schema, expected
// challenge/scope, semantic state tuple, and complete-payload HMAC. Legacy
// responses intentionally fail with ErrRecoveryProbeSchema (missing version).
func StrictDecodeRecoveryProbe(data []byte, nonce, excluded, token string) (RecoveryProbe, error) {
	probe, err := strictDecodeRecoveryProbe(data)
	if err != nil {
		return RecoveryProbe{}, err
	}
	if probe.Nonce != nonce || probe.Excluded != excluded {
		return RecoveryProbe{}, ErrRecoveryProbeIdentity
	}
	if !verifyVersionedRecoveryProbe(probe, token) {
		return RecoveryProbe{}, ErrRecoveryProbeMAC
	}
	return probe, nil
}

func (s *Server) handleRecoveryProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		token := s.recoveryArchiveToken
		if token == "" || !matchesRecoveryBearer(r.Header.Get("Authorization"), token) {
			writeHTTPError(w, http.StatusForbidden, "forbidden", "recovery probe is not authorized")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
		if err != nil || len(body) > 1<<20 {
			writeHTTPError(w, http.StatusBadRequest, "invalid_request", "membership repair action body is invalid or too large")
			return
		}
		if err := rejectDuplicateJSONKeys(body); err != nil {
			writeHTTPError(w, http.StatusBadRequest, "invalid_request", "membership repair action contains duplicate fields")
			return
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(body, &envelope); err != nil {
			writeHTTPError(w, http.StatusBadRequest, "invalid_request", "membership repair action must be a JSON object")
			return
		}
		if rawAction, present := envelope["action"]; present {
			var action string
			if err := json.Unmarshal(rawAction, &action); err != nil || action != MembershipRepairCompletionAction {
				writeHTTPError(w, http.StatusBadRequest, "invalid_request", "unsupported membership repair action")
				return
			}
			s.handleMembershipRepairCompletion(w, r, body)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		s.handleMembershipRepairProbe(w, r)
		return
	}
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet, http.MethodPost)
		return
	}
	token := s.recoveryArchiveToken
	if token == "" || !matchesRecoveryBearer(r.Header.Get("Authorization"), token) {
		writeHTTPError(w, http.StatusForbidden, "forbidden", "recovery probe is not authorized")
		return
	}
	nonce := r.URL.Query().Get("nonce")
	raw, err := hex.DecodeString(nonce)
	if err != nil || len(raw) != 32 {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "32-byte challenge required")
		return
	}
	versionValues, hasVersion := r.URL.Query()["version"]
	if !hasVersion {
		s.handleLegacyRecoveryProbe(w, r, token, nonce)
		return
	}
	if len(versionValues) != 1 || versionValues[0] != "1" {
		writeHTTPError(w, http.StatusBadRequest, "unsupported_recovery_probe_version", "supported recovery probe version is 1")
		return
	}
	if s.probeStatus == nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "recovery_observation_unavailable", "coherent recovery observation unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), recoveryStatusTimeout)
	defer cancel()
	release, err := s.acquireRead(ctx, false)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	probe := RecoveryProbe{Version: RecoveryProbeProtocolVersion, Excluded: r.URL.Query().Get("exclude"), Nonce: nonce}
	probe.Status = s.probeStatus(ctx, probe.Excluded)
	if !validVersionedRecoveryProbe(probe) {
		writeHTTPError(w, http.StatusServiceUnavailable, "invalid_recovery_observation", "recovery observation is internally inconsistent")
		return
	}
	probe.MAC = recoveryProbeMAC(probe, token)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(probe)
}

func (s *Server) handleMembershipRepairCompletion(w http.ResponseWriter, r *http.Request, body []byte) {
	token := s.recoveryArchiveToken
	if token == "" || !matchesRecoveryBearer(r.Header.Get("Authorization"), token) {
		writeHTTPError(w, http.StatusForbidden, "forbidden", "membership repair completion is not authorized")
		return
	}
	if s.membershipRepairCompletion == nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "recovery_observation_unavailable", "membership repair completion is unavailable")
		return
	}
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		writeHTTPError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "application/json is required")
		return
	}
	request, err := StrictDecodeMembershipRepairCompletionRequest(body)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), recoveryArchiveTimeout)
	defer cancel()
	release, err := s.acquireRead(ctx, true)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	response, err := s.membershipRepairCompletion(ctx, request)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := validateMembershipRepairCompletionResponse(request, response); err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "invalid_recovery_observation", err.Error())
		return
	}
	response.MAC = membershipRepairCompletionMAC(response, token)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) handleMembershipRepairProbe(w http.ResponseWriter, r *http.Request) {
	token := s.recoveryArchiveToken
	if token == "" || !matchesRecoveryBearer(r.Header.Get("Authorization"), token) {
		writeHTTPError(w, http.StatusForbidden, "forbidden", "membership repair probe is not authorized")
		return
	}
	if s.membershipRepairProbe == nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "recovery_observation_unavailable", "membership repair verification is unavailable")
		return
	}
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		writeHTTPError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "application/json is required")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	if err != nil || len(body) > 1<<20 {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", "membership repair probe body is invalid or too large")
		return
	}
	request, err := StrictDecodeMembershipRepairProbeRequest(body)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), recoveryArchiveTimeout)
	defer cancel()
	release, err := s.acquireRead(ctx, true)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	response, err := s.membershipRepairProbe(ctx, request)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if err := validateMembershipRepairProbeResponse(request, response); err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "invalid_recovery_observation", err.Error())
		return
	}
	response.MAC = membershipRepairProbeMAC(response, token)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(response)
}

func StrictDecodeMembershipRepairProbeRequest(data []byte) (MembershipRepairProbeRequest, error) {
	var request MembershipRepairProbeRequest
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return request, fmt.Errorf("invalid membership repair probe: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return request, fmt.Errorf("invalid membership repair probe: %w", err)
	}
	if err := requireExactJSONFields(fields, []string{"version", "challenge", "scope_digest", "source_config_id", "abort_slot", "excluded_old_member_id", "candidate_member", "candidate_bind_ref"}); err != nil {
		return request, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, fmt.Errorf("invalid membership repair probe: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return request, fmt.Errorf("invalid membership repair probe: trailing JSON data")
	}
	if err := validateMembershipRepairProbeRequest(request); err != nil {
		return request, err
	}
	return request, nil
}

func StrictDecodeMembershipRepairProbeResponse(data []byte, request MembershipRepairProbeRequest, token string) (MembershipRepairProbeResponse, error) {
	var response MembershipRepairProbeResponse
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return response, fmt.Errorf("invalid membership repair probe response: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return response, fmt.Errorf("invalid membership repair probe response: %w", err)
	}
	if err := requireExactJSONFields(fields, []string{"version", "challenge", "scope_digest", "verifier_node_id", "verifier_wal_identity", "ready", "voting", "pending", "source_config_id", "source_members", "abort_slot", "excluded_old_member_id", "read_index_slot", "barrier_slot", "barrier_nonce", "prefix_hash", "candidate_member", "candidate_bind_ref", "candidate_prefix_through", "candidate_prefix_hash", "mac"}); err != nil {
		return response, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return response, fmt.Errorf("invalid membership repair probe response: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return response, fmt.Errorf("invalid membership repair probe response: trailing JSON data")
	}
	if err := validateMembershipRepairProbeResponse(request, response); err != nil {
		return response, err
	}
	got, err := hex.DecodeString(response.MAC)
	if err != nil {
		return response, ErrRecoveryProbeMAC
	}
	want, _ := hex.DecodeString(membershipRepairProbeMAC(response, token))
	if token == "" || !hmac.Equal(got, want) {
		return response, ErrRecoveryProbeMAC
	}
	return response, nil
}

func validateMembershipRepairProbeRequest(request MembershipRepairProbeRequest) error {
	challenge, challengeErr := hex.DecodeString(request.Challenge)
	candidate := request.CandidateMember
	if request.Version != 1 || challengeErr != nil || len(challenge) != 32 || hex.EncodeToString(challenge) != request.Challenge ||
		!validProbeDigest(request.ScopeDigest) || request.SourceConfigID == 0 || request.AbortSlot == nil ||
		request.ExcludedOldMemberID == "" || candidate.ID == "" || candidate.ID == quepaxa.NodeID(request.ExcludedOldMemberID) ||
		candidate.URL == "" || candidate.PeerURL == "" || candidate.PublicKey == (quepaxa.PublicKey{}) ||
		!validProbeDigest(candidate.WALIdentity) || request.CandidateBindRef == "" {
		return fmt.Errorf("invalid membership repair probe scope")
	}
	return nil
}

func ValidateMembershipRepairProbeRequest(request MembershipRepairProbeRequest) error {
	return validateMembershipRepairProbeRequest(request)
}

func validateMembershipRepairProbeResponse(request MembershipRepairProbeRequest, response MembershipRepairProbeResponse) error {
	if err := validateMembershipRepairProbeRequest(request); err != nil {
		return err
	}
	if response.Version != request.Version || response.Challenge != request.Challenge || response.ScopeDigest != request.ScopeDigest ||
		response.SourceConfigID != request.SourceConfigID || response.AbortSlot == nil || *response.AbortSlot != *request.AbortSlot ||
		response.ExcludedOldMemberID != request.ExcludedOldMemberID || response.VerifierNodeID == "" ||
		response.VerifierNodeID == request.ExcludedOldMemberID || !validProbeDigest(response.VerifierWALIdentity) || !response.Ready || !response.Voting || response.Pending ||
		response.ReadIndexSlot == 0 || response.BarrierSlot < response.ReadIndexSlot || response.CandidatePrefixThrough != response.BarrierSlot ||
		response.BarrierNonce != membershipRepairBarrierNonceHex(request.Challenge) ||
		response.CandidateMember != request.CandidateMember || response.CandidateBindRef != request.CandidateBindRef ||
		response.CandidatePrefixHash != response.PrefixHash || !validProbeDigest(response.PrefixHash) ||
		!validProbeDigest(response.CandidatePrefixHash) || len(response.SourceMembers) == 0 {
		return fmt.Errorf("membership repair probe response differs from requested scope or lacks required observations")
	}
	verifierCount, oldCount, candidateCount := 0, 0, 0
	seen := make(map[quepaxa.NodeID]struct{}, len(response.SourceMembers))
	for _, member := range response.SourceMembers {
		if member.ID == "" {
			return fmt.Errorf("membership repair probe response has an incomplete source member")
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return fmt.Errorf("membership repair probe response has duplicate source members")
		}
		seen[member.ID] = struct{}{}
		if string(member.ID) == response.VerifierNodeID {
			verifierCount++
			if member.WALIdentity != response.VerifierWALIdentity {
				return fmt.Errorf("membership repair probe verifier WAL differs from source membership")
			}
		}
		if string(member.ID) == response.ExcludedOldMemberID {
			oldCount++
		}
		if member.ID == response.CandidateMember.ID {
			candidateCount++
			if member != response.CandidateMember {
				return fmt.Errorf("membership repair probe candidate ID is present with a different binding")
			}
		}
	}
	if verifierCount != 1 || oldCount != 1 || candidateCount != 0 {
		return fmt.Errorf("membership repair probe source membership does not bind verifier, excluded voter, and candidate")
	}
	return nil
}

func membershipRepairBarrierNonceHex(challenge string) string {
	nonce := MembershipRepairBarrierNonce(challenge)
	return hex.EncodeToString(nonce[:])
}

func validProbeDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func membershipRepairProbeMAC(response MembershipRepairProbeResponse, token string) string {
	response.MAC = ""
	data, _ := json.Marshal(response)
	return recoveryProbeMACBytes(data, token)
}

func (s *Server) handleLegacyRecoveryProbe(w http.ResponseWriter, r *http.Request, token, nonce string) {
	if s.probeStatus == nil && s.voterRecoveryStatus == nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "recovery_observation_unavailable", "recovery observation unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), recoveryStatusTimeout)
	defer cancel()
	release, err := s.acquireRead(ctx, false)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	probe := RecoveryProbe{Nonce: nonce, Excluded: r.URL.Query().Get("exclude")}
	if s.probeStatus != nil {
		probe.Status = s.probeStatus(ctx, probe.Excluded)
	} else {
		probe.Status = s.voterRecoveryStatus(ctx)
		if probe.Excluded != "" {
			if s.core == nil {
				probe.Status.Quorum = false
			} else {
				_, _, err := s.core.ReadIndexExcluding(ctx, quepaxa.NodeID(probe.Excluded))
				probe.Status.Quorum = err == nil
			}
		}
	}
	legacy := probe.legacy()
	legacy.MAC = legacyRecoveryProbeMAC(probe, token)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(legacy)
}
