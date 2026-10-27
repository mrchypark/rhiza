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
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
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
