package network

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	recoveryStatusTimeout  = 2 * time.Second
	recoveryArchiveTimeout = 30 * time.Second
)

// VoterRecoveryStatus reports the read-only facts an operator needs to assess
// voter recovery. It does not grant recovery or fencing authority.
type VoterRecoveryStatus struct {
	NodeID            string   `json:"node_id"`
	ClusterID         string   `json:"cluster_id"`
	Durability        string   `json:"durability"`
	Ready             bool     `json:"ready"`
	ConfigID          uint     `json:"config_id"`
	Voters            []string `json:"voters"`
	WALIdentity       string   `json:"wal_identity"`
	Voting            bool     `json:"voting"`
	Pending           bool     `json:"pending"`
	AbortSlot         uint64   `json:"abort_slot"`
	Quorum            bool     `json:"quorum"`
	QuorumKnown       bool     `json:"quorum_known"`
	QuorumUnavailable bool     `json:"quorum_unavailable"`
	QuorumConfigID    uint     `json:"quorum_config_id"`
	ReadIndexSlot     uint64   `json:"read_index_slot"`
	CertifiedTip      uint64   `json:"certified_tip"`
	AppliedTip        uint64   `json:"applied_tip"`
	ArchiveTip        uint64   `json:"archive_tip"`
}

// VoterRecoveryStatusFunc reports one membership snapshot and a fresh
// read-index result against that snapshot. Excluded is empty for the normal
// quorum or names the voter to exclude for a recovery-survivor probe.
type VoterRecoveryStatusFunc func(context.Context, string) VoterRecoveryStatus

// SetVoterRecoveryStatus installs the legacy voter status source.
func (s *Server) SetVoterRecoveryStatus(status func(context.Context) VoterRecoveryStatus) {
	s.voterRecoveryStatus = status
	s.probeStatus = nil
}

// SetVoterRecoveryObservation installs a coherent membership/quorum source for
// authenticated recovery probes.
func (s *Server) SetVoterRecoveryObservation(status VoterRecoveryStatusFunc) {
	s.probeStatus = status
	s.voterRecoveryStatus = nil
}

// SetRecoveryArchive installs the authenticated operation that publishes the
// local certified suffix to shared archive storage before destructive recovery.
func (s *Server) SetRecoveryArchive(token string, archive func(context.Context) error) {
	s.recoveryArchiveToken = token
	s.recoveryArchive = archive
}

func (s *Server) handleVoterRecoveryStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	if s.voterRecoveryStatus == nil && s.probeStatus == nil {
		writeHTTPError(w, http.StatusNotFound, "not_a_voter", "not a voter")
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
	w.Header().Set("Content-Type", "application/json")
	var status VoterRecoveryStatus
	if s.probeStatus != nil {
		status = s.probeStatus(ctx, "")
	} else {
		status = s.voterRecoveryStatus(ctx)
	}
	// Preserve the legacy unauthenticated endpoint's limited response. The new
	// membership and WAL identity fields are available only through the
	// authenticated, challenge-bound recovery probe.
	_ = json.NewEncoder(w).Encode(struct {
		NodeID       string `json:"node_id"`
		ClusterID    string `json:"cluster_id"`
		Durability   string `json:"durability"`
		Ready        bool   `json:"ready"`
		Quorum       bool   `json:"quorum"`
		CertifiedTip uint64 `json:"certified_tip"`
		AppliedTip   uint64 `json:"applied_tip"`
		ArchiveTip   uint64 `json:"archive_tip"`
	}{status.NodeID, status.ClusterID, status.Durability, status.Ready, status.Quorum, status.CertifiedTip, status.AppliedTip, status.ArchiveTip})
}

func (s *Server) handleRecoveryArchive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if s.recoveryArchiveToken == "" || s.recoveryArchive == nil || !matchesRecoveryBearer(r.Header.Get("Authorization"), s.recoveryArchiveToken) {
		writeHTTPError(w, http.StatusForbidden, "forbidden", "recovery archive is not authorized")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), recoveryArchiveTimeout)
	defer cancel()
	if err := s.recoveryArchive(ctx); err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Status string `json:"status"`
	}{Status: "archived"})
}

func matchesRecoveryBearer(header, token string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(header, prefix)), []byte(token)) == 1
}
