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
	NodeID       string `json:"node_id"`
	ClusterID    string `json:"cluster_id"`
	Durability   string `json:"durability"`
	Ready        bool   `json:"ready"`
	Quorum       bool   `json:"quorum"`
	CertifiedTip uint64 `json:"certified_tip"`
	AppliedTip   uint64 `json:"applied_tip"`
	ArchiveTip   uint64 `json:"archive_tip"`
}

// SetVoterRecoveryStatus installs the voter-only recovery status source.
func (s *Server) SetVoterRecoveryStatus(status func(context.Context) VoterRecoveryStatus) {
	s.voterRecoveryStatus = status
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
	if s.voterRecoveryStatus == nil {
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
	_ = json.NewEncoder(w).Encode(s.voterRecoveryStatus(ctx))
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
