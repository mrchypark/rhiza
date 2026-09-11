package network

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"net/http"
)

// RecoveryProbe binds a fresh challenge to the real ReadIndex-backed status.
// It is not a fencing proof and a timeout is not evidence of process death.
type RecoveryProbe struct {
	Excluded string              `json:"excluded,omitempty"`
	Nonce    string              `json:"nonce"`
	Status   VoterRecoveryStatus `json:"status"`
	MAC      string              `json:"mac"`
}

func recoveryProbeMAC(p RecoveryProbe, token string) string {
	p.MAC = ""
	data, _ := json.Marshal(p)
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}
func VerifyRecoveryProbe(p RecoveryProbe, nonce, token string) bool {
	if token == "" || p.Nonce != nonce {
		return false
	}
	got, err := hex.DecodeString(p.MAC)
	if err != nil {
		return false
	}
	want, _ := hex.DecodeString(recoveryProbeMAC(p, token))
	return hmac.Equal(got, want)
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
	probe := RecoveryProbe{Nonce: nonce, Status: s.voterRecoveryStatus(ctx), Excluded: r.URL.Query().Get("exclude")}
	if probe.Excluded != "" {
		if s.core == nil {
			probe.Status.Quorum = false
		} else {
			_, _, err := s.core.ReadIndexExcluding(ctx, quepaxa.NodeID(probe.Excluded))
			probe.Status.Quorum = err == nil
		}
	}
	probe.MAC = recoveryProbeMAC(probe, token)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(probe)
}
