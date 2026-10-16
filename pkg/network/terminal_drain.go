package network

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// TerminalDrainRequest binds destructive admission to an observed engine identity.
type TerminalDrainRequest struct {
	ClusterID   string `json:"clusterID"`
	NodeID      string `json:"nodeID"`
	WALIdentity string `json:"walIdentity"`
}

// SetTerminalDrain installs a cooperative engine-stop operation before serving.
func (s *Server) SetTerminalDrain(drain func(context.Context, TerminalDrainRequest) (any, error)) {
	s.terminalDrain = drain
}

func (s *Server) handleTerminalDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if s.recoveryArchiveToken == "" || s.terminalDrain == nil || !matchesRecoveryBearer(r.Header.Get("Authorization"), s.recoveryArchiveToken) {
		writeHTTPError(w, http.StatusForbidden, "forbidden", "terminal drain is not authorized")
		return
	}
	var expected TerminalDrainRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&expected) != nil || decoder.Decode(new(any)) != io.EOF || expected.ClusterID == "" || expected.NodeID == "" || expected.WALIdentity == "" {
		writeHTTPError(w, http.StatusBadRequest, "invalid_drain_identity", "terminal drain requires the expected cluster, node, and WAL identity")
		return
	}
	result, err := s.terminalDrain(r.Context(), expected)
	if err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "drain_unconfirmed", "terminal drain outcome is not confirmed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
