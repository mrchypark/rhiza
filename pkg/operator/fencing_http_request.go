package operator

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const httpFenceRequestPath = "/v1/fences"

// handleHTTPFenceRequest is a thin authenticated adapter over the existing
// RhizaFence CRD journal. It never stores a second request record or accepts a
// caller-supplied proof.
func (e *FencingExecutor) handleHTTPFenceRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != httpFenceRequestPath || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if err := e.authenticateController(r.Context(), r.Header.Get("Authorization")); err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "fencing authority unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "controller identity rejected", http.StatusForbidden)
		}
		return
	}

	var request FenceRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid fence request", http.StatusBadRequest)
		return
	}
	if request.OperationID == "" || r.Header.Get("Idempotency-Key") != request.OperationID {
		http.Error(w, "fence idempotency key differs from operation", http.StatusBadRequest)
		return
	}

	proof, err := (&KubernetesFencer{Kube: e.Kube}).Fence(r.Context(), request)
	if errors.Is(err, ErrFencePending) {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "fencing authority unavailable", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "fence request rejected", http.StatusConflict)
		}
		return
	}
	if err := validateFenceProof(request, proof); err != nil {
		http.Error(w, "fence proof rejected", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(proof); err != nil {
		return
	}
}
