package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type GraphExecuteResponse struct {
	types.MutationReceipt
}

type GraphQueryRequest struct {
	Cypher      string         `json:"cypher"`
	Args        map[string]any `json:"args,omitempty"`
	Consistency string         `json:"consistency,omitempty"`
}

type GraphReachableRequest = types.GraphReachableRequest
type GraphReachableResult = types.GraphReachableResult

type GraphStreamReadRequest struct {
	Stream        string `json:"stream,omitempty"`
	AfterSequence uint64 `json:"after_sequence,omitempty"`
	Limit         uint   `json:"limit,omitempty"`
	WaitMS        uint32 `json:"wait_ms,omitempty"`
	Consistency   string `json:"consistency,omitempty"`
}

type GraphStreamReadResponse struct {
	Records      []types.GraphStreamRecord `json:"records"`
	AppliedSlot  uint64                    `json:"applied_slot"`
	ConsensusTip uint64                    `json:"consensus_tip"`
}

type GraphStreamOffsetRequest struct {
	RequestID   string `json:"request_id,omitempty"`
	Stream      string `json:"stream"`
	Consumer    string `json:"consumer"`
	Sequence    uint64 `json:"sequence,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

type GraphStreamOffsetResponse struct {
	Sequence     uint64 `json:"sequence"`
	Found        bool   `json:"found"`
	AppliedSlot  uint64 `json:"applied_slot"`
	ConsensusTip uint64 `json:"consensus_tip"`
}

type GraphStreamTrimRequest struct {
	RequestID       string `json:"request_id"`
	Stream          string `json:"stream"`
	ThroughSequence uint64 `json:"through_sequence"`
}

func (s *Server) handleGraphExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var command types.GraphCommand
	if err := decodeJSON(w, r, &command); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	response, err := s.GraphExecute(r.Context(), command)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) GraphExecute(ctx context.Context, command types.GraphCommand) (GraphExecuteResponse, error) {
	if s.expectedPolicy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return s.graphExecuteManaged(ctx, command)
	}
	if managedKVExecutionPolicy(s.expectedPolicy) {
		return GraphExecuteResponse{}, ErrInvalidRequest
	}
	if !s.writable || !s.Ready() {
		return GraphExecuteResponse{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return GraphExecuteResponse{}, err
	}
	charge, err := mutationCharge(command)
	if err != nil {
		return GraphExecuteResponse{}, err
	}
	lease, err := s.admitMutation(charge)
	if err != nil {
		return GraphExecuteResponse{}, err
	}
	defer lease.releaseCaller()
	command, err = cloneGraphCommand(command)
	if err != nil {
		return GraphExecuteResponse{}, err
	}
	if err := materializer.ValidateGraphCommandAdmission(command); err != nil {
		return GraphExecuteResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	unlock, err := s.lockRequest(ctx, command.RequestID)
	if err != nil {
		return GraphExecuteResponse{}, err
	}
	defer unlock()
	if matches, err := s.material.GraphRequestMatches(ctx, command); err != nil {
		return GraphExecuteResponse{}, err
	} else if !matches {
		return GraphExecuteResponse{}, ErrRequestConflict
	}
	if receipt, found, err := s.material.GraphMutationReceipt(ctx, command.RequestID); err != nil {
		return GraphExecuteResponse{}, err
	} else if found {
		if err := s.waitReceiptDurable(ctx, receipt, command.RequestID); err != nil {
			return GraphExecuteResponse{}, err
		}
		return GraphExecuteResponse{MutationReceipt: receipt}, nil
	}
	submitSlot, err := s.graphBatcher.submit(ctx, command, lease)
	if err != nil {
		return GraphExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, err)
	}
	receipt, found, err := s.material.GraphMutationReceipt(ctx, command.RequestID)
	if err != nil {
		return GraphExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, err)
	}
	if !found {
		return GraphExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, fmt.Errorf("graph mutation receipt is unavailable"))
	}
	if matches, err := s.material.GraphRequestMatches(ctx, command); err != nil {
		return GraphExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, err)
	} else if !matches {
		return GraphExecuteResponse{}, ErrRequestConflict
	}
	return GraphExecuteResponse{MutationReceipt: receipt}, nil
}

func (s *Server) graphExecuteManaged(ctx context.Context, command types.GraphCommand) (GraphExecuteResponse, error) {
	if reason := s.managedGraphReadinessFailure(); reason != "" {
		log.Printf("MANAGED_GRAPH_READINESS_REFUSED reason=%s", reason)
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_readiness", ErrNotReady)
	}
	if err := ctx.Err(); err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_context", err)
	}
	frame, err := PrepareManagedGraphExecution(command)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_prepare", err)
	}
	owned, recognized, err := types.DecodeManagedGraphExecution(frame)
	if err != nil || !recognized {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_decode", ErrInvalidRequest)
	}
	fingerprint, err := types.ManagedGraphIntentFingerprint(owned)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_fingerprint", err)
	}
	charge, err := mutationCharge(owned)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_charge", err)
	}
	lease, err := s.admitMutation(charge)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_admission", err)
	}
	defer lease.releaseCaller()
	unlock, err := s.lockRequest(ctx, owned.RequestID)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_request_lock", err)
	}
	defer unlock()
	receipt, found, matches, err := s.material.GraphRequestStatusFingerprint(ctx, owned.RequestID, fingerprint)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_receipt_lookup", err)
	}
	if found && !matches {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_receipt_lookup", ErrRequestConflict)
	}
	if found {
		if err := s.waitReceiptDurable(ctx, receipt, owned.RequestID); err != nil {
			return GraphExecuteResponse{}, managedGraphExecuteFailure("network_receipt_durability", err)
		}
		return GraphExecuteResponse{MutationReceipt: receipt}, nil
	}
	slot, err := s.graphBatcher.submitEncoded(ctx, owned, frame, lease)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_submit", commitUnknown(slot, owned.RequestID, err))
	}
	receipt, found, matches, err = s.material.GraphRequestStatusFingerprint(ctx, owned.RequestID, fingerprint)
	if err != nil {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_receipt_lookup", commitUnknown(slot, owned.RequestID, err))
	}
	if !found {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_receipt_lookup", commitUnknown(slot, owned.RequestID, fmt.Errorf("graph mutation receipt is unavailable")))
	}
	if !matches {
		return GraphExecuteResponse{}, managedGraphExecuteFailure("network_receipt_lookup", ErrRequestConflict)
	}
	return GraphExecuteResponse{MutationReceipt: receipt}, nil
}

// Evaluate readiness once: callbacks can change while a request is in flight.
func (s *Server) managedGraphReadinessFailure() string {
	if !s.writable {
		return "not_writable"
	}
	if s.ready == nil {
		return "callback_missing"
	}
	if !s.ready() {
		return "callback_false"
	}
	s.proposeMu.Lock()
	failed := s.lifecycleFailure != nil
	s.proposeMu.Unlock()
	if failed {
		return "lifecycle_failed"
	}
	if s.material == nil {
		return "material_missing"
	}
	return ""
}

func managedGraphExecuteFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	cause := "other"
	switch {
	case errors.Is(err, context.Canceled):
		cause = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		cause = "deadline"
	case errors.Is(err, ErrNotReady):
		cause = "not_ready"
	case errors.Is(err, ErrInvalidRequest):
		cause = "invalid_request"
	case errors.Is(err, ErrRequestConflict):
		cause = "request_conflict"
	case errors.Is(err, ErrOverloaded):
		cause = "overloaded"
	case errors.Is(err, ErrDurabilityUnavailable):
		cause = "durability_unavailable"
	case errors.Is(err, ErrCommitUnknown):
		cause = "commit_unknown"
	case errors.Is(err, quepaxa.ErrQuorumUnavailable):
		cause = "quorum_unavailable"
	}
	log.Printf("MANAGED_GRAPH_EXECUTE_FAILURE stage=%s cause=%s", stage, cause)
	return err
}

func (s *Server) handleGraphQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var request GraphQueryRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	release, err := s.acquireRead(r.Context(), false)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	result, err := s.graphQuery(r.Context(), request)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *Server) GraphQuery(ctx context.Context, request GraphQueryRequest) (types.GraphCommandResult, error) {
	release, err := s.acquireRead(ctx, false)
	if err != nil {
		return types.GraphCommandResult{}, err
	}
	defer release()
	return s.graphQuery(ctx, request)
}

func (s *Server) graphQuery(ctx context.Context, request GraphQueryRequest) (types.GraphCommandResult, error) {
	if request.Cypher == "" {
		return types.GraphCommandResult{}, ErrInvalidRequest
	}
	if err := s.readBarrier(ctx, request.Consistency); err != nil {
		return types.GraphCommandResult{}, fmt.Errorf("Graph query certified read barrier: %w", err)
	}
	result, err := s.material.GraphQuery(ctx, request.Cypher, request.Args)
	if err != nil {
		return types.GraphCommandResult{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	result.ConsensusTip = uint64(s.core.Tip())
	return result, nil
}

func (s *Server) GraphReachable(ctx context.Context, request GraphReachableRequest) (GraphReachableResult, error) {
	release, err := s.acquireRead(ctx, false)
	if err != nil {
		return GraphReachableResult{}, err
	}
	defer release()
	return s.graphReachable(ctx, request)
}

func (s *Server) graphReachable(ctx context.Context, request GraphReachableRequest) (GraphReachableResult, error) {
	if err := materializer.ValidateGraphReachableRequest(request); err != nil {
		return GraphReachableResult{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if err := s.readBarrier(ctx, request.Consistency); err != nil {
		return GraphReachableResult{}, err
	}
	result, err := s.material.GraphReachable(ctx, request)
	result.ConsensusTip = uint64(s.core.Tip())
	if err != nil {
		if ctx.Err() != nil {
			return GraphReachableResult{}, ctx.Err()
		}
		switch {
		case errors.Is(err, materializer.ErrGraphResourceLimit):
			return GraphReachableResult{}, ErrGraphResourceLimit
		case errors.Is(err, materializer.ErrReadVersionMismatch):
			return result, fmt.Errorf("%w: %v", ErrReadVersionMismatch, err)
		default:
			return GraphReachableResult{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	}
	return result, nil
}

func (s *Server) GraphChanges(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	request.Stream = "__lattice_changes"
	return s.graphStreamReadAdmitted(ctx, request)
}

func (s *Server) GraphStreamRead(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	return s.graphStreamReadAdmitted(ctx, request)
}

func (s *Server) graphStreamReadAdmitted(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	release, err := s.acquireRead(ctx, request.WaitMS != 0)
	if err != nil {
		return GraphStreamReadResponse{}, err
	}
	defer release()
	return s.graphStreamRead(ctx, request)
}

func (s *Server) graphStreamRead(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	if request.Limit == 0 {
		request.Limit = 100
	}
	if request.Limit > materializer.MaxGraphStreamRecords || request.WaitMS > 30_000 {
		return GraphStreamReadResponse{}, ErrInvalidRequest
	}
	if err := s.readBarrier(ctx, request.Consistency); err != nil {
		return GraphStreamReadResponse{}, err
	}
	records, appliedSlot, err := s.material.GraphReadStream(ctx, request.Stream, request.AfterSequence, request.Limit, time.Duration(request.WaitMS)*time.Millisecond)
	if err != nil {
		if ctx.Err() != nil {
			return GraphStreamReadResponse{}, ctx.Err()
		}
		return GraphStreamReadResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return GraphStreamReadResponse{Records: records, AppliedSlot: appliedSlot, ConsensusTip: uint64(s.core.Tip())}, nil
}

func (s *Server) GraphStreamOffset(ctx context.Context, request GraphStreamOffsetRequest) (GraphStreamOffsetResponse, error) {
	release, err := s.acquireRead(ctx, false)
	if err != nil {
		return GraphStreamOffsetResponse{}, err
	}
	defer release()
	return s.graphStreamOffset(ctx, request)
}

func (s *Server) graphStreamOffset(ctx context.Context, request GraphStreamOffsetRequest) (GraphStreamOffsetResponse, error) {
	if err := s.readBarrier(ctx, request.Consistency); err != nil {
		return GraphStreamOffsetResponse{}, err
	}
	sequence, found, appliedSlot, err := s.material.GraphStreamOffset(ctx, request.Stream, request.Consumer)
	if err != nil {
		if ctx.Err() != nil {
			return GraphStreamOffsetResponse{}, ctx.Err()
		}
		return GraphStreamOffsetResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return GraphStreamOffsetResponse{Sequence: sequence, Found: found, AppliedSlot: appliedSlot, ConsensusTip: uint64(s.core.Tip())}, nil
}

func (s *Server) SetGraphStreamOffset(ctx context.Context, request GraphStreamOffsetRequest) error {
	_, err := s.GraphExecute(ctx, types.GraphCommand{RequestID: request.RequestID, StreamOffset: &types.GraphStreamOffsetMutation{Stream: request.Stream, Consumer: request.Consumer, Sequence: request.Sequence}})
	return err
}

func (s *Server) TrimGraphStream(ctx context.Context, request GraphStreamTrimRequest) error {
	_, err := s.GraphExecute(ctx, types.GraphCommand{RequestID: request.RequestID, StreamTrim: &types.GraphStreamTrimMutation{Stream: request.Stream, ThroughSequence: request.ThroughSequence}})
	return err
}

func (s *Server) handleGraphChanges(w http.ResponseWriter, r *http.Request) {
	s.serveGraphStreamRead(w, r, true)
}

func (s *Server) handleGraphStreamRead(w http.ResponseWriter, r *http.Request) {
	s.serveGraphStreamRead(w, r, false)
}

func (s *Server) serveGraphStreamRead(w http.ResponseWriter, r *http.Request, changes bool) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var request GraphStreamReadRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	release, err := s.acquireRead(r.Context(), request.WaitMS != 0)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	if changes {
		request.Stream = "__lattice_changes"
	}
	response, err := s.graphStreamRead(r.Context(), request)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGraphStreamOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeMethodNotAllowed(w, http.MethodPost, http.MethodPut)
		return
	}
	var request GraphStreamOffsetRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if r.Method == http.MethodPut {
		if err := s.SetGraphStreamOffset(r.Context(), request); err != nil {
			writeAPIError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	release, err := s.acquireRead(r.Context(), false)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	response, err := s.graphStreamOffset(r.Context(), request)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGraphStreamTrim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var request GraphStreamTrimRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := s.TrimGraphStream(r.Context(), request); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
