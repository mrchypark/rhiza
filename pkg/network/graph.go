package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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
	if !s.writable || !s.ready() {
		return GraphExecuteResponse{}, ErrNotReady
	}
	if err := materializer.ValidateGraphCommandAdmission(command); err != nil {
		return GraphExecuteResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	defer s.lockRequest(command.RequestID)()
	if matches, err := s.material.GraphRequestMatches(ctx, command); err != nil {
		return GraphExecuteResponse{}, err
	} else if !matches {
		return GraphExecuteResponse{}, ErrRequestConflict
	}
	if receipt, found, err := s.material.GraphMutationReceipt(ctx, command.RequestID); err != nil {
		return GraphExecuteResponse{}, err
	} else if found {
		return GraphExecuteResponse{MutationReceipt: receipt}, nil
	}
	value, err := types.EncodeGraphBatch([]types.GraphCommand{command})
	if err != nil {
		return GraphExecuteResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if len(value) > quepaxa.MaxReplicatedValueBytes {
		return GraphExecuteResponse{}, fmt.Errorf("%w: encoded command exceeds %d bytes", ErrInvalidRequest, quepaxa.MaxReplicatedValueBytes)
	}
	_, err = s.graphBatcher.submit(ctx, command)
	if err != nil {
		return GraphExecuteResponse{}, err
	}
	receipt, found, err := s.material.GraphMutationReceipt(ctx, command.RequestID)
	if err != nil {
		return GraphExecuteResponse{}, err
	}
	if !found {
		return GraphExecuteResponse{}, fmt.Errorf("graph mutation receipt is unavailable")
	}
	if matches, err := s.material.GraphRequestMatches(ctx, command); err != nil || !matches {
		return GraphExecuteResponse{}, ErrRequestConflict
	}
	return GraphExecuteResponse{MutationReceipt: receipt}, nil
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
	result, err := s.GraphQuery(r.Context(), request)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *Server) GraphQuery(ctx context.Context, request GraphQueryRequest) (types.GraphCommandResult, error) {
	if request.Cypher == "" {
		return types.GraphCommandResult{}, ErrInvalidRequest
	}
	if err := s.readBarrier(ctx, request.Consistency); err != nil {
		return types.GraphCommandResult{}, err
	}
	result, err := s.material.GraphQuery(ctx, request.Cypher, request.Args)
	if err != nil {
		return types.GraphCommandResult{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	result.ConsensusTip = uint64(s.core.Tip())
	return result, nil
}

func (s *Server) GraphChanges(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	request.Stream = "__lattice_changes"
	return s.GraphStreamRead(ctx, request)
}

func (s *Server) GraphStreamRead(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
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
	var response GraphStreamReadResponse
	var err error
	if changes {
		response, err = s.GraphChanges(r.Context(), request)
	} else {
		response, err = s.GraphStreamRead(r.Context(), request)
	}
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
	response, err := s.GraphStreamOffset(r.Context(), request)
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
