package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
)

type GraphExecuteResponse struct {
	Slot    uint64                   `json:"slot"`
	Success bool                     `json:"success"`
	Error   string                   `json:"error,omitempty"`
	Result  types.GraphCommandResult `json:"result"`
}

type GraphQueryRequest struct {
	Cypher      string         `json:"cypher"`
	Args        map[string]any `json:"args,omitempty"`
	Consistency string         `json:"consistency,omitempty"`
}

func (s *Server) handleGraphExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var command types.GraphCommand
	if err := decodeJSON(w, r, &command); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	if err := materializer.ValidateGraphCommand(command); err != nil {
		return GraphExecuteResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if matches, err := s.material.GraphRequestMatches(ctx, command); err != nil {
		return GraphExecuteResponse{}, err
	} else if !matches {
		return GraphExecuteResponse{}, ErrRequestConflict
	}
	value, err := types.EncodeGraphCommand(command)
	if err != nil {
		return GraphExecuteResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	slot, err := s.proposeHedged(ctx, value)
	if err == nil {
		err = s.applyDecisions(ctx, slot)
	}
	if err != nil {
		return GraphExecuteResponse{}, err
	}
	result, err := s.material.GraphRequestResult(ctx, command.RequestID)
	return GraphExecuteResponse{Slot: uint64(slot), Success: result.Error == "", Error: result.Error, Result: result}, err
}

func (s *Server) handleGraphQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request GraphQueryRequest
	if err := decodeJSON(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	return result, nil
}
