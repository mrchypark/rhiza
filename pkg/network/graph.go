package network

//lint:file-ignore SA4023 Graph-enabled builds use a different materializer implementation that can return nil errors.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

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
	if !materializer.GraphEnabled() {
		return GraphExecuteResponse{}, fmt.Errorf("%w: graph is unavailable in this build", ErrInvalidRequest)
	}
	if err := materializer.ValidateGraphCommand(command); err != nil {
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
	if !materializer.GraphEnabled() {
		return types.GraphCommandResult{}, fmt.Errorf("%w: graph is unavailable in this build", ErrInvalidRequest)
	}
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
