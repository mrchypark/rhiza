package network

//lint:file-ignore SA4023 Graph-enabled builds use a different materializer implementation that can return nil errors.

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
	Records []types.GraphStreamRecord `json:"records"`
}

type GraphStreamOffsetRequest struct {
	Stream   string `json:"stream"`
	Consumer string `json:"consumer"`
	Sequence uint64 `json:"sequence,omitempty"`
}

type GraphStreamOffsetResponse struct {
	Sequence uint64 `json:"sequence"`
	Found    bool   `json:"found"`
}

type GraphStreamTrimRequest struct {
	Stream          string `json:"stream"`
	ThroughSequence uint64 `json:"through_sequence"`
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

func (s *Server) GraphChanges(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	request.Stream = "__lattice_changes"
	return s.GraphStreamRead(ctx, request)
}

func (s *Server) GraphStreamRead(ctx context.Context, request GraphStreamReadRequest) (GraphStreamReadResponse, error) {
	if !materializer.GraphEnabled() {
		return GraphStreamReadResponse{}, fmt.Errorf("%w: graph is unavailable in this build", ErrInvalidRequest)
	}
	if request.Limit == 0 {
		request.Limit = 100
	}
	if request.Limit > materializer.MaxGraphStreamRecords || request.WaitMS > 30_000 {
		return GraphStreamReadResponse{}, ErrInvalidRequest
	}
	if err := s.readBarrier(ctx, request.Consistency); err != nil {
		return GraphStreamReadResponse{}, err
	}
	records, err := s.material.GraphReadStream(ctx, request.Stream, request.AfterSequence, request.Limit, time.Duration(request.WaitMS)*time.Millisecond)
	if err != nil {
		if ctx.Err() != nil {
			return GraphStreamReadResponse{}, ctx.Err()
		}
		return GraphStreamReadResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return GraphStreamReadResponse{Records: records}, nil
}

func (s *Server) GraphStreamOffset(ctx context.Context, request GraphStreamOffsetRequest) (GraphStreamOffsetResponse, error) {
	if !materializer.GraphEnabled() {
		return GraphStreamOffsetResponse{}, fmt.Errorf("%w: graph is unavailable in this build", ErrInvalidRequest)
	}
	sequence, found, err := s.material.GraphStreamOffset(ctx, request.Stream, request.Consumer)
	if err != nil {
		if ctx.Err() != nil {
			return GraphStreamOffsetResponse{}, ctx.Err()
		}
		return GraphStreamOffsetResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return GraphStreamOffsetResponse{Sequence: sequence, Found: found}, nil
}

func (s *Server) SetGraphStreamOffset(ctx context.Context, request GraphStreamOffsetRequest) error {
	if !materializer.GraphEnabled() {
		return fmt.Errorf("%w: graph is unavailable in this build", ErrInvalidRequest)
	}
	if err := s.material.SetGraphStreamOffset(ctx, request.Stream, request.Consumer, request.Sequence); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return nil
}

func (s *Server) TrimGraphStream(ctx context.Context, request GraphStreamTrimRequest) error {
	if !materializer.GraphEnabled() {
		return fmt.Errorf("%w: graph is unavailable in this build", ErrInvalidRequest)
	}
	if err := s.material.TrimGraphStream(ctx, request.Stream, request.ThroughSequence); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return nil
}

func (s *Server) handleGraphChanges(w http.ResponseWriter, r *http.Request) {
	s.serveGraphStreamRead(w, r, true)
}

func (s *Server) handleGraphStreamRead(w http.ResponseWriter, r *http.Request) {
	s.serveGraphStreamRead(w, r, false)
}

func (s *Server) serveGraphStreamRead(w http.ResponseWriter, r *http.Request, changes bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request GraphStreamReadRequest
	if err := decodeJSON(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request GraphStreamOffsetRequest
	if err := decodeJSON(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request GraphStreamTrimRequest
	if err := decodeJSON(w, r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.TrimGraphStream(r.Context(), request); err != nil {
		writeAPIError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
