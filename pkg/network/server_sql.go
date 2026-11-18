package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func (a *MigrationAdmission) Release() {
	if a != nil && a.state.CompareAndSwap(0, 1) {
		a.lease.releaseCaller()
	}
}

func (a *MigrationAdmission) consume(server *Server, charge int) bool {
	return a != nil && a.server == server && a.charge == charge && a.lease != nil && a.state.CompareAndSwap(0, 2)
}

// ValidateExecuteRequest applies the same mutation contract and encoded-size
// check as Execute without submitting the command.
func ValidateExecuteRequest(req ExecuteRequest) error {
	_, _, err := validatedSQLCommand(req)
	return err
}

func validatedSQLCommand(req ExecuteRequest) (types.SQLCommand, []byte, error) {
	if req.RequestID == "" {
		return types.SQLCommand{}, nil, fmt.Errorf("%w: request_id is required", ErrInvalidRequest)
	}
	command := types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, RequireOne: req.RequireOne, Statements: req.Statements}
	encoded, err := validateEncodeSQLCommand(command)
	return command, encoded, err
}

func validateEncodeSQLCommand(command types.SQLCommand) ([]byte, error) {
	if err := validateSQLCommand(command); err != nil {
		return nil, err
	}
	return encodeSQLCommand(command)
}

func validateSQLCommand(command types.SQLCommand) error {
	if command.RequestID == "" {
		return fmt.Errorf("%w: request_id is required", ErrInvalidRequest)
	}
	if err := materializer.ValidateSQLCommand(command); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return nil
}

func encodeSQLCommand(command types.SQLCommand) ([]byte, error) {
	encoded, err := types.EncodeSQLBatchItem(command)
	if err != nil {
		return nil, fmt.Errorf("%w: encode SQL command: %v", ErrInvalidRequest, err)
	}
	if types.SQLBatchEncodedSize([][]byte{encoded}) > quepaxa.MaxReplicatedValueBytes {
		return nil, fmt.Errorf("%w: encoded command exceeds %d bytes", ErrInvalidRequest, quepaxa.MaxReplicatedValueBytes)
	}
	return encoded, nil
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req ExecuteRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	resp, err := s.Execute(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleExecuteReturning(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req ExecuteRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	resp, err := s.ExecuteReturning(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleExecuteReturningOne(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req ExecuteRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	resp, err := s.ExecuteReturningOne(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ExecuteReturning executes one replicated mutation and returns its bounded rows.
func (s *Server) ExecuteReturning(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	req.WantRows = true
	return s.execute(ctx, req, true)
}

// ExecuteReturningOne commits only when exactly one row is returned.
func (s *Server) ExecuteReturningOne(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	req.WantRows = true
	req.RequireOne = true
	return s.execute(ctx, req, true)
}

// Execute applies one SQL statement or an atomic statements transaction.
func (s *Server) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	return s.execute(ctx, req, false)
}

func (s *Server) execute(ctx context.Context, req ExecuteRequest, oneStatement bool) (ExecuteResponse, error) {
	if !s.writable || !s.Ready() {
		return ExecuteResponse{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return ExecuteResponse{}, err
	}
	command := types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, RequireOne: req.RequireOne, Statements: req.Statements}
	charge, err := mutationCharge(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	lease, err := s.admitMutation(charge)
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer lease.releaseCaller()
	command, err = cloneSQLCommand(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if oneStatement && len(command.Statements) != 0 {
		return ExecuteResponse{}, fmt.Errorf("%w: ExecuteReturning accepts one SQL statement", ErrInvalidRequest)
	}
	if err := validateSQLCommand(command); err != nil {
		return ExecuteResponse{}, err
	}
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	encoded, err := encodeSQLCommand(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	return s.executeSQLCommand(ctx, command, encoded, fingerprint, lease)
}

// Migrate applies one engine-owned migration atomically with its ledger row.
func (s *Server) Migrate(ctx context.Context, req MigrationRequest) (ExecuteResponse, error) {
	if managedKVExecutionPolicy(s.expectedPolicy) {
		return ExecuteResponse{}, ErrInvalidRequest
	}
	if !s.writable || !s.Ready() {
		return ExecuteResponse{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return ExecuteResponse{}, err
	}
	command := types.SQLCommand{RequestID: req.RequestID, Statements: req.Statements, Migration: &types.SQLMigration{Version: req.Version, Name: req.Name, Checksum: req.Checksum}}
	charge, err := mutationCharge(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	lease, err := s.admitMutation(charge)
	if err != nil {
		return ExecuteResponse{}, err
	}
	admission := &MigrationAdmission{server: s, charge: charge, lease: lease}
	defer admission.Release()
	return s.MigrateAdmitted(ctx, req, admission)
}

// AdmitMigration reserves bounded capacity before the embedded wrapper
// computes a checksum or validates/encodes caller-owned statements.
func (s *Server) AdmitMigration(ctx context.Context, version int64, name string, statements []types.SQLStatement) (*MigrationAdmission, error) {
	if managedKVExecutionPolicy(s.expectedPolicy) {
		return nil, ErrInvalidRequest
	}
	if !s.writable || !s.Ready() {
		return nil, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	command := types.SQLCommand{RequestID: strings.Repeat("0", 64), Statements: statements, Migration: &types.SQLMigration{Version: version, Name: name, Checksum: strings.Repeat("0", 64)}}
	charge, err := mutationCharge(command)
	if err != nil {
		return nil, err
	}
	lease, err := s.admitMutation(charge)
	if err != nil {
		return nil, err
	}
	return &MigrationAdmission{server: s, charge: charge, lease: lease}, nil
}

// MigrateAdmitted submits a migration using a reservation acquired before
// checksum construction and request validation.
func (s *Server) MigrateAdmitted(ctx context.Context, req MigrationRequest, admission *MigrationAdmission) (ExecuteResponse, error) {
	if managedKVExecutionPolicy(s.expectedPolicy) {
		return ExecuteResponse{}, ErrInvalidRequest
	}
	if admission == nil || admission.lease == nil {
		return ExecuteResponse{}, ErrInvalidRequest
	}
	if !s.writable || !s.Ready() {
		return ExecuteResponse{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return ExecuteResponse{}, err
	}
	command := types.SQLCommand{RequestID: req.RequestID, Statements: req.Statements, Migration: &types.SQLMigration{Version: req.Version, Name: req.Name, Checksum: req.Checksum}}
	charge, err := mutationCharge(command)
	if err != nil || !admission.consume(s, charge) {
		return ExecuteResponse{}, ErrInvalidRequest
	}
	lease := admission.lease
	defer lease.releaseCaller()
	command, err = cloneSQLCommand(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if err := validateSQLCommand(command); err != nil {
		return ExecuteResponse{}, err
	}
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	encoded, err := encodeSQLCommand(command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	return s.executeSQLCommand(ctx, command, encoded, fingerprint, lease)
}

func (s *Server) executeSQLCommand(ctx context.Context, command types.SQLCommand, encoded []byte, fingerprint [32]byte, lease *mutationLease) (ExecuteResponse, error) {
	var err error
	wantRows := command.WantRows
	for _, statement := range command.Statements {
		wantRows = wantRows || statement.WantRows
	}
	unlock, err := s.lockRequest(ctx, command.RequestID)
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer unlock()
	receipt, sqlResult, found, matches, err := s.material.SQLRequestResultFingerprint(ctx, command.RequestID, fingerprint, wantRows)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if !matches {
		return ExecuteResponse{}, ErrRequestConflict
	}
	if found {
		if err := s.waitReceiptDurable(ctx, receipt, command.RequestID); err != nil {
			return ExecuteResponse{}, err
		}
		return ExecuteResponse{MutationReceipt: receipt, Statements: sqlResult.Statements}, nil
	}
	submitSlot, err := s.sqlBatcher.submitEncoded(ctx, command, encoded, lease)
	if err != nil {
		return ExecuteResponse{}, err
	}
	receipt, sqlResult, found, matches, err = s.material.SQLRequestResultFingerprint(ctx, command.RequestID, fingerprint, wantRows)
	if managedKVExecutionPolicy(s.expectedPolicy) {
		if err != nil {
			return ExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, err)
		}
		if !found {
			return ExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, fmt.Errorf("SQL mutation receipt is unavailable"))
		}
		if !matches {
			return ExecuteResponse{}, ErrRequestConflict
		}
		return ExecuteResponse{MutationReceipt: receipt, Statements: sqlResult.Statements}, nil
	}
	if err != nil {
		return ExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, err)
	}
	if !matches {
		return ExecuteResponse{}, ErrRequestConflict
	}
	if !found {
		return ExecuteResponse{}, commitUnknown(submitSlot, command.RequestID, fmt.Errorf("SQL mutation receipt is unavailable"))
	}
	return ExecuteResponse{MutationReceipt: receipt, Statements: sqlResult.Statements}, nil
}

// QueryRequest is the request body for query.
type QueryRequest struct {
	SQL         string `json:"sql"`
	Args        []any  `json:"args,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

// QueryResponse is the response body for query.
type QueryResponse struct {
	Columns      []string        `json:"columns"`
	Rows         [][]interface{} `json:"rows"`
	AppliedSlot  uint64          `json:"applied_slot"`
	ConsensusTip uint64          `json:"consensus_tip"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}

	var req QueryRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	release, err := s.acquireRead(r.Context(), false)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer release()
	resp, err := s.query(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Query reads SQL locally or after a linearizable consensus barrier.
func (s *Server) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	if s.expectedPolicy == sqlpolicy.ManagedRequestMaterializationPolicy || s.expectedPolicy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		ctx = context.WithValue(ctx, managedQueryDiagnosticKey{}, true)
	}
	release, err := s.acquireRead(ctx, false)
	if err != nil {
		return QueryResponse{}, logManagedQueryFailure(ctx, "server_read_acquire", err)
	}
	defer release()
	return s.query(ctx, req)
}

func (s *Server) query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	if req.SQL == "" {
		return QueryResponse{}, fmt.Errorf("%w: sql is required", ErrInvalidRequest)
	}
	if err := s.readBarrier(ctx, req.Consistency); err != nil {
		return QueryResponse{}, err
	}
	result, appliedSlot, err := s.material.QueryResultAt(ctx, req.SQL, req.Args)
	if err != nil {
		logManagedQueryFailure(ctx, "server_materialize", err)
		return QueryResponse{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	return QueryResponse{Columns: result.Columns, Rows: result.Rows, AppliedSlot: appliedSlot, ConsensusTip: uint64(s.core.Tip())}, nil
}

type managedQueryDiagnosticKey struct{}

func managedQueryDiagnostic(ctx context.Context, stage string) string {
	if enabled, _ := ctx.Value(managedQueryDiagnosticKey{}).(bool); !enabled {
		return ""
	}
	switch stage {
	case "server_read_acquire", "server_read_index", "server_catchup", "server_apply", "server_materialize":
		return "MANAGED_SQL_QUERY_FAILURE stage=" + stage
	default:
		return ""
	}
}

func logManagedQueryFailure(ctx context.Context, stage string, err error) error {
	if err != nil {
		if line := managedQueryDiagnostic(ctx, stage); line != "" {
			log.Print(line)
		}
	}
	return err
}

// ErrorResponse is the stable JSON error envelope returned by the HTTP adapter.
type ErrorResponse struct {
	Code             string `json:"code"`
	Error            string `json:"error"`
	RequestID        string `json:"request_id,omitempty"`
	Slot             uint64 `json:"slot,omitempty"`
	RetryThroughSlot uint64 `json:"retry_through_slot,omitempty"`
}

func writeHTTPError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Code: code, Error: message})
}

func writeMethodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeHTTPError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeAPIError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal_error"
	switch {
	case errors.Is(err, ErrInvalidRequest), errors.Is(err, errConsistency):
		status = http.StatusBadRequest
		code = "invalid_request"
	case errors.Is(err, ErrGraphResourceLimit):
		status = http.StatusBadRequest
		code = "graph_resource_limit"
	case errors.Is(err, ErrReadVersionMismatch):
		status = http.StatusConflict
		code = "read_version_mismatch"
	case errors.Is(err, ErrRequestConflict):
		status = http.StatusConflict
		code = "request_conflict"
	case errors.Is(err, ErrNotReady):
		status = http.StatusServiceUnavailable
		code = "not_ready"
	case errors.Is(err, ErrOverloaded):
		status = http.StatusServiceUnavailable
		code = "overloaded"
	case errors.Is(err, ErrDurabilityUnavailable):
		status = http.StatusServiceUnavailable
		code = "durability_unavailable"
	case errors.Is(err, ErrCommitUnknown):
		status = http.StatusServiceUnavailable
		code = "commit_unknown"
	case errors.Is(err, quepaxa.ErrQuorumUnavailable):
		status = http.StatusServiceUnavailable
		code = "quorum_unavailable"
	}
	var unknown *CommitUnknownError
	if errors.As(err, &unknown) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(ErrorResponse{Code: "commit_unknown", Error: err.Error(), RequestID: unknown.RequestID, Slot: uint64(unknown.Slot), RetryThroughSlot: unknown.RetryThroughSlot})
		return
	}
	writeHTTPError(w, status, code, err.Error())
}

type RequestStatusRequest struct {
	Kind      string `json:"kind"`
	RequestID string `json:"request_id"`
}

type RequestStatusResponse struct {
	State   string                 `json:"state"`
	Tip     uint64                 `json:"tip"`
	Receipt *types.MutationReceipt `json:"receipt,omitempty"`
}

func (s *Server) handleRequestStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	var req RequestStatusRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeHTTPError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	response, err := s.RequestStatus(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) RequestStatus(ctx context.Context, req RequestStatusRequest) (RequestStatusResponse, error) {
	if s.localFailure() != nil {
		return RequestStatusResponse{}, ErrNotReady
	}
	if req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes {
		return RequestStatusResponse{}, ErrInvalidRequest
	}
	kind, ok := mutationKind(req.Kind)
	if !ok {
		return RequestStatusResponse{}, ErrInvalidRequest
	}
	tip := s.material.Tip()
	var receipt types.MutationReceipt
	var found bool
	var err error
	if kind == types.MutationGraph {
		receipt, found, err = s.material.GraphMutationReceipt(ctx, req.RequestID)
	} else {
		receipt, found, err = s.material.MutationReceipt(ctx, kind, req.RequestID)
	}
	if err != nil {
		return RequestStatusResponse{}, err
	}
	if !found {
		return RequestStatusResponse{State: "unknown_or_expired", Tip: tip}, nil
	}
	return RequestStatusResponse{State: string(receipt.Status), Tip: tip, Receipt: &receipt}, nil
}

func mutationKind(kind string) (types.MutationKind, bool) {
	switch kind {
	case "sql":
		return types.MutationSQL, true
	case "kv":
		return types.MutationKV, true
	case "notify":
		return types.MutationNotify, true
	case "graph":
		return types.MutationGraph, true
	default:
		return 0, false
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("bad request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("bad request: trailing JSON")
	}
	return nil
}
