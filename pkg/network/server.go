package network

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const maxRequestBody = 1 << 20

var (
	ErrNotReady        = errors.New("node is not ready")
	ErrRequestConflict = errors.New("request ID conflict")
	ErrInvalidRequest  = errors.New("invalid request")
)

// Server is the HTTP server for client API.
type Server struct {
	core       *quepaxa.Core
	material   *materializer.Materializer
	cluster    types.ClusterID
	mux        *http.ServeMux
	ready      func() bool
	writable   bool
	batcher    *sqlBatcher
	transport  *Transport
	members    []quepaxa.Member
	hedgeDelay time.Duration
	applyMu    sync.Mutex
	routeMu    sync.Mutex
	routeBase  quepaxa.NodeID
	routeFirst quepaxa.NodeID
	routeGen   uint64
}

// NewServer creates a new HTTP server.
func NewServer(core *quepaxa.Core, material *materializer.Materializer, cluster types.ClusterID, writable bool, transport *Transport, members []quepaxa.Member, hedgeDelay time.Duration, ready ...func() bool) *Server {
	s := &Server{
		core:       core,
		material:   material,
		cluster:    cluster,
		mux:        http.NewServeMux(),
		ready:      func() bool { return true },
		writable:   writable,
		transport:  transport,
		members:    append([]quepaxa.Member(nil), members...),
		hedgeDelay: hedgeDelay,
	}
	if len(ready) > 0 {
		s.ready = ready[0]
	}
	s.batcher = newSQLBatcher(s.proposeHedged, s.applyDecisions)
	s.routes()
	return s
}

// routes registers HTTP routes.
func (s *Server) routes() {
	// Client API
	if materializer.GraphEnabled() {
		s.mux.HandleFunc("/v1/graph/execute", s.handleGraphExecute)
		s.mux.HandleFunc("/v1/graph/query", s.handleGraphQuery)
	} else {
		s.mux.HandleFunc("/v1/sql/execute", s.handleExecute)
		s.mux.HandleFunc("/v1/sql/transaction", s.handleExecute)
		s.mux.HandleFunc("/v1/sql/query", s.handleQuery)
	}
	s.mux.HandleFunc("/v1/kv/put", s.handleKVPut)
	s.mux.HandleFunc("/v1/kv/get", s.handleKVGet)
	s.mux.HandleFunc("/v1/kv/delete", s.handleKVDelete)
	s.mux.HandleFunc("/v1/kv/cas", s.handleKVCAS)
	s.mux.HandleFunc("/v1/notify/publish", s.handleNotifyPublish)
	s.mux.HandleFunc("/v1/notify/subscribe", s.handleNotifySubscribe)

	// Health
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
}

type DecisionsResponse struct {
	ClusterID  types.ClusterID        `json:"cluster_id"`
	ProposerID quepaxa.NodeID         `json:"proposer_id"`
	ConfigID   uint                   `json:"config_id"`
	Tip        quepaxa.Slot           `json:"tip"`
	Decisions  []quepaxa.DecidedValue `json:"decisions"`
}

func (s *Server) proposeHedged(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	if len(s.members) <= 1 || s.transport == nil {
		slot, _, err := s.core.Propose(ctx, value)
		return slot, err
	}
	type result struct {
		slot   quepaxa.Slot
		err    error
		member quepaxa.NodeID
		rank   int
		worked bool
	}
	hedgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	plan := s.proposerPlan()
	members := plan.members
	results := make(chan result, len(members))
	var firstErr error
	for rank, member := range members {
		go func(rank int, member quepaxa.Member) {
			delay := time.Duration(rank) * s.hedgeDelay
			if delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-hedgeCtx.Done():
					return
				case <-timer.C:
				}
			}
			if slot, ok := s.core.DecidedSlot(value); ok {
				results <- result{slot: slot, member: member.ID, rank: rank}
				return
			}
			proposeCtx, cancelPropose := context.WithTimeout(hedgeCtx, 30*time.Second)
			defer cancelPropose()
			if member.ID == s.core.NodeID() {
				slot, _, err := s.core.Propose(proposeCtx, value)
				results <- result{slot: slot, err: err, member: member.ID, rank: rank, worked: true}
				return
			}
			decision, err := s.transport.Propose(proposeCtx, member.ID, value)
			if err == nil {
				err = s.acceptFrom(proposeCtx, member.ID, decision)
			}
			results <- result{slot: decision.Slot, err: err, member: member.ID, rank: rank, worked: true}
		}(rank, member)
	}
	for range members {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case result := <-results:
			if result.err == nil {
				if result.worked {
					s.observeProposer(plan, result.member, result.rank)
				}
				return result.slot, nil
			}
			if firstErr == nil {
				firstErr = result.err
			}
		}
	}
	if firstErr != nil {
		return 0, fmt.Errorf("%w: %v", quepaxa.ErrQuorumUnavailable, firstErr)
	}
	return 0, quepaxa.ErrQuorumUnavailable
}

func (s *Server) acceptFrom(ctx context.Context, source quepaxa.NodeID, decision quepaxa.DecidedValue) error {
	for s.core.Tip() < decision.Slot {
		from := s.core.Tip() + 1
		response, err := s.transport.FetchDecisions(ctx, source, from, 256)
		if err != nil {
			return err
		}
		if len(response.Decisions) == 0 || response.Decisions[0].Slot != from {
			return fmt.Errorf("peer %s omitted decision slot %d", source, from)
		}
		if err := s.core.AcceptCertifiedHints(response.Decisions); err != nil {
			return err
		}
	}
	if certified, ok := s.core.CertifiedValue(decision.Slot); !ok || certified.Hash != decision.Hash {
		return fmt.Errorf("peer %s returned inconsistent decision slot %d", source, decision.Slot)
	}
	return nil
}

type proposerPlan struct {
	members    []quepaxa.Member
	base       quepaxa.NodeID
	generation uint64
}

func (s *Server) proposerPlan() proposerPlan {
	byID := make(map[quepaxa.NodeID]quepaxa.Member, len(s.members))
	for _, member := range s.members {
		byID[member.ID] = member
	}
	agreed := s.core.ProposerOrder()
	s.routeMu.Lock()
	if len(agreed) > 0 && s.routeBase != agreed[0] {
		s.routeBase = agreed[0]
		s.routeFirst = agreed[0]
		s.routeGen++
	}
	first, generation, base := s.routeFirst, s.routeGen, s.routeBase
	s.routeMu.Unlock()

	ordered := make([]quepaxa.Member, 0, len(s.members))
	if member, ok := byID[first]; ok {
		ordered = append(ordered, member)
	}
	for _, id := range agreed {
		if id == first {
			continue
		}
		if member, ok := byID[id]; ok {
			ordered = append(ordered, member)
		}
	}
	if len(ordered) != len(s.members) {
		ordered = append([]quepaxa.Member(nil), s.members...)
	}
	return proposerPlan{members: ordered, base: base, generation: generation}
}

func (s *Server) observeProposer(plan proposerPlan, winner quepaxa.NodeID, rank int) {
	if rank == 0 {
		return
	}
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	if s.routeGen != plan.generation || s.routeBase != plan.base {
		return
	}
	s.routeFirst = winner
	s.routeGen++
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// ExecuteRequest is the request body for execute.
type ExecuteRequest struct {
	RequestID  string               `json:"request_id"`
	SQL        string               `json:"sql,omitempty"`
	Args       []any                `json:"args,omitempty"`
	WantRows   bool                 `json:"want_rows,omitempty"`
	Statements []types.SQLStatement `json:"statements,omitempty"`
}

// ExecuteResponse is the response body for execute.
type ExecuteResponse struct {
	Slot    uint64                 `json:"slot"`
	Success bool                   `json:"success"`
	Error   string                 `json:"error,omitempty"`
	Result  types.SQLCommandResult `json:"result"`
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecuteRequest
	if err := decodeJSON(w, r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

// Execute applies one SQL statement or an atomic statements transaction.
func (s *Server) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	if !s.writable || !s.ready() {
		return ExecuteResponse{}, ErrNotReady
	}
	command := types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, Statements: req.Statements}
	if req.RequestID == "" {
		return ExecuteResponse{}, fmt.Errorf("%w: request_id is required", ErrInvalidRequest)
	}
	if err := materializer.ValidateSQLCommand(command); err != nil {
		return ExecuteResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if matches, err := s.material.SQLRequestMatches(ctx, command); err != nil {
		return ExecuteResponse{}, err
	} else if !matches {
		return ExecuteResponse{}, ErrRequestConflict
	}
	slot, err := s.batcher.submit(ctx, command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	result, err := s.material.RequestResult(ctx, req.RequestID)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if matches, err := s.material.SQLRequestMatches(ctx, command); err != nil || !matches {
		return ExecuteResponse{}, ErrRequestConflict
	}
	return ExecuteResponse{Slot: uint64(slot), Success: result.Error == "", Error: result.Error, Result: result}, nil
}

// QueryRequest is the request body for query.
type QueryRequest struct {
	SQL         string `json:"sql"`
	Args        []any  `json:"args,omitempty"`
	Consistency string `json:"consistency,omitempty"`
}

// QueryResponse is the response body for query.
type QueryResponse struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req QueryRequest
	if err := decodeJSON(w, r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.Query(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Query reads SQL locally or after a linearizable consensus barrier.
func (s *Server) Query(ctx context.Context, req QueryRequest) (QueryResponse, error) {
	if req.SQL == "" {
		return QueryResponse{}, fmt.Errorf("%w: sql is required", ErrInvalidRequest)
	}
	if err := s.readBarrier(ctx, req.Consistency); err != nil {
		return QueryResponse{}, err
	}
	result, err := s.material.QueryResult(ctx, req.SQL, req.Args)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return QueryResponse{Columns: result.Columns, Rows: result.Rows}, nil
}

func writeAPIError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrInvalidRequest), errors.Is(err, errConsistency):
		status = http.StatusBadRequest
	case errors.Is(err, ErrRequestConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrNotReady), errors.Is(err, quepaxa.ErrQuorumUnavailable):
		status = http.StatusServiceUnavailable
	}
	http.Error(w, err.Error(), status)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
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

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	s.handleKVMutation(w, r, "put")
}

type KVGetRequest struct{ Key, Consistency string }
type KVGetResponse struct {
	Found bool   `json:"found"`
	Value []byte `json:"value,omitempty"`
}
type KVMutationRequest struct {
	RequestID      string `json:"request_id"`
	Key            string `json:"key"`
	Value          []byte `json:"value,omitempty"`
	Expected       []byte `json:"expected,omitempty"`
	ExpectedExists bool   `json:"expected_exists,omitempty"`
	TTLMS          int64  `json:"ttl_ms,omitempty"`
}
type KVMutationResponse struct {
	Slot    uint64 `json:"slot"`
	Applied bool   `json:"applied"`
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Key         string `json:"key"`
		Consistency string `json:"consistency,omitempty"`
	}
	if err := decodeJSON(w, r, &req); err != nil || req.Key == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	response, err := s.KVGet(r.Context(), KVGetRequest{Key: req.Key, Consistency: req.Consistency})
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) KVGet(ctx context.Context, req KVGetRequest) (KVGetResponse, error) {
	if req.Key == "" {
		return KVGetResponse{}, fmt.Errorf("%w: key is required", ErrInvalidRequest)
	}
	if err := s.readBarrier(ctx, req.Consistency); err != nil {
		return KVGetResponse{}, err
	}
	value, found, err := s.material.KVGet(ctx, req.Key, time.Now())
	return KVGetResponse{Found: found, Value: value}, err
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	s.handleKVMutation(w, r, "delete")
}

func (s *Server) handleKVCAS(w http.ResponseWriter, r *http.Request) {
	s.handleKVMutation(w, r, "cas")
}

func (s *Server) handleKVMutation(w http.ResponseWriter, r *http.Request, operation string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req KVMutationRequest
	if err := decodeJSON(w, r, &req); err != nil || req.RequestID == "" || req.Key == "" || len(req.Key) > 1024 || req.TTLMS < 0 || len(req.Value) > 16<<20 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	response, err := s.KVMutate(r.Context(), operation, req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) KVPut(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return s.KVMutate(ctx, "put", req)
}
func (s *Server) KVDelete(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return s.KVMutate(ctx, "delete", req)
}
func (s *Server) KVCAS(ctx context.Context, req KVMutationRequest) (KVMutationResponse, error) {
	return s.KVMutate(ctx, "cas", req)
}

func (s *Server) KVMutate(ctx context.Context, operation string, req KVMutationRequest) (KVMutationResponse, error) {
	if !s.writable || !s.ready() {
		return KVMutationResponse{}, ErrNotReady
	}
	if req.RequestID == "" || req.Key == "" || len(req.Key) > 1024 || req.TTLMS < 0 || len(req.Value) > 16<<20 {
		return KVMutationResponse{}, ErrInvalidRequest
	}
	now := time.Now().UnixMilli()
	command := types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, ObservedAtUnixMS: now}
	if req.TTLMS > 0 {
		command.ExpiresAtUnixMS = now + req.TTLMS
	}
	if matches, err := s.material.KVRequestMatches(ctx, command); err != nil {
		return KVMutationResponse{}, err
	} else if !matches {
		return KVMutationResponse{}, ErrRequestConflict
	}
	value, err := types.EncodeKVCommand(command)
	if err != nil {
		return KVMutationResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	slot, err := s.proposeHedged(ctx, value)
	if err == nil {
		err = s.applyDecisions(ctx, slot)
	}
	if err != nil {
		return KVMutationResponse{}, err
	}
	result, err := s.material.KVRequestResult(ctx, req.RequestID)
	if err != nil {
		return KVMutationResponse{}, err
	}
	if matches, err := s.material.KVRequestMatches(ctx, command); err != nil || !matches {
		return KVMutationResponse{}, ErrRequestConflict
	}
	return KVMutationResponse{Slot: uint64(slot), Applied: result.Applied}, nil
}

var errConsistency = errors.New("consistency must be local or linearizable")

func (s *Server) readBarrier(ctx context.Context, consistency string) error {
	switch consistency {
	case "", "local":
		return nil
	case "linearizable":
		var nonce [types.ReadBarrierNonceSize]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		slot, err := s.proposeHedged(ctx, types.EncodeReadBarrier(nonce))
		if err != nil {
			return err
		}
		return s.applyDecisions(ctx, slot)
	default:
		return errConsistency
	}
}

func (s *Server) handleNotifyPublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req types.NotifyCommand
	if err := decodeJSON(w, r, &req); err != nil || req.RequestID == "" || req.Topic == "" || len(req.Topic) > 256 || len(req.Payload) > 1<<20 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	slot, err := s.NotifyPublish(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]uint64{"slot": slot})
}

func (s *Server) NotifyPublish(ctx context.Context, req types.NotifyCommand) (uint64, error) {
	if !s.writable || !s.ready() {
		return 0, ErrNotReady
	}
	if req.RequestID == "" || req.Topic == "" || len(req.Topic) > 256 || len(req.Payload) > 1<<20 {
		return 0, ErrInvalidRequest
	}
	value, err := types.EncodeNotifyCommand(req)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	slot, err := s.proposeHedged(ctx, value)
	if err == nil {
		err = s.applyDecisions(ctx, slot)
	}
	return uint64(slot), err
}

func (s *Server) NotifySubscribe(topic string) (<-chan []byte, func(), error) {
	if topic == "" || len(topic) > 256 {
		return nil, nil, ErrInvalidRequest
	}
	ch, cancel := s.material.Subscribe(topic)
	return ch, cancel, nil
}

func (s *Server) handleNotifySubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	if topic == "" || len(topic) > 256 {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	ch, cancel, err := s.NotifySubscribe(topic)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	defer cancel()
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case payload := <-ch:
			data, _ := json.Marshal(payload)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func (s *Server) proposeLocal(ctx context.Context, value []byte) (quepaxa.DecidedValue, error) {
	if slot, ok := s.core.DecidedSlot(value); ok {
		if err := s.applyDecisions(ctx, slot); err != nil {
			return quepaxa.DecidedValue{}, err
		}
		decision, _ := s.core.CertifiedValue(slot)
		return decision, nil
	}
	slot, _, err := s.core.Propose(ctx, value)
	if err != nil {
		return quepaxa.DecidedValue{}, err
	}
	if err := s.applyDecisions(ctx, slot); err != nil {
		return quepaxa.DecidedValue{}, err
	}
	decision, ok := s.core.CertifiedValue(slot)
	if !ok {
		return quepaxa.DecidedValue{}, errors.New("decision unavailable")
	}
	return decision, nil
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if !s.ready() {
		http.Error(w, "catching up", http.StatusServiceUnavailable)
		return
	}
	if err := s.material.Health(r.Context()); err != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) applyDecisions(ctx context.Context, through quepaxa.Slot) error {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	if err := s.core.WaitTip(ctx, through); err != nil {
		return err
	}
	for {
		applied := quepaxa.Slot(s.material.Tip())
		if applied >= through {
			return nil
		}
		from := applied + 1
		decisions, _, err := s.core.DecisionsFrom(from, 256)
		if err != nil {
			return err
		}
		if len(decisions) == 0 {
			return errors.New("decision gap")
		}
		for _, decision := range decisions {
			if decision.Slot > through {
				return nil
			}
			if err := s.material.Apply(ctx, uint64(decision.Slot), decision.Value); err != nil {
				return err
			}
		}
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
