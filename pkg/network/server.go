package network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// MaxRequestBodyBytes is the HTTP adapter's JSON decoding limit. Replicated
// mutations have the smaller quepaxa.MaxReplicatedValueBytes consensus limit.
const MaxRequestBodyBytes = 1 << 20

var (
	ErrNotReady              = errors.New("node is not ready")
	ErrRequestConflict       = errors.New("request ID conflict")
	ErrInvalidRequest        = errors.New("invalid request")
	ErrOverloaded            = errors.New("mutation queue overloaded")
	ErrDurabilityUnavailable = errors.New("object-store durability unavailable")
	ErrCommitUnknown         = errors.New("commit outcome unknown")
)

// CommitUnknownError means a mutation may commit despite the failed call.
// Retrying the same request ID resolves the outcome without duplicating it.
type CommitUnknownError struct {
	Slot             quepaxa.Slot
	RequestID        string
	RetryThroughSlot uint64
	Cause            error
}

func (e *CommitUnknownError) Error() string {
	return fmt.Sprintf("%v: request_id=%s slot=%d: %v", ErrCommitUnknown, e.RequestID, e.Slot, e.Cause)
}
func (e *CommitUnknownError) Unwrap() []error { return []error{ErrCommitUnknown, e.Cause} }

func commitUnknown(slot quepaxa.Slot, requestID string, err error) error {
	if err != nil && slot != 0 {
		return &CommitUnknownError{Slot: slot, RequestID: requestID, RetryThroughSlot: uint64(slot) + types.DefaultIdempotencyWindowSlots - 1, Cause: err}
	}
	return err
}

// Server is the HTTP server for client API.
type Server struct {
	core              *quepaxa.Core
	material          *materializer.Materializer
	cluster           types.ClusterID
	mux               *http.ServeMux
	ready             func() bool
	writable          bool
	sqlBatcher        *mutationBatcher[types.SQLCommand]
	graphBatcher      *mutationBatcher[types.GraphCommand]
	kvBatcher         *mutationBatcher[types.KVCommand]
	transport         *Transport
	members           []quepaxa.Member
	hedgeDelay        time.Duration
	applyMu           sync.Mutex
	requestLocks      [256]sync.Mutex
	durability        func(context.Context, quepaxa.Slot) error
	routeMu           sync.Mutex
	routeBase         quepaxa.NodeID
	routeFirst        quepaxa.NodeID
	routeGen          uint64
	proposeMu         sync.Mutex
	inflight          map[[32]byte]*proposalCall
	proposalCtx       context.Context
	proposalStop      context.CancelFunc
	proposalWG        sync.WaitGroup
	closeOnce         sync.Once
	closing           bool
	quiescing         bool
	operationCap      chan struct{}
	localCap          chan struct{}
	peerCap           chan struct{}
	operationB        int
	localB            int
	peerB             int
	peerCounts        map[quepaxa.NodeID]int
	objectStats       func() (map[string]uint64, bool)
	replicaStatus     func() ReplicaStatus
	syncLimit         chan struct{}
	checkpointPrepare func(context.Context, quepaxa.NodeID, quepaxa.CheckpointSeal) error
	compactedHandler  func()
}

// ReplicaStatus is the observable catch-up state of a non-voting replica.
type ReplicaStatus struct {
	Mode        string    `json:"mode"`
	AppliedSlot uint64    `json:"applied_slot"`
	SourceTip   uint64    `json:"source_tip"`
	LagSlots    uint64    `json:"lag_slots"`
	Source      string    `json:"source"`
	LastSync    time.Time `json:"last_sync"`
	LastError   string    `json:"last_error,omitempty"`
}

func (s *Server) SetCheckpointPrepare(prepare func(context.Context, quepaxa.NodeID, quepaxa.CheckpointSeal) error) {
	s.checkpointPrepare = prepare
}

// SetCompactedHandler installs the recovery trigger used when a peer has
// compacted history this node still needs.
func (s *Server) SetCompactedHandler(handler func()) {
	s.proposeMu.Lock()
	s.compactedHandler = handler
	s.proposeMu.Unlock()
}

func (s *Server) handleCompacted() {
	s.proposeMu.Lock()
	handler := s.compactedHandler
	s.proposeMu.Unlock()
	if handler != nil {
		handler()
	}
}

func (s *Server) prepareCheckpoint(ctx context.Context, sender quepaxa.NodeID, seal quepaxa.CheckpointSeal) error {
	if s.checkpointPrepare != nil {
		return s.checkpointPrepare(ctx, sender, seal)
	}
	return s.core.PrepareCheckpoint(ctx, seal)
}

// ProposeControl commits an internal read barrier through the normal bounded
// proposal lifecycle.
func (s *Server) ProposeControl(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
		return 0, fmt.Errorf("read barrier control value is required")
	}
	return s.proposeHedged(ctx, value)
}

func (s *Server) lockRequest(id string) func() {
	hash := sha256.Sum256([]byte(id))
	lock := &s.requestLocks[hash[0]]
	lock.Lock()
	return lock.Unlock
}

type proposalCall struct {
	done chan struct{}
	slot quepaxa.Slot
	err  error
}

// NewServer creates a new HTTP server.
func NewServer(core *quepaxa.Core, material *materializer.Materializer, cluster types.ClusterID, writable bool, transport *Transport, members []quepaxa.Member, hedgeDelay time.Duration, ready ...func() bool) *Server {
	proposalCtx, proposalStop := context.WithCancel(context.Background())
	s := &Server{
		core:         core,
		material:     material,
		cluster:      cluster,
		mux:          http.NewServeMux(),
		ready:        func() bool { return true },
		writable:     writable,
		transport:    transport,
		members:      append([]quepaxa.Member(nil), members...),
		hedgeDelay:   hedgeDelay,
		inflight:     make(map[[32]byte]*proposalCall),
		proposalCtx:  proposalCtx,
		proposalStop: proposalStop,
		operationCap: make(chan struct{}, maxProposalOperations),
		localCap:     make(chan struct{}, maxLocalProposals),
		peerCap:      make(chan struct{}, maxPeerProposals),
		peerCounts:   make(map[quepaxa.NodeID]int),
		syncLimit:    make(chan struct{}, 2),
	}
	if len(ready) > 0 {
		s.ready = ready[0]
	}
	s.sqlBatcher = newSQLBatcher(s.proposeHedged, nil)
	s.graphBatcher = newGraphBatcher(s.proposeHedged, nil)
	s.kvBatcher = newKVBatcher(s.proposeHedged, nil)
	s.routes()
	return s
}

// Close stops background request batching.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.proposeMu.Lock()
		s.closing = true
		s.proposalStop()
		s.proposeMu.Unlock()
		if s.sqlBatcher != nil {
			s.sqlBatcher.Close()
		}
		if s.graphBatcher != nil {
			s.graphBatcher.Close()
		}
		s.kvBatcher.Close()
		s.proposalWG.Wait()
	})
}

// SetDurabilityBarrier installs the mutation ACK barrier before the server is exposed.
func (s *Server) SetDurabilityBarrier(barrier func(context.Context, quepaxa.Slot) error) {
	s.durability = barrier
}

func (s *Server) waitDurable(ctx context.Context, slot quepaxa.Slot) error {
	if s.durability == nil {
		return nil
	}
	if err := s.durability(ctx, slot); err != nil {
		return fmt.Errorf("%w: %v", ErrDurabilityUnavailable, err)
	}
	return nil
}

// routes registers HTTP routes.
func (s *Server) routes() {
	// Client API
	s.mux.HandleFunc("/sql/execute", s.handleExecute)
	s.mux.HandleFunc("/sql/transaction", s.handleExecute)
	s.mux.HandleFunc("/sql/query", s.handleQuery)
	s.mux.HandleFunc("/graph/execute", s.handleGraphExecute)
	s.mux.HandleFunc("/graph/query", s.handleGraphQuery)
	s.mux.HandleFunc("/graph/changes", s.handleGraphChanges)
	s.mux.HandleFunc("/graph/streams/read", s.handleGraphStreamRead)
	s.mux.HandleFunc("/graph/streams/offset", s.handleGraphStreamOffset)
	s.mux.HandleFunc("/graph/streams/trim", s.handleGraphStreamTrim)
	s.mux.HandleFunc("/kv/put", s.handleKVPut)
	s.mux.HandleFunc("/kv/get", s.handleKVGet)
	s.mux.HandleFunc("/kv/delete", s.handleKVDelete)
	s.mux.HandleFunc("/kv/cas", s.handleKVCAS)
	s.mux.HandleFunc("/notify/publish", s.handleNotifyPublish)
	s.mux.HandleFunc("/notify/subscribe", s.handleNotifySubscribe)
	s.mux.HandleFunc("/request/status", s.handleRequestStatus)
	s.mux.HandleFunc("/metrics/object-store", s.handleObjectStoreStats)
	s.mux.HandleFunc("/replica/status", s.handleReplicaStatus)

	// Health
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
}

func (s *Server) SetObjectStoreStats(stats func() (map[string]uint64, bool)) {
	s.objectStats = stats
}

func (s *Server) SetReplicaStatus(status func() ReplicaStatus) { s.replicaStatus = status }

func (s *Server) handleReplicaStatus(w http.ResponseWriter, _ *http.Request) {
	if s.replicaStatus == nil {
		http.Error(w, "not a replica", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.replicaStatus())
}

func (s *Server) handleObjectStoreStats(w http.ResponseWriter, _ *http.Request) {
	if s.objectStats == nil {
		http.Error(w, "object store disabled", http.StatusNotFound)
		return
	}
	stats, ok := s.objectStats()
	if !ok {
		http.Error(w, "object store disabled", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

type DecisionsResponse struct {
	ClusterID  types.ClusterID        `json:"cluster_id"`
	ProposerID quepaxa.NodeID         `json:"proposer_id"`
	ConfigID   uint                   `json:"config_id"`
	Tip        quepaxa.Slot           `json:"tip"`
	Decisions  []quepaxa.DecidedValue `json:"decisions"`
}

func (s *Server) proposeHedged(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	hash := sha256.Sum256(value)
	for {
		s.proposeMu.Lock()
		if s.closing || s.quiescing {
			s.proposeMu.Unlock()
			return 0, ErrNotReady
		}
		call := s.inflight[hash]
		s.proposeMu.Unlock()
		if call != nil {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-call.done:
				return call.slot, call.err
			}
		}
		select {
		case s.localCap <- struct{}{}:
		default:
			return 0, ErrOverloaded
		}
		select {
		case s.operationCap <- struct{}{}:
		default:
			<-s.localCap
			return 0, ErrOverloaded
		}
		s.proposeMu.Lock()
		if s.closing || s.quiescing {
			<-s.operationCap
			<-s.localCap
			s.proposeMu.Unlock()
			return 0, ErrNotReady
		}
		if s.inflight[hash] != nil {
			<-s.operationCap
			<-s.localCap
			s.proposeMu.Unlock()
			continue
		}
		if len(value) > maxInflightEncodedByte-s.localB || len(value) > maxProposalEncodedByte-s.operationB {
			<-s.operationCap
			<-s.localCap
			s.proposeMu.Unlock()
			return 0, ErrOverloaded
		}
		call = &proposalCall{done: make(chan struct{})}
		s.inflight[hash] = call
		s.operationB += len(value)
		s.localB += len(value)
		s.proposalWG.Add(1)
		go s.runProposal(hash, call, bytes.Clone(value))
		s.proposeMu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-call.done:
			return call.slot, call.err
		}
	}
}

// Quiesce drains proposals and excludes decision application while a certified
// checkpoint replaces local consensus and materialized state.
func (s *Server) Quiesce(ctx context.Context) (func(), error) {
	s.proposeMu.Lock()
	if s.closing || s.quiescing {
		s.proposeMu.Unlock()
		return nil, ErrNotReady
	}
	s.quiescing = true
	s.proposeMu.Unlock()
	done := make(chan struct{})
	go func() {
		s.proposalWG.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		go func() {
			<-done
			s.proposeMu.Lock()
			if !s.closing {
				s.quiescing = false
			}
			s.proposeMu.Unlock()
		}()
		return nil, ctx.Err()
	case <-done:
	}
	s.applyMu.Lock()
	return func() {
		s.applyMu.Unlock()
		s.proposeMu.Lock()
		if !s.closing {
			s.quiescing = false
		}
		s.proposeMu.Unlock()
	}, nil
}

func (s *Server) runProposal(hash [32]byte, call *proposalCall, value []byte) {
	defer s.proposalWG.Done()
	ctx, cancel := context.WithTimeout(s.proposalCtx, 30*time.Second)
	call.slot, call.err = s.proposeHedgedOnce(ctx, value)
	if call.err == nil {
		call.err = s.applyDecisions(ctx, call.slot)
	}
	if call.err == nil {
		call.err = s.waitDurable(ctx, call.slot)
	}
	cancel()
	s.proposeMu.Lock()
	if s.inflight[hash] == call {
		delete(s.inflight, hash)
	}
	s.operationB -= len(value)
	s.localB -= len(value)
	<-s.operationCap
	<-s.localCap
	close(call.done)
	s.proposeMu.Unlock()
}

func (s *Server) proposeHedgedOnce(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	if slot, ok := s.core.DecidedSlot(value); ok {
		if _, err := s.core.CompleteDecision(ctx, slot); err == nil {
			return slot, nil
		} else if !errors.Is(err, quepaxa.ErrCompacted) {
			return slot, err
		}
	}
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
	var uncertainSlot quepaxa.Slot
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
				if _, err := s.core.CompleteDecision(hedgeCtx, slot); !errors.Is(err, quepaxa.ErrCompacted) {
					results <- result{slot: slot, err: err, member: member.ID, rank: rank}
					return
				}
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
			if uncertainSlot == 0 && result.slot != 0 {
				uncertainSlot = result.slot
			}
			if firstErr == nil {
				firstErr = result.err
			}
		}
	}
	if firstErr != nil {
		return uncertainSlot, fmt.Errorf("%w: %v", quepaxa.ErrQuorumUnavailable, firstErr)
	}
	return uncertainSlot, quepaxa.ErrQuorumUnavailable
}

func (s *Server) acceptFrom(ctx context.Context, source quepaxa.NodeID, decision quepaxa.DecidedValue) error {
	if err := s.core.AcceptCertifiedHints([]quepaxa.DecidedValue{decision}); err != nil {
		return err
	}
	if err := s.catchUpFrom(ctx, source, decision.Slot); err != nil {
		return err
	}
	if certified, ok := s.core.CertifiedValue(decision.Slot); !ok || certified.Hash != decision.Hash {
		return fmt.Errorf("peer %s returned inconsistent decision slot %d", source, decision.Slot)
	}
	return nil
}

func (s *Server) catchUpFrom(ctx context.Context, source quepaxa.NodeID, through quepaxa.Slot) error {
	select {
	case s.syncLimit <- struct{}{}:
		defer func() { <-s.syncLimit }()
	case <-ctx.Done():
		return ctx.Err()
	}
	backoff := [...]time.Duration{0, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond}
	for s.core.Tip() < through {
		from := s.core.Tip() + 1
		var response DecisionsResponse
		var err error
		for attempt, delay := range backoff {
			if delay != 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			pageCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			response, err = s.transport.FetchDecisions(pageCtx, source, from, 128)
			cancel()
			if err == nil || attempt == len(backoff)-1 {
				break
			}
		}
		if err != nil {
			if errors.Is(err, quepaxa.ErrCompacted) {
				s.handleCompacted()
				return ErrNotReady
			}
			return err
		}
		if len(response.Decisions) == 0 || response.Decisions[0].Slot != from {
			return fmt.Errorf("peer %s omitted decision slot %d", source, from)
		}
		if err := s.core.AcceptCertifiedHints(response.Decisions); err != nil {
			if errors.Is(err, quepaxa.ErrCompacted) {
				s.handleCompacted()
				return ErrNotReady
			}
			return err
		}
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
	RequestID string `json:"request_id"`
	SQL       string `json:"sql,omitempty"`
	Args      []any  `json:"args,omitempty"`
	// WantRows is unsupported for replicated mutations; use Query after Execute.
	WantRows   bool                 `json:"want_rows,omitempty"`
	Statements []types.SQLStatement `json:"statements,omitempty"`
}

// ExecuteResponse contains the bounded aggregate receipt retained for retries.
// Replicated statement rows are not returned; use Query after Execute.
type ExecuteResponse struct {
	types.MutationReceipt
}

// ValidateExecuteRequest applies the same mutation contract and encoded-size
// check as Execute without submitting the command.
func ValidateExecuteRequest(req ExecuteRequest) error {
	_, err := validatedSQLCommand(req)
	return err
}

func validatedSQLCommand(req ExecuteRequest) (types.SQLCommand, error) {
	if req.RequestID == "" {
		return types.SQLCommand{}, fmt.Errorf("%w: request_id is required", ErrInvalidRequest)
	}
	command := types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, Statements: req.Statements}
	if err := materializer.ValidateSQLCommand(command); err != nil {
		return types.SQLCommand{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	encoded, err := types.EncodeSQLBatch([]types.SQLCommand{command})
	if err != nil {
		return types.SQLCommand{}, fmt.Errorf("%w: encode SQL command: %v", ErrInvalidRequest, err)
	}
	if len(encoded) > quepaxa.MaxReplicatedValueBytes {
		return types.SQLCommand{}, fmt.Errorf("%w: encoded command exceeds %d bytes", ErrInvalidRequest, quepaxa.MaxReplicatedValueBytes)
	}
	return command, nil
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
	command, err := validatedSQLCommand(req)
	if err != nil {
		return ExecuteResponse{}, err
	}
	defer s.lockRequest(req.RequestID)()
	if matches, err := s.material.SQLRequestMatches(ctx, command); err != nil {
		return ExecuteResponse{}, err
	} else if !matches {
		return ExecuteResponse{}, ErrRequestConflict
	}
	if receipt, found, err := s.material.MutationReceipt(ctx, types.MutationSQL, req.RequestID); err != nil {
		return ExecuteResponse{}, err
	} else if found {
		return ExecuteResponse{MutationReceipt: receipt}, nil
	}
	_, err = s.sqlBatcher.submit(ctx, command)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if matches, err := s.material.SQLRequestMatches(ctx, command); err != nil || !matches {
		return ExecuteResponse{}, ErrRequestConflict
	}
	receipt, found, err := s.material.MutationReceipt(ctx, types.MutationSQL, req.RequestID)
	if err != nil {
		return ExecuteResponse{}, err
	}
	if !found {
		return ExecuteResponse{}, fmt.Errorf("SQL mutation receipt is unavailable")
	}
	return ExecuteResponse{MutationReceipt: receipt}, nil
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
	result, appliedSlot, err := s.material.QueryResultAt(ctx, req.SQL, req.Args)
	if err != nil {
		return QueryResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return QueryResponse{Columns: result.Columns, Rows: result.Rows, AppliedSlot: appliedSlot, ConsensusTip: uint64(s.core.Tip())}, nil
}

func writeAPIError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrInvalidRequest), errors.Is(err, errConsistency):
		status = http.StatusBadRequest
	case errors.Is(err, ErrRequestConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrNotReady), errors.Is(err, ErrOverloaded), errors.Is(err, ErrDurabilityUnavailable), errors.Is(err, ErrCommitUnknown), errors.Is(err, quepaxa.ErrQuorumUnavailable):
		status = http.StatusServiceUnavailable
	}
	var unknown *CommitUnknownError
	if errors.As(err, &unknown) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": "commit_unknown", "request_id": unknown.RequestID, "slot": unknown.Slot, "retry_through_slot": unknown.RetryThroughSlot})
		return
	}
	http.Error(w, err.Error(), status)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RequestStatusRequest
	if err := decodeJSON(w, r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	s.handleKVMutation(w, r, "put")
}

type KVGetRequest struct{ Key, Consistency string }
type KVGetResponse struct {
	Found        bool   `json:"found"`
	Value        []byte `json:"value,omitempty"`
	AppliedSlot  uint64 `json:"applied_slot"`
	ConsensusTip uint64 `json:"consensus_tip"`
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
	types.MutationReceipt
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
	value, found, appliedSlot, err := s.material.KVGetAt(ctx, req.Key, time.Now())
	return KVGetResponse{Found: found, Value: value, AppliedSlot: appliedSlot, ConsensusTip: uint64(s.core.Tip())}, err
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
	if err := decodeJSON(w, r, &req); err != nil || req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Key == "" || len(req.Key) > 1024 || req.TTLMS < 0 || len(req.Value) > 16<<20 {
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
	if operation != "put" && operation != "delete" && operation != "cas" {
		return KVMutationResponse{}, ErrInvalidRequest
	}
	if req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Key == "" || len(req.Key) > 1024 || req.TTLMS < 0 || len(req.Value) > 16<<20 {
		return KVMutationResponse{}, ErrInvalidRequest
	}
	intent := types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS}
	defer s.lockRequest(req.RequestID)()
	if matches, err := s.material.KVRequestMatches(ctx, intent); err != nil {
		return KVMutationResponse{}, err
	} else if !matches {
		return KVMutationResponse{}, ErrRequestConflict
	}
	if receipt, found, err := s.material.MutationReceipt(ctx, types.MutationKV, req.RequestID); err != nil {
		return KVMutationResponse{}, err
	} else if found {
		return KVMutationResponse{MutationReceipt: receipt}, nil
	}
	command := intent
	now := time.Now().UnixMilli()
	if req.TTLMS > math.MaxInt64-now {
		return KVMutationResponse{}, ErrInvalidRequest
	}
	command.ObservedAtUnixMS = now
	if req.TTLMS > 0 {
		command.ExpiresAtUnixMS = now + req.TTLMS
	}
	_, err := s.kvBatcher.submit(ctx, command)
	if err != nil {
		return KVMutationResponse{}, err
	}
	if matches, err := s.material.KVRequestMatches(ctx, intent); err != nil || !matches {
		return KVMutationResponse{}, ErrRequestConflict
	}
	receipt, found, err := s.material.MutationReceipt(ctx, types.MutationKV, req.RequestID)
	if err != nil {
		return KVMutationResponse{}, err
	}
	if !found {
		return KVMutationResponse{}, fmt.Errorf("KV mutation receipt is unavailable")
	}
	return KVMutationResponse{MutationReceipt: receipt}, nil
}

var errConsistency = errors.New("consistency must be local or linearizable")

func (s *Server) readBarrier(ctx context.Context, consistency string) error {
	switch consistency {
	case "", "local":
		return nil
	case "linearizable":
		index, source, err := s.core.ReadIndex(ctx)
		if err != nil {
			return err
		}
		if index > s.core.Tip() {
			if s.transport == nil || source == s.core.NodeID() {
				return fmt.Errorf("read-index source cannot supply slot %d", index)
			}
			if err := s.catchUpFrom(ctx, source, index); err != nil {
				return err
			}
		}
		return s.applyDecisions(ctx, index)
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
	if err := decodeJSON(w, r, &req); err != nil || req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Topic == "" || len(req.Topic) > 256 || len(req.Payload) > 1<<20 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	receipt, err := s.NotifyPublish(r.Context(), req)
	if err != nil {
		writeAPIError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(receipt)
}

func (s *Server) NotifyPublish(ctx context.Context, req types.NotifyCommand) (types.MutationReceipt, error) {
	if !s.writable || !s.ready() {
		return types.MutationReceipt{}, ErrNotReady
	}
	if req.RequestID == "" || len(req.RequestID) > types.MaxRequestIDBytes || req.Topic == "" || len(req.Topic) > 256 || len(req.Payload) > 1<<20 {
		return types.MutationReceipt{}, ErrInvalidRequest
	}
	defer s.lockRequest(req.RequestID)()
	if matches, err := s.material.NotifyRequestMatches(ctx, req); err != nil {
		return types.MutationReceipt{}, err
	} else if !matches {
		return types.MutationReceipt{}, ErrRequestConflict
	}
	if receipt, found, err := s.material.MutationReceipt(ctx, types.MutationNotify, req.RequestID); err != nil {
		return types.MutationReceipt{}, err
	} else if found {
		return receipt, nil
	}
	value, err := types.EncodeNotifyCommand(req)
	if err != nil {
		return types.MutationReceipt{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if len(value) > quepaxa.MaxReplicatedValueBytes {
		return types.MutationReceipt{}, fmt.Errorf("%w: encoded command exceeds %d bytes", ErrInvalidRequest, quepaxa.MaxReplicatedValueBytes)
	}
	slot, err := s.proposeHedged(ctx, value)
	if err == nil {
		if matches, matchErr := s.material.NotifyRequestMatches(ctx, req); matchErr != nil {
			err = matchErr
		} else if !matches {
			err = ErrRequestConflict
		}
	}
	if err != nil {
		return types.MutationReceipt{}, commitUnknown(slot, req.RequestID, err)
	}
	receipt, found, err := s.material.MutationReceipt(ctx, types.MutationNotify, req.RequestID)
	if err != nil {
		return types.MutationReceipt{}, err
	}
	if !found {
		return types.MutationReceipt{}, fmt.Errorf("notification receipt is unavailable")
	}
	return receipt, nil
}

func (s *Server) NotifySubscribe(topic string) (<-chan []byte, func(), error) {
	if topic == "" || len(topic) > 256 {
		return nil, nil, ErrInvalidRequest
	}
	ch, cancel, err := s.material.Subscribe(topic)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrOverloaded, err)
	}
	return ch, cancel, nil
}

func (s *Server) NotificationDrops() uint64 {
	return s.material.NotificationDrops()
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
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
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
		case payload, ok := <-ch:
			if !ok {
				return
			}
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
	if err := validateReplicatedMutation(value); err != nil {
		return quepaxa.DecidedValue{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if slot, ok := s.core.DecidedSlot(value); ok {
		if decision, err := s.core.CompleteDecision(ctx, slot); err == nil {
			if err := s.applyDecisions(ctx, slot); err != nil {
				return quepaxa.DecidedValue{}, err
			}
			return decision, nil
		} else if !errors.Is(err, quepaxa.ErrCompacted) {
			return quepaxa.DecidedValue{}, err
		}
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

func (s *Server) proposePeer(ctx context.Context, sender quepaxa.NodeID, value []byte) (quepaxa.DecidedValue, error) {
	select {
	case s.peerCap <- struct{}{}:
	default:
		return quepaxa.DecidedValue{}, ErrOverloaded
	}
	select {
	case s.operationCap <- struct{}{}:
	default:
		<-s.peerCap
		return quepaxa.DecidedValue{}, ErrOverloaded
	}
	s.proposeMu.Lock()
	if s.closing || s.quiescing || s.peerCounts[sender] >= 2 || len(value) > maxPeerEncodedByte-s.peerB || len(value) > maxProposalEncodedByte-s.operationB {
		<-s.operationCap
		<-s.peerCap
		s.proposeMu.Unlock()
		if s.closing || s.quiescing {
			return quepaxa.DecidedValue{}, ErrNotReady
		}
		return quepaxa.DecidedValue{}, ErrOverloaded
	}
	s.operationB += len(value)
	s.peerB += len(value)
	s.peerCounts[sender]++
	s.proposalWG.Add(1)
	s.proposeMu.Unlock()
	defer func() {
		s.proposeMu.Lock()
		s.operationB -= len(value)
		s.peerB -= len(value)
		s.peerCounts[sender]--
		if s.peerCounts[sender] == 0 {
			delete(s.peerCounts, sender)
		}
		<-s.operationCap
		<-s.peerCap
		s.proposeMu.Unlock()
		s.proposalWG.Done()
	}()
	operationCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.proposalCtx, cancel)
	defer func() { stop(); cancel() }()
	return s.proposeLocal(operationCtx, value)
}

func validateReplicatedMutation(value []byte) error {
	if len(value) == 0 || len(value) > quepaxa.MaxReplicatedValueBytes {
		return fmt.Errorf("encoded command must be between 1 and %d bytes", quepaxa.MaxReplicatedValueBytes)
	}
	if barrier, err := types.DecodeReadBarrier(value); err != nil {
		return err
	} else if barrier {
		return nil
	}
	if commands, ok, err := types.DecodeSQLBatch(value); err != nil {
		return err
	} else if ok {
		for _, command := range commands {
			if command.RequestID == "" {
				return fmt.Errorf("request_id is required")
			}
			if err := materializer.ValidateSQLCommand(command); err != nil {
				return err
			}
		}
		return nil
	}
	if commands, ok, err := types.DecodeKVBatch(value); err != nil {
		return err
	} else if ok {
		for _, command := range commands {
			if command.RequestID == "" || len(command.RequestID) > types.MaxRequestIDBytes || command.Key == "" || len(command.Key) > 1024 || command.TTLMS < 0 || len(command.Value) > 16<<20 {
				return fmt.Errorf("invalid KV command")
			}
			switch command.Operation {
			case "put", "delete", "cas":
			default:
				return fmt.Errorf("unsupported KV operation %q", command.Operation)
			}
		}
		return nil
	}
	if commands, ok, err := types.DecodeGraphBatch(value); err != nil {
		return err
	} else if ok {
		for _, command := range commands {
			if err := materializer.ValidateGraphCommandAdmission(command); err != nil {
				return err
			}
		}
		return nil
	}
	if command, ok, err := types.DecodeKVCommand(value); err != nil {
		return err
	} else if ok {
		if command.RequestID == "" || len(command.RequestID) > types.MaxRequestIDBytes || command.Key == "" || len(command.Key) > 1024 || command.ExpiresAtUnixMS < 0 {
			return fmt.Errorf("invalid KV command")
		}
		switch command.Operation {
		case "put", "delete", "cas":
			return nil
		default:
			return fmt.Errorf("invalid KV operation")
		}
	}
	if command, ok, err := types.DecodeNotifyCommand(value); err != nil {
		return err
	} else if ok {
		if command.RequestID == "" || len(command.RequestID) > types.MaxRequestIDBytes || command.Topic == "" || len(command.Topic) > 256 || len(command.Payload) > 1<<20 {
			return fmt.Errorf("invalid notification command")
		}
		return nil
	}
	return fmt.Errorf("unknown replicated command")
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
	if err := s.core.Health(); err != nil {
		http.Error(w, "WAL sync failed", http.StatusServiceUnavailable)
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
		end := len(decisions)
		for i, decision := range decisions {
			if decision.Slot > through {
				end = i
				break
			}
		}
		if err := s.material.ApplyBatch(ctx, decisions[:end]); err != nil {
			return err
		}
		if end < len(decisions) {
			return nil
		}
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if err := s.material.Health(r.Context()); err != nil {
		http.Error(w, "materializer unhealthy", http.StatusServiceUnavailable)
		return
	}
	if err := s.core.Health(); err != nil {
		http.Error(w, "WAL sync failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
