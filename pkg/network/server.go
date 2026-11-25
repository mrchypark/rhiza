package network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// MaxRequestBodyBytes is the HTTP adapter's JSON decoding limit. Replicated
// mutations have the smaller quepaxa.MaxReplicatedValueBytes consensus limit.
const MaxRequestBodyBytes = 1 << 20

var (
	ErrNotReady                = errors.New("node is not ready")
	ErrRequestConflict         = errors.New("request ID conflict")
	ErrInvalidRequest          = errors.New("invalid request")
	ErrOverloaded              = errors.New("mutation queue overloaded")
	ErrDurabilityUnavailable   = errors.New("object-store durability unavailable")
	ErrCommitUnknown           = errors.New("commit outcome unknown")
	ErrLocalMaintenanceRefused = errors.New("local checkpoint maintenance refused before mutation")
	ErrGraphResourceLimit      = errors.New("graph resource limit exceeded")
	ErrReadVersionMismatch     = errors.New("read version mismatch")
)

// proposalAdmissionOverload identifies only a proposal rejected before it was
// admitted. It preserves the public overload text and errors.Is contract.
type proposalAdmissionOverload struct{}

func (proposalAdmissionOverload) Error() string { return ErrOverloaded.Error() }
func (proposalAdmissionOverload) Unwrap() error { return ErrOverloaded }

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
	if err != nil && (slot != 0 || errors.Is(err, ErrCommitUnknown)) {
		unknown := &CommitUnknownError{Slot: slot, RequestID: requestID, Cause: err}
		if slot != 0 {
			unknown.RetryThroughSlot = uint64(slot) + types.DefaultIdempotencyWindowSlots - 1
		}
		return unknown
	}
	return err
}

// Server is the HTTP server for client API.
type Server struct {
	expectedPolicy       sqlpolicy.MaterializationPolicy
	managedProfile       *ManagedPeerProfile
	core                 *quepaxa.Core
	material             *materializer.Materializer
	cluster              types.ClusterID
	mux                  *http.ServeMux
	ready                func() bool
	learnReady           func() bool
	writable             bool
	sqlBatcher           *mutationBatcher[types.SQLCommand]
	graphBatcher         *mutationBatcher[types.GraphCommand]
	kvBatcher            *mutationBatcher[types.KVCommand]
	transport            *Transport
	applyMu              sync.Mutex
	requestLocks         [4096]chan struct{}
	mutationMu           sync.Mutex
	mutationAdmission    mutationAdmission
	durability           func(context.Context, quepaxa.Slot) error
	proposeMu            sync.Mutex
	admissionChanged     chan struct{}
	admissionEpoch       uint64
	admissionWaiters     int
	inflight             map[[32]byte]*proposalCall
	proposalCtx          context.Context
	proposalStop         context.CancelFunc
	proposalWG           sync.WaitGroup
	logicalWG            sync.WaitGroup
	closeOnce            sync.Once
	closing              bool
	quiescing            bool
	localMode            bool
	lifecycleFailure     error
	localFailureHandler  func(error)
	localReclaimHandler  func(context.Context) error
	operationCap         chan struct{}
	localCap             chan struct{}
	operationB           int
	localB               int
	objectStats          func() (map[string]uint64, bool)
	replicaStatus        func() ReplicaStatus
	voterRecoveryStatus  func(context.Context) VoterRecoveryStatus
	recoveryArchiveToken string
	membershipToken      string
	membershipChange     func(context.Context, MembershipChange) error
	membershipAbort      func(context.Context, MembershipChange) error
	recoveryArchive      func(context.Context) error
	terminalDrain        func(context.Context, TerminalDrainRequest) (any, error)
	handlerMu            sync.Mutex
	handlerWG            sync.WaitGroup
	handlerClosing       bool
	syncLimit            chan struct{}
	syncMu               sync.Mutex
	syncSources          map[quepaxa.NodeID]chan struct{}
	checkpointPrepare    func(context.Context, quepaxa.NodeID, quepaxa.CheckpointSeal) error
	compactedHandler     func()
	readAdmissionMu      sync.RWMutex
	readAdmission        *readAdmission
}

// ReadAdmissionLimits bounds concurrent read work in one Server. Long-poll
// stream reads additionally use MaxLongPoll so they leave capacity for normal
// queries. Configure the limits before exposing the Server.
type ReadAdmissionLimits struct {
	MaxConcurrent int
	MaxLongPoll   int
}

var defaultReadAdmissionLimits = ReadAdmissionLimits{MaxConcurrent: 64, MaxLongPoll: 8}

type readAdmission struct {
	concurrent chan struct{}
	longPoll   chan struct{}
}

func newReadAdmission(limits ReadAdmissionLimits) (*readAdmission, error) {
	if limits.MaxConcurrent <= 0 || limits.MaxLongPoll < 0 || limits.MaxLongPoll > limits.MaxConcurrent {
		return nil, fmt.Errorf("invalid read admission limits")
	}
	return &readAdmission{concurrent: make(chan struct{}, limits.MaxConcurrent), longPoll: make(chan struct{}, limits.MaxLongPoll)}, nil
}

// ReplicaStatus is the observable catch-up state of a non-voting replica.
type ReplicaStatus struct {
	Mode                   string    `json:"mode"`
	AppliedSlot            uint64    `json:"applied_slot"`
	SourceTip              uint64    `json:"source_tip"`
	LagSlots               uint64    `json:"lag_slots"`
	Source                 string    `json:"source"`
	LastSync               time.Time `json:"last_sync"`
	LastError              string    `json:"last_error,omitempty"`
	CheckpointBaseSlot     uint64    `json:"checkpoint_base_slot,omitempty"`
	CheckpointRootHash     string    `json:"checkpoint_root_hash,omitempty"`
	CheckpointStateHash    string    `json:"checkpoint_state_hash,omitempty"`
	CheckpointPrefixHash   string    `json:"checkpoint_prefix_hash,omitempty"`
	ArchiveHeadVersionType string    `json:"archive_head_version_type,omitempty"`
	ArchiveHeadVersion     string    `json:"archive_head_version,omitempty"`
	RecoveryPinID          string    `json:"recovery_pin_id,omitempty"`
	RecoveryPinsCreated    uint64    `json:"recovery_pins_created,omitempty"`
	RecoveryPinsReleased   uint64    `json:"recovery_pins_released,omitempty"`
}

// SetCheckpointPrepare replaces the handler for future peer calls. Calls already
// in progress retain their handler; nil restores the Core preparation path.
func (s *Server) SetCheckpointPrepare(prepare func(context.Context, quepaxa.NodeID, quepaxa.CheckpointSeal) error) {
	s.proposeMu.Lock()
	s.checkpointPrepare = prepare
	s.proposeMu.Unlock()
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
	s.proposeMu.Lock()
	prepare := s.checkpointPrepare
	s.proposeMu.Unlock()
	if prepare != nil {
		return prepare(ctx, sender, seal)
	}
	return s.core.PrepareCheckpoint(ctx, seal)
}

// ProposeControl commits an internal read barrier through the normal bounded
// proposal lifecycle.
func (s *Server) ProposeControl(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
		return 0, fmt.Errorf("read barrier control value is required")
	}
	return s.propose(ctx, value)
}

// ProposeRetentionControl waits for accepted work to actually finish, even if
// its deadline expires, so the maintenance worker cannot overlap proposals.
func (s *Server) ProposeRetentionControl(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
		return 0, fmt.Errorf("read barrier control value is required")
	}
	return s.proposeWithAdmission(ctx, value, nil, 0, 0, nil, nil, true)
}

// ProposeLocalRetentionControl keeps an accepted Local maintenance barrier
// alive until completion or server shutdown, including any WAL reclamation.
// It must only be used by the node's single retention maintenance worker.
func (s *Server) ProposeLocalRetentionControl(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	if !s.localMode {
		return 0, fmt.Errorf("Local retention control requires Local mode")
	}
	if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
		return 0, fmt.Errorf("read barrier control value is required")
	}
	return s.proposeWithAdmission(ctx, value, nil, 0, 0, nil, nil, true)
}

// ProposeCheckpointBarrier is reserved for the checkpoint publisher's internal
// read barrier. General control proposals retain ProposeControl semantics.
func (s *Server) ProposeCheckpointBarrier(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	return retryCheckpointBarrier(ctx, value, s.ProposeControl)
}

func retryCheckpointBarrier(ctx context.Context, value []byte, propose func(context.Context, []byte) (quepaxa.Slot, error)) (quepaxa.Slot, error) {
	const maxAttempts = 50
	const retryDelay = 20 * time.Millisecond
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		slot, err := propose(ctx, value)
		if err == nil {
			return slot, nil
		}
		// This private concrete value is returned only before proposal
		// registration. A wrapped, joined, or otherwise ambiguous error is
		// terminal, even when errors.Is also matches ErrOverloaded.
		if _, preAdmission := err.(proposalAdmissionOverload); !preAdmission || slot != 0 || attempt+1 == maxAttempts {
			return slot, err
		}
		if localtesthooks.Enabled {
			localtesthooks.Hit("network:checkpoint-barrier:typed-pre-admission-refused")
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		}
	}
	panic("unreachable checkpoint barrier retry state")
}

func (s *Server) lockRequest(ctx context.Context, id string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(id))
	lock := s.requestLocks[uint16(hash[0])<<4|uint16(hash[1])>>4]
	select {
	case lock <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-lock
			return nil, err
		}
		select {
		case <-s.proposalCtx.Done():
			<-lock
			return nil, ErrNotReady
		default:
		}
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.proposalCtx.Done():
		return nil, ErrNotReady
	}
}

type proposalCall struct {
	done             chan struct{}
	slot             quepaxa.Slot
	err              error
	deadline         time.Time
	hasDeadline      bool
	retentionControl bool
	leases           []*mutationLease
}

// NewServer creates a legacy-policy HTTP server. Incompatible explicit
// dependencies return nil; use NewServerForPolicy to inspect that error.
func NewServer(core *quepaxa.Core, material *materializer.Materializer, cluster types.ClusterID, writable bool, transport *Transport, ready ...func() bool) *Server {
	s, _ := NewServerForPolicy(core, material, cluster, writable, transport, sqlpolicy.LegacyMaterializationPolicy, ready...)
	return s
}

// NewServerForPolicy fixes execution and peer semantics before batchers start.
func NewServerForPolicy(core *quepaxa.Core, material *materializer.Materializer, cluster types.ClusterID, writable bool, transport *Transport, expected sqlpolicy.MaterializationPolicy, ready ...func() bool) (*Server, error) {
	return newServerForProfile(core, material, cluster, writable, transport, expected, nil, ready...)
}

// NewServerForManagedProfile fixes append-only execution semantics and requires
// the peer transport to carry the same common generation/plan binding.
func NewServerForManagedProfile(core *quepaxa.Core, material *materializer.Materializer, cluster types.ClusterID, writable bool, transport *Transport, profile ManagedPeerProfile, ready ...func() bool) (*Server, error) {
	expected, _, err := types.ManagedApplicationPolicy(profile.ApplicationProtocol)
	if err != nil {
		return nil, err
	}
	return newServerForProfile(core, material, cluster, writable, transport, expected, &profile, ready...)
}

func newServerForProfile(core *quepaxa.Core, material *materializer.Materializer, cluster types.ClusterID, writable bool, transport *Transport, expected sqlpolicy.MaterializationPolicy, profile *ManagedPeerProfile, ready ...func() bool) (*Server, error) {
	if err := expected.Validate(); err != nil {
		return nil, err
	}
	var profileCopy *ManagedPeerProfile
	if profile != nil {
		if err := profile.validate(expected); err != nil {
			return nil, err
		}
		copy := *profile
		profileCopy = &copy
	}
	if material != nil && material.MaterializationPolicy() != expected {
		return nil, sqlpolicy.ErrIncompatible
	}
	if transport != nil {
		if transport.expectedPolicy != expected || !sameManagedPeerProfile(transport.managedProfile, profileCopy) {
			return nil, sqlpolicy.ErrIncompatible
		}
	}
	proposalCtx, proposalStop := context.WithCancel(context.Background())
	localMode := core != nil && core.LocalMode()
	localLimit := maxLocalProposals
	if localMode {
		localLimit = 1
	}
	s := &Server{
		expectedPolicy: expected, managedProfile: profileCopy,
		core:             core,
		material:         material,
		cluster:          cluster,
		mux:              http.NewServeMux(),
		ready:            func() bool { return true },
		writable:         writable,
		transport:        transport,
		inflight:         make(map[[32]byte]*proposalCall),
		admissionChanged: make(chan struct{}),
		proposalCtx:      proposalCtx,
		proposalStop:     proposalStop,
		operationCap:     make(chan struct{}, maxProposalOperations),
		localCap:         make(chan struct{}, localLimit),
		localMode:        localMode,
		syncLimit:        make(chan struct{}, 2),
	}
	for i := range s.requestLocks {
		s.requestLocks[i] = make(chan struct{}, 1)
	}
	admission, err := newReadAdmission(defaultReadAdmissionLimits)
	if err != nil {
		panic(err)
	}
	s.readAdmission = admission
	if len(ready) > 0 {
		s.ready = ready[0]
	}
	s.sqlBatcher = newSQLBatcher(s.proposeInternalBatch, nil)
	if expected == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		s.graphBatcher = newManagedGraphBatcher(s.proposeInternalBatch, nil)
	} else {
		s.graphBatcher = newGraphBatcher(s.proposeInternalBatch, nil)
	}
	if managedKVExecutionPolicy(expected) {
		s.kvBatcher = newManagedKVBatcher(s.proposeInternalBatch, nil)
	} else {
		s.kvBatcher = newKVBatcher(s.proposeInternalBatch, nil)
	}
	s.routes()
	return s, nil
}

// SetLocalFailureHandler installs the root-readiness hook before the server is
// exposed. It is called once, on the first failed Local lifecycle.
func (s *Server) SetLocalFailureHandler(handler func(error)) {
	s.proposeMu.Lock()
	s.localFailureHandler = handler
	if s.lifecycleFailure != nil && handler != nil {
		handler(s.lifecycleFailure)
	}
	s.proposeMu.Unlock()
}

// SetLocalReclaimHandler installs the one-shot Local checkpoint coordinator.
// The handler runs only after a trusted new-proposal prewrite refusal and only
// after that attempt has left proposalWG and returned its physical permits.
func (s *Server) SetLocalReclaimHandler(handler func(context.Context) error) {
	s.proposeMu.Lock()
	s.localReclaimHandler = handler
	s.proposeMu.Unlock()
}

// FailLocalLifecycle closes readiness before a quiesced Local maintenance
// handler reopens proposal admission after an uncertain state transition.
func (s *Server) FailLocalLifecycle(err error) {
	if err == nil || !s.localMode {
		return
	}
	s.proposeMu.Lock()
	if s.lifecycleFailure == nil {
		s.lifecycleFailure = err
		s.signalAdmissionLocked(true)
		if s.localFailureHandler != nil {
			s.localFailureHandler(err)
		}
	}
	s.proposeMu.Unlock()
}

// Ready reports root readiness, including a sticky Local lifecycle failure.
// SetLearnReady separates certified decision delivery from new client admission.
// Install it before publishing the peer listener; absent means Ready as before.
func (s *Server) SetLearnReady(ready func() bool) { s.learnReady = ready }

func (s *Server) readyToLearn() bool {
	if s.learnReady != nil {
		return s.learnReady()
	}
	return s.Ready()
}

func (s *Server) Ready() bool {
	if s.ready == nil || !s.ready() {
		return false
	}
	s.proposeMu.Lock()
	defer s.proposeMu.Unlock()
	return s.lifecycleFailure == nil
}

func (s *Server) localFailure() error {
	s.proposeMu.Lock()
	defer s.proposeMu.Unlock()
	return s.lifecycleFailure
}

// SetReadAdmissionLimits replaces admission limits for subsequently started
// reads. It is intended for setup before the Server is exposed.
func (s *Server) SetReadAdmissionLimits(limits ReadAdmissionLimits) error {
	admission, err := newReadAdmission(limits)
	if err != nil {
		return err
	}
	s.readAdmissionMu.Lock()
	s.readAdmission = admission
	s.readAdmissionMu.Unlock()
	return nil
}

func (s *Server) acquireRead(ctx context.Context, longPoll bool) (func(), error) {
	if s.localFailure() != nil {
		return nil, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.readAdmissionMu.RLock()
	admission := s.readAdmission
	s.readAdmissionMu.RUnlock()
	select {
	case admission.concurrent <- struct{}{}:
	default:
		return nil, ErrOverloaded
	}
	if longPoll {
		select {
		case admission.longPoll <- struct{}{}:
		default:
			<-admission.concurrent
			return nil, ErrOverloaded
		}
	}
	return func() {
		if longPoll {
			<-admission.longPoll
		}
		<-admission.concurrent
	}, nil
}

// Close stops background request batching.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.handlerMu.Lock()
		s.handlerClosing = true
		s.handlerMu.Unlock()
		s.proposeMu.Lock()
		s.closing = true
		s.signalAdmissionLocked(true)
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
		s.logicalWG.Wait()
		s.handlerWG.Wait()
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
		if s.localMode {
			return fmt.Errorf("local durability wait: %w", err)
		}
		return fmt.Errorf("%w: %v", ErrDurabilityUnavailable, err)
	}
	return nil
}

// waitReceiptDurable confirms that a previously applied mutation still meets
// the configured before-ack durability policy before its receipt is returned.
func (s *Server) waitReceiptDurable(ctx context.Context, receipt types.MutationReceipt, requestID string) error {
	if !s.localMode {
		return commitUnknown(quepaxa.Slot(receipt.Slot), requestID, s.waitDurable(ctx, quepaxa.Slot(receipt.Slot)))
	}
	if err := s.beginLocalLifecycle(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		durableCtx, cancel := context.WithTimeout(s.proposalCtx, 30*time.Second)
		err := s.waitDurable(durableCtx, quepaxa.Slot(receipt.Slot))
		cancel()
		s.finishLocalLifecycle(err)
		done <- err
	}()
	select {
	case <-ctx.Done():
		return commitUnknown(quepaxa.Slot(receipt.Slot), requestID, ctx.Err())
	case err := <-done:
		return commitUnknown(quepaxa.Slot(receipt.Slot), requestID, err)
	}
}

// routes registers HTTP routes.
func (s *Server) routes() {
	// Client API
	s.mux.HandleFunc("/sql/execute", s.handleExecute)
	s.mux.HandleFunc("/sql/execute-returning", s.handleExecuteReturning)
	s.mux.HandleFunc("/sql/execute-returning-one", s.handleExecuteReturningOne)
	s.mux.HandleFunc("/sql/transaction", s.handleExecute)
	s.mux.HandleFunc("/sql/query", s.handleQuery)
	s.mux.HandleFunc("/graph/execute", s.handleGraphExecute)
	s.mux.HandleFunc("/graph/query", s.handleGraphQuery)
	s.mux.HandleFunc("/graph/reachable", s.handleGraphReachable)
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
	s.mux.HandleFunc("/recovery/status", s.handleVoterRecoveryStatus)
	s.mux.HandleFunc("/recovery/probe", s.handleRecoveryProbe)
	s.mux.HandleFunc("/membership/status", s.handleMembershipStatus)
	s.mux.HandleFunc("/membership/change", s.handleMembershipChange)
	s.mux.HandleFunc("/membership/abort", s.handleMembershipAbort)
	s.mux.HandleFunc("/recovery/archive", s.handleRecoveryArchive)
	s.mux.HandleFunc("/recovery/drain", s.handleTerminalDrain)

	// Health
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeHTTPError(w, http.StatusNotFound, "not_found", "route not found")
	})
}

func (s *Server) SetObjectStoreStats(stats func() (map[string]uint64, bool)) {
	s.objectStats = stats
}

func (s *Server) SetReplicaStatus(status func() ReplicaStatus) { s.replicaStatus = status }

func (s *Server) handleReplicaStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	if s.replicaStatus == nil {
		writeHTTPError(w, http.StatusNotFound, "not_a_replica", "not a replica")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.replicaStatus())
}

func (s *Server) handleObjectStoreStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	if s.objectStats == nil {
		writeHTTPError(w, http.StatusNotFound, "object_store_disabled", "object store disabled")
		return
	}
	stats, ok := s.objectStats()
	if !ok {
		writeHTTPError(w, http.StatusNotFound, "object_store_disabled", "object store disabled")
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

func (s *Server) propose(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	return s.proposeWithLease(ctx, value, nil)
}

func (s *Server) proposeWithLease(ctx context.Context, value []byte, lease *mutationLease) (quepaxa.Slot, error) {
	return s.proposeWithAdmission(ctx, value, lease, 0, 0, nil, nil, false)
}

// These finite bounds are an uncalibrated internal-batch candidate. Direct
// proposals and control operations continue to use the fail-fast path.
const (
	internalBatchAdmissionBudget  = 2 * time.Second
	internalBatchExecutionReserve = 5 * time.Second
)

func (s *Server) proposeInternalBatch(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	return s.proposeBatch(ctx, value, internalBatchAdmissionBudget, internalBatchExecutionReserve)
}

// proposeBatch permits same-package tests to use shorter finite bounds.
func (s *Server) proposeBatch(ctx context.Context, value []byte, admissionBudget, executionReserve time.Duration) (quepaxa.Slot, error) {
	if !localtesthooks.Enabled {
		return s.proposeWithAdmission(ctx, value, nil, admissionBudget, executionReserve, nil, nil, false)
	}
	trace := localtesthooks.NewBatchAdmissionTrace()
	result := &batchAdmissionResult{}
	slot, err := s.proposeWithAdmission(ctx, value, nil, admissionBudget, executionReserve, &trace, result, false)
	reason := result.reason
	if reason == "" {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			reason = localtesthooks.BatchAdmissionContextCanceled
		case errors.Is(err, ErrNotReady):
			reason = localtesthooks.BatchAdmissionServerNotReady
		case result.impossibleBytes:
			reason = localtesthooks.BatchAdmissionByteRejected
		case errors.Is(err, ErrOverloaded):
			reason = localtesthooks.BatchAdmissionWaitBudgetExhausted
		}
	}
	if event, ok := trace.Event(reason, reason == localtesthooks.BatchAdmissionAccepted && err == nil); ok {
		localtesthooks.HitBatchAdmission(event)
	}
	return slot, err
}

type batchAdmissionResult struct {
	reason          string
	impossibleBytes bool
}

func (s *Server) signalAdmissionLocked(invalidate bool) {
	if invalidate {
		s.admissionEpoch++
	}
	if s.admissionWaiters == 0 {
		return
	}
	close(s.admissionChanged)
	s.admissionChanged = make(chan struct{})
}

func (s *Server) proposeWithAdmission(ctx context.Context, value []byte, lease *mutationLease, admissionBudget, executionReserve time.Duration, trace *localtesthooks.BatchAdmissionTrace, result *batchAdmissionResult, retentionControl bool) (quepaxa.Slot, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	hash := sha256.Sum256(value)
	var admissionDeadline time.Time
	if admissionBudget > 0 {
		admissionDeadline = time.Now().Add(admissionBudget)
		if jobDeadline, ok := ctx.Deadline(); ok {
			latest := jobDeadline.Add(-executionReserve)
			if latest.Before(admissionDeadline) {
				admissionDeadline = latest
			}
		}
	}
	s.proposeMu.Lock()
	epoch := s.admissionEpoch
	for {
		if err := ctx.Err(); err != nil {
			s.proposeMu.Unlock()
			return 0, err
		}
		if epoch != s.admissionEpoch {
			s.proposeMu.Unlock()
			return 0, ErrNotReady
		}
		if s.closing || s.quiescing || s.lifecycleFailure != nil {
			s.proposeMu.Unlock()
			return 0, ErrNotReady
		}
		if call := s.inflight[hash]; call != nil {
			if retentionControl && !call.retentionControl {
				s.proposeMu.Unlock()
				return 0, proposalAdmissionOverload{}
			}
			if !s.retainProposalLease(call, lease) {
				s.proposeMu.Unlock()
				return 0, ErrInvalidRequest
			}
			s.proposeMu.Unlock()
			if trace != nil {
				trace.MarkAccepted()
				result.reason = localtesthooks.BatchAdmissionJoinedExisting
			}
			select {
			case <-ctx.Done():
				if retentionControl {
					<-call.done
					return call.slot, call.err
				}
				return 0, fmt.Errorf("%w: %w", ErrCommitUnknown, ctx.Err())
			case <-call.done:
				return call.slot, call.err
			}
		}
		if len(value) > maxInflightEncodedByte || len(value) > maxProposalEncodedByte {
			s.proposeMu.Unlock()
			if result != nil {
				result.impossibleBytes = true
			}
			localtesthooks.Hit("network:proposal-admission:byte-budget-rejected")
			return 0, proposalAdmissionOverload{}
		}
		if admissionBudget > 0 && !time.Now().Before(admissionDeadline) {
			s.proposeMu.Unlock()
			return 0, proposalAdmissionOverload{}
		}
		blocked := ""
		switch {
		case len(value) > maxInflightEncodedByte-s.localB || len(value) > maxProposalEncodedByte-s.operationB:
			blocked = "network:proposal-admission:byte-budget-rejected"
		default:
			select {
			case s.localCap <- struct{}{}:
				select {
				case s.operationCap <- struct{}{}:
				default:
					<-s.localCap
					blocked = "network:proposal-admission:operation-cap-rejected"
				}
			default:
				blocked = "network:proposal-admission:local-cap-rejected"
			}
		}
		if blocked != "" {
			if admissionBudget <= 0 || !time.Now().Before(admissionDeadline) {
				s.proposeMu.Unlock()
				localtesthooks.Hit(blocked)
				return 0, proposalAdmissionOverload{}
			}
			changed := s.admissionChanged
			s.admissionWaiters++
			if trace != nil {
				trace.MarkWait()
			}
			s.proposeMu.Unlock()
			timer := time.NewTimer(time.Until(admissionDeadline))
			select {
			case <-changed:
			case <-ctx.Done():
			case <-s.proposalCtx.Done():
			case <-timer.C:
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.proposeMu.Lock()
			s.admissionWaiters--
			continue
		}
		// A permit is tentative until the caller and admission epoch are still
		// valid. Rollback is under proposeMu, without waking our own waiter.
		if err := ctx.Err(); err != nil {
			<-s.operationCap
			<-s.localCap
			s.proposeMu.Unlock()
			return 0, err
		}
		if epoch != s.admissionEpoch || s.closing || s.quiescing || s.lifecycleFailure != nil {
			<-s.operationCap
			<-s.localCap
			s.proposeMu.Unlock()
			return 0, ErrNotReady
		}
		if admissionBudget > 0 && !time.Now().Before(admissionDeadline) {
			<-s.operationCap
			<-s.localCap
			s.proposeMu.Unlock()
			return 0, proposalAdmissionOverload{}
		}
		call := &proposalCall{done: make(chan struct{}), retentionControl: retentionControl}
		if !s.retainProposalLease(call, lease) {
			<-s.operationCap
			<-s.localCap
			s.proposeMu.Unlock()
			return 0, ErrInvalidRequest
		}
		call.deadline, call.hasDeadline = ctx.Deadline()
		s.inflight[hash] = call
		if trace != nil {
			trace.MarkAccepted()
			result.reason = localtesthooks.BatchAdmissionAccepted
		}
		s.operationB += len(value)
		s.localB += len(value)
		s.proposalWG.Add(1)
		s.logicalWG.Add(1)
		go s.runProposal(hash, call, bytes.Clone(value))
		s.proposeMu.Unlock()
		select {
		case <-ctx.Done():
			if retentionControl {
				<-call.done
				return call.slot, call.err
			}
			return 0, fmt.Errorf("%w: %w", ErrCommitUnknown, ctx.Err())
		case <-call.done:
			return call.slot, call.err
		}
	}
}

// retainProposalLease transfers ownership only after a proposal is accepted
// or joined. The logical proposal, rather than its initiating caller, then
// retains the charged input until the underlying work has really completed.
func (s *Server) retainProposalLease(call *proposalCall, lease *mutationLease) bool {
	if lease != nil {
		if !lease.transfer() {
			return false
		}
		call.leases = append(call.leases, lease)
	}
	return true
}

// Quiesce drains proposals and excludes decision application while a certified
// checkpoint replaces local consensus and materialized state.
func (s *Server) Quiesce(ctx context.Context) (func(), error) {
	s.proposeMu.Lock()
	if s.closing || s.quiescing || s.lifecycleFailure != nil {
		s.proposeMu.Unlock()
		return nil, ErrNotReady
	}
	s.quiescing = true
	s.signalAdmissionLocked(true)
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
				s.signalAdmissionLocked(true)
			}
			s.proposeMu.Unlock()
		}()
		return nil, ctx.Err()
	case <-done:
	}
	s.proposeMu.Lock()
	failed := s.lifecycleFailure != nil
	s.proposeMu.Unlock()
	if failed {
		return nil, ErrNotReady
	}
	s.applyMu.Lock()
	return func() {
		s.applyMu.Unlock()
		s.proposeMu.Lock()
		if !s.closing {
			s.quiescing = false
			s.signalAdmissionLocked(true)
		}
		s.proposeMu.Unlock()
	}, nil
}

func (s *Server) runProposal(hash [32]byte, call *proposalCall, value []byte) {
	ctx, cancel := s.proposalWorkContext(call)
	defer cancel()
	if call.retentionControl {
		localtesthooks.Hit("network:retention-control:accepted")
	}
	var coreStarted, prewriteDenied bool
	call.slot, call.err, coreStarted, prewriteDenied = s.runProposalAttempt(ctx, value)
	if s.localMode && prewriteDenied {
		s.releaseProposalAttempt()
		s.proposeMu.Lock()
		handler := s.localReclaimHandler
		s.proposeMu.Unlock()
		if handler != nil && ctx.Err() == nil {
			maintenanceErr := handler(ctx)
			if maintenanceErr == nil {
				if err := s.beginLocalRetry(ctx); err != nil {
					call.err = err
				} else {
					call.slot, call.err, coreStarted, prewriteDenied = s.runProposalAttempt(ctx, value)
					if coreStarted && call.err != nil && !prewriteDenied {
						s.FailLocalLifecycle(call.err)
					}
					s.releaseProposalAttempt()
				}
			} else if !errors.Is(maintenanceErr, ErrLocalMaintenanceRefused) && !errors.Is(maintenanceErr, ErrNotReady) {
				s.FailLocalLifecycle(maintenanceErr)
				call.err = maintenanceErr
				// Maintenance ran after a Core capacity denial. Any failure other
				// than a clean refusal means the original proposal's outcome must
				// be treated as unknown, even if maintenance itself failed before
				// the retry reached Core.
				coreStarted = true
				prewriteDenied = false
			} else if !errors.Is(maintenanceErr, ErrLocalMaintenanceRefused) {
				call.err = maintenanceErr
			}
		}
	} else {
		if s.localMode && coreStarted && call.err != nil && !prewriteDenied {
			s.FailLocalLifecycle(call.err)
		}
		s.releaseProposalAttempt()
	}
	if s.localMode && coreStarted && call.err != nil && !prewriteDenied && !errors.Is(call.err, ErrLocalMaintenanceRefused) {
		failure := s.localFailure()
		if failure == nil {
			failure = call.err
		}
		call.err = fmt.Errorf("%w: local lifecycle requires reopen: %w", ErrCommitUnknown, failure)
	}
	s.finishProposalLogical(hash, call, len(value))
}

func (s *Server) proposalWorkContext(call *proposalCall) (context.Context, context.CancelFunc) {
	if s.localMode && call.retentionControl {
		return context.WithCancel(s.proposalCtx)
	}
	deadline := time.Now().Add(30 * time.Second)
	if call.hasDeadline && call.deadline.Before(deadline) {
		deadline = call.deadline
	}
	return context.WithDeadline(s.proposalCtx, deadline)
}

func (s *Server) runProposalAttempt(ctx context.Context, value []byte) (quepaxa.Slot, error, bool, bool) {
	if s.localMode {
		if err := s.validateExecutionPolicy(value); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrInvalidRequest, err), false, false
		}
		if err := s.validateReplicatedMutation(value); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrInvalidRequest, err), false, false
		}
	}
	slot, err := s.proposeOnce(ctx, value)
	if err != nil {
		return slot, err, true, s.localMode && quepaxa.IsLocalAdmissionDenial(err)
	}
	if err = s.applyDecisions(ctx, slot); err == nil {
		err = s.waitDurable(ctx, slot)
	}
	return slot, err, true, false
}

func (s *Server) releaseProposalAttempt() {
	s.proposeMu.Lock()
	<-s.operationCap
	<-s.localCap
	s.signalAdmissionLocked(false)
	s.proposeMu.Unlock()
	s.proposalWG.Done()
}

func (s *Server) beginLocalRetry(ctx context.Context) error {
	select {
	case s.localCap <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.proposalCtx.Done():
		return ErrNotReady
	}
	select {
	case s.operationCap <- struct{}{}:
	case <-ctx.Done():
		<-s.localCap
		s.proposeMu.Lock()
		s.signalAdmissionLocked(false)
		s.proposeMu.Unlock()
		return ctx.Err()
	case <-s.proposalCtx.Done():
		<-s.localCap
		s.proposeMu.Lock()
		s.signalAdmissionLocked(false)
		s.proposeMu.Unlock()
		return ErrNotReady
	}
	s.proposeMu.Lock()
	if s.closing || s.quiescing || s.lifecycleFailure != nil {
		s.proposeMu.Unlock()
		<-s.operationCap
		<-s.localCap
		s.proposeMu.Lock()
		s.signalAdmissionLocked(false)
		s.proposeMu.Unlock()
		return ErrNotReady
	}
	s.proposalWG.Add(1)
	s.proposeMu.Unlock()
	return nil
}

func (s *Server) finishProposalLogical(hash [32]byte, call *proposalCall, size int) {
	s.proposeMu.Lock()
	if s.inflight[hash] == call {
		delete(s.inflight, hash)
	}
	s.operationB -= size
	s.localB -= size
	for _, lease := range call.leases {
		lease.releaseWorker()
	}
	call.leases = nil
	close(call.done)
	s.signalAdmissionLocked(false)
	s.proposeMu.Unlock()
	s.logicalWG.Done()
}

func (s *Server) beginLocalLifecycle() error {
	s.proposeMu.Lock()
	defer s.proposeMu.Unlock()
	if s.closing || s.quiescing || s.lifecycleFailure != nil {
		return ErrNotReady
	}
	select {
	case s.localCap <- struct{}{}:
	default:
		return ErrOverloaded
	}
	select {
	case s.operationCap <- struct{}{}:
	default:
		<-s.localCap
		return ErrOverloaded
	}
	s.proposalWG.Add(1)
	return nil
}

func (s *Server) finishLocalLifecycle(err error) {
	s.proposeMu.Lock()
	if err != nil && s.lifecycleFailure == nil {
		s.lifecycleFailure = err
		if s.localFailureHandler != nil {
			s.localFailureHandler(err)
		}
	}
	<-s.operationCap
	<-s.localCap
	s.signalAdmissionLocked(err != nil)
	s.proposeMu.Unlock()
	s.proposalWG.Done()
}
func (s *Server) proposeOnce(ctx context.Context, value []byte) (quepaxa.Slot, error) {
	if err := s.validateExecutionPolicy(value); err != nil {
		return 0, err
	}
	if slot, ok := s.core.DecidedSlot(value); ok {
		if _, err := s.core.CompleteDecision(ctx, slot); err == nil {
			return slot, nil
		} else if !errors.Is(err, quepaxa.ErrCompacted) {
			return slot, err
		}
	}
	slot, _, err := s.core.Propose(ctx, value)
	return slot, err
}

func (s *Server) acceptFrom(ctx context.Context, source quepaxa.NodeID, decision quepaxa.DecidedValue) error {
	if err := s.core.AcceptCertifiedValueForAck(decision); err != nil {
		return err
	}
	if err := s.catchUpFrom(ctx, source, decision.Slot, true); err != nil {
		return err
	}
	if certified, ok := s.core.CertifiedValue(decision.Slot); !ok || certified.Hash != decision.Hash {
		return fmt.Errorf("peer %s returned inconsistent decision slot %d", source, decision.Slot)
	}
	return nil
}

// lockCatchUp waits for the previous source owner, then claims a fresh entry.
// Waiters never consume a global syncLimit token or inherit an owner's error.
func (s *Server) lockCatchUp(ctx context.Context, source quepaxa.NodeID) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.syncMu.Lock()
		done := s.syncSources[source]
		if done == nil {
			if s.syncSources == nil {
				s.syncSources = make(map[quepaxa.NodeID]chan struct{})
			}
			done = make(chan struct{})
			s.syncSources[source] = done
			s.syncMu.Unlock()
			return func() {
				s.syncMu.Lock()
				delete(s.syncSources, source)
				close(done)
				s.syncMu.Unlock()
			}, nil
		}
		s.syncMu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *Server) catchUpFrom(ctx context.Context, source quepaxa.NodeID, through quepaxa.Slot, durable bool) error {
	return s.catchUpFromTip(ctx, source, through, durable, s.core.Tip)
}

// The per-call reader keeps the fetch-range calculation independently testable
// with a real Core advancing between observations; production reads Core.Tip.
func (s *Server) catchUpFromTip(ctx context.Context, source quepaxa.NodeID, through quepaxa.Slot, durable bool, tip func() quepaxa.Slot) error {
	unlock, err := s.lockCatchUp(ctx, source)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !durable && s.core.Tip() >= through {
		return nil
	}
	select {
	case s.syncLimit <- struct{}{}:
		defer func() { <-s.syncLimit }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		current := tip()
		if current >= through {
			break
		}
		from := current + 1
		response, err := fetchDecisionsWithRetry(ctx, s.transport.FetchDecisions, source, from)
		if err != nil {
			return s.handleCatchUpFetchError(err)
		}
		if len(response.Decisions) == 0 || response.Decisions[0].Slot != from {
			return fmt.Errorf("peer %s omitted decision slot %d", source, from)
		}
		accept := s.core.AcceptCertifiedValues
		for _, decision := range response.Decisions {
			if err := s.validateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if !durable {
			accept = s.core.AcceptCertifiedHints
		}
		if err := accept(response.Decisions); err != nil {
			if errors.Is(err, quepaxa.ErrCompacted) {
				s.handleCompacted()
				return ErrNotReady
			}
			return err
		}
	}
	if durable {
		return s.core.EnsureDurableThrough(ctx, through)
	}
	return ctx.Err()
}

func (s *Server) handleCatchUpFetchError(err error) error {
	if errors.Is(err, quepaxa.ErrCompacted) {
		s.handleCompacted()
		return ErrNotReady
	}
	return err
}

func fetchDecisionsWithRetry(
	ctx context.Context,
	fetch func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (DecisionsResponse, error),
	source quepaxa.NodeID,
	from quepaxa.Slot,
) (DecisionsResponse, error) {
	backoff := [...]time.Duration{0, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond}
	var response DecisionsResponse
	var err error
	for attempt, delay := range backoff {
		if delay != 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return DecisionsResponse{}, ctx.Err()
			}
		}
		pageCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		response, err = fetch(pageCtx, source, from, 128)
		cancel()
		if err == nil || errors.Is(err, quepaxa.ErrCompacted) || attempt == len(backoff)-1 {
			break
		}
	}
	return response, err
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Drain calls cannot join themselves; their handler validates identity and
	// invokes the same idempotent terminal operation after other work stops.
	if r.URL.Path != "/recovery/drain" {
		s.handlerMu.Lock()
		if s.handlerClosing {
			s.handlerMu.Unlock()
			writeAPIError(w, ErrNotReady)
			return
		}
		s.handlerWG.Add(1)
		s.handlerMu.Unlock()
		defer s.handlerWG.Done()
	}
	s.mux.ServeHTTP(w, r)
}

// ExecuteRequest is the request body for execute.
type ExecuteRequest struct {
	RequestID string `json:"request_id"`
	SQL       string `json:"sql,omitempty"`
	Args      []any  `json:"args,omitempty"`
	// WantRows requests bounded rows from a replicated mutation.
	WantRows   bool                 `json:"want_rows,omitempty"`
	RequireOne bool                 `json:"require_one,omitempty"`
	Statements []types.SQLStatement `json:"statements,omitempty"`
}

// ExecuteResponse contains the bounded aggregate receipt and requested rows.
type ExecuteResponse struct {
	types.MutationReceipt
	Statements []types.SQLStatementResult `json:"statements,omitempty"`
}

// MigrationRequest is the in-process engine command used by DB.Migrate.
type MigrationRequest struct {
	RequestID  string
	Version    int64
	Name       string
	Checksum   string
	Statements []types.SQLStatement
}

// MigrationAdmission is an opaque, server-bound, single-use reservation used
// by the embedded migration wrapper while it prepares checksums and validation.
type MigrationAdmission struct {
	server *Server
	charge int
	lease  *mutationLease
	state  atomic.Uint32 // 0 available, 1 released, 2 consumed
}

// Release returns a migration reservation that was not consumed. It is safe
// to call after MigrateAdmitted as well.
func (s *Server) proposeLocal(ctx context.Context, value []byte) (quepaxa.DecidedValue, error) {
	if err := s.validateReplicatedMutation(value); err != nil {
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

func (s *Server) validateExecutionPolicy(value []byte) error {
	if managedKVExecutionPolicy(s.expectedPolicy) {
		return s.validateReplicatedMutation(value)
	}
	return types.ValidateExecutionPolicy(value)
}

func (s *Server) validateReplicatedMutation(value []byte) error {
	if s.expectedPolicy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if err := types.ValidateManagedGraphExecutionPolicy(value); err != nil {
			return err
		}
		if _, graph, err := types.DecodeManagedGraphExecution(value); graph || err != nil {
			return err
		}
	}
	if !managedKVExecutionPolicy(s.expectedPolicy) {
		return validateReplicatedMutation(value)
	}
	if err := types.ValidateManagedKVExecutionPolicy(value); err != nil {
		return err
	}
	if commands, ok, err := types.DecodeSQLBatch(value); ok || err != nil {
		if err != nil {
			return err
		}
		for _, command := range commands {
			if command.RequestID == "" || command.Migration != nil {
				return ErrInvalidRequest
			}
			if err := materializer.ValidateSQLCommand(command); err != nil {
				return err
			}
		}
		return nil
	}
	commands, ok, err := types.DecodeManagedKVBatch(value)
	if err != nil {
		return err
	}
	if !ok {
		command, singleton, err := types.DecodeManagedKVCommand(value)
		if err != nil {
			return err
		}
		if !singleton {
			return nil
		} // Already allowlisted control only.
		commands = []types.KVCommand{command}
	}
	for _, command := range commands {
		if command.ObservedAtUnixMS < 0 || command.ExpiresAtUnixMS < 0 {
			return ErrInvalidRequest
		}
		if command.TTLMS == 0 {
			if command.ExpiresAtUnixMS != 0 {
				return ErrInvalidRequest
			}
		} else if command.TTLMS > math.MaxInt64-command.ObservedAtUnixMS || command.ExpiresAtUnixMS != command.ObservedAtUnixMS+command.TTLMS {
			return ErrInvalidRequest
		}
	}
	return nil
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
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	if !s.Ready() {
		writeHTTPError(w, http.StatusServiceUnavailable, "catching_up", "catching up")
		return
	}
	if err := s.material.Health(r.Context()); err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "not_ready", "not ready")
		return
	}
	if err := s.core.Health(); err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "wal_unhealthy", "WAL sync failed")
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
		// HA certified hints need WAL persistence before application. Local
		// owns its durability pipeline and cannot accept external hints.
		if !s.core.LocalMode() && end > 0 {
			if err := s.core.EnsureDurableThrough(ctx, decisions[end-1].Slot); err != nil {
				return err
			}
		}
		if err := s.material.ApplyBatch(ctx, decisions[:end]); err != nil {
			return fmt.Errorf("certified replay apply first_slot=%d last_slot=%d last_hash=%x ctx_canceled=%t: %w", decisions[0].Slot, decisions[end-1].Slot, decisions[end-1].Hash, ctx.Err() != nil, err)
		}
		if end < len(decisions) {
			return nil
		}
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	if err := s.material.Health(r.Context()); err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "materializer_unhealthy", "materializer unhealthy")
		return
	}
	if err := s.core.Health(); err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "wal_unhealthy", "WAL sync failed")
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
