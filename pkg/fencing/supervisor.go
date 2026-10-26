package fencing

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Authority is implemented by the durable, authenticated Kubernetes service.
// Register and Close use the same CAS record. None of these methods may derive
// termination or storage proof from Pod status or API absence.
type Authority interface {
	Register(context.Context, Identity) error
	Bind(context.Context, Identity) (BindReceipt, error)
	Stop(context.Context, Identity) (StopRequest, error)
	Drain(context.Context, DrainReceipt) error
	Exit(context.Context, StopRequest) error
	NeverBoundExit(context.Context, StopRequest) error
}

// StartupGrantAuthority is an optional role-aware extension. Keeping it
// separate preserves existing static-authority implementations while making
// any learner role an authenticated response rather than a child assertion.
type StartupGrantAuthority interface {
	RegisterStartup(context.Context, Identity) (StartupGrant, error)
}

// InitialStartupGrantAuthority is used only when the local, read-only
// preflight classified this Run as a pristine first registration. The
// authority must independently prove that classification against its
// authenticated workload and canonical root before creating the Run.
type InitialStartupGrantAuthority interface {
	RegisterInitialStartup(context.Context, Identity) (StartupGrant, error)
}

type RegistrationContinuationAuthority interface {
	RegisterStartupContinuation(context.Context, Identity, string) (StartupGrant, error)
}

type LearnerIdentityAuthority interface {
	RegisterLearnerIdentity(context.Context, Identity, LearnerIdentityRegistration) error
}

type LearnerPromotionAuthority interface {
	PromoteLearner(context.Context, Identity) (BindReceipt, error)
}

// EngineRepairRunnerAuthority exposes canonical repair operations only to an
// authenticated active voter session. The record and proof are always loaded
// or derived by the authority; a caller can propose only a digest-CAS revision.
type EngineRepairRunnerAuthority interface {
	CurrentEngineRepair(context.Context, Identity) (EngineRepairRecord, bool, error)
	CompareAndSwapEngineRepair(context.Context, Identity, string, EngineRepairRecord) (EngineRepairRecord, error)
	CloseEngineRepairStorageEffects(context.Context, Identity, string) (StorageEffectsExcluded, error)
	VerifyEngineRemovalStorageEffects(context.Context, Identity, quepaxa.EngineRemoval) error
	ConfirmEngineRepair(context.Context, Identity, string) (EngineRepairRecord, error)
	ListPendingEngineRepairs(context.Context, Identity) ([]PendingEngineRepairRequest, error)
	AdoptPendingEngineRepair(context.Context, Identity, string, string, EngineRepairRecord) (EngineRepairRecord, error)
}

// PendingEngineRepairAuthority records a physical preflight proposal without
// registering a run or issuing a startup grant.
type PendingEngineRepairAuthority interface {
	CreatePendingEngineRepair(context.Context, Identity, PendingEngineRepairProposal) error
}

// StartupPreflightResult is a local, unauthorised classification. Exactly one
// of InitialRegistration, RegistrationWALIdentity or PendingEngineRepair may be set. Initial
// registration is not a repair proposal and does not mutate remote state.
type StartupPreflightResult struct {
	InitialRegistration     bool
	RegistrationWALIdentity string
	PendingEngineRepair     *PendingEngineRepairProposal
}

// EngineRepairPreflight builds local, unauthorised startup evidence before the
// application child starts. Implementations may inspect local WAL identity,
// but must not acquire authority or mutate remote state.
type EngineRepairPreflight func(context.Context, Identity) (StartupPreflightResult, error)

// Supervisor supports only a dedicated, private Linux process namespace in
// which it is PID 1. A crash never produces a receipt; restart must reserve a
// fresh run and therefore cannot erase the unresolved prior run.
type Supervisor struct {
	Authority  Authority
	Identity   Identity
	SocketPath string
	Command    []string
	Preflight  EngineRepairPreflight
	mu         sync.Mutex
	requested  Identity
	bound      Identity
	grant      StartupGrant
	binding    *BindReceipt
	drain      *DrainReceipt
	ticket     string
}

func (s *Supervisor) Run(ctx context.Context) error {
	if os.Getpid() != 1 {
		return fmt.Errorf("fencing: supervisor must be PID 1 in a private process namespace")
	}
	if s.Authority == nil || len(s.Command) == 0 || s.SocketPath == "" {
		return fmt.Errorf("fencing: incomplete supervisor configuration")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	s.Identity.RunID = hex.EncodeToString(nonce[:16])
	s.Identity.WALIdentity = ""
	s.requested = s.Identity
	s.bound = s.Identity
	s.grant = StartupGrant{Identity: s.Identity, Role: StartupRoleVoterActive}
	s.binding = nil
	s.ticket = hex.EncodeToString(nonce[16:])
	preflight := StartupPreflightResult{}
	if s.Preflight != nil {
		result, err := s.Preflight(ctx, s.Identity)
		if err != nil {
			return fmt.Errorf("fencing: engine repair preflight: %w", err)
		}
		if (result.InitialRegistration && result.PendingEngineRepair != nil) || (result.RegistrationWALIdentity != "" && (result.InitialRegistration || result.PendingEngineRepair != nil)) {
			return fmt.Errorf("fencing: startup preflight has conflicting classifications")
		}
		if result.PendingEngineRepair != nil {
			_, ok := s.Authority.(PendingEngineRepairAuthority)
			if !ok {
				return fmt.Errorf("fencing: pending engine repair authority is unavailable")
			}
			proposal := result.PendingEngineRepair
			if proposal.BootstrapIdentity != s.Identity || proposal.SourceClusterID != s.Identity.SourceClusterID {
				return fmt.Errorf("fencing: preflight proposal differs from supervisor identity")
			}
			if err := proposal.ValidatePreflight(); err != nil {
				return err
			}
		}
		preflight = result
	}
	// Must precede listener readiness and exec. The authority can close this
	// reservation concurrently; the child must also pass Connect and BindWAL.
	if authority, ok := s.Authority.(StartupGrantAuthority); ok {
		grant, err := s.registerStartup(ctx, authority, preflight)
		if err != nil {
			return err
		}
		if err := grant.Validate(s.Identity); err != nil {
			return err
		}
		s.grant, s.bound = grant, grant.Identity
	} else {
		if preflight.InitialRegistration || preflight.PendingEngineRepair != nil || preflight.RegistrationWALIdentity != "" {
			return fmt.Errorf("fencing: startup authority cannot enforce preflight classification")
		}
		if err := s.Authority.Register(ctx, s.Identity); err != nil {
			return err
		}
	}
	listener, err := listenSupervisor(s.SocketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(s.SocketPath)
	if err := os.Chmod(s.SocketPath, 0600); err != nil {
		return err
	}
	server := &http.Server{Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 35 * time.Second}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	child := exec.Command(s.Command[0], s.Command[1:]...)
	child.Env = supervisorChildEnvironment(os.Environ(), s.grant, s.SocketPath, s.ticket)
	child.Stdout, child.Stderr, child.Stdin = os.Stdout, os.Stderr, os.Stdin
	if err := child.Start(); err != nil {
		return err
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()
	childWaited := false
	defer func() {
		if !childWaited {
			_ = child.Process.Kill()
			<-waited
		}
	}()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-waited:
			childWaited = true
			completionErr := s.finishExitedChild(ctx, terminatePrivateNamespace)
			if completionErr == nil {
				return nil
			}
			return fmt.Errorf("fencing: child exited without a terminal receipt: %w", errors.Join(err, completionErr))
		case <-ctx.Done():
			_ = child.Process.Kill()
			<-waited
			childWaited = true
			return ctx.Err()
		case <-tick.C:
			s.mu.Lock()
			receipt := s.drain
			s.mu.Unlock()
			if receipt == nil || !receipt.Clean() {
				continue
			}
			return s.finishDrain(ctx, *receipt, func() error {
				if err := child.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					return err
				}
				<-waited
				childWaited = true
				return nil
			}, terminatePrivateNamespace)
		}
	}
}

// supervisorChildEnvironment is the exact scrub-and-apply boundary used for
// the application process. Keeping it pure lets package tests verify that
// stale parent identity/grant values cannot override the authority grant.
func supervisorChildEnvironment(parent []string, grant StartupGrant, socketPath, ticket string) []string {
	env := make([]string, 0, len(parent)+len(grant.Environment)+6)
	for _, entry := range parent {
		if !strings.HasPrefix(entry, "RHIZA_FENCE_SOCKET=") && !strings.HasPrefix(entry, "RHIZA_FENCE_TICKET=") &&
			!strings.HasPrefix(entry, "RHIZA_CLUSTER_ID=") && !strings.HasPrefix(entry, "RHIZA_NODE_ID=") && !strings.HasPrefix(entry, "RHIZA_POD_NAME=") && !strings.HasPrefix(entry, "RHIZA_STARTUP_ROLE=") &&
			!strings.HasPrefix(entry, "RHIZA_LEARNER=") && !strings.HasPrefix(entry, "RHIZA_PEER_TOKEN=") && !strings.HasPrefix(entry, "RHIZA_PEER_TOKENS=") && !strings.HasPrefix(entry, "RHIZA_CLUSTER_MEMBERS=") {
			env = append(env, entry)
		}
	}
	env = append(env, "RHIZA_CLUSTER_ID="+grant.Identity.SourceClusterID, "RHIZA_NODE_ID="+grant.Identity.NodeID,
		"RHIZA_POD_NAME="+grant.Identity.PodName, "RHIZA_STARTUP_ROLE="+string(grant.Role))
	for key, value := range grant.Environment {
		env = append(env, key+"="+value)
	}
	return append(env, "RHIZA_FENCE_SOCKET="+socketPath, "RHIZA_FENCE_TICKET="+ticket)
}

// registerStartup retries only idempotent operations with the exact same
// physical run and proposal. In particular, no child exists while either the
// pending request or its survivor adoption is unresolved.
func (s *Supervisor) registerStartup(ctx context.Context, authority StartupGrantAuthority, preflight StartupPreflightResult) (StartupGrant, error) {
	if s == nil || ctx == nil || authority == nil {
		return StartupGrant{}, fmt.Errorf("fencing: startup registration is incomplete")
	}
	if (preflight.InitialRegistration && preflight.PendingEngineRepair != nil) || (preflight.RegistrationWALIdentity != "" && (preflight.InitialRegistration || preflight.PendingEngineRepair != nil)) {
		return StartupGrant{}, fmt.Errorf("fencing: startup preflight has conflicting classifications")
	}
	if preflight.RegistrationWALIdentity != "" {
		continuation, ok := s.Authority.(RegistrationContinuationAuthority)
		if !ok {
			return StartupGrant{}, fmt.Errorf("fencing: registration continuation authority is unavailable")
		}
		grant, err := continuation.RegisterStartupContinuation(ctx, s.Identity, preflight.RegistrationWALIdentity)
		if err != nil {
			return StartupGrant{}, err
		}
		if err := grant.Validate(s.Identity); err != nil {
			return StartupGrant{}, err
		}
		if grant.Role != StartupRoleVoterActive || grant.InitialRegistration != nil {
			return StartupGrant{}, fmt.Errorf("fencing: invalid registration continuation grant")
		}
		return grant, nil
	}
	if preflight.InitialRegistration {
		initial, ok := s.Authority.(InitialStartupGrantAuthority)
		if !ok {
			return StartupGrant{}, fmt.Errorf("fencing: initial startup authority is unavailable")
		}
		grant, err := initial.RegisterInitialStartup(ctx, s.Identity)
		if err != nil {
			return StartupGrant{}, err
		}
		if err := grant.Validate(s.Identity); err != nil {
			return StartupGrant{}, err
		}
		if grant.InitialRegistration == nil {
			return StartupGrant{}, fmt.Errorf("fencing: initial startup grant lacks first-registration evidence")
		}
		return grant, nil
	}
	proposal := preflight.PendingEngineRepair
	if proposal != nil {
		pendingAuthority, ok := s.Authority.(PendingEngineRepairAuthority)
		if !ok {
			return StartupGrant{}, fmt.Errorf("fencing: pending engine repair authority is unavailable")
		}
		for {
			err := pendingAuthority.CreatePendingEngineRepair(ctx, s.Identity, *proposal)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrUnavailable) {
				return StartupGrant{}, err
			}
			if err := waitStartupRegistration(ctx); err != nil {
				return StartupGrant{}, err
			}
		}
	}
	for {
		grant, err := authority.RegisterStartup(ctx, s.Identity)
		if err == nil || proposal == nil || (!errors.Is(err, ErrPending) && !errors.Is(err, ErrUnavailable)) {
			return grant, err
		}
		if err := waitStartupRegistration(ctx); err != nil {
			return StartupGrant{}, err
		}
	}
}

func waitStartupRegistration(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// finishExitedChild is called only after the direct child's Wait completed.
// An accepted clean receipt remains valid if the application exits before the
// polling tick. There is no second Kill/Wait in this branch.
func (s *Supervisor) finishExitedChild(ctx context.Context, namespace func(context.Context) error) error {
	s.mu.Lock()
	receipt := s.drain
	s.mu.Unlock()
	if receipt != nil {
		return s.finishDrain(ctx, *receipt, func() error { return nil }, namespace)
	}
	return s.recordNeverBoundExit(ctx, namespace)
}

func retryAuthorityTransition(ctx context.Context, transition func() error) error {
	for {
		err := transition()
		if !errors.Is(err, ErrUnavailable) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Both natural exit and supervisor-initiated termination use this exact
// evidence order. Callbacks permit deterministic ordering tests without ever
// inspecting or killing the test runner's process namespace.
func (s *Supervisor) finishDrain(ctx context.Context, receipt DrainReceipt, waitChild func() error, namespace func(context.Context) error) error {
	s.mu.Lock()
	identity := s.bound
	s.mu.Unlock()
	if !receipt.Clean() || receipt.Request.Identity != identity || identity.WALIdentity == "" || receipt.Request.OperationID == "" || receipt.Request.RequestHash == "" {
		return fmt.Errorf("fencing: clean bound drain required")
	}
	var request StopRequest
	if err := retryAuthorityTransition(ctx, func() error { var err error; request, err = s.Authority.Stop(ctx, identity); return err }); err != nil {
		return err
	}
	if request != receipt.Request {
		return fmt.Errorf("fencing: accepted drain differs from immutable stop")
	}
	if err := retryAuthorityTransition(ctx, func() error { return s.Authority.Drain(ctx, receipt) }); err != nil {
		return err
	}
	if err := waitChild(); err != nil {
		return err
	}
	if err := namespace(ctx); err != nil {
		return err
	}
	return retryAuthorityTransition(ctx, func() error { return s.Authority.Exit(ctx, receipt.Request) })
}

// A retained emptyDir can contain a stale socket after a container restart.
// This check never proves old processes safe: their ledger runs remain.
func listenSupervisor(path string) (net.Listener, error) {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("fencing: socket path contains a non-socket")
		}
		connection, dialErr := net.DialTimeout("unix", path, time.Second)
		if dialErr == nil {
			connection.Close()
			return nil, fmt.Errorf("fencing: existing supervisor listener is active")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !os.IsNotExist(dialErr) {
			return nil, fmt.Errorf("fencing: prior socket state is uncertain: %w", dialErr)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return net.Listen("unix", path)
}

func (s *Supervisor) recordNeverBoundExit(ctx context.Context, namespace func(context.Context) error) error {
	s.mu.Lock()
	identity := s.bound
	s.mu.Unlock()
	if identity.WALIdentity != "" {
		return ErrPending
	}
	var request StopRequest
	for {
		var err error
		request, err = s.Authority.Stop(ctx, identity)
		if !errors.Is(err, ErrUnavailable) {
			if err != nil {
				return err
			}
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if request.Identity != identity || request.Identity.WALIdentity != "" || request.OperationID == "" || request.RequestHash == "" {
		return fmt.Errorf("fencing: unbound closure identity differs")
	}
	if err := namespace(ctx); err != nil {
		return err
	}
	for {
		err := s.Authority.NeverBoundExit(ctx, request)
		if !errors.Is(err, ErrUnavailable) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *Supervisor) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.ticket)) != 1 {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		}
		if sourceRelayPath(r.URL.Path) {
			s.sourceRelay(w, r)
			return
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
		decoder.DisallowUnknownFields()
		var err error
		var result any
		if r.URL.Path == "/v1/drained" {
			var receipt DrainReceipt
			err = decoder.Decode(&receipt)
			if err == nil {
				s.mu.Lock()
				bound := s.bound
				s.mu.Unlock()
				var request StopRequest
				request, err = s.Authority.Stop(r.Context(), bound)
				if err == nil && request != receipt.Request {
					err = fmt.Errorf("fencing: unmatched drain request")
				}
				if err == nil {
					s.mu.Lock()
					if s.drain == nil {
						s.drain = &receipt
					} else if *s.drain != receipt {
						err = fmt.Errorf("fencing: drain changed")
					}
					s.mu.Unlock()
				}
			}
		} else if r.URL.Path == "/v1/learner-identity" {
			var request struct {
				Identity Identity                    `json:"identity"`
				Object   LearnerIdentityRegistration `json:"object"`
			}
			err = decoder.Decode(&request)
			if err == nil {
				err = decoder.Decode(new(any))
				if err == io.EOF {
					err = s.registerLearnerIdentity(r.Context(), request.Identity, request.Object)
				} else if err == nil {
					err = fmt.Errorf("fencing: trailing learner identity request")
				}
			}
		} else if strings.HasPrefix(r.URL.Path, "/v1/engine-repair/") {
			var request EngineRepairRequest
			err = decoder.Decode(&request)
			if err == nil {
				err = decoder.Decode(new(any))
				if err == io.EOF {
					result, err = s.engineRepairRequest(r.Context(), r.URL.Path, request)
				} else if err == nil {
					err = fmt.Errorf("fencing: trailing engine repair request")
				}
			}
		} else {
			var identity Identity
			err = decoder.Decode(&identity)
			if err == nil {
				result, err = s.identityRequest(r.Context(), r.URL.Path, identity)
			}
		}
		if err == ErrPending {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if err == ErrClosed {
			w.WriteHeader(http.StatusGone)
			return
		}
		if errors.Is(err, ErrUnavailable) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
			return
		}
		if err != nil {
			http.Error(w, "fencing request rejected", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if result == nil {
			result = struct{}{}
		}
		_ = json.NewEncoder(w).Encode(result)
	})
}

func (s *Supervisor) engineRepairRequest(ctx context.Context, path string, request EngineRepairRequest) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Identity != s.bound || s.grant.Role != StartupRoleVoterActive || request.Identity.WALIdentity == "" {
		return nil, fmt.Errorf("fencing: engine repair requires an active bound voter")
	}
	if path != "/v1/engine-repair/storage-effects-verify" && request.RemovalOperation != nil {
		return nil, fmt.Errorf("fencing: unexpected engine removal operation payload")
	}
	authority, ok := s.Authority.(EngineRepairRunnerAuthority)
	if !ok {
		return nil, ErrUnavailable
	}
	switch path {
	case "/v1/engine-repair/current":
		if request.OperationID != "" || request.ExpectedDigest != "" || request.Record != nil {
			return nil, fmt.Errorf("fencing: malformed current engine repair request")
		}
		record, found, err := authority.CurrentEngineRepair(ctx, request.Identity)
		if err != nil {
			return nil, err
		}
		response := EngineRepairResponse{Found: found}
		if found {
			if err := record.Validate(); err != nil || record.Binding.SourceClusterID != request.Identity.SourceClusterID {
				return nil, fmt.Errorf("fencing: invalid canonical engine repair response")
			}
			response.Record = &record
		}
		return response, nil
	case "/v1/engine-repair/cas":
		if request.OperationID == "" || !mutationDigestValid(request.ExpectedDigest) || request.Record == nil ||
			request.Record.Binding.OperationID != request.OperationID || request.Record.Binding.SourceClusterID != request.Identity.SourceClusterID {
			return nil, fmt.Errorf("fencing: malformed engine repair CAS request")
		}
		record, err := authority.CompareAndSwapEngineRepair(ctx, request.Identity, request.ExpectedDigest, *request.Record)
		if err != nil {
			return nil, err
		}
		return EngineRepairResponse{Found: true, Record: &record}, nil
	case "/v1/engine-repair/storage-effects":
		if request.OperationID == "" || request.ExpectedDigest != "" || request.Record != nil {
			return nil, fmt.Errorf("fencing: malformed engine repair storage-closure request")
		}
		proof, err := authority.CloseEngineRepairStorageEffects(ctx, request.Identity, request.OperationID)
		if err != nil {
			return nil, err
		}
		if proof.Binding.SourceClusterID != request.Identity.SourceClusterID || proof.Binding.OperationID != request.OperationID {
			return nil, fmt.Errorf("fencing: engine repair storage-closure response differs")
		}
		return EngineRepairResponse{Proof: &proof}, nil
	case "/v1/engine-repair/storage-effects-verify":
		if request.OperationID == "" || request.ExpectedDigest != "" || request.Record != nil || request.PendingID != "" || request.PendingDigest != "" ||
			request.RemovalOperation == nil || request.RemovalOperation.OperationID != request.OperationID || request.RemovalOperation.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) {
			return nil, fmt.Errorf("fencing: malformed engine removal storage-effects verification request")
		}
		if err := authority.VerifyEngineRemovalStorageEffects(ctx, request.Identity, *request.RemovalOperation); err != nil {
			return nil, err
		}
		return EngineRepairResponse{}, nil
	case "/v1/engine-repair/confirm":
		if request.OperationID == "" || request.ExpectedDigest != "" || request.Record != nil {
			return nil, fmt.Errorf("fencing: malformed engine repair confirmation request")
		}
		record, err := authority.ConfirmEngineRepair(ctx, request.Identity, request.OperationID)
		if err != nil {
			return nil, err
		}
		if err := record.Validate(); err != nil || record.Binding.OperationID != request.OperationID || record.Binding.SourceClusterID != request.Identity.SourceClusterID || record.Phase != RepairActive {
			return nil, fmt.Errorf("fencing: invalid confirmed engine repair record")
		}
		return EngineRepairResponse{Found: true, Record: &record}, nil
	case "/v1/engine-repair/pending":
		if request.OperationID != "" || request.ExpectedDigest != "" || request.Record != nil || request.PendingID != "" || request.PendingDigest != "" {
			return nil, fmt.Errorf("fencing: malformed pending engine repair query")
		}
		requests, err := authority.ListPendingEngineRepairs(ctx, request.Identity)
		if err != nil {
			return nil, err
		}
		for _, pending := range requests {
			if pending.Validate() != nil || pending.SourceClusterID != request.Identity.SourceClusterID {
				return nil, fmt.Errorf("fencing: invalid pending engine repair response")
			}
		}
		return EngineRepairResponse{PendingRequests: requests}, nil
	case "/v1/engine-repair/adopt":
		if request.PendingID == "" || !mutationDigestValid(request.PendingDigest) || request.OperationID != "" || request.ExpectedDigest != "" || request.Record == nil {
			return nil, fmt.Errorf("fencing: malformed pending engine repair adoption")
		}
		record, err := authority.AdoptPendingEngineRepair(ctx, request.Identity, request.PendingID, request.PendingDigest, *request.Record)
		if err != nil {
			return nil, err
		}
		if record.Validate() != nil || record.Binding.SourceClusterID != request.Identity.SourceClusterID || record.BootstrapRequestDigest != request.PendingDigest {
			return nil, fmt.Errorf("fencing: invalid adopted engine repair response")
		}
		return EngineRepairResponse{Found: true, Record: &record}, nil
	default:
		return nil, fmt.Errorf("fencing: unknown engine repair operation")
	}
}

func (s *Supervisor) registerLearnerIdentity(ctx context.Context, identity Identity, request LearnerIdentityRegistration) error {
	s.mu.Lock()
	bound, role := s.bound, s.grant.Role
	s.mu.Unlock()
	if role != StartupRoleLearnerPendingRemove || identity != bound || len(request.Data) == 0 || len(request.Data) > 64<<10 {
		return fmt.Errorf("fencing: learner identity registration is unavailable")
	}
	authority, ok := s.Authority.(LearnerIdentityAuthority)
	if !ok {
		return fmt.Errorf("fencing: learner identity authority is unavailable")
	}
	return authority.RegisterLearnerIdentity(ctx, identity, request)
}

func (s *Supervisor) identityRequest(ctx context.Context, path string, identity Identity) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch path {
	case "/v1/promote-learner":
		if identity != s.bound || s.grant.Role != StartupRoleLearnerPendingRemove || identity.WALIdentity == "" {
			return nil, fmt.Errorf("fencing: learner promotion identity differs")
		}
		authority, ok := s.Authority.(LearnerPromotionAuthority)
		if !ok {
			return nil, ErrUnavailable
		}
		receipt, err := authority.PromoteLearner(ctx, identity)
		if err != nil {
			return nil, err
		}
		if err := receipt.Validate(identity); err != nil || receipt.Role != StartupRoleVoterActive {
			return nil, fmt.Errorf("fencing: invalid learner promotion receipt")
		}
		copy := receipt.clone()
		s.binding = &copy
		s.grant.Role = StartupRoleVoterActive
		return receipt, nil
	case "/v1/startup":
		requested := s.requested
		if requested.NodeID == "" {
			requested = s.Identity
		}
		if identity.SourceClusterID != requested.SourceClusterID || identity.NodeID != requested.NodeID || identity.PodName != requested.PodName || identity.PodUID != "" || identity.RunID != "" || identity.WALIdentity != "" {
			return nil, fmt.Errorf("fencing: startup identity mismatch")
		}
		if _, err := s.Authority.Stop(ctx, s.bound); err != ErrPending {
			if err == nil {
				return nil, ErrClosed
			}
			return nil, err
		}
		return s.grant, nil
	case "/v1/bind":
		if identity.WALIdentity == "" {
			return nil, fmt.Errorf("fencing: empty WAL")
		}
		before := identity
		before.WALIdentity = s.bound.WALIdentity
		if before != s.bound || (s.bound.WALIdentity != "" && s.bound.WALIdentity != identity.WALIdentity) {
			return nil, fmt.Errorf("fencing: binding differs")
		}
		receipt, err := s.Authority.Bind(ctx, identity)
		if err != nil {
			return nil, err
		}
		if err := receipt.Validate(identity); err != nil {
			return nil, err
		}
		if s.binding != nil && !s.binding.SameBinding(receipt) {
			return nil, fmt.Errorf("fencing: supervisor producer binding changed")
		}
		copy := receipt.clone()
		s.binding = &copy
		s.bound = identity
		return receipt, nil
	case "/v1/stop":
		if identity != s.bound {
			return nil, fmt.Errorf("fencing: incarnation differs")
		}
		return s.Authority.Stop(ctx, identity)
	default:
		return nil, fmt.Errorf("fencing: unknown endpoint")
	}
}
