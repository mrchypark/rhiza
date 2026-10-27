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
)

// Authority is implemented by the durable, authenticated Kubernetes service.
// Register and Close use the same CAS record. None of these methods may derive
// termination or storage proof from Pod status or API absence.
type Authority interface {
	Register(context.Context, Identity) error
	Bind(context.Context, Identity) error
	Stop(context.Context, Identity) (StopRequest, error)
	Drain(context.Context, DrainReceipt) error
	Exit(context.Context, StopRequest) error
	NeverBoundExit(context.Context, StopRequest) error
}

// Supervisor supports only a dedicated, private Linux process namespace in
// which it is PID 1. A crash never produces a receipt; restart must reserve a
// fresh run and therefore cannot erase the unresolved prior run.
type Supervisor struct {
	Authority  Authority
	Identity   Identity
	SocketPath string
	Command    []string
	mu         sync.Mutex
	bound      Identity
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
	s.bound = s.Identity
	s.ticket = hex.EncodeToString(nonce[16:])
	// Must precede listener readiness and exec. The authority can close this
	// reservation concurrently; the child must also pass Connect and BindWAL.
	if err := s.Authority.Register(ctx, s.Identity); err != nil {
		return err
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
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "RHIZA_FENCE_SOCKET=") && !strings.HasPrefix(entry, "RHIZA_FENCE_TICKET=") {
			child.Env = append(child.Env, entry)
		}
	}
	child.Env = append(child.Env, "RHIZA_FENCE_SOCKET="+s.SocketPath, "RHIZA_FENCE_TICKET="+s.ticket)
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
		} else if r.URL.Path == "/v1/peer-closure/request" || r.URL.Path == "/v1/peer-closure/status" {
			var message AuthorityMessage
			err = decoder.Decode(&message)
			if err == nil && decoder.Decode(new(any)) != io.EOF {
				err = fmt.Errorf("fencing: trailing peer-closure request")
			}
			if err == nil {
				s.mu.Lock()
				bound := s.bound
				s.mu.Unlock()
				if message.Identity != bound || message.PeerClosure == nil || message.Drain != nil || message.Request != nil {
					err = fmt.Errorf("fencing: peer-closure caller differs from bound incarnation")
				} else if validateErr := message.PeerClosure.Validate(bound); validateErr != nil {
					err = validateErr
				} else if authority, ok := s.Authority.(PeerClosureAuthority); !ok {
					err = fmt.Errorf("fencing: peer-closure authority unavailable")
				} else if r.URL.Path == "/v1/peer-closure/request" && message.Action == "peer-closure-request" {
					result, err = authority.RequestPeerClosure(r.Context(), bound, *message.PeerClosure)
				} else if r.URL.Path == "/v1/peer-closure/status" && message.Action == "peer-closure-status" {
					result, err = authority.ReadPeerClosure(r.Context(), bound, *message.PeerClosure)
				} else {
					err = fmt.Errorf("fencing: peer-closure action differs")
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
		if err == ErrNotFound {
			http.Error(w, "peer-closure request not found", http.StatusNotFound)
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

func (s *Supervisor) identityRequest(ctx context.Context, path string, identity Identity) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch path {
	case "/v1/startup":
		if identity.SourceClusterID != s.bound.SourceClusterID || identity.NodeID != s.bound.NodeID {
			return nil, fmt.Errorf("fencing: startup identity mismatch")
		}
		if _, err := s.Authority.Stop(ctx, s.bound); err != ErrPending {
			if err == nil {
				return nil, ErrClosed
			}
			return nil, err
		}
		return s.bound, nil
	case "/v1/bind":
		if identity.WALIdentity == "" {
			return nil, fmt.Errorf("fencing: empty WAL")
		}
		before := identity
		before.WALIdentity = s.bound.WALIdentity
		if before != s.bound || (s.bound.WALIdentity != "" && s.bound.WALIdentity != identity.WALIdentity) {
			return nil, fmt.Errorf("fencing: binding differs")
		}
		if err := s.Authority.Bind(ctx, identity); err != nil {
			return nil, err
		}
		s.bound = identity
		return nil, nil
	case "/v1/stop":
		if identity != s.bound {
			return nil, fmt.Errorf("fencing: incarnation differs")
		}
		return s.Authority.Stop(ctx, identity)
	default:
		return nil, fmt.Errorf("fencing: unknown endpoint")
	}
}
