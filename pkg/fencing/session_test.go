package fencing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

type sessionAuthority struct {
	mu   sync.Mutex
	id   Identity
	stop *StopRequest
}

type learnerIdentityTestAuthority struct {
	sessionAuthority
	registered *LearnerIdentityRegistration
}

func (a *learnerIdentityTestAuthority) RegisterLearnerIdentity(_ context.Context, _ Identity, registration LearnerIdentityRegistration) error {
	copy := registration
	copy.Data = append([]byte(nil), registration.Data...)
	a.registered = &copy
	return nil
}

func TestPendingLearnerIdentityRegistrationIsSingleScopedAuthorityCall(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = "wal"
	authority := &learnerIdentityTestAuthority{}
	s := &Supervisor{Authority: authority, bound: id, grant: StartupGrant{Identity: id, Role: StartupRoleLearnerPendingRemove}}
	request := LearnerIdentityRegistration{Key: "source/voters/identity.json", Data: []byte(`{"registered":true}`)}
	if err := s.registerLearnerIdentity(t.Context(), id, request); err != nil {
		t.Fatal(err)
	}
	if authority.registered == nil || authority.registered.Key != request.Key || string(authority.registered.Data) != string(request.Data) {
		t.Fatal("identity request did not reach the authority unchanged")
	}
	active := &Supervisor{Authority: authority, bound: id, grant: StartupGrant{Identity: id, Role: StartupRoleVoterActive}}
	if err := active.registerLearnerIdentity(t.Context(), id, request); err == nil {
		t.Fatal("active voter used pending-learner identity path")
	}
}

func TestSessionTransient503AndConnectionFailureRemainDistinctFromIdentity(t *testing.T) {
	var unavailable atomic.Bool
	id := testIdentity()
	socket := filepath.Join(t.TempDir(), "session.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/v1/startup" {
			_ = json.NewEncoder(w).Encode(StartupGrant{Identity: id, Role: StartupRoleVoterActive})
			return
		}
		_ = json.NewEncoder(w).Encode(id)
	})}
	defer server.Close()
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	session, err := Connect(t.Context(), socket, "ticket", id.SourceClusterID, id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	unavailable.Store(true)
	if _, err := session.WaitStop(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("503 lost transient classification: %v", err)
	}
	server.Close()
	if _, err := session.WaitStop(t.Context()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("connection failure lost transient classification: %v", err)
	}
	if session.Identity() != id {
		t.Fatal("transient outage changed identity")
	}
}


type sessionRoundTrip func(*http.Request) (*http.Response, error)

func (f sessionRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type interruptedSessionBody struct{}

func (interruptedSessionBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (interruptedSessionBody) Close() error             { return nil }

func TestSessionInterruptedSuccessfulBodyIsTransient(t *testing.T) {
	s := &Session{ticket: "ticket", identity: testIdentity(), client: &http.Client{Transport: sessionRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: interruptedSessionBody{}}, nil
	})}}
	if _, err := s.WaitStop(t.Context()); !errors.Is(err, ErrUnavailable) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("body interruption lost cause: %v", err)
	}
}

func TestSupervisorSocketRestartKeepsLiveListenerAndOtherFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	live, err := listenSupervisor(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := listenSupervisor(path); err == nil {
		other.Close()
		t.Fatal("replaced a live supervisor")
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("owned unrelated file"), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := listenSupervisor(path); err == nil {
		other.Close()
		t.Fatal("removed a non-socket file")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "owned unrelated file" {
		t.Fatal("non-socket file changed")
	}
}

func (a *sessionAuthority) Register(context.Context, Identity) error { return nil }
func (a *sessionAuthority) Bind(_ context.Context, id Identity) (BindReceipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.id = id
	return BindReceipt{Identity: id}, nil
}
func (a *sessionAuthority) Stop(_ context.Context, id Identity) (StopRequest, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if id != a.id {
		return StopRequest{}, ErrClosed
	}
	if a.stop == nil {
		return StopRequest{}, ErrPending
	}
	return *a.stop, nil
}
func (a *sessionAuthority) Drain(context.Context, DrainReceipt) error         { return nil }
func (a *sessionAuthority) Exit(context.Context, StopRequest) error           { return nil }
func (a *sessionAuthority) NeverBoundExit(context.Context, StopRequest) error { return nil }

func TestSupervisorSessionBindsExactRunAndStop(t *testing.T) {
	id := testIdentity()
	id.PodName = id.NodeID
	authority := &sessionAuthority{id: id}
	s := &Supervisor{Authority: authority, Identity: id, bound: id, grant: StartupGrant{Identity: id, Role: StartupRoleVoterActive}, ticket: "private-ticket"}
	socket := filepath.Join(t.TempDir(), "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: s.handler()}
	defer server.Close()
	defer listener.Close()
	go func() { _ = server.Serve(listener) }()
	if _, err := Connect(t.Context(), socket, "wrong-ticket", id.SourceClusterID, id.NodeID); err == nil {
		t.Fatal("wrong ticket accepted")
	}
	session, err := Connect(t.Context(), socket, "private-ticket", id.SourceClusterID, id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.BindWAL(t.Context(), "wal"); err != nil {
		t.Fatal(err)
	}
	if _, enabled := session.MutationBinding(); enabled {
		t.Fatal("cooperative-only receipt enabled mutation client")
	}
	if err := session.BindWAL(t.Context(), "other"); err == nil {
		t.Fatal("changed WAL accepted")
	}
	if _, err := session.WaitStop(t.Context()); err != ErrPending {
		t.Fatalf("unrequested stop: %v", err)
	}
	authority.mu.Lock()
	request := StopRequest{OperationID: "op", RequestHash: "hash", Identity: authority.id}
	authority.stop = &request
	authority.mu.Unlock()
	got, err := session.WaitStop(t.Context())
	if err != nil || got != request {
		t.Fatalf("stop=%+v err=%v", got, err)
	}
	wrong := request
	wrong.Identity.RunID = "other"
	if err := session.AcknowledgeDrain(t.Context(), DrainReceipt{Request: wrong, WritersJoined: true}); err == nil {
		t.Fatal("other run accepted")
	}
	if err := session.AcknowledgeDrain(t.Context(), DrainReceipt{Request: request, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	receipt := s.drain
	s.mu.Unlock()
	if receipt == nil || receipt.Request != request {
		t.Fatal("matching drain not delivered")
	}
}
