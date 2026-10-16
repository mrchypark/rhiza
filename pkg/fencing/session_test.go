package fencing

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
)

type sessionAuthority struct {
	mu   sync.Mutex
	id   Identity
	stop *StopRequest
}

func (a *sessionAuthority) Register(context.Context, Identity) error { return nil }
func (a *sessionAuthority) Bind(_ context.Context, id Identity) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.id = id
	return nil
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
func (a *sessionAuthority) Drain(context.Context, DrainReceipt) error { return nil }
func (a *sessionAuthority) Exit(context.Context, StopRequest) error   { return nil }

func TestSupervisorSessionBindsExactRunAndStop(t *testing.T) {
	id := testIdentity()
	authority := &sessionAuthority{id: id}
	s := &Supervisor{Authority: authority, Identity: id, bound: id, ticket: "private-ticket"}
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
