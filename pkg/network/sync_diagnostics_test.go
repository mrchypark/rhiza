package network

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

type syncGateContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *syncGateContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestSyncDiagnosticSuccessfulConnectionThenReadFailure(t *testing.T) {
	for _, reused := range []bool{false, true} {
		t.Run(fmt.Sprint("reused=", reused), func(t *testing.T) { testSuccessfulConnectionThenReadFailure(t, reused) })
	}
}

func testSuccessfulConnectionThenReadFailure(t *testing.T, reused bool) {
	member := testMember("cluster", "b", "b-token")
	cert, err := peerCertificate("cluster", "b", "b-token")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{peerALPN}, MinVersion: tls.VersionTLS13, ClientAuth: tls.RequestClientCert}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept(ctx)
		if err != nil {
			return
		}
		defer conn.CloseWithError(0, "cleanup")
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		if _, err = readPeerFrame(stream); err != nil {
			return
		}
		stream.CancelWrite(77)
		<-ctx.Done()
	}()
	t.Cleanup(func() { cancel(); _ = listener.Close(); <-done })
	member.PeerURL = "quic://" + listener.Addr().String()
	transport := NewTransport("cluster", "a", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}, "a-token")
	t.Cleanup(func() { _ = transport.Close() })
	if reused {
		conn, err := transport.connection(ctx, "b")
		if err != nil {
			t.Fatal(err)
		}
		transport.release("b", conn)
	}
	gate := transport.peers["b"].gate
	gate <- struct{}{}
	observed := &syncGateContext{Context: ctx, entered: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := transport.callContext(observed, "b", &peerfb.RequestT{Operation: peerfb.OperationSync})
		result <- err
	}()
	select {
	case <-observed.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	<-gate
	select {
	case err = <-result:
		var record interface{ RPCDiagnostic() PeerRPCDiagnostic }
		var cause *quic.StreamError
		if !errors.As(err, &record) || !errors.As(err, &cause) || cause.ErrorCode != 77 {
			t.Fatalf("real response failure lost: %v", err)
		}
		d := record.RPCDiagnostic()
		if d.Phase != "response-read" || d.GateWait <= 0 || !d.ConnectionKnown || !d.GateWaitKnown || !d.MutexWaitKnown || !d.RequestKnown || !d.TotalKnown {
			t.Fatalf("successful connection observations discarded: %+v", d)
		}
		if d.DialMeasured == reused || (reused && (d.DialElapsed != 0 || d.DialDeadlineKnown)) || (!reused && (d.DialElapsed <= 0 || !d.DialDeadlineKnown)) {
			t.Fatalf("reused/no-dial confused with measured zero: %+v", d)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestSyncDiagnosticPreservesTypedRPCPhase(t *testing.T) {
	err := peerRPCPhaseError("response-read", time.Now(), context.DeadlineExceeded)
	var diagnostic interface{ RPCPhase() string }
	if !errors.As(err, &diagnostic) || diagnostic.RPCPhase() != "response-read" {
		t.Fatal("RPC phase is only text; safe diagnostics cannot recover its stage")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("RPC diagnostic lost original deadline cause")
	}
}

func TestSyncDiagnosticCanceledConnectionGate(t *testing.T) {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	transport := &Transport{members: map[quepaxa.NodeID]quepaxa.Member{"peer": {ID: "peer"}}, peers: map[quepaxa.NodeID]*peerConnection{"peer": {gate: gate}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := transport.connection(ctx, "peer")
	var observed interface{ RPCDiagnostic() PeerRPCDiagnostic }
	if !errors.Is(err, context.Canceled) || !errors.As(err, &observed) {
		t.Fatalf("missing canceled gate observation: %v", err)
	}
	d := observed.RPCDiagnostic()
	if d.Phase != "connection-gate" || d.GateWait < 0 || d.DialElapsed != 0 || d.DialDeadlineKnown || d.MutexWait != 0 {
		t.Fatalf("gate wait mislabeled as actual dial: %+v", d)
	}
	if len(gate) != 1 {
		t.Fatal("caller changed an existing gate owner's token")
	}
	<-gate
}

func TestSyncDiagnosticSourceWrapPreservesCause(t *testing.T) {
	source := quepaxa.NodeID("secret\npeer")
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	transport := &Transport{members: map[quepaxa.NodeID]quepaxa.Member{source: {ID: source}}, peers: map[quepaxa.NodeID]*peerConnection{source: {gate: gate}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := transport.FetchDecisions(ctx, source, 3, 128)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), fmt.Sprintf("operation=sync source_hash=%x", sha256.Sum256([]byte(source)))) || strings.Contains(err.Error(), string(source)) {
		t.Fatalf("sync error lost safe source or unwrap: %v", err)
	}
	<-gate
}

func TestSyncDiagnosticMutexWaitIsNotDial(t *testing.T) {
	peer := &peerConnection{gate: make(chan struct{}, 1)}
	peer.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(peer.mu.Unlock) }
	t.Cleanup(unlock)
	transport := &Transport{members: map[quepaxa.NodeID]quepaxa.Member{"peer": {ID: "peer"}}, peers: map[quepaxa.NodeID]*peerConnection{"peer": peer}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := transport.connection(ctx, "peer"); result <- err }()
	limit := time.Now().Add(time.Second)
	for len(peer.gate) == 0 && time.Now().Before(limit) {
		runtime.Gosched()
	}
	if len(peer.gate) != 1 {
		t.Fatal("connection did not enter existing peer gate")
	}
	cancel()
	unlock()
	select {
	case err := <-result:
		var observed interface{ RPCDiagnostic() PeerRPCDiagnostic }
		if !errors.As(err, &observed) {
			t.Fatal("missing connection diagnostic")
		}
		d := observed.RPCDiagnostic()
		if d.Phase != "peer-identity" || d.MutexWait < 0 || d.DialElapsed != 0 || d.DialDeadlineKnown {
			t.Fatalf("mutex/identity failure mislabeled dial: %+v", d)
		}
		if len(peer.gate) != 0 {
			t.Fatal("connection leaked own gate token")
		}
	case <-time.After(time.Second):
		t.Fatal("connection did not return after mutex release")
	}
}
