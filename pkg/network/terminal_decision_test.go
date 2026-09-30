//go:build rhiza_local_testhooks

package network

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestTerminalDecisionCancelsUnneededPeerHandshake(t *testing.T) {
	clusterID := types.ClusterID("cluster")
	a := testMember(clusterID, "a", "a-token")
	b, _, bAcked := terminalACKPeerControlled(t, clusterID, "b", "b-token", nil)
	dAckGate := make(chan struct{})
	d, dReceived, dAcked := terminalACKPeerControlled(t, clusterID, "d", "d-token", dAckGate)
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blackhole.Close() })
	blackholeReceived := make(chan struct{})
	go func() {
		buffer := make([]byte, 2048)
		if n, _, err := blackhole.ReadFrom(buffer); err == nil && n > 0 {
			close(blackholeReceived)
		}
	}()
	c := testMember(clusterID, "c", "c-token")
	c.PeerURL = "quic://" + blackhole.LocalAddr().String()
	old := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{a, b, c}}
	next := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{a, b, d}}
	transport := NewTransport(clusterID, "a", &old, "a-token")
	transport.quic = &quic.Config{HandshakeIdleTimeout: 500 * time.Millisecond, MaxIdleTimeout: 500 * time.Millisecond}
	t.Cleanup(func() { _ = transport.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cDialResult := make(chan string, 1)
	restoreHook := localtesthooks.Set(func(event string) {
		if strings.Contains(event, ":peer=c:") && strings.Contains(event, ":phase=result:error=") {
			select {
			case cDialResult <- event:
			default:
			}
		}
	})
	defer restoreHook()
	done := make(chan error, 1)
	go func() { done <- transport.sendTerminalDecision(ctx, quepaxa.Decision{Slot: 1}, old, next) }()
	awaitTerminalSignal(t, blackholeReceived, "blackhole UDP packet")
	awaitTerminalSignal(t, bAcked, "old peer ACK")
	awaitTerminalSignal(t, dReceived, "added peer request")
	select {
	case err := <-done:
		t.Fatalf("terminal decision returned before required added-peer ACK: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(dAckGate)
	awaitTerminalSignal(t, dAcked, "added peer ACK")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("terminal decision after required ACK: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal decision did not return after required ACK; blackholed peer handshake was not canceled")
	}
	select {
	case event := <-cDialResult:
		if !strings.Contains(event, "context canceled") {
			t.Fatalf("blackholed peer result=%s, want observable context cancellation", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blackholed peer dial did not report cancellation after terminal quorum")
	}
}

func terminalACKPeerControlled(t *testing.T, clusterID types.ClusterID, id quepaxa.NodeID, token string, ackGate <-chan struct{}) (quepaxa.Member, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	member := testMember(clusterID, id, token)
	received, acked := make(chan struct{}), make(chan struct{})
	certificate, err := peerCertificate(clusterID, id, token)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{peerALPN},
		MinVersion:   tls.VersionTLS13,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	member.PeerURL = "quic://" + listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.CloseWithError(0, "test complete")
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					if _, err := readPeerFrame(stream); err == nil {
						close(received)
						if ackGate != nil {
							select {
							case <-ctx.Done():
								return
							case <-ackGate:
							}
						}
						if err := writePeerFrame(stream, encodePeerResponse(&peerfb.ResponseT{})); err == nil {
							close(acked)
						}
					}
					_ = stream.Close()
				}
			}()
		}
	}()
	t.Cleanup(func() { cancel(); _ = listener.Close(); wg.Wait() })
	return member, received, acked
}

func awaitTerminalSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
