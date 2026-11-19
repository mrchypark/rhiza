//go:build rhiza_local_testhooks

package network

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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
	b, _, _ := terminalACKPeerControlled(t, clusterID, "b", "b-token", nil)
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
	bQuorumsConsumed := make(chan struct{})
	cDialResult := make(chan string, 1)
	restoreHook := localtesthooks.Set(func(event string) {
		if strings.Contains(event, ":peer=b:phase=consumed:") && strings.Contains(event, "old-acks=2/2:next-acks=2/2") {
			select {
			case <-bQuorumsConsumed:
			default:
				close(bQuorumsConsumed)
			}
		}
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
	awaitTerminalSignal(t, dReceived, "added peer request")
	awaitTerminalSignal(t, bQuorumsConsumed, "old and next quorum consumption")
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
	t.Run("drains-before-close", testTerminalDecisionDrainsBeforeClose)
}

func TestFreezeDecisionReturnsOnOldQuorumAfterRecorderLoss(t *testing.T) {
	clusterID := types.ClusterID("freeze-quorum-loss")
	a := testMember(clusterID, "a", "a-token")
	b, _, bAcked := terminalACKPeerControlled(t, clusterID, "b", "b-token", nil)
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blackhole.Close() })
	c := testMember(clusterID, "c", "c-token")
	c.PeerURL = "quic://" + blackhole.LocalAddr().String()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{a, b, c}}
	transport := NewTransport(clusterID, "a", &cluster, "a-token")
	transport.quic = &quic.Config{HandshakeIdleTimeout: 500 * time.Millisecond, MaxIdleTimeout: 500 * time.Millisecond}
	t.Cleanup(func() { _ = transport.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	decision := quepaxa.Decision{Slot: 1, Summaries: []quepaxa.Summary{
		{RecorderID: "a", Step: 4},
		{RecorderID: "c", Step: 4},
	}}
	go func() { done <- transport.sendFreezeDecision(ctx, decision) }()
	awaitTerminalSignal(t, bAcked, "surviving voter ACK")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("freeze decision with one recorder lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("freeze decision did not complete on the surviving old-voter quorum")
	}
}

func testTerminalDecisionDrainsBeforeClose(t *testing.T) {
	clusterID := types.ClusterID("cluster-drain")
	a := testMember(clusterID, "a", "a-token")
	b, _, bAcked := terminalACKPeerControlled(t, clusterID, "b", "b-token", nil)
	c, cReceived, _ := terminalACKPeerControlled(t, clusterID, "c", "c-token", make(chan struct{}))
	dAckGate := make(chan struct{})
	d, dReceived, dAcked := terminalACKPeerControlled(t, clusterID, "d", "d-token", dAckGate)
	old := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{a, b, c}}
	next := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{a, b, d}}
	transport := NewTransport(clusterID, "a", &old, "a-token")
	transport.quic = &quic.Config{HandshakeIdleTimeout: 500 * time.Millisecond, MaxIdleTimeout: 500 * time.Millisecond}
	t.Cleanup(func() { _ = transport.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	releaseEntered := make(chan struct{}, 1)
	result := make(chan string, 1)
	release := make(chan struct{})
	var released atomic.Bool
	var closeBeforeRelease atomic.Bool
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	restoreHook := localtesthooks.Set(func(event string) {
		switch {
		case strings.Contains(event, "network:peer-call:peer=c:phase=before-release"):
			select {
			case releaseEntered <- struct{}{}:
			default:
			}
			<-release
		case strings.Contains(event, "network:peer-call:peer=c:phase=after-release"):
			released.Store(true)
		case strings.Contains(event, "network:terminal-decision:slot=1:phase=close-start"):
			if !released.Load() {
				closeBeforeRelease.Store(true)
			}
		case strings.Contains(event, ":peer=c:") && strings.Contains(event, ":phase=result:error="):
			select {
			case result <- event:
			default:
			}
		}
	})
	defer restoreHook()
	done := make(chan error, 1)
	go func() { done <- transport.sendTerminalDecision(ctx, quepaxa.Decision{Slot: 1}, old, next) }()
	awaitTerminalSignal(t, cReceived, "connected unneeded peer request")
	awaitTerminalSignal(t, bAcked, "old peer ACK")
	awaitTerminalSignal(t, dReceived, "added peer request")
	close(dAckGate)
	awaitTerminalSignal(t, dAcked, "added peer ACK")
	awaitTerminalSignal(t, releaseEntered, "canceled peer before release")
	<-time.After(time.Second)
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("terminal decision after canceled call release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal decision did not return within cancellation bound")
	}
	select {
	case event := <-result:
		if !strings.Contains(event, "context canceled") {
			t.Fatalf("unneeded peer result=%s, want observable context cancellation", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unneeded peer did not report cancellation")
	}
	if closeBeforeRelease.Load() {
		t.Fatal("peer close started before canceled call released")
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
	var receivedOnce, ackedOnce sync.Once
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
						receivedOnce.Do(func() { close(received) })
						if ackGate != nil {
							select {
							case <-ctx.Done():
								return
							case <-ackGate:
							}
						}
						if err := writePeerFrame(stream, encodePeerResponse(&peerfb.ResponseT{})); err == nil {
							ackedOnce.Do(func() { close(acked) })
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
