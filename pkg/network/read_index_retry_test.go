package network

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestReadIndexRetriesTransientlyUnreadyVoterWithoutCountingDownPeer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		recover bool
	}{{"voter_recovers", true}, {"no_quorum", false}} {
		t.Run(tc.name, func(t *testing.T) {
			members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token"), testMember("cluster", "c", "c-token")}
			var calls atomic.Int32
			bCore := mustCore(t, "b", members, nil, nil)
			bServer := NewServer(bCore, nil, "cluster", false, nil, func() bool { n := calls.Add(1); return tc.recover && n >= 7 })
			defer bServer.Close()
			peer, err := StartPeerServer(context.Background(), "127.0.0.1:0", bServer, members, "b-token", "admin")
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			members[1].PeerURL = "quic://" + peer.Addr()
			dead, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer dead.Close()
			members[2].PeerURL = "quic://" + dead.LocalAddr().String()
			cluster := &quepaxa.Cluster{Members: members}
			transport := NewTransport("cluster", "a", cluster, "a-token")
			defer transport.Close()
			core := mustCore(t, "a", members, nil, transport)
			duration := 300 * time.Millisecond
			if tc.recover {
				duration = 3 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), duration)
			defer cancel()
			_, _, err = core.ReadIndex(ctx)
			if tc.recover && (err != nil || calls.Load() < 7) {
				t.Fatalf("ready voter was not retried: calls=%d err=%v", calls.Load(), err)
			}
			if !tc.recover && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("read without quorum did not honor deadline: %v", err)
			}
			if !tc.recover && calls.Load() > 8 {
				t.Fatalf("unready voter retry was not bounded: calls=%d", calls.Load())
			}
		})
	}
}

type trackingRetryTransport struct {
	*Transport
	once        sync.Once
	oldCanceled chan struct{}
}

func (t *trackingRetryTransport) ReadTip(ctx context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	first := false
	if to == "b" {
		t.once.Do(func() { first = true })
	}
	slot, err := t.Transport.ReadTip(ctx, to)
	if first && ctx.Err() != nil {
		close(t.oldCanceled)
	}
	return slot, err
}

func TestReadIndexRetryCancelsOldRoundOnCertifiedConfigurationChange(t *testing.T) {
	members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token"), testMember("cluster", "c", "c-token")}
	var sockets [3]net.PacketConn
	var transports [2]*quic.Transport
	for i := range sockets {
		var err error
		sockets[i], err = net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sockets[i].Close() })
		members[i].PeerURL = "quic://" + sockets[i].LocalAddr().String()
		if i < len(transports) {
			transports[i] = &quic.Transport{Conn: sockets[i]}
			t.Cleanup(func() { _ = transports[i].Close() })
		}
	}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	peers := [2]*Transport{NewTransport("cluster", "a", &cluster, "a-token"), NewTransport("cluster", "b", &cluster, "b-token")}
	for _, p := range peers {
		t.Cleanup(func() { _ = p.Close() })
	}
	tracking := &trackingRetryTransport{Transport: peers[0], oldCanceled: make(chan struct{})}
	var cores [2]*quepaxa.Core
	for i := range cores {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		var tr quepaxa.Transport = peers[i]
		if i == 0 {
			tr = tracking
		}
		cores[i], err = quepaxa.New(quepaxa.Config{NodeID: members[i].ID, Cluster: cluster, WAL: wal, Transport: tr, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		peers[i].BindCore(cores[i])
	}
	var ready atomic.Bool
	rejected := make(chan struct{}, 1)
	for i := range cores {
		material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		isReady := func() bool { return true }
		if i == 1 {
			isReady = func() bool {
				if !ready.Load() {
					select {
					case rejected <- struct{}{}:
					default:
					}
					return false
				}
				return true
			}
		}
		server := NewServer(cores[i], material, "cluster", false, peers[i], isReady)
		server.SetLearnReady(func() bool { return true })
		t.Cleanup(server.Close)
		peer, err := StartPeerServerOnTransport(context.Background(), transports[i], server, members, []string{"a-token", "b-token"}[i], "admin")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	readDone := make(chan error, 1)
	go func() { _, _, err := cores[0].ReadIndex(ctx); readDone <- err }()
	select {
	case <-rejected:
	case <-ctx.Done():
		t.Fatal("old config did not reach unready peer")
	}
	target := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), members[:2]...)}
	if _, err := cores[0].BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := cores[0].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if got := cores[0].ConfigID(); got != target.ConfigID {
		t.Fatalf("configuration=%d", got)
	}
	select {
	case <-tracking.oldCanceled:
	case <-ctx.Done():
		t.Fatal("old read retry was not canceled")
	}
	ready.Store(true)
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("fresh configuration read: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("fresh read did not complete")
	}
}

func TestReadTipDoesNotRetryAuthenticationOrConfigurationErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		token    string
		configID uint
	}{{"authentication", "wrong-token", 0}, {"configuration", "a-token", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token")}
			bCore := mustCore(t, "b", members, nil, nil)
			server := NewServer(bCore, nil, "cluster", false, nil)
			defer server.Close()
			peer, err := StartPeerServer(context.Background(), "127.0.0.1:0", server, members, "b-token", "admin")
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			var requests atomic.Int32
			observe := func(phase string, err error) {
				if phase == "request-read" && err == nil {
					requests.Add(1)
				}
			}
			peer.diagnostic.Store(&observe)
			defer peer.diagnostic.Store(nil)
			members[1].PeerURL = "quic://" + peer.Addr()
			transport := NewTransport("cluster", "a", &quepaxa.Cluster{ConfigID: tc.configID, Members: members}, tc.token)
			defer transport.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := transport.ReadTip(ctx, "b"); err == nil {
				t.Fatal("invalid peer request succeeded")
			}
			if count := requests.Load(); count != 1 {
				t.Fatalf("expected one request, got %d", count)
			}
		})
	}
}

func TestReadTipCancelsRetryWait(t *testing.T) {
	members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token")}
	called := make(chan struct{}, 1)
	bCore := mustCore(t, "b", members, nil, nil)
	server := NewServer(bCore, nil, "cluster", false, nil, func() bool {
		select {
		case called <- struct{}{}:
		default:
		}
		return false
	})
	defer server.Close()
	peer, err := StartPeerServer(context.Background(), "127.0.0.1:0", server, members, "b-token", "admin")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	members[1].PeerURL = "quic://" + peer.Addr()
	transport := NewTransport("cluster", "a", &quepaxa.Cluster{Members: members}, "a-token")
	defer transport.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := transport.ReadTip(ctx, "b"); done <- err }()
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("request never reached peer")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry did not stop after cancellation")
	}
}

func TestReadTipRetryWithoutDeadlineIsBounded(t *testing.T) {
	members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token")}
	bCore := mustCore(t, "b", members, nil, nil)
	server := NewServer(bCore, nil, "cluster", false, nil, func() bool { return false })
	defer server.Close()
	peer, err := StartPeerServer(context.Background(), "127.0.0.1:0", server, members, "b-token", "admin")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	members[1].PeerURL = "quic://" + peer.Addr()
	transport := NewTransport("cluster", "a", &quepaxa.Cluster{Members: members}, "a-token")
	defer transport.Close()
	done := make(chan error, 1)
	go func() { _, err := transport.ReadTip(context.Background(), "b"); done <- err }()
	select {
	case err := <-done:
		var retryable *readTipRetryableError
		if !errors.As(err, &retryable) {
			t.Fatalf("expected readiness error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("unbounded read-tip retry")
	}
}

func TestReadTipDoesNotRetryMalformedTransportResponse(t *testing.T) {
	certificate, err := peerCertificate("cluster", "b", "b-token")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, NextProtos: []string{peerALPN}, MinVersion: tls.VersionTLS13}, newPeerQUICConfig(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var connections sync.WaitGroup
	acceptDone := make(chan struct{})
	var requests atomic.Int32
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer conn.CloseWithError(0, "test complete")
				stream, err := conn.AcceptStream(ctx)
				if err != nil {
					return
				}
				defer stream.Close()
				if _, err := readPeerFrame(stream); err != nil {
					return
				}
				requests.Add(1)
				_ = writePeerFrame(stream, []byte{1})
			}()
		}
	}()
	t.Cleanup(func() { cancel(); _ = listener.Close(); <-acceptDone; connections.Wait() })
	members := []quepaxa.Member{testMember("cluster", "a", "a-token"), testMember("cluster", "b", "b-token")}
	members[1].PeerURL = "quic://" + listener.Addr().String()
	transport := NewTransport("cluster", "a", &quepaxa.Cluster{Members: members}, "a-token")
	defer transport.Close()
	callCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if _, err := transport.ReadTip(callCtx, "b"); err == nil {
		t.Fatal("malformed response succeeded")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("transport error was retried: requests=%d", got)
	}
}
