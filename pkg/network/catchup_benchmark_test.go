package network

import (
	"context"
	"crypto/tls"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

// The peer is an external QUIC boundary; certificates and follower WAL handling
// use the real Core. Tests control replies without production instrumentation.
type catchUpPeer struct {
	member    quepaxa.Member
	calls     atomic.Int64
	delivered atomic.Int64
}

func startCatchUpPeer(t testing.TB, id quepaxa.NodeID, values []quepaxa.DecidedValue, beforeReply func(context.Context, *peerfb.RequestT) error) *catchUpPeer {
	t.Helper()
	p := &catchUpPeer{member: quepaxa.Member{ID: id, Token: string(id) + "-token"}}
	cert, err := peerCertificate("cluster", id, p.member.Token)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{peerALPN}, MinVersion: tls.VersionTLS13}, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.member.PeerURL = "quic://" + listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			wg.Go(func() {
				defer conn.CloseWithError(0, "test complete")
				for {
					stream, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					wg.Go(func() {
						defer stream.CancelRead(0)
						defer stream.Close()
						data, err := readPeerFrame(stream)
						if err != nil {
							return
						}
						req, err := decodePeerRequest(data)
						if err != nil {
							return
						}
						if req.Operation != peerfb.OperationSync || req.Token != "admin" || req.ClusterId != "cluster" || req.ConfigId != 1 {
							_ = writePeerFrame(stream, encodePeerResponse(&peerfb.ResponseT{Error: "invalid test request"}))
							return
						}
						p.calls.Add(1)
						if beforeReply != nil {
							if err := beforeReply(ctx, req); err != nil {
								return
							}
						}
						response := &peerfb.ResponseT{ClusterId: "cluster", ProposerId: string(id), ConfigId: 1, Tip: uint64(len(values))}
						for _, value := range values {
							if uint64(value.Slot) >= req.From && len(response.Decisions) < int(req.Limit) {
								response.Decisions = append(response.Decisions, decidedToWire(value))
							}
						}
						if err := writePeerFrame(stream, encodePeerResponse(response)); err == nil {
							p.delivered.Add(int64(len(response.Decisions)))
						}
					})
				}
			})
		}
	})
	t.Cleanup(func() { cancel(); _ = listener.Close(); wg.Wait() })
	return p
}

func catchUpValues(t testing.TB, count int) (quepaxa.Cluster, []quepaxa.DecidedValue) {
	t.Helper()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "a", Token: "a-token"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "a", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if _, _, err := core.Propose(context.Background(), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	values, _, err := core.DecisionsFrom(1, count)
	if err != nil {
		t.Fatal(err)
	}
	return config, values
}

func newCatchUpFollower(t testing.TB, dir string, config quepaxa.Cluster, transport *Transport, reconfiguration bool) (*Server, *qlog.WAL) {
	t.Helper()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "follower", Cluster: config, WAL: wal, EnableReconfiguration: reconfiguration})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	return NewServer(core, nil, "cluster", false, transport), wal
}

func BenchmarkCatchUpSameSourceQUIC(b *testing.B) {
	config, values := catchUpValues(b, 8)
	peer := startCatchUpPeer(b, "a", values, func(ctx context.Context, _ *peerfb.RequestT) error {
		timer := time.NewTimer(5 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	transport := NewTransport("cluster", "follower", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{peer.member}}, "admin")
	defer transport.Close()
	ctx := context.Background()
	if _, err := transport.FetchDecisions(ctx, "a", 9, 1); err != nil {
		b.Fatal(err)
	}
	peer.calls.Store(0)
	peer.delivered.Store(0)
	dir := b.TempDir()
	installed := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		server, wal := newCatchUpFollower(b, filepath.Join(dir, fmt.Sprint(i)), config, transport, false)
		b.StartTimer()
		start, done := make(chan struct{}), make(chan error, 2)
		for range 2 {
			go func() { <-start; done <- server.catchUpFrom(ctx, "a", 8, false) }()
		}
		close(start)
		for range 2 {
			if err := <-done; err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if server.core.Tip() != 8 {
			b.Fatal("incomplete follower")
		}
		decisions, _, err := server.core.DecisionsFrom(1, 8)
		if err != nil || len(decisions) != 8 {
			b.Fatalf("installed prefix: %d, %v", len(decisions), err)
		}
		installed += len(decisions)
		server.Close()
		if err := wal.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(installed)/float64(b.N), "unique_installed/op")
	b.ReportMetric(float64(peer.calls.Load())/float64(b.N), "fetches/op")
	b.ReportMetric(float64(peer.delivered.Load())/float64(b.N), "decisions_fetched/op")
}
