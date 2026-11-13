package network

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestPeerKeepAliveSurvivesActiveRPCAndRetiresIdleConfiguration(t *testing.T) {
	const idleTimeout = 250 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addr := startLongReadTipPeer(t, ctx, idleTimeout)

	n1 := testMember("cluster", "n1", "n1-token")
	n2 := testMember("cluster", "n2", "n2-token")
	n2.PeerURL = "quic://" + addr
	config1 := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{n1, n2}}
	parent := NewTransport("cluster", n1.ID, &config1, "n1-token")
	parent.quic = newPeerQUICConfig(idleTimeout)
	defer parent.Close()
	old, err := parent.transportFor(config1)
	if err != nil {
		t.Fatal(err)
	}
	old.quic = newPeerQUICConfig(idleTimeout)

	// Config 1 deliberately pauses for longer than MaxIdleTimeout without
	// writing. Keep-alive must preserve the active stream until its response.
	if _, err := old.ReadTip(ctx, n2.ID); err != nil {
		t.Fatalf("active RPC crossed idle timeout: %v", err)
	}
	oldConn := old.peers[n2.ID].conn
	if oldConn == nil || oldConn.Context().Err() != nil {
		t.Fatal("active RPC did not retain its connection")
	}

	config2 := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{n1, n2}}
	parent.BindCore(testClusterResolver{current: config2, slots: map[quepaxa.Slot]quepaxa.Cluster{1: config1}})
	current, err := parent.transportForCurrent()
	if err != nil {
		t.Fatal(err)
	}
	current.quic = newPeerQUICConfig(idleTimeout)
	if old.retired.Load() || parent.dynamic[1] != old {
		t.Fatal("immediate predecessor configuration was not retained")
	}

	if _, err := current.ReadTip(ctx, n2.ID); err != nil {
		t.Fatalf("current-generation RPC failed: %v", err)
	}
	currentConn := current.peers[n2.ID].conn
	if currentConn == nil || currentConn.Context().Err() != nil {
		t.Fatal("redial did not establish a connection")
	}

	config3 := quepaxa.Cluster{ConfigID: 3, Members: []quepaxa.Member{n1, n2}}
	parent.BindCore(testClusterResolver{current: config3, slots: map[quepaxa.Slot]quepaxa.Cluster{1: config1, 3: config3}})
	slotCurrent, err := parent.transportForSlot(3)
	if err != nil {
		t.Fatal(err)
	}
	if !old.retired.Load() || parent.dynamic[1] != nil {
		t.Fatal("slot-routed current configuration did not retire older history")
	}
	select {
	case <-oldConn.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("idle older configuration connection remained open")
	}
	if current.retired.Load() || parent.dynamic[2] != current {
		t.Fatal("immediate predecessor configuration was not retained")
	}
	historical, closeHistorical, err := parent.transportForSlotLease(1)
	if err != nil {
		t.Fatal(err)
	}
	if closeHistorical == nil {
		t.Fatal("older historical slot was cached instead of leased")
	}
	defer func() {
		if closeHistorical != nil {
			closeHistorical()
		}
	}()
	if slotCurrent.retired.Load() || parent.dynamic[3] != slotCurrent || parent.dynamic[1] != nil || len(parent.dynamic) != 2 || parent.dynamicCount.Load() != 2 {
		t.Fatal("historical-slot lease changed the bounded cache")
	}
	if _, err := historical.ReadTip(ctx, n2.ID); err != nil {
		t.Fatalf("historical-slot redial after retirement: %v", err)
	}
	historicalConn := historical.peers[n2.ID].conn
	closeHistorical()
	closeHistorical = nil
	select {
	case <-historicalConn.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("historical-slot transient connection remained open")
	}
}

func startLongReadTipPeer(t *testing.T, ctx context.Context, idleTimeout time.Duration) string {
	t.Helper()
	certificate, err := peerCertificate("cluster", "n2", "n2-token")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{peerALPN},
		MinVersion:   tls.VersionTLS13,
	}, newPeerQUICConfig(idleTimeout))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			go serveLongReadTipConnection(ctx, t, conn, idleTimeout)
		}
	}()
	return listener.Addr().String()
}

func serveLongReadTipConnection(ctx context.Context, t *testing.T, conn *quic.Conn, idleTimeout time.Duration) {
	defer conn.CloseWithError(0, "test complete")
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() {
			defer stream.Close()
			data, err := readPeerFrame(stream)
			if err != nil {
				t.Error(err)
				return
			}
			request, err := decodePeerRequest(data)
			if err != nil {
				t.Error(err)
				return
			}
			if request.ConfigId == 1 {
				time.Sleep(3 * idleTimeout)
			}
			response := &peerfb.ResponseT{ClusterId: "cluster", ProposerId: "n2", ConfigId: request.ConfigId, Tip: 1}
			if err := writePeerFrame(stream, encodePeerResponse(response)); err != nil {
				t.Error(err)
			}
		}()
	}
}
