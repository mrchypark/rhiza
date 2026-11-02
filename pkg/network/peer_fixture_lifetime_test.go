package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/peertest"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestPeerFixtureTransportSurvivesRPCAndRestart(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	transport := &quic.Transport{Conn: conn}
	t.Cleanup(func() { _ = transport.Close() })
	address := conn.LocalAddr().String()
	assertHeld := func() {
		t.Helper()
		other, err := net.ListenPacket("udp", address)
		if err == nil {
			_ = other.Close()
			t.Fatal("fixture endpoint released")
		}
	}
	member := testMember("cluster", "n1", "voter")
	member.PeerURL = "quic://" + address
	config := quepaxa.Cluster{Members: []quepaxa.Member{member}}
	core := mustCore(t, "n1", config.Members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	assertHeld()
	for range 2 {
		parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ctx := peertest.WithTransport(parent, "cluster", "n1", address, transport)
		peer, err := StartPeerServer(ctx, address, server, config.Members, "voter", "admin")
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
		assertHeld()
		client := NewTransport("cluster", "n1", &config, "voter")
		_, callErr := client.ReadTip(ctx, "n1")
		_ = client.Close()
		if callErr != nil {
			_ = peer.Close()
			cancel()
			t.Fatal(callErr)
		}
		assertHeld()
		if err := peer.Close(); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		assertHeld()
	}
	// Failed identity and closed transport must not close the caller's socket
	// or silently create a new address-owned listener.
	ctx := peertest.WithTransport(context.Background(), "cluster", "n1", address, transport)
	if p, err := StartPeerServer(ctx, address, server, config.Members, "wrong-token", "admin"); err == nil || p != nil {
		if p != nil {
			_ = p.Close()
		}
		t.Fatal("invalid identity accepted")
	}
	assertHeld()
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if p, err := StartPeerServer(ctx, address, server, config.Members, "voter", "admin"); err == nil || p != nil {
		if p != nil {
			_ = p.Close()
		}
		t.Fatal("closed transport fell back")
	}
	assertHeld()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := net.ListenPacket("udp", address)
	if err != nil {
		t.Fatalf("owner cleanup did not release endpoint: %v", err)
	}
	_ = other.Close()
}
