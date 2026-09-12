package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestPeerServerCloseReleasesOwnedSocketAfterConnection(t *testing.T) {
	config := quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1", Token: "voter"}}}
	core := mustCore(t, "n1", config.Members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, config.Members, "admin")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	addr := peer.Addr()
	config.Members[0].PeerURL = "quic://" + addr
	client := NewTransport("cluster", "n1", &config, "admin")
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.ReadTip(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	rebound, err := net.ListenPacket("udp", addr)
	if err != nil {
		t.Fatalf("closed peer retained UDP socket: %v", err)
	}
	_ = rebound.Close()
}

func TestPeerServerClosePreservesCallerTransport(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	transport := &quic.Transport{Conn: conn}
	defer transport.Close()
	members := []quepaxa.Member{{ID: "n1", Token: "voter"}}
	core := mustCore(t, "n1", members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	peer, err := StartPeerServerOnTransport(context.Background(), transport, server, members, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := StartPeerServerOnTransport(context.Background(), transport, server, members, "admin")
	if err != nil {
		t.Fatalf("peer closed caller-owned transport: %v", err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}
