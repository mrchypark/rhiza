package network

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// TestRetiredPeerTokenCannotAuthenticate proves that a credential disclosed by
// an old token-bearing archive cannot authenticate a voter after rotation. The
// process fails closed at startup, and a certificate derived from the retired
// token fails the per-RPC authorization gate even for a valid sender ID.
func TestRetiredPeerTokenCannotAuthenticate(t *testing.T) {
	rotated := []quepaxa.Member{testMember("cluster", "a", "a-token-v2"), testMember("cluster", "b", "b-token-v2")}
	core := mustCore(t, "a", rotated, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, rotated, "a-token", "admin"); peer != nil || err == nil {
		t.Fatalf("retired local token accepted: peer=%v err=%v", peer, err)
	}
	current, err := StartPeerServer(ctx, "127.0.0.1:0", server, rotated, "a-token-v2", "admin")
	if err != nil {
		t.Fatalf("current token rejected: %v", err)
	}
	defer current.Close()

	peer := &PeerServer{server: server, adminToken: "admin"}
	if _, err := peer.handle(ctx, PeerPublicKey("cluster", "b", "b-token-v2"), &peerfb.RequestT{
		Operation: peerfb.OperationSync, ClusterId: "cluster", SenderId: "b", ConfigId: uint64(core.ConfigID()), From: 1, Limit: 1,
	}); err != nil {
		t.Fatalf("current token certificate rejected: %v", err)
	}
	if _, err := peer.handle(ctx, PeerPublicKey("cluster", "b", "b-token"), &peerfb.RequestT{
		Operation: peerfb.OperationSync, ClusterId: "cluster", SenderId: "b", ConfigId: uint64(core.ConfigID()), From: 1, Limit: 1,
	}); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("retired token certificate error=%v, want authentication failure", err)
	}
}
