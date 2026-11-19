package network

import (
	"context"
	"crypto/sha256"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestSendFreezeDecisionSucceedsOnQuorumWhenFreezeRecorderUnavailable(t *testing.T) {
	clusterID := types.ClusterID("freeze-quorum")
	a := testMember(clusterID, "a", "a-token")
	c := testMember(clusterID, "c", "c-token")

	// Bind b to a dead UDP blackhole socket so dials fail/timeout without ACK.
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blackhole.Close() })
	b := testMember(clusterID, "b", "b-token")
	b.PeerURL = "quic://" + blackhole.LocalAddr().String()

	cCore := mustCore(t, "c", []quepaxa.Member{a, b, c}, nil, nil)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "db.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	cServer := NewServer(cCore, material, clusterID, true, nil)
	t.Cleanup(cServer.Close)
	peerCtx, peerCancel := context.WithCancel(context.Background())
	t.Cleanup(peerCancel)
	cPeer, err := StartPeerServer(peerCtx, "127.0.0.1:0", cServer, []quepaxa.Member{a, b, c}, "c-token", "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cPeer.Close() })
	c.PeerURL = "quic://" + cPeer.Addr()

	cluster := quepaxa.Cluster{ConfigID: 0, Members: []quepaxa.Member{a, b, c}}
	transport := NewTransport(clusterID, "a", &cluster, "a-token")
	transport.quic = &quic.Config{HandshakeIdleTimeout: 300 * time.Millisecond, MaxIdleTimeout: 300 * time.Millisecond}
	t.Cleanup(func() { _ = transport.Close() })

	// Freeze was signed by {a, b}, while b is now down and clean voter c is up.
	value := policySQL(t, "SELECT 1")
	var maxPrio quepaxa.Priority
	for i := range maxPrio {
		maxPrio[i] = 0xff
	}
	proposal := quepaxa.Proposal{
		Priority:   maxPrio,
		ProposerID: "a",
		Hash:       sha256.Sum256(value),
		Value:      value,
	}
	decision := quepaxa.Decision{
		Slot:     1,
		Step:     4,
		ConfigID: 0,
		Proposal: proposal,
		Summaries: []quepaxa.Summary{
			{RecorderID: "a", Step: 4, FirstCurrent: &proposal},
			{RecorderID: "b", Step: 4, FirstCurrent: &proposal},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := transport.sendFreezeDecision(ctx, decision); err != nil {
		t.Fatalf("sendFreezeDecision failed with clean quorum {a, c}: %v", err)
	}
}
