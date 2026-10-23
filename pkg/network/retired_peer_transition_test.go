package network

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestRetiredPeerTransitionRejectsCachedIdentityAndAdmitsNewIdentity(t *testing.T) {
	const clusterID types.ClusterID = "retired-peer-transition"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := map[quepaxa.NodeID]string{"a": "token-a", "b": "token-b", "c": "token-c"}
	config := quepaxa.Cluster{ConfigID: 1}
	packets := make([]net.PacketConn, 0, len(ids))
	listeners := make([]*quic.Transport, 0, len(ids))
	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, packet)
		t.Cleanup(func() { _ = packet.Close() })
		listeners = append(listeners, &quic.Transport{Conn: packet})
		member := testMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + packet.LocalAddr().String()
		config.Members = append(config.Members, member)
	}
	for _, listener := range listeners {
		t.Cleanup(func() { _ = listener.Close() })
	}

	cores := make(map[quepaxa.NodeID]*quepaxa.Core, len(ids))
	servers := make(map[quepaxa.NodeID]*Server, len(ids))
	transports := make(map[quepaxa.NodeID]*Transport, len(ids))
	for _, member := range config.Members {
		transport := NewTransport(clusterID, member.ID, &config, tokens[member.ID])
		transports[member.ID] = transport
		t.Cleanup(func() { _ = transport.Close() })
		wal, err := qlog.Open(t.TempDir() + "/qlog")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: config, WAL: wal, Transport: transport,
			EnableReconfiguration: true,
			ReconfigurationAdmission: func(context.Context, quepaxa.Cluster, quepaxa.Slot, [32]byte) error {
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		transport.BindCore(core)
		cores[member.ID] = core
		material, err := materializer.Open(filepath.Join(t.TempDir(), string(member.ID)+".sqlite"), 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		servers[member.ID] = NewServer(core, material, clusterID, true, transport)
		t.Cleanup(servers[member.ID].Close)
	}
	for i, member := range config.Members {
		peer, err := StartPeerServerOnTransport(ctx, listeners[i], servers[member.ID], config.Members, tokens[member.ID], "admin-token")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
	}

	// C establishes and keeps a real QUIC/TLS connection to A under the old
	// committed membership before its removal.
	oldClient := NewTransport(clusterID, "c", &config, tokens["c"])
	t.Cleanup(func() { _ = oldClient.Close() })
	if _, err := oldClient.ReadTip(ctx, "a"); err != nil {
		t.Fatalf("old voter could not establish its connection: %v", err)
	}
	oldConn := cachedPeerConnection(oldClient, "a")
	if oldConn == nil || !oldConn.ConnectionState().TLS.HandshakeComplete {
		t.Fatal("old voter connection was not established")
	}

	withoutC := quepaxa.Cluster{ConfigID: 2, Members: append([]quepaxa.Member(nil), config.Members[:2]...)}
	if _, err := cores["a"].BeginReconfiguration(ctx, withoutC); err != nil {
		t.Fatalf("begin REMOVE: %v", err)
	}
	if err := cores["a"].FinishReconfigurationAt(ctx, withoutC, 0); err != nil {
		t.Fatalf("commit durable REMOVE: %v", err)
	}
	if got := cores["a"].CurrentCluster(); got.ConfigID != withoutC.ConfigID || !sameNetworkMembers(got, withoutC) {
		t.Fatalf("REMOVE did not activate at receiver A: %+v", got)
	}

	// A delayed voter record names a slot certified by the historical config.
	// It must still fail on the already-open connection because A also checks
	// the active configuration for every RPC.
	delayed := &peerfb.RequestT{
		Operation: peerfb.OperationRecord, ClusterId: string(clusterID), SenderId: "c", ConfigId: 1,
		Record: &peerfb.RecordRequestT{Slot: 1, Step: 4, Proposal: &peerfb.ProposalT{Priority: make([]byte, 32), ProposerId: "c", Hash: make([]byte, 32)}},
	}
	if _, err := oldClient.call(ctx, "a", delayed); err == nil || !errors.Is(err, errPeerRejected) || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("cached retired identity RPC error=%v, want peer authentication rejection", err)
	}
	if got := cachedPeerConnection(oldClient, "a"); got != oldConn || !oldConn.ConnectionState().TLS.HandshakeComplete {
		t.Fatal("test did not reuse the original live QUIC/TLS connection")
	}

	// The same logical ID cannot be resurrected, even with a new member record.
	reuseC := quepaxa.Cluster{ConfigID: 3, Members: append([]quepaxa.Member(nil), withoutC.Members...)}
	retiredC := config.Members[2]
	retiredC.PublicKey = quepaxa.PublicKey(PeerPublicKey(clusterID, "c", "token-c-v2"))
	retiredC.WALIdentity = strings.Repeat("c", 64)
	reuseC.Members = append(reuseC.Members, retiredC)
	if err := cores["a"].ValidateReconfigurationTarget(reuseC); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("re-adding retired node ID error=%v, want retired-ID rejection", err)
	}

	// D is a distinct logical identity. Before the committed ADD, its token is
	// not an admitted voter credential at A.
	dToken := "token-d"
	dMember := testMember(clusterID, "d", dToken)
	dPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dPacket.Close() })
	dListener := &quic.Transport{Conn: dPacket}
	t.Cleanup(func() { _ = dListener.Close() })
	dMember.PeerURL = "quic://" + dPacket.LocalAddr().String()
	peerIdentities := make([]PeerIdentity, 0, len(config.Members))
	for _, member := range config.Members {
		identity, err := NewPeerIdentity(clusterID, member)
		if err != nil {
			t.Fatal(err)
		}
		peerIdentities = append(peerIdentities, identity)
	}
	learnerTransport := NewLearnerTransport(clusterID, "d", config.ConfigID, peerIdentities, "admin-token")
	t.Cleanup(func() { _ = learnerTransport.Close() })
	dWal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dWal.Close() })
	dCore, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "d", Cluster: config, WAL: dWal, Transport: learnerTransport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	learnerTransport.BindCore(dCore)
	dMember.WALIdentity = dCore.WALIdentity()
	dMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "d.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dMaterial.Close() })
	dServer := NewServer(dCore, dMaterial, clusterID, true, learnerTransport)
	t.Cleanup(dServer.Close)
	dPeer, err := StartPeerServerOnTransport(ctx, dListener, dServer, append(append([]quepaxa.Member(nil), config.Members...), dMember), dToken, "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dPeer.Close() })
	dBeforeAdd := NewTransport(clusterID, "d", &withoutC, dToken)
	t.Cleanup(func() { _ = dBeforeAdd.Close() })
	if _, err := dBeforeAdd.ReadTip(ctx, "a"); err == nil || !errors.Is(err, errPeerRejected) {
		t.Fatalf("new identity before committed ADD error=%v, want rejection", err)
	}
	// The replacement must first learn the committed REMOVE through the
	// authenticated, read-only sync path. Otherwise its history resolves the
	// later ADD terminal under config 1 and correctly rejects the config-2
	// envelope before it can apply that terminal.
	if err := dServer.catchUpFrom(ctx, "a", cores["a"].Tip(), true); err != nil {
		t.Fatalf("replacement learner catch-up through committed REMOVE: %v", err)
	}
	if got := dCore.CurrentCluster(); got.ConfigID != withoutC.ConfigID || !sameNetworkMembers(got, withoutC) || dCore.IsVoter() {
		t.Fatalf("replacement learner did not remain non-voting at post-REMOVE config: voter=%t cluster=%+v", dCore.IsVoter(), got)
	}

	withD := quepaxa.Cluster{ConfigID: 3, Members: append([]quepaxa.Member(nil), withoutC.Members...)}
	withD.Members = append(withD.Members, dMember)
	if _, err := cores["a"].BeginReconfiguration(ctx, withD); err != nil {
		t.Fatalf("begin ADD: %v", err)
	}
	if err := cores["a"].FinishReconfigurationAt(ctx, withD, 0); err != nil {
		t.Fatalf("commit durable ADD: %v", err)
	}
	if got := cores["a"].CurrentCluster(); got.ConfigID != withD.ConfigID || !sameNetworkMembers(got, withD) {
		t.Fatalf("ADD did not activate at receiver A: %+v", got)
	}
	if !dCore.IsVoter() || dCore.CurrentCluster().ConfigID != withD.ConfigID {
		t.Fatalf("new logical identity did not apply committed ADD: voter=%t cluster=%+v", dCore.IsVoter(), dCore.CurrentCluster())
	}
	dAfterAdd := NewTransport(clusterID, "d", &withD, dToken)
	t.Cleanup(func() { _ = dAfterAdd.Close() })
	if _, err := dAfterAdd.ReadTip(ctx, "a"); err != nil {
		t.Fatalf("new logical identity rejected after committed ADD: %v", err)
	}
	if _, err := oldClient.call(ctx, "a", delayed); err == nil || !errors.Is(err, errPeerRejected) || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("old identity RPC after new generation error=%v, want rejection", err)
	}
}

func cachedPeerConnection(transport *Transport, id quepaxa.NodeID) *quic.Conn {
	peer := transport.peers[id]
	if peer == nil {
		return nil
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.conn
}

func sameNetworkMembers(left, right quepaxa.Cluster) bool {
	if left.ConfigID != right.ConfigID || len(left.Members) != len(right.Members) {
		return false
	}
	for i := range left.Members {
		if left.Members[i] != right.Members[i] {
			return false
		}
	}
	return true
}
