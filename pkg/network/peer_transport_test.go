package network

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

type testClusterResolver struct {
	current quepaxa.Cluster
	slots   map[quepaxa.Slot]quepaxa.Cluster
}

func (r testClusterResolver) CurrentCluster() quepaxa.Cluster { return r.current }
func (r testClusterResolver) ConfigID() uint                  { return r.current.ConfigID }
func (r testClusterResolver) ConfigIDForSlot(slot quepaxa.Slot) uint {
	return r.ClusterForSlot(slot).ConfigID
}
func (r testClusterResolver) ClusterForSlot(slot quepaxa.Slot) quepaxa.Cluster {
	if config, ok := r.slots[slot]; ok {
		return config
	}
	return r.current
}

func TestQUICFlatBuffersRecordRoundTrip(t *testing.T) {
	member := testMember("cluster", "n1", "secret")
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member, testMember("cluster", "n2", "admin-secret")}, "admin-secret", "admin-secret"); peer != nil || err == nil {
		t.Fatalf("reused admin token as peer identity peer=%v error=%v", peer, err)
	}
	if peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member, {ID: "n2"}}, "secret", "admin-secret"); peer != nil || err == nil {
		t.Fatalf("missing voter public key peer=%v error=%v", peer, err)
	}
	// A distinct admin token that derives a voter's pinned key would let a
	// read-only admin client present that voter's certificate and pass the gate.
	if peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member, testMember("cluster", "n2", "admin-secret")}, "secret", "admin-secret"); peer != nil || err == nil {
		t.Fatalf("admin token derived a voter key peer=%v error=%v", peer, err)
	}
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "secret", "admin-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	member.PeerURL = "quic://" + peer.Addr()
	transport := NewTransport("cluster", "n1", &quepaxa.Cluster{Members: []quepaxa.Member{member}}, "secret")
	defer transport.Close()
	callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
	defer callCancel()
	request := quepaxa.RecordRequest{Slot: 1, Step: 4, Proposal: quepaxa.Proposal{ProposerID: "n1", Value: []byte("value")}}
	request.Proposal.Priority[31] = 1
	request.Proposal.Hash = sha256.Sum256(request.Proposal.Value)
	if err := transport.StageValue(callCtx, member.ID, request.Proposal.Hash, request.Proposal.Value); err != nil {
		t.Fatal(err)
	}
	summary, err := transport.SendRecord(callCtx, member.ID, request)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecorderID != member.ID || summary.Step != request.Step {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	for range 300 {
		if _, err := transport.ReadTip(callCtx, member.ID); err != nil {
			t.Fatalf("stream was not released: %v", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := transport.tls.ClientSessionCache.Get(string(member.ID)); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("QUIC session ticket was not cached")
		}
		time.Sleep(10 * time.Millisecond)
	}
	first := transport.peers[member.ID].conn
	transport.peers[member.ID].mu.Lock()
	transport.peers[member.ID].active[first]++ // Simulate another in-flight stream.
	transport.peers[member.ID].mu.Unlock()
	canceledCtx, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if _, err := transport.SendRecord(canceledCtx, member.ID, request); err == nil {
		t.Fatal("canceled stream succeeded")
	}
	if first.Context().Err() != nil {
		t.Fatal("failed stream closed a connection with another active stream")
	}
	transport.invalidate(member.ID, first)
	if first.Context().Err() != nil {
		t.Fatal("invalidated connection closed before its last active stream")
	}
	transport.release(member.ID, first)
	select {
	case <-first.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("invalidated connection remained open after its last release")
	}
	request.Step++
	if _, err := transport.SendRecord(callCtx, member.ID, request); err != nil {
		t.Fatalf("request after stream cancellation: %v", err)
	}
	second := transport.peers[member.ID].conn
	transport.invalidate(member.ID, second)
	select {
	case <-second.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("idle invalidated connection remained open")
	}
	request.Step++
	if _, err := transport.SendRecord(callCtx, member.ID, request); err != nil {
		t.Fatal(err)
	}
	// A restarted peer has lost its TLS ticket keys. Voter authorization is
	// bound to the handshake certificate, so the request must wait for the
	// completed handshake and never travel as replayable early data.
	oldConn := transport.peers[member.ID].conn
	transport.invalidate(member.ID, oldConn)
	_ = oldConn.CloseWithError(0, "peer restart")
	replacement, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "secret", "admin-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	member.PeerURL = "quic://" + replacement.Addr()
	transport.members[member.ID] = member
	if _, err := transport.FetchDecisions(callCtx, member.ID, 1, 1); err != nil {
		t.Fatalf("Sync after peer restart failed: %v", err)
	}
	wrongMember := member
	wrongMember.PublicKey = quepaxa.PublicKey(PeerPublicKey("cluster", "n1", "wrong"))
	wrong := NewTransport("cluster", "n1", &quepaxa.Cluster{Members: []quepaxa.Member{wrongMember}}, "wrong")
	defer wrong.Close()
	if _, err := wrong.SendRecord(callCtx, member.ID, request); err == nil {
		t.Fatal("peer with the wrong certificate identity was accepted")
	}
}

func TestDecisionHasRecorder(t *testing.T) {
	decision := quepaxa.Decision{Summaries: []quepaxa.Summary{{RecorderID: "n1"}, {RecorderID: "n2"}}}
	if !decisionHasRecorder(decision, "n1") || !decisionHasRecorder(decision, "n2") || decisionHasRecorder(decision, "n3") {
		t.Fatal("decision recorder membership mismatch")
	}
}

func TestPeerIdentityRequiresVoterCredential(t *testing.T) {
	if identity, err := NewPeerIdentity("cluster", quepaxa.Member{ID: "n1", PeerURL: "quic://127.0.0.1:1"}); identity != (PeerIdentity{}) || err == nil {
		t.Fatalf("identity=%+v error=%v", identity, err)
	}
}

func TestStartLearnerPeerServerValidatesIdentity(t *testing.T) {
	member := testMember("cluster", "n1", "voter")
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	ctx := context.Background()
	if peer, err := StartLearnerPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "learner-token", "admin", quepaxa.Member{ID: "n2", PublicKey: quepaxa.PublicKey(PeerPublicKey("cluster", "n2", "learner-token"))}); peer != nil || err == nil {
		t.Fatalf("foreign learner peer=%v err=%v", peer, err)
	}
	if peer, err := StartLearnerPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "voter", "admin", member); peer != nil || err == nil {
		t.Fatalf("voter learner peer=%v err=%v", peer, err)
	}
}

func TestNonMemberLearnerMayOnlyFetchCertifiedDecisions(t *testing.T) {
	member := testMember("cluster", "n1", "voter-token")
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	peer := &PeerServer{server: server, members: map[quepaxa.NodeID]quepaxa.Member{member.ID: member}, adminToken: "learner-token"}
	request := &peerfb.RequestT{
		Operation: peerfb.OperationSync, ClusterId: "cluster", SenderId: "learner-1",
		ConfigId: uint64(core.ConfigID()), AdminToken: "learner-token", From: 1, Limit: 1,
	}
	if _, err := peer.handle(context.Background(), nil, request); err != nil {
		t.Fatalf("learner sync rejected: %v", err)
	}
	request.Operation = peerfb.OperationReadIndex
	if _, err := peer.handle(context.Background(), nil, request); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("learner non-sync operation error=%v", err)
	}
	request.SenderId = string(member.ID)
	if _, err := peer.handle(context.Background(), nil, request); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("learner voter impersonation error=%v", err)
	}
	request.Operation, request.AdminToken = peerfb.OperationSync, "wrong"
	if _, err := peer.handle(context.Background(), nil, request); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("learner wrong-token error=%v", err)
	}
}

func TestPeerConnectionWaitHonorsContext(t *testing.T) {
	member := quepaxa.Member{ID: "n1", PeerURL: "quic://127.0.0.1:1"}
	transport := NewTransport("cluster", "n1", &quepaxa.Cluster{Members: []quepaxa.Member{member}}, "secret")
	peer := transport.peers[member.ID]
	peer.gate <- struct{}{}
	defer func() { <-peer.gate }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := transport.connection(ctx, member.ID); err == nil {
		t.Fatal("connection wait ignored cancellation")
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("connection cancellation took %v", elapsed)
	}
}

func TestPeerCodecRejectsMalformedFrame(t *testing.T) {
	if _, err := decodePeerRequest([]byte("not-flatbuffers")); err == nil {
		t.Fatal("malformed frame accepted")
	}
}

func TestPeerCodecRejectsWrongMarker(t *testing.T) {
	request := &peerfb.RequestT{Magic: 2, Operation: peerfb.OperationRecord}
	builder := flatbuffers.NewBuilder(64)
	offset := request.Pack(builder)
	peerfb.FinishRequestBuffer(builder, offset)
	if _, err := decodePeerRequest(builder.FinishedBytes()); err == nil {
		t.Fatal("accepted peer frame with wrong marker")
	}
}

func TestRecordEnvelopeCarriesConfigurationAndReconfigurationID(t *testing.T) {
	var reconfigurationID quepaxa.ValueHash
	reconfigurationID[0] = 7
	encoded := encodePeerRequest(&peerfb.RequestT{
		Operation: peerfb.OperationRecord, ClusterId: "cluster", SenderId: "n1", ConfigId: 9,
		Hash: reconfigurationID[:], Record: &peerfb.RecordRequestT{Slot: 12, Step: 4, Proposal: &peerfb.ProposalT{Priority: make([]byte, 32), ProposerId: "n1", Hash: make([]byte, 32)}},
	})
	decoded, err := decodePeerRequest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ConfigId != 9 || !bytes.Equal(decoded.Hash, reconfigurationID[:]) {
		t.Fatalf("configuration envelope lost: config=%d hash=%x", decoded.ConfigId, decoded.Hash)
	}
}

func TestSummaryCarriesReconfigurationID(t *testing.T) {
	var id quepaxa.ValueHash
	id[0] = 9
	summary, err := summaryFromWire(summaryToWire(quepaxa.Summary{RecorderID: "n1", Step: 4, ReconfigurationID: id}))
	if err != nil {
		t.Fatal(err)
	}
	if summary.ReconfigurationID != id {
		t.Fatalf("reconfiguration ID lost: %x", summary.ReconfigurationID)
	}
}

func TestBoundTransportSeparatesConfigurationConnectionPools(t *testing.T) {
	old := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{testMember("cluster", "n1", "old"), testMember("cluster", "n2", "old-2")}}
	current := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{testMember("cluster", "n1", "new"), testMember("cluster", "n3", "new-3")}}
	transport := NewTransport("cluster", "n1", &old, "old")
	defer transport.Close()
	transport.BindCore(testClusterResolver{current: current, slots: map[quepaxa.Slot]quepaxa.Cluster{1: old}})
	historical, err := transport.transportFor(transport.clusterForSlot(1))
	if err != nil {
		t.Fatal(err)
	}
	active, err := transport.transportFor(transport.currentCluster())
	if err != nil {
		t.Fatal(err)
	}
	if historical == transport || active == transport || historical == active {
		t.Fatal("configurations reused a connection pool")
	}
	// The process identity is shared; each configuration keeps its own copy of
	// the published member keys, so a stale configuration cannot authorize a
	// replacement member.
	if historical.peerToken != "old" || active.peerToken != "old" {
		t.Fatalf("process identity not shared: old=%q active=%q", historical.peerToken, active.peerToken)
	}
	if historical.members["n1"].PublicKey != old.Members[0].PublicKey || historical.members["n2"].PublicKey != old.Members[1].PublicKey || active.members["n1"].PublicKey != current.Members[0].PublicKey || active.members["n3"].PublicKey != current.Members[1].PublicKey {
		t.Fatal("configuration public identities leaked across pools")
	}
}

func TestBoundTransportSyncRoutesThroughCurrentConfiguration(t *testing.T) {
	historical := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{testMember("cluster", "a", "a"), testMember("cluster", "b", "b")}}
	current := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{testMember("cluster", "a", "a"), testMember("cluster", "d", "d")}}
	transport := NewTransport("cluster", "a", &historical, "a")
	defer transport.Close()
	transport.BindCore(testClusterResolver{current: current, slots: map[quepaxa.Slot]quepaxa.Cluster{1: historical}})
	peer, err := transport.transportForCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := peer.members["d"]; !ok {
		t.Fatal("current replacement cannot serve historical sync")
	}
	if _, ok := peer.members["b"]; ok {
		t.Fatal("historical membership selected for active sync")
	}
}

func TestBoundTransportCloseDoesNotCreateNewPools(t *testing.T) {
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{testMember("cluster", "n1", "token")}}
	transport := NewTransport("cluster", "n1", &config, "token")
	transport.BindCore(testClusterResolver{current: config})
	if _, err := transport.transportForCurrent(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.transportForCurrent(); !errors.Is(err, errTransportClosed) {
		t.Fatalf("post-close pool error = %v", err)
	}
}

func TestVerifyLearnerRejectsCurrentVoter(t *testing.T) {
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{testMember("cluster", "n1", "one"), testMember("cluster", "n2", "two")}}
	transport := NewTransport("cluster", "n1", &config, "one")
	defer transport.Close()
	if err := transport.VerifyLearner(context.Background(), config.Members[1], 1, quepaxa.ValueHash{1}); err == nil {
		t.Fatal("current voter accepted as learner")
	}
}

func TestDecodeWALIdentityRejectsNonCanonicalAndMismatchedValues(t *testing.T) {
	valid := strings.Repeat("a", 64)
	identity, err := decodeWALIdentity(valid)
	if err != nil || identity[0] != 0xaa {
		t.Fatalf("valid identity=%x err=%v", identity, err)
	}
	if _, err := decodeWALIdentity(strings.ToUpper(valid)); err == nil {
		t.Fatal("uppercase identity accepted")
	}
	if _, err := decodeWALIdentity(valid[:63]); err == nil {
		t.Fatal("short identity accepted")
	}
	wal, err := qlog.Open(t.TempDir() + "/qlog")
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	learner, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "n2", Cluster: quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := decodeWALIdentity(learner.WALIdentity())
	if err != nil {
		t.Fatal(err)
	}
	wrong := append([]byte(nil), actual[:]...)
	wrong[0] ^= 1
	if matchesWALIdentity(learner.WALIdentity(), wrong) {
		t.Fatal("mismatched learner WAL identity accepted")
	}
}

func TestDecisionCatchUpPageIsBoundedByEncodedBytes(t *testing.T) {
	member := testMember("cluster", "n1", "secret")
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	for slot := quepaxa.Slot(1); slot <= 256; slot++ {
		value := make([]byte, 8<<10)
		value[0] = byte(slot)
		if _, _, err := core.Propose(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	response, err := serverPeerHandleDecisions(server, member, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Decisions) == 0 || len(response.Decisions) >= 256 || len(encodePeerResponse(response)) > maxPeerFrame {
		t.Fatalf("decisions=%d encoded=%d", len(response.Decisions), len(encodePeerResponse(response)))
	}
	next, err := serverPeerHandleDecisions(server, member, uint64(len(response.Decisions)+1))
	if err != nil || len(next.Decisions) == 0 {
		t.Fatalf("next page decisions=%d err=%v", len(next.Decisions), err)
	}
}

func TestFetchDecisionsPreservesCompactedError(t *testing.T) {
	member := testMember("cluster", "n1", "secret")
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if _, _, err := core.Propose(context.Background(), []byte("state")); err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(1)
	if !ok {
		t.Fatal("missing checkpoint prefix")
	}
	order, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: core.ConfigID(), Index: 1, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: prefix, NextLeaderOrder: order, FollowingLeaderOrder: following}
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(1, seal.RootHash); err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, nil, "cluster", true, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "secret", "admin-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	member.PeerURL = "quic://" + peer.Addr()
	transport := NewTransport("cluster", member.ID, &quepaxa.Cluster{Members: []quepaxa.Member{member}}, "secret")
	defer transport.Close()
	if _, err := transport.FetchDecisions(ctx, member.ID, 1, 1); !errors.Is(err, quepaxa.ErrCompacted) {
		t.Fatalf("fetch error = %v, want %v", err, quepaxa.ErrCompacted)
	}
}

func serverPeerHandleDecisions(server *Server, member quepaxa.Member, from uint64) (*peerfb.ResponseT, error) {
	peer := &PeerServer{server: server, members: map[quepaxa.NodeID]quepaxa.Member{member.ID: member}}
	return peer.handle(context.Background(), ed25519.PublicKey(member.PublicKey[:]), &peerfb.RequestT{
		Operation: peerfb.OperationSync, ClusterId: string(server.cluster), SenderId: string(member.ID),
		ConfigId: uint64(server.core.ConfigID()), From: from, Limit: 128,
	})
}

func TestPeerTransportRetainsUDPPortAcrossRestart(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	bound := &quic.Transport{Conn: conn}
	defer bound.Close()
	members := []quepaxa.Member{testMember("cluster", "n1", "voter")}
	members[0].PeerURL = "quic://" + conn.LocalAddr().String()
	core := mustCore(t, "n1", members, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 2 {
		peer, err := StartPeerServerOnTransport(ctx, bound, server, members, "voter", "admin")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
		config := core.CurrentCluster()
		client := NewTransport("cluster", "n1", &config, "voter")
		_, callErr := client.ReadTip(ctx, "n1")
		_ = client.Close()
		if callErr != nil {
			t.Fatal(callErr)
		}
		if err := peer.Close(); err != nil {
			t.Fatal(err)
		}
		other, err := net.ListenPacket("udp", conn.LocalAddr().String())
		if err == nil {
			_ = other.Close()
			t.Fatal("closing a listener released the reserved UDP port")
		}
	}
}
