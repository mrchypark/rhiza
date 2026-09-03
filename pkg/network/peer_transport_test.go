package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestQUICFlatBuffersRecordRoundTrip(t *testing.T) {
	member := quepaxa.Member{ID: "n1", Token: "secret"}
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member, {ID: "n2", Token: "admin-secret"}}, "admin-secret"); peer != nil || err == nil {
		t.Fatalf("reused non-local voter token peer=%v error=%v", peer, err)
	}
	if peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member, {ID: "n2"}}, "admin-secret"); peer != nil || err == nil {
		t.Fatalf("missing voter token peer=%v error=%v", peer, err)
	}
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "admin-secret")
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
	// Record is physically durable, so it must wait for the handshake even on
	// a resumed connection. The connection may still report 0-RTT capability;
	// the operation policy below controls when its stream may be opened.
	if allows0RTT(peerfb.OperationRecord) {
		t.Fatal("durable Record unexpectedly allowed QUIC 0-RTT")
	}
	// A restarted peer has lost its TLS ticket keys. The first Sync is therefore
	// attempted as 0-RTT and rejected; transport must promote the connection and
	// replay it before this periodic catch-up round is reported as failed.
	oldConn := transport.peers[member.ID].conn
	transport.invalidate(member.ID, oldConn)
	_ = oldConn.CloseWithError(0, "peer restart")
	replacement, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "admin-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	member.PeerURL = "quic://" + replacement.Addr()
	transport.members[member.ID] = member
	if _, err := transport.FetchDecisions(callCtx, member.ID, 1, 1); err != nil {
		t.Fatalf("Sync after 0-RTT rejection did not retry at 1-RTT: %v", err)
	}
	if transport.peers[member.ID].conn.ConnectionState().Used0RTT {
		t.Fatal("restarted peer unexpectedly accepted its previous 0-RTT ticket")
	}
	wrongMember := member
	wrongMember.Token = "wrong"
	wrong := NewTransport("cluster", "n1", &quepaxa.Cluster{Members: []quepaxa.Member{wrongMember}}, "wrong")
	defer wrong.Close()
	if _, err := wrong.SendRecord(callCtx, member.ID, request); err == nil {
		t.Fatal("peer with the wrong token-bound certificate identity was accepted")
	}
}

func TestAllows0RTTOnlyForReadOperations(t *testing.T) {
	allowed := map[peerfb.Operation]bool{
		peerfb.OperationSync:       true,
		peerfb.OperationReadIndex:  true,
		peerfb.OperationFetchValue: true,
	}
	for operation := range peerfb.EnumNamesOperation {
		if got := allows0RTT(operation); got != allowed[operation] {
			t.Fatalf("allows0RTT(%s) = %v, want %v", operation, got, allowed[operation])
		}
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

func TestNonMemberLearnerMayOnlyFetchCertifiedDecisions(t *testing.T) {
	member := quepaxa.Member{ID: "n1", Token: "voter-token"}
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	peer := &PeerServer{server: server, members: map[quepaxa.NodeID]quepaxa.Member{member.ID: member}, token: "learner-token"}
	request := &peerfb.RequestT{
		Operation: peerfb.OperationSync, ClusterId: "cluster", SenderId: "learner-1",
		ConfigId: uint64(core.ConfigID()), Token: "learner-token", From: 1, Limit: 1,
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
	request.Operation, request.Token = peerfb.OperationSync, "wrong"
	if _, err := peer.handle(context.Background(), nil, request); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("learner wrong-token error=%v", err)
	}
}

func TestPeerServerRejectsMutatingEarlyData(t *testing.T) {
	member := quepaxa.Member{ID: "n1", Token: "secret"}
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	peer := &PeerServer{server: server, members: map[quepaxa.NodeID]quepaxa.Member{member.ID: member}, token: member.Token}
	for operation := range peerfb.EnumNamesOperation {
		if allows0RTT(operation) {
			continue
		}
		_, err := peer.handle(context.Background(), nil, &peerfb.RequestT{
			Operation: operation,
			ClusterId: string(server.cluster),
			SenderId:  string(member.ID),
			ConfigId:  uint64(core.ConfigID()),
			Token:     member.Token,
		})
		if err == nil || !strings.Contains(err.Error(), "not accepted as replayable early data") {
			t.Fatalf("operation %s early-data error = %v", operation, err)
		}
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
	if _, err := transport.connection(ctx, member.ID, false); err == nil {
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

func TestDecisionCatchUpPageIsBoundedByEncodedBytes(t *testing.T) {
	member := quepaxa.Member{ID: "n1", Token: "secret"}
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
	member := quepaxa.Member{ID: "n1", Token: "secret"}
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
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "admin-secret")
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
	peer := &PeerServer{server: server, members: map[quepaxa.NodeID]quepaxa.Member{member.ID: member}, token: member.Token}
	return peer.handle(context.Background(), nil, &peerfb.RequestT{
		Operation: peerfb.OperationSync, ClusterId: string(server.cluster), SenderId: string(member.ID),
		ConfigId: uint64(server.core.ConfigID()), Token: member.Token, From: from, Limit: 128,
	})
}
