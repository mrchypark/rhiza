//go:build !graph

package network

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
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
	server := NewServer(core, material, "cluster", true, nil, []quepaxa.Member{member}, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer, err := StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	member.PeerURL = "quic://" + peer.Addr()
	transport := NewTransport("cluster", "n1", &quepaxa.Cluster{Members: []quepaxa.Member{member}}, "secret")
	defer transport.Close()
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	request := quepaxa.RecordRequest{Slot: 1, Step: 4, Proposal: quepaxa.Proposal{ProposerID: "n1", Value: []byte("value")}}
	request.Proposal.Priority[31] = 1
	request.Proposal.Hash = sha256.Sum256(request.Proposal.Value)
	summary, err := transport.SendRecord(callCtx, member.ID, request)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RecorderID != member.ID || summary.Step != request.Step {
		t.Fatalf("unexpected summary: %+v", summary)
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
	transport.release(member.ID, first)
	request.Step++
	if _, err := transport.SendRecord(callCtx, member.ID, request); err != nil {
		t.Fatalf("request after stream cancellation: %v", err)
	}
	transport.drop(member.ID, first)
	request.Step++
	if _, err := transport.SendRecord(callCtx, member.ID, request); err != nil {
		t.Fatal(err)
	}
	if !transport.peers[member.ID].conn.ConnectionState().Used0RTT {
		t.Fatal("replay-safe Record did not use QUIC 0-RTT")
	}
	wrongMember := member
	wrongMember.Token = "wrong"
	wrong := NewTransport("cluster", "n1", &quepaxa.Cluster{Members: []quepaxa.Member{wrongMember}}, "wrong")
	defer wrong.Close()
	if _, err := wrong.SendRecord(callCtx, member.ID, request); err == nil {
		t.Fatal("peer with the wrong token-bound certificate identity was accepted")
	}
}

func TestPeerCodecRejectsMalformedFrame(t *testing.T) {
	if _, err := decodePeerRequest([]byte("not-flatbuffers")); err == nil {
		t.Fatal("malformed frame accepted")
	}
}
