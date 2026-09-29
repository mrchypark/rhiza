package network

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNotifyCancellationReportsUnknownWithRequestID(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/state.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	server.SetDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		close(entered)
		<-release
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := server.NotifyPublish(ctx, types.NotifyCommand{RequestID: "cancel-notify", Topic: "topic", Payload: []byte("event")})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not reach durability barrier")
	}
	cancel()
	select {
	case err := <-done:
		var unknown *CommitUnknownError
		if !errors.As(err, &unknown) || unknown.RequestID != "cancel-notify" || unknown.Slot != 0 || unknown.RetryThroughSlot != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("expected unknown outcome with request ID and cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call did not cancel")
	}
	admissionCount := func() int {
		server.mutationMu.Lock()
		defer server.mutationMu.Unlock()
		return server.mutationAdmission.count
	}
	if got := admissionCount(); got != 1 {
		t.Fatalf("canceled caller released proposal-owned mutation lease: count=%d", got)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for admissionCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := admissionCount(); got != 0 {
		t.Fatalf("proposal completion leaked mutation lease: count=%d", got)
	}
	receipt, found, err := material.MutationReceipt(context.Background(), types.MutationNotify, "cancel-notify")
	if err != nil || !found || receipt.Slot == 0 {
		t.Fatalf("receipt=%+v found=%v err=%v", receipt, found, err)
	}
	// Cancellation before admission must not be classified as an uncertain commit.
	_, err = server.NotifyPublish(ctx, types.NotifyCommand{RequestID: "never-submitted", Topic: "topic"})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrCommitUnknown) {
		t.Fatalf("pre-admission cancellation: %v", err)
	}
}

func TestQUICCancellationReleasesStreamAndPreservesConnection(t *testing.T) {
	for _, prepare := range []bool{false, true} {
		name := "rpc"
		if prepare {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			members := []quepaxa.Member{testMember("cluster", "n1", "secret1"), testMember("cluster", "n2", "secret2")}
			core := mustCore(t, "n1", members, nil, nil)
			material, err := materializer.Open(t.TempDir()+"/state.sqlite", 1)
			if err != nil {
				t.Fatal(err)
			}
			defer material.Close()
			server := NewServer(core, material, "cluster", true, nil)
			defer server.Close()
			entered, released, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
			server.SetCheckpointPrepare(func(ctx context.Context, _ quepaxa.NodeID, _ quepaxa.CheckpointSeal) error {
				close(entered)
				select {
				case <-cleanup:
				case <-ctx.Done():
				}
				close(released)
				return ctx.Err()
			})
			peer, err := StartPeerServer(context.Background(), "127.0.0.1:0", server, members, "secret1", "admin")
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			defer close(cleanup)
			members[0].PeerURL = "quic://" + peer.Addr()
			transport := NewTransport("cluster", "n2", &quepaxa.Cluster{Members: members}, "secret2")
			defer transport.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			conn, err := transport.connection(ctx, "n1")
			if err != nil {
				t.Fatal(err)
			}
			transport.release("n1", conn)
			seal := quepaxa.CheckpointSeal{Index: 1, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: [32]byte{3}, NextLeaderOrder: []quepaxa.NodeID{"n1", "n2"}}
			value, err := quepaxa.EncodeCheckpointSeal(seal)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				if prepare {
					done <- transport.PrepareCheckpoint(ctx, seal)
					return
				}
				req := transport.request(peerfb.OperationPrepareCheckpoint)
				req.Value = value
				_, err := transport.callContext(ctx, "n1", req)
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("checkpoint handler was not reached")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("canceled call remained blocked")
			}
			select {
			case <-released:
			case <-time.After(5 * time.Second):
				t.Fatal("peer stream remained active after cancellation")
			}
			if conn.Context().Err() != nil {
				t.Fatal("cancellation closed the shared connection")
			}
			if _, err := transport.ReadTip(context.Background(), "n1"); err != nil {
				t.Fatalf("subsequent request: %v", err)
			}
			transport.peers["n1"].mu.Lock()
			reused := transport.peers["n1"].conn == conn
			transport.peers["n1"].mu.Unlock()
			if !reused {
				t.Fatal("cancellation discarded the shared connection")
			}
		})
	}
}
