package rhiza

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestReadReplicaPeerCursorAdvancesAfterCallerDeadline(t *testing.T) {
	unauthorizedErr := errors.New("peer authorization rejected")
	unavailableErr := errors.New("peer unavailable")
	var calls []quepaxa.NodeID
	firstPeerCalls := 0
	replica := newPeerCursorTestReplica(t, []network.PeerIdentity{
		{ID: "genesis-voter"},
		{ID: "current-voter"},
	}, func(ctx context.Context, peer quepaxa.NodeID, _ quepaxa.Slot, _ int) (network.DecisionsResponse, error) {
		calls = append(calls, peer)
		switch peer {
		case "genesis-voter":
			firstPeerCalls++
			if firstPeerCalls == 1 {
				<-ctx.Done()
				return network.DecisionsResponse{}, ctx.Err()
			}
			return network.DecisionsResponse{}, unavailableErr
		case "current-voter":
			return network.DecisionsResponse{}, unauthorizedErr
		default:
			return network.DecisionsResponse{}, errors.New("unexpected peer")
		}
	})
	replica.peerCursor = 1
	preCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := replica.syncPeer(preCanceled)
	if !errors.Is(err, context.Canceled) || replica.peerCursor != 1 || len(calls) != 0 {
		t.Fatalf("pre-canceled sync err=%v cursor=%d calls=%v; want canceled, unchanged cursor, and no fetch", err, replica.peerCursor, calls)
	}
	replica.peerCursor = 0

	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = replica.syncPeer(shortCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first short sync error = %v, want caller deadline", err)
	}
	if len(calls) != 1 || calls[0] != "genesis-voter" {
		t.Fatalf("short sync calls = %v, want only the silent first peer", calls)
	}

	calls = nil
	nextCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = replica.syncPeer(nextCtx)
	if !errors.Is(err, unauthorizedErr) {
		t.Fatalf("next sync error = %v, want authorization rejection from current peer", err)
	}
	if len(calls) == 0 || calls[0] != "current-voter" {
		t.Fatalf("next sync began with %v, want current-voter", calls)
	}
	if replica.status.Source != "" {
		t.Fatalf("unauthorized peer was marked as a sync source: %q", replica.status.Source)
	}
}

func TestReadReplicaPeerCursorUsesRoundSnapshotAfterFastFailures(t *testing.T) {
	var calls []quepaxa.NodeID
	replica := newPeerCursorTestReplica(t, []network.PeerIdentity{
		{ID: "peer-0"},
		{ID: "peer-1"},
		{ID: "peer-2"},
	}, func(_ context.Context, peer quepaxa.NodeID, _ quepaxa.Slot, _ int) (network.DecisionsResponse, error) {
		calls = append(calls, peer)
		return network.DecisionsResponse{}, errors.New("peer unavailable")
	})
	replica.peerCursor = 2

	err := replica.syncPeer(context.Background())
	if err == nil {
		t.Fatal("sync succeeded without a peer response")
	}
	want := []quepaxa.NodeID{"peer-2", "peer-0", "peer-1"}
	if !slices.Equal(calls, want) {
		t.Fatalf("round calls = %v, want snapshot order %v", calls, want)
	}
	if replica.peerCursor != 2 {
		t.Fatalf("cursor after full failed round = %d, want next position 2", replica.peerCursor)
	}
}

func TestReadReplicaPeerCursorContinuesAfterPeerRPCDeadline(t *testing.T) {
	var calls []quepaxa.NodeID
	unauthorizedErr := errors.New("peer authorization rejected")
	replica := newPeerCursorTestReplica(t, []network.PeerIdentity{
		{ID: "silent-peer"},
		{ID: "live-peer"},
	}, func(_ context.Context, peer quepaxa.NodeID, _ quepaxa.Slot, _ int) (network.DecisionsResponse, error) {
		calls = append(calls, peer)
		if peer == "silent-peer" {
			return network.DecisionsResponse{}, context.DeadlineExceeded
		}
		return network.DecisionsResponse{}, unauthorizedErr
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := replica.syncPeer(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("sync error=%v callerErr=%v; want per-peer timeout with live caller", err, ctx.Err())
	}
	if !slices.Equal(calls, []quepaxa.NodeID{"silent-peer", "live-peer"}) {
		t.Fatalf("same-round calls = %v, want silent then live peer", calls)
	}
	if replica.status.Source != "" {
		t.Fatalf("authorization rejection was marked as a sync source: %q", replica.status.Source)
	}
}

func newPeerCursorTestReplica(t *testing.T, peers []network.PeerIdentity, fetch func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (network.DecisionsResponse, error)) *ReadReplica {
	t.Helper()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID:  "reader",
		Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "bootstrap"}}},
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &ReadReplica{core: core, config: ReplicaConfig{Members: peers}, fetch: fetch}
}
