package network

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnedOwnedApplyExpiredDeadlineUntagged(t *testing.T) {
	server := NewServer(nil, nil, "test", true, nil)
	defer server.Close()
	ctx := context.WithValue(context.Background(), peerStreamLifetimeKey{}, peerStreamLifetime{ctx: context.Background(), deadline: time.Now().Add(-time.Second)})
	if err := server.applyLearnedOwned(ctx, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired deadline: %v", err)
	}
}

func TestLearnedQualificationUsesOneDeadlineAndLocalLifetimes(t *testing.T) {
	for _, mode := range []string{"deadline", "stream", "peer", "server", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			stream, stopStream := context.WithCancel(context.Background())
			defer stopStream()
			peer, stopPeer := context.WithCancel(context.Background())
			defer stopPeer()
			server, stopServer := context.WithCancel(context.Background())
			defer stopServer()
			deadline := time.Now().Add(50 * time.Millisecond)
			qualified, finish := learnedQualificationContext(stream, peer, server, deadline)
			defer finish()
			if got, ok := qualified.Deadline(); !ok || !got.Equal(deadline) {
				t.Fatalf("deadline=%v known=%t expected=%v", got, ok, deadline)
			}
			switch mode {
			case "stream":
				stopStream()
			case "peer":
				stopPeer()
			case "server":
				stopServer()
			case "cleanup":
				finish()
			}
			select {
			case <-qualified.Done():
			case <-time.After(time.Second):
				t.Fatal("qualification did not terminate")
			}
			want := context.Canceled
			if mode == "deadline" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(qualified.Err(), want) {
				t.Fatalf("qualification err=%v want=%v", qualified.Err(), want)
			}
		})
	}
}

func TestLearnedOwnedStreamUntaggedRealClock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	response, err := p.servers["n1"].KVPut(ctx, KVMutationRequest{RequestID: "clock-check", Key: "clock-check", Value: []byte("fixed")})
	if err != nil || response.Status != types.MutationCommitted || response.Slot == 0 {
		t.Fatalf("real peer quorum: response=%+v err=%v", response, err)
	}
	for _, id := range []quepaxa.NodeID{"n2", "n3"} {
		receipt, found, err := p.materials[id].MutationReceipt(ctx, types.MutationKV, "clock-check")
		if err == nil && found && receipt.Status == types.MutationCommitted && receipt.Slot == response.Slot {
			return
		}
	}
	t.Fatal("no peer applied the acknowledged learned decision")
}

func TestOrdinaryApplyAlreadySatisfiedIgnoresCanceledContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	response, err := p.servers["n1"].KVPut(ctx, KVMutationRequest{RequestID: "ordinary-check", Key: "ordinary-check", Value: []byte("fixed")})
	if err != nil || response.Slot == 0 {
		t.Fatalf("prepare applied decision: response=%+v err=%v", response, err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := p.servers["n1"].applyDecisions(canceled, quepaxa.Slot(response.Slot)); err != nil {
		t.Fatalf("ordinary already-applied result changed: %v", err)
	}
}
