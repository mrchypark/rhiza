package network

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestCatchUpTipAdvanceDoesNotFetchBeyondThrough(t *testing.T) {
	config, values := catchUpValues(t, 16)
	const through = quepaxa.Slot(16)
	var requested atomic.Uint64
	peer := startCatchUpPeer(t, "a", values, func(_ context.Context, request *peerfb.RequestT) error { requested.Store(request.From); return nil })
	transport := NewTransport("cluster", "follower", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{peer.member}}, "admin")
	transport.SetAdminToken("admin")
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Error(err)
		}
	})
	follower, wal := newCatchUpFollower(t, filepath.Join(t.TempDir(), "receiver"), config, transport, false)
	t.Cleanup(func() { follower.Close(); _ = wal.Close() })
	if err := follower.core.AcceptCertifiedValues(values[:15]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	advance := make(chan struct{})
	advanced := make(chan error, 1)
	var advanceWG sync.WaitGroup
	advanceWG.Add(1)
	go func() {
		defer advanceWG.Done()
		select {
		case <-advance:
			advanced <- follower.core.AcceptCertifiedValues(values[15:])
		case <-ctx.Done():
			advanced <- ctx.Err()
		}
	}()
	defer func() { cancel(); advanceWG.Wait() }()
	var once sync.Once
	readTip := func() quepaxa.Slot {
		before := follower.core.Tip()
		once.Do(func() {
			close(advance)
			if err := <-advanced; err != nil {
				t.Errorf("actual concurrent certified delivery: %v", err)
			}
		})
		return before
	}
	err := follower.catchUpFromTip(ctx, "a", through, true, readTip)
	if err != nil || requested.Load() > uint64(through) {
		t.Fatalf("catch-up requested from=%d through=%d receiver-tip=%d source-tip=%d: %v", requested.Load(), through, follower.core.Tip(), len(values), err)
	}
	if requested.Load() != 16 || follower.core.Tip() != through {
		t.Fatalf("valid neighboring fetch/tip=%d/%d", requested.Load(), follower.core.Tip())
	}
	if _, err := follower.core.DurablePrefix(through); err != nil {
		t.Fatalf("receiver prefix not durable: %v", err)
	}
}

func TestCatchUpTipSnapshotStillRejectsMissingDecision(t *testing.T) {
	config, values := catchUpValues(t, 16)
	peer := startCatchUpPeer(t, "a", values[:15], nil)
	transport := NewTransport("cluster", "follower", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{peer.member}}, "admin")
	transport.SetAdminToken("admin")
	t.Cleanup(func() { _ = transport.Close() })
	follower, wal := newCatchUpFollower(t, filepath.Join(t.TempDir(), "receiver"), config, transport, false)
	t.Cleanup(func() { follower.Close(); _ = wal.Close() })
	if err := follower.core.AcceptCertifiedValues(values[:15]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := follower.catchUpFrom(ctx, "a", 16, true)
	if err == nil || !strings.Contains(err.Error(), "omitted decision slot 16") || follower.core.Tip() != 15 {
		t.Fatalf("actual missing decision was not refused: tip=%d err=%v", follower.core.Tip(), err)
	}
}
