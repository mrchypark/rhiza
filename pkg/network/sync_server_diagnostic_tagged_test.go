//go:build rhiza_local_testhooks && rhiza_latency_diag

package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestSyncServerBoundaryCancellation(t *testing.T) {
	for _, phase := range []string{"network:sync:phase=entered:", "network:sync:phase=before-response-write", "core-lock"} {
		t.Run(phase, func(t *testing.T) {
			cluster := newForegroundAPIPeers(t, t.TempDir())
			entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			finished := make(chan struct{}, 1)
			waiting := make(chan struct{}, 1)
			var enter, unblock sync.Once
			defer unblock.Do(func() { close(release) })
			restore := localtesthooks.Set(func(event string) {
				if event == "network:sync:phase=stream-finished" {
					select {
					case finished <- struct{}{}:
					default:
					}
					return
				}
				if phase == "core-lock" {
					if event == "core:sync:before-read-lock" {
						select {
						case waiting <- struct{}{}:
						default:
						}
						return
					}
					if !strings.Contains(event, "network:sync:phase=entered:node=n2:") {
						return
					}
					enter.Do(func() {
						go func() { cluster.cores["n2"].TestOnlyHoldSyncLock(entered, release); close(exited) }()
						<-entered
					})
					return
				}
				if !strings.Contains(event, phase) {
					return
				}
				enter.Do(func() { close(entered); <-release; close(exited) })
			})
			defer restore()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := cluster.transports[0].FetchDecisions(ctx, "n2", 1, 128); done <- err }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("server gate not reached")
			}
			if phase == "core-lock" {
				select {
				case <-waiting:
				case <-time.After(3 * time.Second):
					t.Fatal("sync never attempted core read lock")
				}
			}
			select {
			case err := <-done:
				var timeout net.Error
				if !errors.Is(err, context.DeadlineExceeded) && !(errors.As(err, &timeout) && timeout.Timeout()) {
					t.Fatalf("expected deadline: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("client did not honor deadline")
			}
			unblock.Do(func() { close(release) })
			select {
			case <-exited:
			case <-time.After(3 * time.Second):
				t.Fatal("server gate did not drain")
			}
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("original handler did not terminate")
			}
			restore()
			retryCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			if _, err := cluster.transports[0].FetchDecisions(retryCtx, "n2", 1, 128); err != nil {
				t.Fatalf("server did not recover: %v", err)
			}
			closed := make(chan error, 1)
			go func() { closed <- cluster.peerServers[1].Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("close: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("peer server close blocked")
			}
		})
	}
}

func TestSyncDiagnosticConcurrentRequestCorrelation(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	releaseFlush := latencydiag.HoldSyncTestFlush()
	defer releaseFlush()
	entered, releaseFirst, releaseSecond := make(chan struct{}, 2), make(chan struct{}), make(chan struct{})
	finished := make(chan struct{}, 2)
	var firstOnce, secondOnce sync.Once
	defer firstOnce.Do(func() { close(releaseFirst) })
	defer secondOnce.Do(func() { close(releaseSecond) })
	restore := localtesthooks.Set(func(event string) {
		if event == "network:sync:phase=stream-finished" {
			finished <- struct{}{}
			return
		}
		if strings.Contains(event, "network:sync:phase=entered:node=n2:") {
			entered <- struct{}{}
			if strings.HasSuffix(event, ":from=1") {
				<-releaseFirst
			} else {
				<-releaseSecond
			}
			return
		}
	})
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, 2)
	for _, from := range []uint64{1, 2} {
		go func(from uint64) {
			_, err := cluster.transports[0].FetchDecisions(ctx, "n2", quepaxa.Slot(from), 128)
			results <- err
		}(from)
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("request did not enter")
		}
	}
	firstOnce.Do(func() { close(releaseFirst) })
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("first handler did not finish")
	}
	secondOnce.Do(func() { close(releaseSecond) })
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("second handler did not finish")
	}
	retained, err := completeSyncEvidence()
	if err != nil {
		t.Fatal(err)
	}
	events := make(map[uint64]map[string]uint64)
	for _, e := range retained {
		if !strings.HasPrefix(e.Stage, "peer_sync_") {
			continue
		}
		if events[e.ID] == nil {
			events[e.ID] = make(map[string]uint64)
		}
		events[e.ID][e.Stage] = e.Slot
	}
	if len(events) != 2 {
		t.Fatalf("want two request ids, got %v", events)
	}
	seen := map[uint64]bool{}
	for id, stages := range events {
		slot := stages["peer_sync_receive_untrusted"]
		if id == 0 || (slot != 1 && slot != 2) || seen[slot] {
			t.Fatalf("invalid correlation id=%d stages=%v", id, stages)
		}
		seen[slot] = true
		for _, stage := range []string{"peer_sync_core_lock", "peer_sync_fetch", "peer_sync_prepare", "peer_sync_handle", "peer_sync_encode", "peer_sync_write"} {
			if got, ok := stages[stage]; !ok || got != slot {
				t.Fatalf("id=%d missing/mismatched %s: %v", id, stage, stages)
			}
		}
	}
}

func completeSyncEvidence() ([]latencydiag.SyncTestEvent, error) {
	events, dropped := latencydiag.SyncTestSnapshot()
	if dropped != 0 {
		return nil, fmt.Errorf("incomplete evidence: ring dropped %d events", dropped)
	}
	return events, nil
}

func TestSyncDiagnosticRejectsDroppedEvidence(t *testing.T) {
	release := latencydiag.HoldSyncTestFlush()
	defer release()
	for i := 0; i < 300; i++ {
		latencydiag.TraceSync("peer_sync_fetch", uint64(i+1), 1, 1, "test", nil, time.Now())
	}
	if _, err := completeSyncEvidence(); err == nil {
		t.Fatal("ring loss was accepted as complete")
	}
}
