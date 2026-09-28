package quepaxa

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalExecutionOwnerSerializesProposeAndRecovery(t *testing.T) {
	for _, first := range []string{"propose", "recovery"} {
		t.Run(first, func(t *testing.T) {
			core, wal := localReserveCore(t, t.TempDir(), "local")
			defer wal.Close()
			started, release, secondRecord := makeObservedRecordBarrier(core)
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			firstResult := make(chan error, 1)
			go func() {
				if first == "propose" {
					_, _, err := core.Propose(context.Background(), []byte("first"))
					firstResult <- err
					return
				}
				firstResult <- core.RecoverThrough(context.Background(), 1)
			}()
			awaitRecordBarrier(t, started, firstResult)

			ctx, cancel := context.WithCancel(context.Background())
			secondResult := make(chan error, 1)
			go func() {
				if first == "propose" {
					secondResult <- core.RecoverThrough(ctx, 1)
					return
				}
				_, _, err := core.Propose(ctx, []byte("second"))
				secondResult <- err
			}()
			select {
			case <-secondRecord:
				t.Fatal("second local operation reached Record while first held the execution owner")
			default:
			}
			select {
			case err := <-secondResult:
				t.Fatalf("second local operation returned before owner release: %v", err)
			case <-secondRecord:
				t.Fatal("second local operation entered Record before owner release")
			case <-time.After(50 * time.Millisecond):
			}
			cancel()
			select {
			case err := <-secondResult:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("second local operation error=%v, want cancellation", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("second local operation did not observe cancellation while waiting for owner")
			}

			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-firstResult:
				if err != nil {
					t.Fatalf("first local operation: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("first local operation did not finish after release")
			}
		})
	}
}

func TestLocalProposeRecoversPendingRecorderBeforeAllocating(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	proposal := newProposal(highestPriority, "local", []byte("already recorded"))
	if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if core.RecorderTip() <= core.Tip() {
		t.Fatalf("test setup lacks pending recorder state: recorder=%d tip=%d", core.RecorderTip(), core.Tip())
	}
	// This recovery runs inside propose while it owns localExecutionOwner.
	// Reacquiring that owner here would deadlock.
	if slot, _, err := core.Propose(context.Background(), []byte("new value")); err != nil {
		t.Fatal(err)
	} else if slot != 2 {
		t.Fatalf("proposal slot=%d, want 2 after recovering slot 1", slot)
	}
	if core.Tip() != 2 {
		t.Fatalf("tip=%d, want recovered pending slot plus new proposal", core.Tip())
	}
}

func TestLocalPendingRecoveryFailureOrCancellationDoesNotAllocate(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local")
		defer wal.Close()
		proposal := newProposal(highestPriority, "local", []byte("pending"))
		if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err != nil {
			t.Fatal(err)
		}
		started, release := makeOneShotRecordBarrier(core)
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, _, err := core.Propose(ctx, []byte("must wait"))
			result <- err
		}()
		awaitRecordBarrier(t, started, result)
		cancel()
		releaseOnce.Do(func() { close(release) })
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Propose error=%v, want cancellation", err)
		}
		if got := core.nextSlot; got != 1 {
			t.Fatalf("next slot=%d, want no allocation before recovery", got)
		}
	})

	t.Run("recovery error", func(t *testing.T) {
		core, wal := localReserveCore(t, t.TempDir(), "local")
		proposal := newProposal(highestPriority, "local", []byte("pending"))
		if _, err := localRecordForTest(t, core, RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err != nil {
			t.Fatal(err)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := core.Propose(context.Background(), []byte("must not allocate")); err == nil {
			t.Fatal("Propose succeeded despite failed pending recovery")
		}
		if got := core.nextSlot; got != 1 {
			t.Fatalf("next slot=%d, want no allocation after failed recovery", got)
		}
	})
}

func TestNonLocalExecutionDoesNotUseLocalOwner(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	core.localMode = false
	core.localExecutionOwner <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := core.acquireLocalExecution(ctx); err != nil {
		t.Fatalf("non-local acquire error=%v, want bypass", err)
	}
	<-core.localExecutionOwner
}

func TestLocalPublicDurabilityHelpersDoNotReenterOwner(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if slot, _, err := core.Propose(context.Background(), []byte("durability helpers")); err != nil || slot != 1 {
		t.Fatalf("Propose slot=%d err=%v", slot, err)
	}
	if err := core.EnsureDurable(1); err != nil {
		t.Fatalf("EnsureDurable: %v", err)
	}
	if err := core.EnsureDurableThrough(context.Background(), 1); err != nil {
		t.Fatalf("EnsureDurableThrough: %v", err)
	}
	if _, err := core.CompleteDecision(context.Background(), 1); err != nil {
		t.Fatalf("CompleteDecision: %v", err)
	}
	if _, err := core.DurablePrefix(1); err != nil {
		t.Fatalf("DurablePrefix: %v", err)
	}
}

func makeOneShotRecordBarrier(core *Core) (started, release chan struct{}) {
	started, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	core.recordBeforeAppend = func() {
		once.Do(func() {
			close(started)
			<-release
		})
	}
	return started, release
}

func makeObservedRecordBarrier(core *Core) (started, release, second chan struct{}) {
	started, release, second = make(chan struct{}), make(chan struct{}), make(chan struct{})
	var secondOnce sync.Once
	var calls atomic.Int32
	core.recordBeforeAppend = func() {
		if calls.Add(1) > 1 {
			secondOnce.Do(func() { close(second) })
			return
		}
		close(started)
		<-release
	}
	return started, release, second
}
