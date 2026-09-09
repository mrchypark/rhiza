package network

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLockCatchUpCancellationAndCleanup(t *testing.T) {
	s := &Server{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Step 1: Acquire source A -- must succeed immediately.
	unlockA, errA := s.lockCatchUp(ctx, quepaxa.NodeID("a"))
	if errA != nil {
		t.Fatalf("acquire A: %v", errA)
	}

	// Step 2: Source B must acquire independently (different key).
	unlockB, errB := s.lockCatchUp(ctx, quepaxa.NodeID("b"))
	if errB != nil {
		t.Fatalf("acquire B: %v", errB)
	}

	// Observe the actual wait select before cancellation.
	waitCtx, stopWaiter := context.WithCancel(ctx)
	defer stopWaiter()
	waiting := &catchUpWaitingContext{Context: waitCtx, seen: make(chan struct{})}
	waiterResult := make(chan error, 1)
	go func() {
		unlock, err := s.lockCatchUp(waiting, "a")
		if unlock != nil {
			unlock()
		}
		waiterResult <- err
	}()
	select {
	case <-waiting.seen:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopWaiter()
	if err := awaitCatchUp(t, ctx, waiterResult); err != context.Canceled {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	// Step 4: Release A, then reacquire -- proves cleanup of the map entry.
	unlockA()
	unlockA2, errA2 := s.lockCatchUp(ctx, quepaxa.NodeID("a"))
	if errA2 != nil {
		t.Fatalf("reacquire A: %v", errA2)
	}
	unlockA2()

	// Step 5: Release everything, assert syncSources empty.
	unlockB()

	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if len(s.syncSources) != 0 {
		t.Fatalf("syncSources not empty after all releases: %v", s.syncSources)
	}
}
