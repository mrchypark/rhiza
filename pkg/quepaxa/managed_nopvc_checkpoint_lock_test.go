//go:build rhiza_local_testhooks

package quepaxa

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestManagedNoPVCFixedMembershipCheckpointValidationWithQueuedWriter(t *testing.T) {
	core, _ := newManagedAppendOnlyTestCore(t, func(_ context.Context, _ ManagedCheckpointCandidate) error { return nil })
	seal := managedAppendOnlyTestSeal()
	seal.GenerationAnchorHash = core.GenerationAnchorHash()
	seal.NextLeaderOrder, seal.FollowingLeaderOrder, _ = core.CheckpointLeaderOrders(seal.Index)
	candidate := ManagedCheckpointCandidate{Seal: seal, DecisionSlot: seal.Index + 1, ConfigID: 1}

	locked, resume := make(chan struct{}), make(chan struct{})
	var hookOnce sync.Once
	restore := localtesthooks.Set(func(name string) {
		if name == "core:managed-checkpoint-fixed-membership-under-read-lock" {
			hookOnce.Do(func() {
				close(locked)
				<-resume
			})
		}
	})
	defer restore()
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(release)

	validation := make(chan error, 1)
	go func() { validation <- core.validateManagedCheckpointRootLocal(candidate) }()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("fixed-membership validation did not reach the under-read-lock boundary")
	}
	writerStarted, writerAcquired := make(chan struct{}), make(chan struct{})
	go func() {
		close(writerStarted)
		core.mu.Lock()
		close(writerAcquired)
		core.mu.Unlock()
	}()
	<-writerStarted
	deadline := time.Now().Add(2 * time.Second)
	for core.mu.TryRLock() {
		core.mu.RUnlock()
		if time.Now().After(deadline) {
			// Release the test boundary and let validation settle before failing.
			release()
			select {
			case <-validation:
			case <-time.After(time.Second):
			}
			t.Fatal("writer did not queue behind the fixed-membership validator read lock")
		}
		runtime.Gosched()
	}
	release()
	select {
	case err := <-validation:
		if err != nil {
			t.Fatalf("fixed-membership candidate rejected: %v", err)
		}
	case <-time.After(2 * time.Second):
		// Keep this Core private to the test. If the implementation regresses,
		// the blocked goroutine cannot affect other tests or external resources.
		t.Fatal("fixed-membership validation deadlocked behind a queued writer")
	}
	select {
	case <-writerAcquired:
	case <-time.After(2 * time.Second):
		t.Fatal("queued writer did not proceed after validation released Core.mu")
	}
}
