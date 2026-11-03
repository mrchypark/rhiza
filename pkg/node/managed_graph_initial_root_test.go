package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedGraphInitialRootWaitCancellation(t *testing.T) {
	n := New(&types.ExecutionConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	// A waiter must not acquire the publisher's singleflight lock or launch
	// another producer while the existing publisher owns it.
	n.compactionMu.Lock()
	finished := make(chan error, 1)
	go func() { finished <- n.ensureManagedGraphInitialRoot(ctx) }()
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		n.compactionMu.Unlock()
		t.Fatal("initial root waiter did not cancel while publisher owns compaction lock")
	}
	n.compactionMu.Unlock()
	if n.managedGraphInitialRootComplete() {
		t.Fatal("cancellation marked an unauthenticated root complete")
	}
	if err := n.ensureManagedGraphInitialRoot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled caller lost its cause", err)
	}
}
