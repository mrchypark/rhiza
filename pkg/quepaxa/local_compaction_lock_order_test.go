package quepaxa

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPrepareCheckpointOwnsLocalExecutionWhileWaitingForCheckpointLock(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	core.checkpointMu.Lock()
	locked := true
	defer func() {
		if locked {
			core.checkpointMu.Unlock()
		}
	}()
	finished := make(chan error, 1)
	go func() { finished <- core.PrepareCheckpoint(context.Background(), CheckpointSeal{}) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(core.localExecutionOwner) == 0 {
		select {
		case err := <-finished:
			t.Fatalf("prepare returned before checkpoint lock: %v", err)
		case <-deadline.C:
			t.Fatal("prepare did not acquire Local execution owner")
		case <-tick.C:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := core.acquireLocalExecution(ctx); !errors.Is(err, context.DeadlineExceeded) {
		if err == nil {
			core.releaseLocalExecution()
		}
		t.Fatalf("owner acquisition while prepare waits on checkpoint lock: %v", err)
	}
	core.checkpointMu.Unlock()
	locked = false
	select {
	case <-finished:
	case <-deadline.C:
		t.Fatal("prepare did not finish after unlock")
	}
}

// A drain cannot complete if a proposal waits for an owner held by prepare,
// while prepare waits for the compaction's checkpoint lock.
func TestLocalCompactionDrainBlockedByPrepareOwner(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	core.checkpointMu.Lock()
	locked := true
	defer func() {
		if locked {
			core.checkpointMu.Unlock()
		}
	}()
	prepared := make(chan error, 1)
	go func() { prepared <- core.PrepareCheckpoint(context.Background(), CheckpointSeal{}) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(core.localExecutionOwner) == 0 {
		select {
		case err := <-prepared:
			t.Fatalf("prepare returned too early: %v", err)
		case <-deadline.C:
			t.Fatal("prepare did not acquire owner")
		case <-tick.C:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	proposed := make(chan error, 1)
	go func() { _, _, err := core.Propose(ctx, []byte("foreground")); proposed <- err }()
	select {
	case err := <-proposed:
		t.Fatalf("proposal completed while owner was held: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	core.checkpointMu.Unlock()
	locked = false
	select {
	case <-prepared:
	case <-deadline.C:
		t.Fatal("prepare did not finish")
	}
	select {
	case err := <-proposed:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("proposal did not finish")
	}
}
