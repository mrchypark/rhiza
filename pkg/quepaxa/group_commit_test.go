package quepaxa

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupCommitSharesOneSync(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	commit := newGroupCommit(func() error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	})
	commit.delay = 1 << 62
	var wait sync.WaitGroup
	wait.Add(recorderCommitMax)
	for range recorderCommitMax {
		go func() {
			defer wait.Done()
			if err := commit.Sync(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	<-started
	close(release)
	wait.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("sync calls=%d, want 1", got)
	}
}

func TestGroupCommitWakeTimerRaceMakesProgress(t *testing.T) {
	commit := newGroupCommit(func() error { return nil })
	commit.delay = time.Nanosecond
	commit.max = 1
	for i := 0; i < 10_000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := commit.Sync(ctx)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d stopped making progress: %v", i, err)
		}
	}
}
