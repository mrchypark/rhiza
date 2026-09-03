package quepaxa

import (
	"context"
	"errors"
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

func TestGroupCommitBroadcastsSyncError(t *testing.T) {
	want := errors.New("sync failed")
	var calls atomic.Int32
	commit := newGroupCommit(func() error {
		if calls.Add(1) == 1 {
			return want
		}
		return nil
	})
	commit.delay = 1 << 62
	commit.max = 8

	errs := make(chan error, commit.max)
	for range commit.max {
		go func() { errs <- commit.Sync(context.Background()) }()
	}
	for range commit.max {
		if err := <-errs; !errors.Is(err, want) {
			t.Fatalf("sync error=%v, want %v", err, want)
		}
	}

	commit.max = 1
	if err := commit.Sync(context.Background()); err != nil {
		t.Fatalf("next batch sync: %v", err)
	}
}

func TestGroupCommitSeparatesWaitersDuringSync(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	commit := newGroupCommit(func() error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	commit.delay = 1 << 62
	commit.max = 1

	first := make(chan error, 1)
	go func() { first <- commit.Sync(context.Background()) }()
	<-started
	second := make(chan error, 1)
	go func() { second <- commit.Sync(context.Background()) }()
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("sync calls=%d, want 2", got)
	}
}

func TestGroupCommitCancellationDoesNotStrandNextBatch(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	commit := newGroupCommit(func() error {
		select {
		case <-started:
		default:
			close(started)
			<-release
		}
		return nil
	})
	commit.delay = 1 << 62
	commit.max = 1

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- commit.Sync(ctx) }()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sync=%v, want %v", err, context.Canceled)
	}
	close(release)

	next, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := commit.Sync(next); err != nil {
		t.Fatalf("next batch sync: %v", err)
	}
}
