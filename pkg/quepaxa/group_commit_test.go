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

// Done is first evaluated by syncBatch after registration and wake publication.
// This makes batch membership observable without sleeps or polling internals.
type groupCommitJoinedContext struct {
	context.Context
	joined chan struct{}
	once   sync.Once
}

func (c *groupCommitJoinedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	return c.Context.Done()
}

func TestGroupCommitImmediateSharesBatchAndErrors(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "sync-error"}[fail], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var calls atomic.Int32
			want := errors.New("actual shared sync failure")
			commit := newGroupCommit(func() error {
				calls.Add(1)
				if fail {
					return want
				}
				return nil
			})
			commit.delay = time.Hour
			results := make(chan error, 2)
			for range 2 {
				joined := &groupCommitJoinedContext{Context: ctx, joined: make(chan struct{})}
				go func() { results <- commit.Sync(joined) }()
				select {
				case <-joined.joined:
				case <-ctx.Done():
					t.Fatal("ordinary waiter did not join", ctx.Err())
				}
			}
			immediateErr := commit.syncBatch(ctx, true)
			for _, err := range []error{immediateErr, <-results, <-results} {
				if fail && !errors.Is(err, want) || !fail && err != nil {
					t.Fatalf("shared sync error=%v fail=%t", err, fail)
				}
			}
			if calls.Load() != 1 {
				t.Fatal("immediate waiter bypassed coalescing", calls.Load())
			}
		})
	}
}

func TestGroupCommitImmediateCancellationDrainsAndKeepsNextBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	commit := newGroupCommit(func() error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	commit.delay = time.Hour
	caller, stop := context.WithCancel(ctx)
	first := make(chan error, 1)
	go func() { first <- commit.syncBatch(caller, true) }()
	select {
	case <-entered:
	case <-ctx.Done():
		close(release)
		t.Fatal("immediate sync did not enter callback", ctx.Err())
	}
	stop()
	firstErr := <-first
	// Register B while A is physically syncing; wake B twice to exercise the
	// bounded existing wake channel without creating a second sync worker.
	second, third := make(chan error, 1), make(chan error, 1)
	for _, result := range []chan error{second, third} {
		joined := &groupCommitJoinedContext{Context: ctx, joined: make(chan struct{})}
		go func() { result <- commit.syncBatch(joined, true) }()
		select {
		case <-joined.joined:
		case <-ctx.Done():
			close(release)
			t.Fatal("next immediate batch did not register", ctx.Err())
		}
	}
	prematureCalls := calls.Load()
	close(release)
	secondErr, thirdErr := <-second, <-third
	if !errors.Is(firstErr, context.Canceled) || secondErr != nil || thirdErr != nil {
		t.Fatalf("cancel/next results=%v/%v/%v", firstErr, secondErr, thirdErr)
	}
	if prematureCalls != 1 || calls.Load() != 2 {
		t.Fatalf("WAL Sync was not serial/coalesced: before release=%d total=%d", prematureCalls, calls.Load())
	}
}
