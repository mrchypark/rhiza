//go:build rhiza_local_testhooks

package network

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestRequestLocksConcurrentFirstMissCreatesSingleEntry(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(server.Close)
	if got := requestLockEntryCount(server); got != 0 {
		t.Fatalf("new server allocated %d request locks, want 0", got)
	}

	const waiters = 32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	misses := make(chan struct{}, waiters)
	releaseHook := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(releaseHook) }) }
	t.Cleanup(releaseAll)
	var hookCalls atomic.Int64
	restore := localtesthooks.Set(func(event string) {
		if event != "network:request-lock:first-miss" {
			return
		}
		hookCalls.Add(1)
		misses <- struct{}{}
		<-releaseHook
	})
	t.Cleanup(restore)

	entered := make(chan struct{}, waiters)
	releaseLocks := make(chan struct{})
	var releaseLocksOnce sync.Once
	releaseLocksAll := func() { releaseLocksOnce.Do(func() { close(releaseLocks) }) }
	t.Cleanup(releaseLocksAll)
	results := make(chan error, waiters)
	for range waiters {
		go func() {
			<-start
			unlock, err := server.lockRequest(ctx, "shared")
			if err != nil {
				results <- err
				return
			}
			entered <- struct{}{}
			<-releaseLocks
			unlock()
			results <- nil
		}()
	}
	close(start)
	for range waiters {
		select {
		case <-misses:
		case <-ctx.Done():
			t.Fatal("waiters did not all reach the first-miss path")
		}
	}
	if got := hookCalls.Load(); got != waiters {
		t.Fatalf("first-miss hook calls=%d, want %d", got, waiters)
	}
	releaseAll()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("initial request lock acquisition did not complete")
	}
	select {
	case <-entered:
		t.Fatal("same-ID waiters acquired the request lock concurrently")
	case <-time.After(20 * time.Millisecond):
	}
	if got := requestLockEntryCount(server); got != 1 {
		t.Fatalf("concurrent first use allocated %d request locks, want 1", got)
	}
	releaseLocksAll()
	for range waiters {
		if err := <-results; err != nil {
			t.Fatalf("same-ID lock wait error: %v", err)
		}
	}
}
