package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestWarmedRequestLockDoesNotAllocate(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	defer server.Close()
	const id = "warmed"
	shard := requestLockShard(id)
	if lock := server.requestLock(shard); lock == nil {
		t.Fatal("warmed request lock entry missing")
	}
	t.Run("direct lookup", func(t *testing.T) {
		if allocs := testing.AllocsPerRun(100, func() {
			if server.requestLock(shard) == nil {
				panic("warmed request lock disappeared")
			}
		}); allocs != 0 {
			t.Fatalf("warmed requestLock allocations=%v, want 0", allocs)
		}
	})
}

func TestRequestLockDoesNotCreateShardAfterConcurrentClose(t *testing.T) {
	type result struct {
		unlock func()
		err    error
	}
	for attempt := range 32 {
		server := NewServer(nil, nil, "cluster", false, nil)
		start := make(chan struct{})
		requestDone := make(chan result, 1)
		closeDone := make(chan struct{})
		go func() {
			<-start
			unlock, err := server.lockRequest(context.Background(), fmt.Sprintf("racing-%d", attempt))
			requestDone <- result{unlock: unlock, err: err}
		}()
		go func() {
			<-start
			server.Close()
			close(closeDone)
		}()
		close(start)
		<-closeDone
		got := <-requestDone
		if got.err != nil && !errors.Is(got.err, ErrNotReady) {
			t.Fatalf("racing lock error=%v", got.err)
		}
		if got.unlock != nil {
			got.unlock()
		}
		before := requestLockEntryCount(server)
		if _, err := server.lockRequest(context.Background(), fmt.Sprintf("after-close-%d", attempt)); !errors.Is(err, ErrNotReady) {
			t.Fatalf("post-close lock error=%v", err)
		}
		if after := requestLockEntryCount(server); after != before {
			t.Fatalf("post-close lock added shard entry: before=%d after=%d", before, after)
		}
	}
}

func TestLegacyShardCollisionUsesDistinctRequestLocks(t *testing.T) {
	const heldID = "parallel-held"
	legacyShard := func(id string) uint16 {
		hash := sha256.Sum256([]byte(id))
		return uint16(hash[0])<<4 | uint16(hash[1])>>4
	}
	otherID := ""
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("parallel-%d", i)
		if legacyShard(candidate) == legacyShard(heldID) && requestLockShard(candidate) != requestLockShard(heldID) {
			otherID = candidate
			break
		}
	}

	server := NewServer(nil, nil, "cluster", false, nil)
	defer server.Close()
	unlockHeld, err := server.lockRequest(context.Background(), heldID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlockHeld()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	unlockOther, err := server.lockRequest(ctx, otherID)
	if err != nil {
		t.Fatalf("legacy 12-bit collision with distinct 16-bit shard blocked: %v", err)
	}
	unlockOther()
}

func requestLockEntryCount(server *Server) int {
	count := 0
	server.requestLocks.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
