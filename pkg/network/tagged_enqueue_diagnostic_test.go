//go:build rhiza_latency_diag && rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestTaggedRequestLockRecordsSameAndDifferentShard(t *testing.T) {
	hold := latencydiag.HoldTagTestFlush()
	defer hold()

	const (
		firstID     = "bench-5242"
		sameShardID = "bench-5247"
		differentID = "bench-5243"
	)
	server := newRequestLockTestServer()
	firstCtx := latencydiag.WithTag(context.Background(), 16)
	secondCtx := latencydiag.WithTag(context.Background(), 32)

	unlockFirst, err := server.lockRequest(firstCtx, firstID)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancelWait := context.WithTimeout(secondCtx, 25*time.Millisecond)
	_, err = server.lockRequest(waitCtx, sameShardID)
	cancelWait()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-shard error=%v", err)
	}
	unlockFirst()

	events, drops, preEnqueueDrops := latencydiag.TagTestSnapshot()
	if drops != 0 || preEnqueueDrops != 0 {
		t.Fatalf("same-shard drops=%d pre_enqueue_drops=%d", drops, preEnqueueDrops)
	}
	firstWait := requireTagEvent(t, events, 16, latencydiag.StageRequestLockWait)
	secondWait := requireTagEvent(t, events, 32, latencydiag.StageRequestLockWait)
	if firstWait.Shard != secondWait.Shard || firstWait.Shard == 0 {
		t.Fatalf("same-shard traces=%+v %+v", firstWait, secondWait)
	}
	if secondWait.Outcome != "cancelled" || firstWait.Outcome != "ok" {
		t.Fatalf("wait outcomes=%q/%q", firstWait.Outcome, secondWait.Outcome)
	}
	if holdEvent := requireTagEvent(t, events, 16, latencydiag.StageRequestLockHold); holdEvent.Shard != firstWait.Shard {
		t.Fatalf("hold shard=%d wait shard=%d", holdEvent.Shard, firstWait.Shard)
	}

	server = newRequestLockTestServer()
	unlockFirst, err = server.lockRequest(firstCtx, firstID)
	if err != nil {
		t.Fatal(err)
	}
	fastCtx, cancelFast := context.WithTimeout(secondCtx, 200*time.Millisecond)
	unlockSecond, err := server.lockRequest(fastCtx, differentID)
	cancelFast()
	if err != nil {
		t.Fatalf("different-shard error=%v", err)
	}
	unlockSecond()
	unlockFirst()

	events, drops, preEnqueueDrops = latencydiag.TagTestSnapshot()
	if drops != 0 || preEnqueueDrops != 0 {
		t.Fatalf("different-shard drops=%d pre_enqueue_drops=%d", drops, preEnqueueDrops)
	}
	firstWait = requireTagEvent(t, events, 16, latencydiag.StageRequestLockWait)
	secondWait = requireTagEvent(t, events, 32, latencydiag.StageRequestLockWait)
	if firstWait.Shard == secondWait.Shard || firstWait.Shard == 0 || secondWait.Shard == 0 {
		t.Fatalf("different-shard traces=%+v %+v", firstWait, secondWait)
	}
	if firstWait.Outcome != "ok" || secondWait.Outcome != "ok" {
		t.Fatalf("different-shard outcomes=%q/%q", firstWait.Outcome, secondWait.Outcome)
	}
}

func TestTaggedRequestLockRecordsCancellationAndServerError(t *testing.T) {
	hold := latencydiag.HoldTagTestFlush()
	defer hold()

	const firstID, secondID, otherID = "bench-5242", "bench-5247", "bench-5243"
	server := newRequestLockTestServer()
	unlockFirst, err := server.lockRequest(latencydiag.WithTag(context.Background(), 16), firstID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(latencydiag.WithTag(context.Background(), 32))
	result := make(chan error, 1)
	go func() {
		_, lockErr := server.lockRequest(ctx, secondID)
		result <- lockErr
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lock error=%v", err)
	}
	unlockFirst()

	events, _, _ := latencydiag.TagTestSnapshot()
	if event := requireTagEvent(t, events, 32, latencydiag.StageRequestLockWait); event.Outcome != "cancelled" {
		t.Fatalf("cancel event=%+v", event)
	}

	server = newRequestLockTestServer()
	proposalCtx, cancelServer := context.WithCancel(context.Background())
	server.proposalCtx = proposalCtx
	cancelServer()
	if _, err := server.lockRequest(latencydiag.WithTag(context.Background(), 32), otherID); !errors.Is(err, ErrNotReady) {
		t.Fatalf("server error=%v", err)
	}
	events, _, _ = latencydiag.TagTestSnapshot()
	if event := requireTagEvent(t, events, 32, latencydiag.StageRequestLockWait); event.Outcome != "error" {
		t.Fatalf("server error event=%+v", event)
	}
}

func TestTaggedRequestLockHoldEndsBeforeRelease(t *testing.T) {
	holdFlush := latencydiag.HoldTagTestFlush()
	defer holdFlush()

	const firstID, waiterID = "bench-5242", "bench-5247"
	server := newRequestLockTestServer()
	unlockFirst, err := server.lockRequest(latencydiag.WithTag(context.Background(), 16), firstID)
	if err != nil {
		t.Fatal(err)
	}

	releaseHook := make(chan struct{})
	released := make(chan struct{})
	var releaseOnce sync.Once
	var releasedOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseHook) })
	restoreHook := localtesthooks.Set(func(name string) {
		if name != "network:request-lock:released-before-hold-trace" {
			return
		}
		releasedOnce.Do(func() {
			close(released)
			<-releaseHook
		})
	})
	defer restoreHook()

	type waiterResult struct {
		unlock func()
		err    error
	}
	waiterStarted := make(chan struct{})
	waiterAcquired := make(chan waiterResult, 1)
	go func() {
		close(waiterStarted)
		unlock, lockErr := server.lockRequest(latencydiag.WithTag(context.Background(), 32), waiterID)
		waiterAcquired <- waiterResult{unlock: unlock, err: lockErr}
	}()
	<-waiterStarted
	time.Sleep(20 * time.Millisecond)

	unlockReturned := make(chan time.Time, 1)
	go func() {
		unlockFirst()
		unlockReturned <- time.Now()
	}()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("request-lock release hook was not reached")
	}
	waiter := <-waiterAcquired
	if waiter.err != nil || waiter.unlock == nil {
		t.Fatalf("waiter did not acquire released shard: err=%v", waiter.err)
	}

	releaseOnce.Do(func() { close(releaseHook) })
	returnedAt := <-unlockReturned
	waiter.unlock()

	events, drops, preEnqueueDrops := latencydiag.TagTestSnapshot()
	if drops != 0 || preEnqueueDrops != 0 {
		t.Fatalf("drops=%d pre_enqueue_drops=%d", drops, preEnqueueDrops)
	}
	holdEvent := requireTagEvent(t, events, 16, latencydiag.StageRequestLockHold)
	waitEvent := requireTagEvent(t, events, 32, latencydiag.StageRequestLockWait)
	if holdEvent.EndUnixNS > waitEvent.EndUnixNS {
		t.Fatalf("hold end extended into waiter acquisition: hold=%+v wait=%+v", holdEvent, waitEvent)
	}
	if holdEvent.EndUnixNS > returnedAt.UnixNano() {
		t.Fatalf("hold end extended beyond release return: hold=%+v returned=%d", holdEvent, returnedAt.UnixNano())
	}
}

func TestTaggedBatcherEnqueueBoundaryAndFailures(t *testing.T) {
	hold := latencydiag.HoldTagTestFlush()
	defer hold()

	ctx := latencydiag.WithTag(context.Background(), 16)
	batcher := minimalSQLBatcher(make(chan *batchItem, 1), 1024)
	result := make(chan error, 1)
	go func() {
		_, err := batcher.submitEncoded(ctx, types.SQLCommand{RequestID: "secret-request"}, []byte("secret-encoded"))
		result <- err
	}()
	item := <-batcher.input
	item.finish(batchResult{slot: 1})
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	events, _, _ := latencydiag.TagTestSnapshot()
	if event := requireTagEvent(t, events, 16, latencydiag.StageBatcherEnqueue); event.Outcome != "ok" {
		t.Fatalf("enqueue event=%+v", event)
	}
	if rendered := fmt.Sprintf("%+v", events); strings.Contains(rendered, "secret-request") || strings.Contains(rendered, "secret-encoded") {
		t.Fatalf("diagnostic exposed request metadata: %s", rendered)
	}

	oversized := minimalSQLBatcher(make(chan *batchItem), 1)
	if _, err := oversized.submitEncoded(ctx, types.SQLCommand{RequestID: "oversized"}, []byte("xx")); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("oversized error=%v", err)
	}
	events, _, _ = latencydiag.TagTestSnapshot()
	if event := requireTagEvent(t, events, 16, latencydiag.StageBatcherEnqueue); event.Outcome != "error" {
		t.Fatalf("oversized event=%+v", event)
	}

	cancelCtx, cancel := context.WithCancel(latencydiag.WithTag(context.Background(), 32))
	cancel()
	cancelledBatcher := minimalSQLBatcher(make(chan *batchItem), 1024)
	if _, err := cancelledBatcher.submitEncoded(cancelCtx, types.SQLCommand{RequestID: "cancelled"}, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled enqueue error=%v", err)
	}
	events, _, _ = latencydiag.TagTestSnapshot()
	if event := requireTagEvent(t, events, 32, latencydiag.StageBatcherEnqueue); event.Outcome != "cancelled" {
		t.Fatalf("cancelled event=%+v", event)
	}
}

func minimalSQLBatcher(input chan *batchItem, valueLimit int) *mutationBatcher[types.SQLCommand] {
	return &mutationBatcher[types.SQLCommand]{
		input: input, ctx: context.Background(), valueLimit: valueLimit,
		requestID: func(command types.SQLCommand) string { return command.RequestID },
	}
}

func requireTagEvent(t *testing.T, events []latencydiag.TagTestEvent, tag uint64, stage string) latencydiag.TagTestEvent {
	t.Helper()
	for _, event := range events {
		if event.Tag == tag && event.Stage == stage {
			return event
		}
	}
	t.Fatalf("missing tag=%d stage=%s in %+v", tag, stage, events)
	return latencydiag.TagTestEvent{}
}
