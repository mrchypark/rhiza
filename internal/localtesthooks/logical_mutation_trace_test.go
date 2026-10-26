//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/api/googleapi"
)

func TestLogicalMutationExecutionTraceIsBoundedAndConcurrent(t *testing.T) {
	trace := NewLogicalMutationExecutionTrace(8)
	ctx := trace.Context(context.Background())
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			HitLogicalMutationExecution(ctx, "producer-private", 7, LogicalMutationProviderReturned, LogicalMutationFailed)
		}()
	}
	wg.Wait()
	events, truncated := trace.Snapshot()
	if len(events) != 8 || !truncated {
		t.Fatalf("trace retained %d events, truncated=%t; want bounded 8 and truncated", len(events), truncated)
	}
	for _, event := range events {
		if event.ProducerHash == "producer-private" || len(event.ProducerHash) != 12 || event.Sequence != 7 ||
			event.Stage != LogicalMutationProviderReturned || event.Outcome != LogicalMutationFailed {
			t.Fatalf("unexpected or unredacted event: %+v", event)
		}
	}
	copyEvents, _ := trace.Snapshot()
	copyEvents[0].Stage = "mutated"
	unchanged, _ := trace.Snapshot()
	if unchanged[0].Stage == "mutated" {
		t.Fatal("snapshot exposed mutable trace storage")
	}
}

func TestLogicalMutationExecutionTraceRejectsUnboundedCategories(t *testing.T) {
	trace := NewLogicalMutationExecutionTrace(4)
	ctx := trace.Context(context.Background())
	HitLogicalMutationExecution(ctx, "producer", 1, "raw-key", LogicalMutationFailed)
	HitLogicalMutationExecution(ctx, "producer", 1, LogicalMutationProviderStarted, "provider error: secret")
	if events, truncated := trace.Snapshot(); len(events) != 0 || truncated {
		t.Fatalf("invalid categories were retained: events=%+v truncated=%t", events, truncated)
	}
}

func TestLogicalMutationExecutionTraceRetainsLatestEventsInOrder(t *testing.T) {
	trace := NewLogicalMutationExecutionTrace(3)
	ctx := trace.Context(context.Background())
	for sequence := uint64(1); sequence <= 5; sequence++ {
		HitLogicalMutationExecution(ctx, "producer", sequence, LogicalMutationProviderReturned, LogicalMutationSucceeded)
	}
	events, truncated := trace.Snapshot()
	if !truncated || len(events) != 3 || events[0].Sequence != 3 || events[1].Sequence != 4 || events[2].Sequence != 5 {
		t.Fatalf("trace events=%+v truncated=%t, want latest 3 in order", events, truncated)
	}
}

func TestLogicalMutationExecutionErrorUsesOnlyCoarseFields(t *testing.T) {
	trace := NewLogicalMutationExecutionTrace(4)
	ctx := trace.Context(context.Background())
	HitLogicalMutationExecutionError(ctx, "producer-private", 9, LogicalMutationProviderReturned,
		fmt.Errorf("provider returned private detail: %w", &googleapi.Error{Code: 412, Message: "private object name"}))
	events, truncated := trace.Snapshot()
	if truncated || len(events) != 1 {
		t.Fatalf("events=%+v truncated=%t, want one bounded event", events, truncated)
	}
	got := events[0]
	if got.ErrorClass != "googleapi-http" || got.HTTPStatus != 412 || got.ContextState != "active" ||
		got.Outcome != LogicalMutationPending || got.Stage != LogicalMutationProviderReturned ||
		got.ProducerHash == "producer-private" || got.Sequence != 9 {
		t.Fatalf("unexpected coarse HTTP error event: %+v", got)
	}

	cancelledTrace := NewLogicalMutationExecutionTrace(2)
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancelledCtx = cancelledTrace.Context(cancelledCtx)
	cancel()
	HitLogicalMutationExecutionError(cancelledCtx, "producer", 10, LogicalMutationReceiptPersisted, context.Canceled)
	cancelled, _ := cancelledTrace.Snapshot()
	if len(cancelled) != 1 || cancelled[0].ErrorClass != "canceled" || cancelled[0].ContextState != "canceled" || cancelled[0].HTTPStatus != 0 {
		t.Fatalf("unexpected cancellation event: %+v", cancelled)
	}
}
