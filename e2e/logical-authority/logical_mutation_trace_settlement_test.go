//go:build localhooks

package logicalauthority_test

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestAssertLogicalMutationTraceSettledAcceptsStartedBoundaries(t *testing.T) {
	trace := localtesthooks.NewLogicalMutationExecutionTrace(16)
	ctx := trace.Context(context.Background())
	for _, event := range settledLogicalMutationEvents("settled-test-producer", 17) {
		localtesthooks.HitLogicalMutationExecution(ctx, "settled-test-producer", 17, event.stage, event.outcome)
	}

	assertLogicalMutationTraceSettled(t, trace)
}

func TestAssertLogicalMutationTraceSettledAcceptsTruncatedCompleteWindow(t *testing.T) {
	trace := localtesthooks.NewLogicalMutationExecutionTrace(9)
	ctx := trace.Context(context.Background())
	localtesthooks.HitLogicalMutationExecution(ctx, "old-prefix-producer", 1,
		localtesthooks.LogicalMutationAttemptStarted, localtesthooks.LogicalMutationStarted)
	for _, event := range settledLogicalMutationEvents("settled-test-producer", 17) {
		localtesthooks.HitLogicalMutationExecution(ctx, "settled-test-producer", 17, event.stage, event.outcome)
	}
	if events, truncated := trace.Snapshot(); !truncated || !logicalMutationTraceHasSettledAttempt(events) {
		t.Fatalf("fixture events=%+v truncated=%t, want a complete retained settlement and truncation", events, truncated)
	}

	assertLogicalMutationTraceSettled(t, trace)
}

func TestLogicalMutationSettlementRejectsInvalidTraceVariants(t *testing.T) {
	valid := settledLogicalMutationEvents("producer", 17)
	tests := []struct {
		name   string
		events []localtesthooks.LogicalMutationExecutionEvent
		want   bool
	}{
		{name: "valid ordered tuple", events: logicalMutationTestEvents(valid), want: true},
		{name: "missing completion phase", events: logicalMutationTestEvents(valid[:len(valid)-1])},
		{name: "provider start must remain started", events: mutateLogicalMutationTestEvents(valid, func(events []localtesthooks.LogicalMutationExecutionEvent) {
			events[3].Outcome = localtesthooks.LogicalMutationSucceeded
		})},
		{name: "failed completion is not settled", events: mutateLogicalMutationTestEvents(valid, func(events []localtesthooks.LogicalMutationExecutionEvent) {
			events[4].Outcome = localtesthooks.LogicalMutationFailed
		})},
		{name: "out of order completion is rejected", events: mutateLogicalMutationTestEvents(valid, func(events []localtesthooks.LogicalMutationExecutionEvent) {
			events[1], events[2] = events[2], events[1]
		})},
		{name: "duplicate phase is rejected", events: func() []localtesthooks.LogicalMutationExecutionEvent {
			events := logicalMutationTestEvents(valid)
			return append(events[:2], append([]localtesthooks.LogicalMutationExecutionEvent{events[1]}, events[2:]...)...)
		}()},
		{name: "split producer and sequence cannot be combined", events: mutateLogicalMutationTestEvents(valid, func(events []localtesthooks.LogicalMutationExecutionEvent) {
			for i := len(events) / 2; i < len(events); i++ {
				events[i].ProducerHash = "other-producer"
				events[i].Sequence++
			}
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := logicalMutationTraceHasSettledAttempt(test.events); got != test.want {
				t.Fatalf("settled=%t, want %t; events=%+v", got, test.want, test.events)
			}
		})
	}
}

func TestLogicalMutationSettlementRejectsTruncatedPartialWindow(t *testing.T) {
	trace := localtesthooks.NewLogicalMutationExecutionTrace(9)
	ctx := trace.Context(context.Background())
	for sequence := uint64(1); sequence <= 2; sequence++ {
		localtesthooks.HitLogicalMutationExecution(ctx, "old-prefix-producer", sequence,
			localtesthooks.LogicalMutationAttemptStarted, localtesthooks.LogicalMutationStarted)
	}
	for _, event := range settledLogicalMutationEvents("partial-test-producer", 17)[:8] {
		localtesthooks.HitLogicalMutationExecution(ctx, "partial-test-producer", 17, event.stage, event.outcome)
	}
	events, truncated := trace.Snapshot()
	if !truncated {
		t.Fatal("fixture did not reach its trace bound")
	}
	if logicalMutationTraceHasSettledAttempt(events) {
		t.Fatalf("truncated partial trace was accepted as settled: %+v", events)
	}
}

func settledLogicalMutationEvents(producer string, sequence uint64) []struct {
	stage   localtesthooks.LogicalMutationStage
	outcome localtesthooks.LogicalMutationOutcome
} {
	return []struct {
		stage   localtesthooks.LogicalMutationStage
		outcome localtesthooks.LogicalMutationOutcome
	}{
		{localtesthooks.LogicalMutationAttemptStarted, localtesthooks.LogicalMutationStarted},
		{localtesthooks.LogicalMutationDispatchGranted, localtesthooks.LogicalMutationSucceeded},
		{localtesthooks.LogicalMutationPayloadOpened, localtesthooks.LogicalMutationSucceeded},
		{localtesthooks.LogicalMutationProviderStarted, localtesthooks.LogicalMutationStarted},
		{localtesthooks.LogicalMutationProviderReturned, localtesthooks.LogicalMutationSucceeded},
		{localtesthooks.LogicalMutationReceiptPersisted, localtesthooks.LogicalMutationSucceeded},
		{localtesthooks.LogicalMutationJournalResolved, localtesthooks.LogicalMutationSucceeded},
		{localtesthooks.LogicalMutationKeyReleased, localtesthooks.LogicalMutationSucceeded},
		{localtesthooks.LogicalMutationAttemptReturned, localtesthooks.LogicalMutationSucceeded},
	}
}

func logicalMutationTestEvents(stages []struct {
	stage   localtesthooks.LogicalMutationStage
	outcome localtesthooks.LogicalMutationOutcome
}) []localtesthooks.LogicalMutationExecutionEvent {
	events := make([]localtesthooks.LogicalMutationExecutionEvent, 0, len(stages))
	for _, stage := range stages {
		events = append(events, localtesthooks.LogicalMutationExecutionEvent{
			ProducerHash: "producer",
			Sequence:     17,
			Stage:        stage.stage,
			Outcome:      stage.outcome,
		})
	}
	return events
}

func mutateLogicalMutationTestEvents(stages []struct {
	stage   localtesthooks.LogicalMutationStage
	outcome localtesthooks.LogicalMutationOutcome
}, mutate func([]localtesthooks.LogicalMutationExecutionEvent)) []localtesthooks.LogicalMutationExecutionEvent {
	events := logicalMutationTestEvents(stages)
	mutate(events)
	return events
}
