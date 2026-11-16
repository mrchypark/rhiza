//go:build rhiza_latency_diag

package latencydiag

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPreEnqueueTraceIsTaggedFixedAndBounded(t *testing.T) {
	flushMu.Lock()
	defer flushMu.Unlock()
	ring.drain()

	ctx := WithTag(context.Background(), 16)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	started := StartTagged(ctx)
	TraceTaggedStage(cancelled, StagePreEnqueueValidate, errors.New("secret request text"), started)
	TraceRequestLockWait(ctx, 3548, nil, StartTagged(ctx))

	events, dropped, preEnqueueDropped := ring.drainWithStats()
	if dropped != 0 || preEnqueueDropped != 0 || len(events) != 2 {
		t.Fatalf("events=%+v dropped=%d pre_enqueue_dropped=%d", events, dropped, preEnqueueDropped)
	}
	if events[0].stage != StagePreEnqueueValidate || events[0].tag != 16 || events[0].outcome != "error" {
		t.Fatalf("validation event=%+v", events[0])
	}
	if events[1].stage != StageRequestLockWait || events[1].count != 3548 || events[1].outcome != "ok" {
		t.Fatalf("lock event=%+v", events[1])
	}
}

func TestPreEnqueueTraceSkipsUnsampledAndCancellationIsClassified(t *testing.T) {
	flushMu.Lock()
	defer flushMu.Unlock()
	ring.drain()

	unsampled := WithTag(context.Background(), 17)
	TraceTaggedStage(unsampled, StagePreEnqueueClone, nil, StartTagged(unsampled))
	ctx := WithTag(context.Background(), 32)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	TraceTaggedStage(cancelled, StagePreEnqueueEncode, context.Canceled, StartTagged(ctx))

	events, _, _ := ring.drainWithStats()
	if len(events) != 1 || events[0].tag != 32 || events[0].outcome != "cancelled" {
		t.Fatalf("events=%+v", events)
	}
}

func TestPreEnqueueRingOverflowCountsOnlyDroppedFamily(t *testing.T) {
	var bounded eventRing
	for i := 0; i < capacity; i++ {
		bounded.add(event{stage: StagePreEnqueueClone, family: familyPreEnqueue, tag: uint64(i), start: time.Now(), end: time.Now()})
	}
	bounded.add(event{stage: "other", family: familyOther, start: time.Now(), end: time.Now()})
	bounded.add(event{stage: StageBatcherEnqueue, family: familyPreEnqueue, tag: 99, start: time.Now(), end: time.Now()})
	events, dropped, preEnqueueDropped := bounded.drainWithStats()
	if len(events) != capacity || dropped != 2 || preEnqueueDropped != 2 {
		t.Fatalf("events=%d dropped=%d pre_enqueue_dropped=%d", len(events), dropped, preEnqueueDropped)
	}
	if events[len(events)-1].stage != StageBatcherEnqueue || events[len(events)-1].tag != 99 {
		t.Fatalf("newest event not retained: %+v", events[len(events)-1])
	}
}
