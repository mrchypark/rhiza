//go:build rhiza_latency_diag

package latencydiag

import (
	"context"
	"errors"
	"runtime"
	"sync"
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
	events, drops := bounded.drainWithDropStats()
	if len(events) != capacity || drops.total != 2 || drops.preEnqueue != 2 ||
		drops.preEnqueueCapacity != 2 || drops.preEnqueueTryLock != 0 {
		t.Fatalf("events=%d drops=%+v", len(events), drops)
	}
	if events[len(events)-1].stage != StageBatcherEnqueue || events[len(events)-1].tag != 99 {
		t.Fatalf("newest event not retained: %+v", events[len(events)-1])
	}
}

func TestPreEnqueueRingDistinguishesCapacityFromTryLockDrops(t *testing.T) {
	var bounded eventRing
	bounded.mu.Lock()
	recorded := make(chan struct{})
	go func() {
		bounded.add(event{stage: StageRequestLockWait, family: familyPreEnqueue, tag: 16, start: time.Now(), end: time.Now()})
		close(recorded)
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		bounded.mu.Unlock()
		t.Fatal("TryLock drop path waited for the contended ring")
	}
	bounded.mu.Unlock()
	events, drops := bounded.drainWithDropStats()
	if len(events) != 0 || drops.total != 1 || drops.preEnqueue != 1 ||
		drops.preEnqueueCapacity != 0 || drops.preEnqueueTryLock != 1 {
		t.Fatalf("events=%d trylock drops=%+v", len(events), drops)
	}
}

func TestPreEnqueueDropReasonsRemainExactUnderConcurrentAccounting(t *testing.T) {
	var bounded eventRing
	const each = 1000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonCapacity)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonTryLock)
		}
	}()
	wg.Wait()
	_, drops := bounded.drainWithDropStats()
	if drops.total != each*2 || drops.preEnqueue != each*2 ||
		drops.preEnqueueCapacity != each || drops.preEnqueueTryLock != each ||
		drops.preEnqueueReasonIncomplete != 0 {
		t.Fatalf("concurrent drops=%+v", drops)
	}
}

func TestPreEnqueueCapacityReasonSaturatesAndMarksIncomplete(t *testing.T) {
	var bounded eventRing
	maxLane := uint32(^uint32(0))
	bounded.preEnqueueDropState.Store(uint64(maxLane) << 32)

	bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonCapacity)
	_, drops := bounded.drainWithDropStats()
	if drops.total != 1 || drops.preEnqueue != uint64(maxLane) ||
		drops.preEnqueueCapacity != uint64(maxLane) || drops.preEnqueueTryLock != 0 ||
		drops.preEnqueueReasonIncomplete != 1 {
		t.Fatalf("saturated capacity drops=%+v", drops)
	}
}

func TestPreEnqueueDropAccountingSurvivesConcurrentDrain(t *testing.T) {
	var bounded eventRing
	const each = 1000
	paused := make(chan struct{})
	firstDrain := make(chan struct{})
	resume := make(chan struct{})
	writerDone := make(chan struct{})

	go func() {
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonCapacity)
		}
		close(paused)
		<-resume
		for i := 0; i < each; i++ {
			bounded.recordDrop(event{family: familyPreEnqueue}, dropReasonTryLock)
		}
		close(writerDone)
	}()

	drained := make(chan dropStats, 1)
	go func() {
		var total dropStats
		signaled := false
		for {
			_, drops := bounded.drainWithDropStats()
			total.total += drops.total
			total.preEnqueue += drops.preEnqueue
			total.preEnqueueCapacity += drops.preEnqueueCapacity
			total.preEnqueueTryLock += drops.preEnqueueTryLock
			total.preEnqueueReasonIncomplete += drops.preEnqueueReasonIncomplete
			if !signaled {
				close(firstDrain)
				signaled = true
			}
			select {
			case <-writerDone:
				_, final := bounded.drainWithDropStats()
				total.total += final.total
				total.preEnqueue += final.preEnqueue
				total.preEnqueueCapacity += final.preEnqueueCapacity
				total.preEnqueueTryLock += final.preEnqueueTryLock
				total.preEnqueueReasonIncomplete += final.preEnqueueReasonIncomplete
				drained <- total
				return
			default:
				runtime.Gosched()
			}
		}
	}()

	<-paused
	<-firstDrain
	close(resume)
	<-writerDone
	drops := <-drained
	if drops.total != each*2 || drops.preEnqueue != each*2 ||
		drops.preEnqueueCapacity != each || drops.preEnqueueTryLock != each ||
		drops.preEnqueueReasonIncomplete != 0 {
		t.Fatalf("writer/drain drops=%+v", drops)
	}
}
