//go:build rhiza_latency_diag

package latencydiag

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const Enabled = true
const capacity = 256

type slotKey struct{}
type syncKey struct{}
type tagKey struct{}

func WithSync(ctx context.Context, id uint64) context.Context {
	return context.WithValue(ctx, syncKey{}, id)
}
func SyncID(ctx context.Context) uint64 { id, _ := ctx.Value(syncKey{}).(uint64); return id }

func WithSlot(ctx context.Context, slot uint64) context.Context {
	return context.WithValue(ctx, slotKey{}, slot)
}
func Slot(ctx context.Context) uint64 { slot, _ := ctx.Value(slotKey{}).(uint64); return slot }
func WithTag(ctx context.Context, tag uint64) context.Context {
	return context.WithValue(ctx, tagKey{}, tag)
}
func Tag(ctx context.Context) uint64 {
	if ctx == nil {
		return 0
	}
	tag, _ := ctx.Value(tagKey{}).(uint64)
	return tag
}
func Start() time.Time { return time.Now() }

type event struct {
	stage, outcome string
	family         eventFamily
	slot           uint64
	batch          uint64
	request        uint64
	attempt        uint64
	tag            uint64
	count          int
	peer           [8]byte
	start, end     time.Time
}

type eventFamily uint8

const (
	familyOther eventFamily = iota
	familyPreEnqueue
)

type dropReason uint8

const (
	dropReasonTryLock dropReason = iota
	dropReasonCapacity
)

type dropStats struct {
	total, preEnqueue, preEnqueueCapacity, preEnqueueTryLock, preEnqueueReasonIncomplete uint64
}

type eventRing struct {
	mu                       sync.Mutex
	items                    [capacity]event
	start, count             int
	dropped                  atomic.Uint64
	preEnqueueDropState      atomic.Uint64
	preEnqueueDropIncomplete atomic.Bool
}

var ring eventRing
var testLossless atomic.Bool
var startWriter sync.Once
var nextBatch atomic.Uint64
var flushMu sync.Mutex
var sampleSlots = os.Getenv("RHIZA_LATENCY_SAMPLE_SLOTS") == "1"

// Batch IDs are unique within a process lifetime; node log boundaries identify
// the process. Slot zero means the caller did not provide a slot.
func NextBatch() uint64 { return nextBatch.Add(1) }

func TraceBatch(stage string, batch, slot uint64, count int, outcome string, begin time.Time) {
	start()
	ring.add(event{stage: stage, batch: batch, slot: slot, count: count, outcome: outcome, start: begin, end: time.Now()})
}

func TraceArchive(stage string, batch, request, attempt, slot uint64, outcome string, begin time.Time) {
	start()
	ring.add(event{stage: stage, batch: batch, request: request, attempt: attempt, slot: slot, outcome: outcome, start: begin, end: time.Now()})
}

func (r *eventRing) add(e event) {
	if testLossless.Load() {
		r.mu.Lock()
	} else if !r.mu.TryLock() {
		r.recordDrop(e, dropReasonTryLock)
		return
	}
	defer r.mu.Unlock()
	if r.count == capacity {
		r.recordDrop(r.items[r.start], dropReasonCapacity)
		r.start = (r.start + 1) % capacity
		r.count--
	}
	r.items[(r.start+r.count)%capacity] = e
	r.count++
}

func (r *eventRing) recordDrop(e event, reason dropReason) {
	r.dropped.Add(1)
	if e.family == familyPreEnqueue {
		r.addPreEnqueueDrop(reason)
	}
}

func (r *eventRing) addPreEnqueueDrop(reason dropReason) {
	// Pack both reasons so flush gets one exact snapshot without blocking drops.
	const maxLane = uint64(^uint32(0))
	for {
		state := r.preEnqueueDropState.Load()
		capacity := uint64(uint32(state >> 32))
		tryLock := uint64(uint32(state))
		switch reason {
		case dropReasonCapacity:
			if capacity == maxLane {
				r.preEnqueueDropIncomplete.Store(true)
				return
			}
			capacity++
		case dropReasonTryLock:
			if tryLock == maxLane {
				r.preEnqueueDropIncomplete.Store(true)
				return
			}
			tryLock++
		default:
			r.preEnqueueDropIncomplete.Store(true)
			return
		}
		if r.preEnqueueDropState.CompareAndSwap(state, capacity<<32|tryLock) {
			return
		}
	}
}

func (r *eventRing) drain() ([]event, uint64) {
	events, dropped, _ := r.drainWithStats()
	return events, dropped
}

func (r *eventRing) drainWithStats() ([]event, uint64, uint64) {
	events, drops := r.drainWithDropStats()
	return events, drops.total, drops.preEnqueue
}

func (r *eventRing) drainWithDropStats() ([]event, dropStats) {
	r.mu.Lock()
	events := make([]event, r.count)
	for i := range events {
		events[i] = r.items[(r.start+i)%capacity]
	}
	r.start, r.count = 0, 0
	r.mu.Unlock()
	preEnqueueState := r.preEnqueueDropState.Swap(0)
	preEnqueueCapacity := uint64(uint32(preEnqueueState >> 32))
	preEnqueueTryLock := uint64(uint32(preEnqueueState))
	preEnqueueReasonIncomplete := uint64(0)
	if r.preEnqueueDropIncomplete.Load() {
		preEnqueueReasonIncomplete = 1
	}
	drops := dropStats{
		total:                      r.dropped.Swap(0),
		preEnqueue:                 preEnqueueCapacity + preEnqueueTryLock,
		preEnqueueCapacity:         preEnqueueCapacity,
		preEnqueueTryLock:          preEnqueueTryLock,
		preEnqueueReasonIncomplete: preEnqueueReasonIncomplete,
	}
	return events, drops
}
func start() {
	startWriter.Do(func() {
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			for range ticker.C {
				Flush()
			}
		}()
	})
}

// Flush synchronously emits currently buffered events. The tagged test uses it
// to avoid relying on scheduler timing; abrupt termination can still lose data.
func Flush() {
	flushMu.Lock()
	defer flushMu.Unlock()
	events, drops := ring.drainWithDropStats()
	if drops.total > 0 {
		fmt.Fprintf(os.Stderr, "rhiza_latency dropped=%d\n", drops.total)
	}
	if drops.preEnqueue > 0 {
		fmt.Fprintf(os.Stderr, "rhiza_latency pre_enqueue_dropped=%d\n", drops.preEnqueue)
		fmt.Fprintf(os.Stderr, "rhiza_latency pre_enqueue_dropped_capacity=%d\n", drops.preEnqueueCapacity)
		fmt.Fprintf(os.Stderr, "rhiza_latency pre_enqueue_dropped_trylock=%d\n", drops.preEnqueueTryLock)
		fmt.Fprintf(os.Stderr, "rhiza_latency pre_enqueue_dropped_reason_incomplete=%d\n", drops.preEnqueueReasonIncomplete)
	}
	for _, e := range events {
		fmt.Fprintf(os.Stderr, "rhiza_latency stage=%s batch=%d slot=%d count=%d peer=%x outcome=%s start_unix_ns=%d end_unix_ns=%d duration_ns=%d request=%d attempt=%d tag=%d\n", e.stage, e.batch, e.slot, e.count, e.peer, e.outcome, e.start.UnixNano(), e.end.UnixNano(), e.end.Sub(e.start).Nanoseconds(), e.request, e.attempt, e.tag)
	}
}
func Observe(stage string, slot uint64, start time.Time) { observe(stage, slot, "", nil, start) }
func ObserveError(stage string, slot uint64, err error, start time.Time) {
	observe(stage, slot, "", err, start)
}
func ObserveBatch(stage string, count int, err error, begin time.Time) {
	end := time.Now()
	if end.Sub(begin) < 100*time.Millisecond {
		return
	}
	start()
	ring.add(batchEvent(stage, count, err, begin, end))
}
func batchEvent(stage string, count int, err error, begin, end time.Time) event {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	return event{stage: stage, count: count, start: begin, end: end, outcome: outcome}
}
func ObservePeer(stage string, slot uint64, peer string, err error, start time.Time) {
	observe(stage, slot, peer, err, start)
}

// TraceTag records every sampled client boundary. Unlike slow observations,
// these markers are emitted without a duration threshold.
func TraceTag(stage string, tag, slot uint64, err error, begin time.Time) {
	if !SampleTag(tag) {
		return
	}
	end := time.Now()
	start()
	e := event{stage: stage, tag: tag, slot: slot, start: begin, end: end, outcome: "ok"}
	if err != nil {
		e.outcome = "error"
	}
	ring.add(e)
}

// StartTagged avoids taking a timestamp for untagged or unsampled requests.
func StartTagged(ctx context.Context) time.Time {
	if !SampleTag(Tag(ctx)) {
		return time.Time{}
	}
	return time.Now()
}

// TraceTaggedStage records one bounded pre-enqueue boundary. Error values are
// reduced to outcome classes; request IDs, SQL, and error text are never kept.
func TraceTaggedStage(ctx context.Context, stage string, err error, begin time.Time) {
	traceTagged(ctx, stage, 0, err, begin)
}

// TraceRequestLockWait records the select wait on a request-lock shard. count
// carries only the low-cardinality shard number, never a request identifier.
func TraceRequestLockWait(ctx context.Context, shard uint64, err error, begin time.Time) {
	traceTagged(ctx, StageRequestLockWait, shard, err, begin)
}

func TraceRequestLockHold(ctx context.Context, shard uint64, begin, end time.Time) {
	traceTaggedRange(ctx, StageRequestLockHold, shard, nil, begin, end)
}

func TraceBatcherEnqueue(ctx context.Context, err error, begin time.Time) {
	traceTagged(ctx, StageBatcherEnqueue, 0, err, begin)
}

func traceTagged(ctx context.Context, stage string, shard uint64, err error, begin time.Time) {
	traceTaggedRange(ctx, stage, shard, err, begin, time.Now())
}

func traceTaggedRange(ctx context.Context, stage string, shard uint64, err error, begin, end time.Time) {
	tag := Tag(ctx)
	if !SampleTag(tag) || begin.IsZero() || end.IsZero() || end.Before(begin) {
		return
	}
	start()
	ring.add(event{
		stage: stage, family: familyPreEnqueue, tag: tag, count: int(shard),
		start: begin, end: end, outcome: taggedOutcome(err),
	})
}

func taggedOutcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "cancelled"
	default:
		return "error"
	}
}

// TraceHTTPResponse records the final status without response headers or body.
func TraceHTTPResponse(tag uint64, status int, begin time.Time) {
	if !SampleTag(tag) {
		return
	}
	start()
	outcome := "ok"
	if status >= 400 {
		outcome = "error"
	}
	ring.add(event{stage: "http_response", tag: tag, count: status, start: begin, end: time.Now(), outcome: outcome})
}

// TraceSync records every Sync stage; batch identifies a server-local request.
// These high-volume events may be dropped by the bounded diagnostic ring.
func TraceSync(stage string, batch, slot uint64, count int, peer string, err error, begin time.Time) {
	start()
	ring.add(syncEvent(stage, batch, slot, count, peer, err, begin))
}

func syncEvent(stage string, batch, slot uint64, count int, peer string, err error, begin time.Time) event {
	e := event{stage: stage, batch: batch, slot: slot, count: count, start: begin, end: time.Now(), outcome: "ok"}
	if err != nil {
		e.outcome = "error"
	}
	if peer != "" {
		sum := sha256.Sum256([]byte(peer))
		copy(e.peer[:], sum[:8])
	}
	return e
}
func observe(stage string, slot uint64, peer string, err error, begin time.Time) {
	end := time.Now()
	if !observeSlot(end.Sub(begin), slot, sampleSlots) {
		return
	}
	start()
	e := event{stage: stage, slot: slot, start: begin, end: end, outcome: "ok"}
	if err != nil {
		e.outcome = "error"
	}
	if peer != "" {
		sum := sha256.Sum256([]byte(peer))
		copy(e.peer[:], sum[:8])
	}
	ring.add(e)
}

func observeSlot(duration time.Duration, slot uint64, sampled bool) bool {
	return duration >= 100*time.Millisecond || (sampled && slot != 0 && slot%16 == 0)
}
