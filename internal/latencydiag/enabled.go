//go:build rhiza_latency_diag

package latencydiag

import (
	"context"
	"crypto/sha256"
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

func WithSync(ctx context.Context, id uint64) context.Context {
	return context.WithValue(ctx, syncKey{}, id)
}
func SyncID(ctx context.Context) uint64 { id, _ := ctx.Value(syncKey{}).(uint64); return id }

func WithSlot(ctx context.Context, slot uint64) context.Context {
	return context.WithValue(ctx, slotKey{}, slot)
}
func Slot(ctx context.Context) uint64 { slot, _ := ctx.Value(slotKey{}).(uint64); return slot }
func Start() time.Time                { return time.Now() }

type event struct {
	stage, outcome string
	slot           uint64
	batch          uint64
	count          int
	peer           [8]byte
	start, end     time.Time
}
type eventRing struct {
	mu           sync.Mutex
	items        [capacity]event
	start, count int
	dropped      atomic.Uint64
}

var ring eventRing
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

func (r *eventRing) add(e event) {
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		return
	}
	defer r.mu.Unlock()
	if r.count == capacity {
		r.start = (r.start + 1) % capacity
		r.count--
		r.dropped.Add(1)
	}
	r.items[(r.start+r.count)%capacity] = e
	r.count++
}
func (r *eventRing) drain() ([]event, uint64) {
	r.mu.Lock()
	events := make([]event, r.count)
	for i := range events {
		events[i] = r.items[(r.start+i)%capacity]
	}
	r.start, r.count = 0, 0
	r.mu.Unlock()
	return events, r.dropped.Swap(0)
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
	events, dropped := ring.drain()
	if dropped > 0 {
		fmt.Fprintf(os.Stderr, "rhiza_latency dropped=%d\n", dropped)
	}
	for _, e := range events {
		fmt.Fprintf(os.Stderr, "rhiza_latency stage=%s batch=%d slot=%d count=%d peer=%x outcome=%s start_unix_ns=%d end_unix_ns=%d duration_ns=%d\n", e.stage, e.batch, e.slot, e.count, e.peer, e.outcome, e.start.UnixNano(), e.end.UnixNano(), e.end.Sub(e.start).Nanoseconds())
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
