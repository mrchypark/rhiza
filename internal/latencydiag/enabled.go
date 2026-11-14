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

func WithSlot(ctx context.Context, slot uint64) context.Context {
	return context.WithValue(ctx, slotKey{}, slot)
}
func Slot(ctx context.Context) uint64 { slot, _ := ctx.Value(slotKey{}).(uint64); return slot }
func Start() time.Time                { return time.Now() }

type event struct {
	stage, outcome string
	slot           uint64
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
				events, dropped := ring.drain()
				if dropped > 0 {
					fmt.Fprintf(os.Stderr, "rhiza_latency dropped=%d\n", dropped)
				}
				for _, e := range events {
					fmt.Fprintf(os.Stderr, "rhiza_latency stage=%s slot=%d count=%d peer=%x outcome=%s start_unix_ns=%d end_unix_ns=%d duration_ns=%d\n", e.stage, e.slot, e.count, e.peer, e.outcome, e.start.UnixNano(), e.end.UnixNano(), e.end.Sub(e.start).Nanoseconds())
				}
			}
		}()
	})
}
func Observe(stage string, slot uint64, start time.Time) { observe(stage, slot, "", nil, start) }
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
func observe(stage string, slot uint64, peer string, err error, begin time.Time) {
	end := time.Now()
	if end.Sub(begin) < 100*time.Millisecond {
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
