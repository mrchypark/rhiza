//go:build rhiza_latency_diag

package latencydiag

import (
	"context"
	"testing"
	"time"
)

func TestSlotAndBoundedSlowEvents(t *testing.T) {
	ctx := WithSlot(context.Background(), 42)
	if Slot(ctx) != 42 || Slot(context.Background()) != 0 {
		t.Fatal("slot correlation failed")
	}
	var r eventRing
	for i := 0; i < capacity+1; i++ {
		r.add(event{slot: uint64(i), start: time.Now()})
	}
	events, dropped := r.drain()
	if dropped != 1 || len(events) != capacity || events[0].slot != 1 || events[capacity-1].slot != capacity {
		t.Fatal("ring did not preserve newest events and report overwritten event")
	}
	r.mu.Lock()
	r.add(event{slot: 7})
	r.mu.Unlock()
	events, dropped = r.drain()
	if dropped != 1 || len(events) != 0 {
		t.Fatal("contention drop was not reported")
	}
}
