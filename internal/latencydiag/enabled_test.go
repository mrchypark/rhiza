//go:build rhiza_latency_diag

package latencydiag

import (
	"context"
	"errors"
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
		r.add(event{slot: uint64(i), count: i, start: time.Now()})
	}
	events, dropped := r.drain()
	if dropped != 1 || len(events) != capacity || events[0].slot != 1 || events[capacity-1].slot != capacity || events[capacity-1].count != capacity {
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

func TestBatchEventReportsSyncFailure(t *testing.T) {
	begin := time.Now()
	end := begin.Add(time.Second)
	for _, tc := range []struct {
		err  error
		want string
	}{{nil, "ok"}, {errors.New("injected sync failure"), "error"}} {
		e := batchEvent("wal_sync", 3, tc.err, begin, end)
		if e.outcome != tc.want || e.count != 3 || e.end.Sub(e.start) != time.Second {
			t.Fatalf("unexpected batch event: %+v", e)
		}
	}
}

func TestObserveErrorRecordsActualOutcome(t *testing.T) {
	// Suppress the asynchronous writer so the captured events remain inspectable.
	startWriter.Do(func() {})
	ring.drain()
	begin := time.Now().Add(-time.Second)
	ObserveError("record_quorum", 1, context.Canceled, begin)
	ObserveError("wal_shared_wait", 2, nil, begin)
	ObservePeer("peer_record", 3, "peer", context.DeadlineExceeded, begin)
	events, dropped := ring.drain()
	if dropped != 0 || len(events) != 3 {
		t.Fatalf("got %d events and %d dropped", len(events), dropped)
	}
	for i, want := range []string{"error", "ok", "error"} {
		if events[i].outcome != want {
			t.Fatalf("event %d outcome=%q, want %q", i, events[i].outcome, want)
		}
	}
}
