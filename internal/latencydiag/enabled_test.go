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

func TestBatchTraceLinksSlotsAndPhysicalSync(t *testing.T) {
	startWriter.Do(func() {})
	ring.drain()
	first, second := NextBatch(), NextBatch()
	if first == 0 || second <= first {
		t.Fatalf("batch IDs are not unique: %d %d", first, second)
	}
	begin := time.Now()
	TraceBatch("wal_group_wait", first, 42, 0, "ok", begin)
	TraceBatch("wal_group_wait", first, 43, 0, "ok", begin)
	TraceBatch("wal_group_file_sync", first, 0, 0, "ok", begin)
	TraceBatch("wal_group_sync", first, 0, 2, "ok", begin)
	TraceBatch("wal_group_file_sync", second, 0, 0, "skipped", begin)
	events, dropped := ring.drain()
	if dropped != 0 || len(events) != 5 {
		t.Fatalf("events=%d dropped=%d", len(events), dropped)
	}
	if events[0].slot != 42 || events[1].slot != 43 || events[2].stage != "wal_group_file_sync" || events[3].count != 2 || events[4].outcome != "skipped" {
		t.Fatalf("unexpected trace: %+v", events)
	}
	for _, e := range events[:4] {
		if e.batch != first {
			t.Fatalf("broken group linkage: %+v", e)
		}
	}
	if events[4].batch != second {
		t.Fatal("independent group mixed")
	}
}

func TestBatchTraceReportsActualRingOverflow(t *testing.T) {
	startWriter.Do(func() {})
	ring.drain()
	for slot := uint64(0); slot < capacity+3; slot++ {
		TraceBatch("wal_group_wait", 17, slot, 0, "ok", time.Now())
	}
	events, dropped := ring.drain()
	if len(events) != capacity || dropped != 3 || events[0].slot != 3 || events[len(events)-1].slot != capacity+2 {
		t.Fatalf("actual trace ring events=%d dropped=%d", len(events), dropped)
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
