//go:build rhiza_latency_diag

package quepaxa

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestTaggedRealWALTraceRecordsLinkSlotsAndBatches(t *testing.T) {
	const childEnv = "RHIZA_TRACE_LINK_TEST_CHILD"
	if os.Getenv(childEnv) == "1" {
		latencydiag.DisableBackgroundWriterForTest()
		TestTaggedGroupCommitCallsRealWALWithBatchID(t)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTaggedRealWALTraceRecordsLinkSlotsAndBatches$")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("tagged child: %v\n%s", err, out)
	}
	type record struct{ batch, slot, outcome, count string }
	groups := make(map[string][]record)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "rhiza_latency dropped=") {
			t.Fatalf("trace dropped events: %s", line)
		}
		if !strings.HasPrefix(line, "rhiza_latency stage=") {
			continue
		}
		fields := map[string]string{}
		for _, field := range strings.Fields(line) {
			k, v, ok := strings.Cut(field, "=")
			if ok {
				fields[k] = v
			}
		}
		groups[fields["stage"]] = append(groups[fields["stage"]], record{fields["batch"], fields["slot"], fields["outcome"], fields["count"]})
	}
	waits := groups["wal_group_wait"]
	if len(waits) != 3 {
		t.Fatalf("wait records=%v output=%s", waits, out)
	}
	bySlot := map[string]record{}
	for _, r := range waits {
		bySlot[r.slot] = r
	}
	a, b, c := bySlot["42"], bySlot["43"], bySlot["44"]
	if a.batch == "" || a.batch == "0" || a.batch != b.batch || a.outcome != "error" || b.outcome != "ok" || c.batch == "" || c.batch == a.batch || c.outcome != "ok" {
		t.Fatalf("slot batch linkage: %v", bySlot)
	}
	check := func(stage, batch, outcome, count string) {
		t.Helper()
		matches := 0
		for _, r := range groups[stage] {
			if r.batch == batch && r.outcome == outcome && (count == "" || r.count == count) {
				matches++
			}
		}
		if matches != 1 {
			t.Fatalf("stage=%s batch=%s outcome=%s count=%s matches=%d output=%s", stage, batch, outcome, count, matches, out)
		}
	}
	check("wal_group_lock", a.batch, "ok", "")
	check("wal_group_file_sync", a.batch, "ok", "")
	check("wal_group_sync", a.batch, "ok", "2")
	check("wal_group_file_sync", c.batch, "skipped", "")
	check("wal_group_sync", c.batch, "ok", "1")
}

func TestTaggedGroupCommitCallsRealWALWithBatchID(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.Append(qlog.Entry{Slot: 42, Type: qlog.EntryProposal, Payload: []byte("test")}); err != nil {
		t.Fatal(err)
	}
	g := newWALGroupCommit(wal)
	g.delay = time.Hour
	g.max = 2
	actual := g.syncTagged
	var seen atomic.Uint64
	var calls atomic.Int32
	g.syncTagged = func(id uint64) error { calls.Add(1); seen.Store(id); return actual(id) }
	firstCtx, cancelFirst := context.WithCancel(latencydiag.WithSlot(context.Background(), 42))
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() { firstResult <- g.Sync(firstCtx) }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		g.mu.Lock()
		registered := g.pending != nil && g.pending.waiters == 1
		g.mu.Unlock()
		if registered {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("first waiter did not register")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancelFirst()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter: %v", err)
	}
	if err := g.Sync(latencydiag.WithSlot(context.Background(), 43)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || seen.Load() == 0 {
		t.Fatalf("calls=%d batch=%d", calls.Load(), seen.Load())
	}
	first := seen.Load()
	g.max = 1
	if err := g.Sync(latencydiag.WithSlot(context.Background(), 44)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || seen.Load() <= first {
		t.Fatalf("second batch not distinct: first=%d second=%d calls=%d", first, seen.Load(), calls.Load())
	}
	latencydiag.Flush()
}
