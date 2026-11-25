//go:build rhiza_local_testhooks

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func localCompactionObservation(valid bool, overlaps, successes int) string {
	switch {
	case !valid:
		return "missing_build_boundaries"
	case overlaps == 0:
		return "no_overlapping_requests"
	case successes == 0:
		return "all_overlapping_requests_failed"
	case successes < 100:
		return "limited_successful_samples"
	default:
		return "p99_sample_sufficient"
	}
}

func TestLocalCompactionObservation(t *testing.T) {
	for _, tc := range []struct {
		valid               bool
		overlaps, successes int
		want                string
	}{
		{false, 0, 0, "missing_build_boundaries"},
		{true, 0, 0, "no_overlapping_requests"},
		{true, 4, 0, "all_overlapping_requests_failed"},
		{true, 99, 99, "limited_successful_samples"},
		{true, 100, 100, "p99_sample_sufficient"},
	} {
		if got := localCompactionObservation(tc.valid, tc.overlaps, tc.successes); got != tc.want {
			t.Errorf("observation=%q want %q", got, tc.want)
		}
	}
}

// TestLocalCompactionCIEvidence is an opt-in paired CI measurement, not a latency gate.
func TestLocalCompactionCIEvidence(t *testing.T) {
	if os.Getenv("RHIZA_LOCAL_COMPACTION_EVIDENCE") != "1" {
		t.Skip("CI evidence only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	root, remote := t.TempDir(), t.TempDir()
	config := types.ExecutionConfig{Local: true, NodeID: "local", ClusterID: "backup", DataDir: root, ObjStoreProvider: "filesystem", ObjStoreDir: remote, ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			_ = n.Shutdown()
		}
	}()
	if config.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		t.Fatal("expected BeforeAck")
	}
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE samples (id INTEGER PRIMARY KEY, payload TEXT)"}); err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 4096)
	for i := 0; i < 256; i++ {
		if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: fmt.Sprintf("seed-%d", i), SQL: fmt.Sprintf("INSERT INTO samples VALUES (%d, '%s')", i, payload)}); err != nil {
			t.Fatal(err)
		}
	}
	type sample struct {
		start, end time.Time
		id         int
		err        error
	}
	var mu sync.Mutex
	var samples []sample
	type window struct{ begin, end time.Time }
	var windows []window
	reset := localtesthooks.Set(func(name string) {
		mu.Lock()
		defer mu.Unlock()
		switch name {
		case "qlog:before-compaction-build":
			windows = append(windows, window{begin: time.Now()})
		case "qlog:after-compaction-build-synced-before-manifest":
			if len(windows) > 0 {
				windows[len(windows)-1].end = time.Now()
			}
		}
	})
	defer reset()
	stop := make(chan struct{})
	var workers sync.WaitGroup
	const workerCount, cycleCount = 8, 32
	ready := make(chan struct{}, workerCount)
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			ready <- struct{}{}
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				id := 1000000 + worker*100000 + seq
				start := time.Now()
				_, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: fmt.Sprintf("foreground-%d", id), SQL: fmt.Sprintf("INSERT INTO samples VALUES (%d, 'foreground')", id)})
				finished := time.Now()
				mu.Lock()
				samples = append(samples, sample{start: start, end: finished, id: id, err: err})
				mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}(worker)
	}
	for i := 0; i < workerCount; i++ {
		<-ready
	}
	reclaimStart := time.Now()
	var reclaimErr error
	completed := 0
	for cycle := 0; cycle < cycleCount; cycle++ {
		id := 256 + cycle
		_, reclaimErr = n.server.Execute(ctx, network.ExecuteRequest{RequestID: fmt.Sprintf("cycle-%d", cycle), SQL: fmt.Sprintf("INSERT INTO samples VALUES (%d, '%s')", id, payload)})
		if reclaimErr != nil {
			break
		}
		reclaimErr = n.reclaimLocalCheckpoint(ctx)
		if reclaimErr != nil {
			break
		}
		completed++
	}
	reclaimEnd := time.Now()
	close(stop)
	workers.Wait()
	boundariesValid := len(windows) == cycleCount && completed == cycleCount
	var buildTime time.Duration
	for i, w := range windows {
		valid := !w.begin.IsZero() && !w.end.IsZero() && !w.end.Before(w.begin)
		if !valid || (i > 0 && w.begin.Before(windows[i-1].end)) {
			boundariesValid = false
		}
		if valid {
			buildTime += w.end.Sub(w.begin)
		}
		cycleData, marshalErr := json.Marshal(map[string]any{"cycle": i, "valid": valid, "build_start_unix_ns": w.begin.UnixNano(), "build_end_unix_ns": w.end.UnixNano()})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		t.Logf("LOCAL_COMPACTION_CYCLE %s", cycleData)
	}
	var latencies []float64
	var acknowledged []int
	errors := 0
	overlaps := 0
	for _, s := range samples {
		status := "acknowledged"
		if s.err != nil {
			status = s.err.Error()
		}
		var overlap any
		var overlapCycles []int
		if boundariesValid {
			for i, w := range windows {
				if s.start.Before(w.end) && s.end.After(w.begin) {
					overlapCycles = append(overlapCycles, i)
				}
			}
			overlap = len(overlapCycles) > 0
		}
		raw, err := json.Marshal(map[string]any{"id": s.id, "start_unix_ns": s.start.UnixNano(), "end_unix_ns": s.end.UnixNano(), "latency_ns": s.end.Sub(s.start).Nanoseconds(), "status": status, "overlap": overlap, "overlap_cycles": overlapCycles})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("LOCAL_COMPACTION_REQUEST %s", raw)
		if s.err == nil {
			acknowledged = append(acknowledged, s.id)
		}
		if boundariesValid && len(overlapCycles) > 0 {
			overlaps++
			if s.err != nil {
				errors++
			} else {
				latencies = append(latencies, float64(s.end.Sub(s.start))/float64(time.Millisecond))
			}
		}
	}
	sort.Float64s(latencies)
	percentile := func(p int) any {
		if len(latencies) == 0 {
			return nil
		}
		return latencies[((len(latencies)-1)*p)/100]
	}
	result := map[string]any{"cycles_total_ms": float64(reclaimEnd.Sub(reclaimStart)) / float64(time.Millisecond), "overlap_requests": overlaps, "overlap_success": len(latencies), "overlap_errors": errors, "p50_ms": percentile(50), "p99_ms": percentile(99), "acknowledged": len(acknowledged), "seed": 256, "workers": workerCount, "cycles_planned": cycleCount, "cycles_completed": completed, "boundaries_valid": boundariesValid}
	result["metrics_present"] = boundariesValid && overlaps > 0
	result["p99_comparable"] = boundariesValid && len(latencies) >= 100
	result["observation"] = localCompactionObservation(boundariesValid, overlaps, len(latencies))
	if boundariesValid {
		result["build_start_unix_ns"] = windows[0].begin.UnixNano()
		result["build_end_unix_ns"] = windows[len(windows)-1].end.UnixNano()
		result["build_ms"] = float64(buildTime) / float64(time.Millisecond)
	}
	if reclaimErr != nil {
		result["reclaim_error"] = reclaimErr.Error()
	}
	result["restore_mode"] = "graceful_shutdown_then_empty_data_dir"
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("LOCAL_COMPACTION_EVIDENCE %s", data)
	if reclaimErr != nil {
		t.Fatalf("actual Local backup reclaim: %v", reclaimErr)
	}
	if !boundariesValid {
		t.Fatal("compaction build boundaries not observed")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	n = nil
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	n = New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatalf("empty-disk restore: %v", err)
	}
	for _, id := range acknowledged {
		rows, err := n.server.Query(ctx, network.QueryRequest{SQL: fmt.Sprintf("SELECT id FROM samples WHERE id = %d", id)})
		if err != nil || len(rows.Rows) != 1 {
			t.Fatalf("ACK %d absent after restore: rows=%v err=%v", id, rows.Rows, err)
		}
	}
	seedRows, err := n.server.Query(ctx, network.QueryRequest{SQL: fmt.Sprintf("SELECT count(*) FROM samples WHERE id < %d", 256+completed)})
	if err != nil || len(seedRows.Rows) != 1 || len(seedRows.Rows[0]) != 1 || fmt.Sprint(seedRows.Rows[0][0]) != fmt.Sprint(256+completed) {
		t.Fatalf("seed ACKs absent after restore: rows=%v err=%v", seedRows.Rows, err)
	}
	t.Logf("LOCAL_COMPACTION_RESTORE mode=graceful_shutdown_then_empty_data_dir verified_acknowledgements=%d separate_crash_test=TestLocalBackupConcurrentCompactionHardKillPreservesACK", len(acknowledged))
	if overlaps == 0 {
		t.Error("not comparable: no foreground request overlapped compaction build")
	}
}
