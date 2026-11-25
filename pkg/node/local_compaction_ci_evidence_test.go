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
	var begin, end time.Time
	reset := localtesthooks.Set(func(name string) {
		mu.Lock()
		defer mu.Unlock()
		switch name {
		case "qlog:before-compaction-build":
			begin = time.Now()
		case "qlog:after-compaction-build-synced-before-manifest":
			end = time.Now()
		}
	})
	defer reset()
	stop := make(chan struct{})
	var workers sync.WaitGroup
	ready := make(chan struct{}, 4)
	for worker := 0; worker < 4; worker++ {
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
				time.Sleep(5 * time.Millisecond)
			}
		}(worker)
	}
	for i := 0; i < 4; i++ {
		<-ready
	}
	reclaimStart := time.Now()
	err := n.reclaimLocalCheckpoint(ctx)
	reclaimEnd := time.Now()
	close(stop)
	workers.Wait()
	reclaimErr := err
	boundariesValid := !begin.IsZero() && !end.IsZero() && !end.Before(begin)
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
		if boundariesValid {
			overlap = s.start.Before(end) && s.end.After(begin)
		}
		raw, err := json.Marshal(map[string]any{"id": s.id, "start_unix_ns": s.start.UnixNano(), "end_unix_ns": s.end.UnixNano(), "latency_ns": s.end.Sub(s.start).Nanoseconds(), "status": status, "overlap": overlap})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("LOCAL_COMPACTION_REQUEST %s", raw)
		if s.err == nil {
			acknowledged = append(acknowledged, s.id)
		}
		if boundariesValid && s.start.Before(end) && s.end.After(begin) {
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
	result := map[string]any{"reclaim_ms": float64(reclaimEnd.Sub(reclaimStart)) / float64(time.Millisecond), "overlap_requests": overlaps, "overlap_success": len(latencies), "overlap_errors": errors, "p50_ms": percentile(50), "p99_ms": percentile(99), "acknowledged": len(acknowledged), "seed": 256, "workers": 4, "boundaries_valid": boundariesValid}
	result["metrics_present"] = boundariesValid && overlaps > 0
	result["p99_comparable"] = boundariesValid && len(latencies) >= 100
	result["observation"] = localCompactionObservation(boundariesValid, overlaps, len(latencies))
	if !begin.IsZero() {
		result["build_start_unix_ns"] = begin.UnixNano()
	}
	if !end.IsZero() {
		result["build_end_unix_ns"] = end.UnixNano()
	}
	if boundariesValid {
		result["build_ms"] = float64(end.Sub(begin)) / float64(time.Millisecond)
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
	seedRows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT count(*) FROM samples WHERE id < 256"})
	if err != nil || len(seedRows.Rows) != 1 || len(seedRows.Rows[0]) != 1 || fmt.Sprint(seedRows.Rows[0][0]) != "256" {
		t.Fatalf("seed ACKs absent after restore: rows=%v err=%v", seedRows.Rows, err)
	}
	t.Logf("LOCAL_COMPACTION_RESTORE mode=graceful_shutdown_then_empty_data_dir verified_acknowledgements=%d separate_crash_test=TestLocalBackupConcurrentCompactionHardKillPreservesACK", len(acknowledged))
	if overlaps == 0 {
		t.Error("not comparable: no foreground request overlapped compaction build")
	}
}
