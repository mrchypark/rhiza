package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimelineCapturesLogicalCompletionBeforeWorkerDelay(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"code":"commit_unknown"}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	var now atomic.Int64
	now.Store(1_999_999_999)
	var reads atomic.Int64
	timeline := &latencyTimeline{samples: make([]latencySample, 1), clock: func() (int64, error) {
		reads.Add(1)
		return now.Load(), nil
	}}
	_, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, `{"request_id":"bench-0","sql":"INSERT INTO benchmark_writes(id) VALUES (0)"}`, 1, true, nil, 0, timeline, time.Now())
	// Simulate worker bookkeeping resuming after the next second begins.
	now.Store(2_000_000_000)
	if retries != 1 || !errors.Is(err, errInvalidInsertReceipt) || attempts.Load() != 2 || reads.Load() != 1 {
		t.Fatalf("retries=%d err=%v attempts=%d reads=%d", retries, err, attempts.Load(), reads.Load())
	}
	buckets := makeLatencyBuckets(timeline.samples)
	if len(buckets) != 1 || buckets[0].StartBootNS != 1_000_000_000 || buckets[0].Errors != 1 || buckets[0].Successes != 0 {
		t.Fatalf("buckets=%+v", buckets)
	}
}

func TestTimelinePreflightBeforeRequests(t *testing.T) {
	if os.Getenv("RHIZA_TEST_TIMELINE_PREFLIGHT") == "1" {
		timelineClock = func() (int64, error) { return 0, errors.New("test clock unavailable") }
		os.Args = []string{"rhiza-bench", "-latency-timeline", "-url", os.Getenv("RHIZA_TEST_TIMELINE_URL"), "-n", "1", "-c", "1"}
		main()
		return
	}
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestTimelinePreflightBeforeRequests$")
	cmd.Env = append(os.Environ(), "RHIZA_TEST_TIMELINE_PREFLIGHT=1", "RHIZA_TEST_TIMELINE_URL="+server.URL)
	output, err := cmd.CombinedOutput()
	if err == nil || requests.Load() != 0 || !strings.Contains(string(output), "latency timeline clock unavailable") {
		t.Fatalf("err=%v requests=%d output=%s", err, requests.Load(), output)
	}
}

func TestLatencyBucketsBoundariesAndErrors(t *testing.T) {
	samples := []latencySample{
		{ns: 2_000_000_000, elapsed: 8 * time.Millisecond},
		{ns: 1_999_999_999, elapsed: 2 * time.Millisecond, success: true},
		{ns: 1_000_000_000, elapsed: time.Millisecond},
	}
	timeline := latencyTimeline{samples: samples}
	encoded, err := json.Marshal(result{LastRequestCompletionBootNS: 1_900_000_000, LastLogicalCompletionBootNS: timeline.lastCompletion()})
	if err != nil || !strings.Contains(string(encoded), `"last_request_completion_boottime_ns":1900000000`) || !strings.Contains(string(encoded), `"last_logical_request_completion_boottime_ns":2000000000`) {
		t.Fatalf("completion encoding=%s err=%v", encoded, err)
	}
	got := makeLatencyBuckets(samples)
	if len(got) != 2 || got[0].StartBootNS != 1_000_000_000 || got[0].Successes != 1 || got[0].Errors != 1 || got[0].SuccessP99MS != 2 || got[0].ErrorP99MS != 1 || got[1].StartBootNS != 2_000_000_000 || got[1].Errors != 1 || got[1].ErrorP99MS != 8 {
		t.Fatalf("unexpected buckets: %+v", got)
	}
}

func TestLatencyBucketNearestRank(t *testing.T) {
	samples := make([]latencySample, 100)
	for i := range samples {
		samples[i] = latencySample{ns: 1, elapsed: time.Duration(i+1) * time.Millisecond, success: true}
	}
	if got := makeLatencyBuckets(samples[:2]); got[0].SuccessP99MS != 2 {
		t.Fatalf("two-sample p99 = %v", got[0].SuccessP99MS)
	}
	if got := makeLatencyBuckets(samples); got[0].SuccessP99MS != 99 {
		t.Fatalf("100-sample p99 = %v", got[0].SuccessP99MS)
	}
	encoded, err := json.Marshal(makeLatencyBuckets(samples)[0])
	if err != nil || !strings.Contains(string(encoded), `"success_p99_nearest_rank_ms":99`) || !strings.Contains(string(encoded), `"error_p99_nearest_rank_ms":0`) {
		t.Fatalf("bucket encoding=%s err=%v", encoded, err)
	}
}

func TestTimelineClockPreflight(t *testing.T) {
	for _, tc := range []struct {
		ns          int64
		err         error
		wantFailure bool
	}{
		{ns: 1},
		{ns: 0, wantFailure: true},
		{ns: -1, wantFailure: true},
		{err: errors.New("unavailable"), wantFailure: true},
	} {
		err := checkTimelineClock(func() (int64, error) { return tc.ns, tc.err })
		if (err != nil) != tc.wantFailure {
			t.Fatalf("ns=%d err=%v got=%v", tc.ns, tc.err, err)
		}
	}
}

func TestLatencyTimelineConcurrentAndClockFailure(t *testing.T) {
	timeline := latencyTimeline{samples: make([]latencySample, 100), clock: func() (int64, error) { return 1, nil }}
	var workers sync.WaitGroup
	for i := range timeline.samples {
		workers.Add(1)
		go func() { defer workers.Done(); timeline.record(i, time.Millisecond, true) }()
	}
	workers.Wait()
	got := makeLatencyBuckets(timeline.samples)
	if timeline.failed.Load() || len(got) != 1 || got[0].Successes != 100 {
		t.Fatalf("failed=%v buckets=%+v", timeline.failed.Load(), got)
	}
	for _, clock := range []func() (int64, error){func() (int64, error) { return 0, nil }, func() (int64, error) { return 0, errors.New("unavailable") }} {
		failed := latencyTimeline{samples: make([]latencySample, 1), clock: clock}
		failed.record(0, time.Millisecond, false)
		if !failed.failed.Load() {
			t.Fatal("clock failure accepted")
		}
	}
}
