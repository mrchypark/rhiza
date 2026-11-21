package main

import (
	"errors"
	"sort"
	"sync/atomic"
	"time"
)

const maxTimelineRequests = 1_000_000

func checkTimelineClock(clock func() (int64, error)) error {
	ns, err := clock()
	if err != nil {
		return err
	}
	if ns <= 0 {
		return errors.New("invalid boot time")
	}
	return nil
}

type latencySample struct {
	ns      int64
	elapsed time.Duration
	success bool
}

type latencyTimeline struct {
	samples []latencySample
	failed  atomic.Bool
	clock   func() (int64, error)
}

func (t *latencyTimeline) record(id int, elapsed time.Duration, success bool) {
	ns, err := t.clock()
	if err != nil || ns <= 0 {
		t.failed.Store(true)
		return
	}
	t.samples[id] = latencySample{ns: ns, elapsed: elapsed, success: success}
}

func (t *latencyTimeline) lastCompletion() int64 {
	var latest int64
	for _, sample := range t.samples {
		if sample.ns > latest {
			latest = sample.ns
		}
	}
	return latest
}

type latencyBucket struct {
	StartBootNS  int64   `json:"start_boottime_ns"`
	Successes    int     `json:"successes"`
	Errors       int     `json:"errors"`
	SuccessP99MS float64 `json:"success_p99_nearest_rank_ms"`
	ErrorP99MS   float64 `json:"error_p99_nearest_rank_ms"`
}

func makeLatencyBuckets(samples []latencySample) []latencyBucket {
	sort.Slice(samples, func(i, j int) bool {
		a, b := samples[i].ns/int64(time.Second), samples[j].ns/int64(time.Second)
		if a != b {
			return a < b
		}
		if samples[i].success != samples[j].success {
			return samples[i].success
		}
		return samples[i].elapsed < samples[j].elapsed
	})
	var buckets []latencyBucket
	for start := 0; start < len(samples); {
		second := samples[start].ns / int64(time.Second)
		end := start
		bucket := latencyBucket{StartBootNS: second * int64(time.Second)}
		for end < len(samples) && samples[end].ns/int64(time.Second) == second {
			if samples[end].success {
				bucket.Successes++
			} else {
				bucket.Errors++
			}
			end++
		}
		// Nearest-rank p99 selects the maximum for fewer than 100 samples.
		if bucket.Successes > 0 {
			bucket.SuccessP99MS = float64(samples[start+(99*bucket.Successes+99)/100-1].elapsed) / float64(time.Millisecond)
		}
		if bucket.Errors > 0 {
			bucket.ErrorP99MS = float64(samples[start+bucket.Successes+(99*bucket.Errors+99)/100-1].elapsed) / float64(time.Millisecond)
		}
		buckets = append(buckets, bucket)
		start = end
	}
	return buckets
}
