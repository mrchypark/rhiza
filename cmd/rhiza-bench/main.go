package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Requests         int               `json:"requests"`
	Successes        int               `json:"successes"`
	Errors           uint64            `json:"errors"`
	DurationMS       float64           `json:"duration_ms"`
	OpsPerSec        float64           `json:"ops_per_sec"`
	SuccessOpsPerSec float64           `json:"success_ops_per_sec"`
	P50MS            float64           `json:"p50_ms"`
	P95MS            float64           `json:"p95_ms"`
	P99MS            float64           `json:"p99_ms"`
	MaxMS            float64           `json:"max_ms"`
	ErrorP50MS       float64           `json:"error_p50_ms,omitempty"`
	ErrorP95MS       float64           `json:"error_p95_ms,omitempty"`
	ErrorP99MS       float64           `json:"error_p99_ms,omitempty"`
	ErrorKinds       map[string]uint64 `json:"error_kinds,omitempty"`
	ErrorSamples     map[string]string `json:"error_samples,omitempty"`
}

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:18090", "Rhiza HTTP base URL")
	requestPath := flag.String("path", "/healthz", "request path")
	body := flag.String("body", "", "JSON body; {{id}} is replaced per request")
	requests := flag.Int("n", 1000, "request count")
	concurrency := flag.Int("c", 16, "worker count")
	timeout := flag.Duration("timeout", 30*time.Second, "request timeout")
	flag.Parse()
	if *requests < 1 || *concurrency < 1 || *concurrency > *requests {
		fmt.Fprintln(os.Stderr, "invalid request count or concurrency")
		os.Exit(2)
	}

	client := &http.Client{Timeout: *timeout, Transport: &http.Transport{
		MaxIdleConns: *concurrency, MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency,
	}}
	jobs := make(chan int)
	latencies := make([]time.Duration, 0, *requests)
	errorLatencies := make([]time.Duration, 0)
	var latencyMu sync.Mutex
	var failures atomic.Uint64
	var transportFailures atomic.Uint64
	var statuses [600]atomic.Uint64
	errorSamples := make(map[int]string)
	var errorSamplesMu sync.Mutex
	var workers sync.WaitGroup
	ctx := context.Background()
	started := time.Now()
	for range *concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range jobs {
				payload := strings.ReplaceAll(*body, "{{id}}", strconv.Itoa(id))
				method := http.MethodGet
				var reader io.Reader
				if payload != "" {
					method, reader = http.MethodPost, bytes.NewBufferString(payload)
				}
				request, err := http.NewRequestWithContext(ctx, method, *baseURL+*requestPath, reader)
				if err != nil {
					failures.Add(1)
					transportFailures.Add(1)
					continue
				}
				if payload != "" {
					request.Header.Set("Content-Type", "application/json")
				}
				begin := time.Now()
				response, err := client.Do(request)
				elapsed := time.Since(begin)
				if err != nil {
					failures.Add(1)
					transportFailures.Add(1)
					latencyMu.Lock()
					errorLatencies = append(errorLatencies, elapsed)
					latencyMu.Unlock()
					continue
				}
				var copyErr error
				if response.StatusCode >= 200 && response.StatusCode < 300 {
					_, copyErr = io.Copy(io.Discard, response.Body)
				} else {
					var sample []byte
					sample, copyErr = io.ReadAll(io.LimitReader(response.Body, 4096))
					if text := strings.TrimSpace(string(sample)); text != "" {
						errorSamplesMu.Lock()
						if _, exists := errorSamples[response.StatusCode]; !exists {
							errorSamples[response.StatusCode] = text
						}
						errorSamplesMu.Unlock()
					}
				}
				response.Body.Close()
				if copyErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
					failures.Add(1)
					if copyErr != nil {
						transportFailures.Add(1)
					}
					latencyMu.Lock()
					errorLatencies = append(errorLatencies, elapsed)
					latencyMu.Unlock()
					if response.StatusCode >= 0 && response.StatusCode < len(statuses) {
						statuses[response.StatusCode].Add(1)
					}
				} else {
					latencyMu.Lock()
					latencies = append(latencies, elapsed)
					latencyMu.Unlock()
				}
			}
		}()
	}
	for id := range *requests {
		jobs <- id
	}
	close(jobs)
	workers.Wait()
	duration := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	sort.Slice(errorLatencies, func(i, j int) bool { return errorLatencies[i] < errorLatencies[j] })
	quantile := func(values []time.Duration, q float64) float64 {
		if len(values) == 0 {
			return 0
		}
		index := int(float64(len(values)-1) * q)
		return float64(values[index]) / float64(time.Millisecond)
	}
	errorKinds := make(map[string]uint64)
	if count := transportFailures.Load(); count != 0 {
		errorKinds["transport"] = count
	}
	for status := range statuses {
		if count := statuses[status].Load(); count != 0 {
			errorKinds["http_"+strconv.Itoa(status)] = count
		}
	}
	samples := make(map[string]string, len(errorSamples))
	for status, sample := range errorSamples {
		samples["http_"+strconv.Itoa(status)] = sample
	}
	output := result{
		Requests: *requests, Successes: len(latencies), Errors: failures.Load(), DurationMS: float64(duration) / float64(time.Millisecond),
		OpsPerSec: float64(*requests) / duration.Seconds(), SuccessOpsPerSec: float64(len(latencies)) / duration.Seconds(), P50MS: quantile(latencies, .50), P95MS: quantile(latencies, .95),
		P99MS: quantile(latencies, .99), MaxMS: quantile(latencies, 1), ErrorP50MS: quantile(errorLatencies, .50),
		ErrorP95MS: quantile(errorLatencies, .95), ErrorP99MS: quantile(errorLatencies, .99), ErrorKinds: errorKinds, ErrorSamples: samples,
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
