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
	Retries          uint64            `json:"retries,omitempty"`
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
	commitUnknownRetries := flag.Int("commit-unknown-retries", 0, "maximum same-request retries after commit_unknown")
	flag.Parse()
	if *requests < 1 || *concurrency < 1 || *concurrency > *requests || *commitUnknownRetries < 0 {
		fmt.Fprintln(os.Stderr, "invalid request count, concurrency, or commit-unknown retry count")
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
	var retries atomic.Uint64
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
				if payload != "" {
					method = http.MethodPost
				}
				begin := time.Now()
				status, sample, retryCount, requestErr := doRequest(ctx, client, method, *baseURL+*requestPath, payload, *commitUnknownRetries)
				retries.Add(uint64(retryCount))
				elapsed := time.Since(begin)
				if status < 200 || status >= 300 {
					if text := strings.TrimSpace(string(sample)); text != "" {
						errorSamplesMu.Lock()
						if _, exists := errorSamples[status]; !exists {
							errorSamples[status] = text
						}
						errorSamplesMu.Unlock()
					}
				}
				if requestErr != nil || status < 200 || status >= 300 {
					failures.Add(1)
					if requestErr != nil {
						transportFailures.Add(1)
					}
					latencyMu.Lock()
					errorLatencies = append(errorLatencies, elapsed)
					latencyMu.Unlock()
					if status >= 0 && status < len(statuses) {
						statuses[status].Add(1)
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
		Requests: *requests, Successes: len(latencies), Errors: failures.Load(), Retries: retries.Load(), DurationMS: float64(duration) / float64(time.Millisecond),
		OpsPerSec: float64(*requests) / duration.Seconds(), SuccessOpsPerSec: float64(len(latencies)) / duration.Seconds(), P50MS: quantile(latencies, .50), P95MS: quantile(latencies, .95),
		P99MS: quantile(latencies, .99), MaxMS: quantile(latencies, 1), ErrorP50MS: quantile(errorLatencies, .50),
		ErrorP95MS: quantile(errorLatencies, .95), ErrorP99MS: quantile(errorLatencies, .99), ErrorKinds: errorKinds, ErrorSamples: samples,
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func doRequest(ctx context.Context, client *http.Client, method, url, payload string, maxRetries int) (int, []byte, int, error) {
	for retries := 0; ; retries++ {
		request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBufferString(payload))
		if err != nil {
			return 0, nil, retries, err
		}
		if payload != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := client.Do(request)
		if err != nil {
			return 0, nil, retries, err
		}
		var body []byte
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			_, err = io.Copy(io.Discard, response.Body)
		} else {
			body, err = io.ReadAll(io.LimitReader(response.Body, 4096))
		}
		response.Body.Close()
		if err != nil || !isCommitUnknown(response.StatusCode, body) || retries == maxRetries {
			return response.StatusCode, body, retries, err
		}
	}
}

func isCommitUnknown(status int, body []byte) bool {
	if status != http.StatusServiceUnavailable {
		return false
	}
	var response struct {
		Code string `json:"code"`
	}
	return json.Unmarshal(body, &response) == nil && response.Code == "commit_unknown"
}
