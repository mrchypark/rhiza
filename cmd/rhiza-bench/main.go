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
	Requests   int     `json:"requests"`
	Errors     uint64  `json:"errors"`
	DurationMS float64 `json:"duration_ms"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	P50MS      float64 `json:"p50_ms"`
	P95MS      float64 `json:"p95_ms"`
	P99MS      float64 `json:"p99_ms"`
	MaxMS      float64 `json:"max_ms"`
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
	latencies := make([]time.Duration, *requests)
	var failures atomic.Uint64
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
					continue
				}
				if payload != "" {
					request.Header.Set("Content-Type", "application/json")
				}
				begin := time.Now()
				response, err := client.Do(request)
				latencies[id] = time.Since(begin)
				if err != nil {
					failures.Add(1)
					continue
				}
				_, copyErr := io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if copyErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
					failures.Add(1)
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
	quantile := func(q float64) float64 {
		index := int(float64(len(latencies)-1) * q)
		return float64(latencies[index]) / float64(time.Millisecond)
	}
	output := result{
		Requests: *requests, Errors: failures.Load(), DurationMS: float64(duration) / float64(time.Millisecond),
		OpsPerSec: float64(*requests) / duration.Seconds(), P50MS: quantile(.50), P95MS: quantile(.95),
		P99MS: quantile(.99), MaxMS: float64(latencies[len(latencies)-1]) / float64(time.Millisecond),
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
