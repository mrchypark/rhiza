package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
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
	ReceiptOracle    string            `json:"receipt_oracle,omitempty"`
	FaultOverlap     bool              `json:"fault_overlap_control,omitempty"`
}

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:18090", "Rhiza HTTP base URL")
	requestPath := flag.String("path", "/healthz", "request path")
	body := flag.String("body", "", "JSON body; {{id}} is replaced per request")
	requests := flag.Int("n", 1000, "request count")
	concurrency := flag.Int("c", 16, "worker count")
	timeout := flag.Duration("timeout", 30*time.Second, "request timeout")
	commitUnknownRetries := flag.Int("commit-unknown-retries", 0, "maximum same-request retries after commit_unknown")
	verifyInsert := flag.Bool("verify-insert-receipt", false, "verify benchmark INSERT committed receipt and exact id")
	faultControlFile := flag.String("fault-control-file", "", "optional owned file for loopback fault overlap control URL")
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
	var receiptFailures atomic.Uint64
	var statuses [600]atomic.Uint64
	errorSamples := make(map[int]string)
	var errorSamplesMu sync.Mutex
	var workers sync.WaitGroup
	ctx := context.Background()
	var progress *faultProgress
	if *faultControlFile != "" {
		progress = &faultProgress{active: make(map[int]bool)}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		progress.token = hex.EncodeToString(nonce[:])
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		control := &http.Server{Handler: progress, ReadHeaderTimeout: 2 * time.Second}
		defer control.Close()
		address, err := os.OpenFile(*faultControlFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			_, err = io.WriteString(address, "http://"+listener.Addr().String()+"/"+progress.token)
			closeErr := address.Close()
			if err == nil {
				err = closeErr
			}
		}
		if err != nil {
			listener.Close()
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		go control.Serve(listener)
	}
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
				requestCtx := ctx
				var finish func()
				if progress != nil {
					requestCtx, finish = progress.traceRequest(ctx, id)
				}
				status, sample, retryCount, requestErr := doRequest(requestCtx, client, method, *baseURL+*requestPath, payload, *commitUnknownRetries, *verifyInsert)
				if finish != nil {
					finish()
				}
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
					if errors.Is(requestErr, errInvalidInsertReceipt) {
						receiptFailures.Add(1)
					} else if requestErr != nil {
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
	if count := receiptFailures.Load(); count != 0 {
		errorKinds["receipt"] = count
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
		FaultOverlap: progress != nil,
	}
	if *verifyInsert {
		output.ReceiptOracle = "sql-insert-id-v1"
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func doRequest(ctx context.Context, client *http.Client, method, url, payload string, maxRetries int, verifyInsert bool) (int, []byte, int, error) {
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
			if verifyInsert {
				body, err = io.ReadAll(io.LimitReader(response.Body, 4097))
				if err == nil {
					err = validateInsertReceipt(payload, body)
				}
			} else {
				_, err = io.Copy(io.Discard, response.Body)
			}
		} else {
			body, err = io.ReadAll(io.LimitReader(response.Body, 4096))
		}
		response.Body.Close()
		if err != nil || !isCommitUnknown(response.StatusCode, body) || retries == maxRetries {
			return response.StatusCode, body, retries, err
		}
	}
}

// SQL receipts do not set Applied or echo request_id. This oracle is specific
// to the runner's one-row INTEGER PRIMARY KEY INSERT, not arbitrary SQL.
var errInvalidInsertReceipt = errors.New("invalid benchmark INSERT receipt")

func validateInsertReceipt(payload string, body []byte) error {
	var request struct {
		RequestID string `json:"request_id"`
		SQL       string `json:"sql"`
	}
	if json.Unmarshal([]byte(payload), &request) != nil || !strings.HasPrefix(request.RequestID, "bench-") {
		return errInvalidInsertReceipt
	}
	id, err := strconv.Atoi(strings.TrimPrefix(request.RequestID, "bench-"))
	if err != nil || id < 0 || request.SQL != fmt.Sprintf("INSERT INTO benchmark_writes(id) VALUES (%d)", id) {
		return errInvalidInsertReceipt
	}
	var receipt struct {
		Slot             uint64 `json:"slot"`
		Status           string `json:"status"`
		ErrorCode        string `json:"error_code"`
		RowsAffected     int64  `json:"rows_affected"`
		LastInsertID     int64  `json:"last_insert_id"`
		RetryThroughSlot uint64 `json:"retry_through_slot"`
	}
	if len(body) > 4096 || json.Unmarshal(body, &receipt) != nil || receipt.Slot == 0 || receipt.Status != "committed" || receipt.ErrorCode != "" || receipt.RowsAffected != 1 || receipt.LastInsertID != int64(id) || receipt.RetryThroughSlot < receipt.Slot {
		return errInvalidInsertReceipt
	}
	return nil
}

// Only fault lanes enable this two-call control. WroteRequest establishes that
// a logical request was sent; the same unfinished request must span both calls.
// No request payloads, per-request disk writes, or wall-clock comparisons.
type faultProgress struct {
	mu     sync.Mutex
	token  string
	active map[int]bool
	before map[int]bool
}

func (p *faultProgress) traceRequest(ctx context.Context, id int) (context.Context, func()) {
	finished := false // guarded by p.mu, including late transport callbacks
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err != nil {
			return
		}
		p.mu.Lock()
		if !finished {
			p.active[id] = true
		}
		p.mu.Unlock()
	}})
	return ctx, func() {
		p.mu.Lock()
		finished = true
		delete(p.active, id)
		p.mu.Unlock()
	}
}

func (p *faultProgress) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	path := r.URL.Path
	if p.token != "" {
		prefix := "/" + p.token + "/"
		if !strings.HasPrefix(path, prefix) {
			http.NotFound(w, r)
			return
		}
		path = "/" + strings.TrimPrefix(path, prefix)
	}
	switch path {
	case "/begin":
		if p.before != nil || len(p.active) == 0 {
			http.Error(w, "no sent request in flight", http.StatusConflict)
			return
		}
		p.before = make(map[int]bool, len(p.active))
		for id := range p.active {
			p.before[id] = true
		}
	case "/end":
		witnesses := 0
		for id := range p.before {
			if p.active[id] {
				witnesses++
			}
		}
		if witnesses == 0 {
			http.Error(w, "no request spans fault", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"overlap\":true,\"witnesses\":%d}\n", witnesses)
	default:
		http.NotFound(w, r)
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
