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
	Requests                    int               `json:"requests"`
	Successes                   int               `json:"successes"`
	Errors                      uint64            `json:"errors"`
	Retries                     uint64            `json:"retries,omitempty"`
	DurationMS                  float64           `json:"duration_ms"`
	OpsPerSec                   float64           `json:"ops_per_sec"`
	SuccessOpsPerSec            float64           `json:"success_ops_per_sec"`
	P50MS                       float64           `json:"p50_ms"`
	P95MS                       float64           `json:"p95_ms"`
	P99MS                       float64           `json:"p99_ms"`
	MaxMS                       float64           `json:"max_ms"`
	ErrorP50MS                  float64           `json:"error_p50_ms,omitempty"`
	ErrorP95MS                  float64           `json:"error_p95_ms,omitempty"`
	ErrorP99MS                  float64           `json:"error_p99_ms,omitempty"`
	ErrorKinds                  map[string]uint64 `json:"error_kinds,omitempty"`
	ErrorSamples                map[string]string `json:"error_samples,omitempty"`
	ReceiptOracle               string            `json:"receipt_oracle,omitempty"`
	FaultOverlap                bool              `json:"fault_overlap_control,omitempty"`
	FaultWindow                 *faultWindow      `json:"fault_window,omitempty"`
	ProfileWindow               *profileWindow    `json:"profile_window,omitempty"`
	LastRequestCompletionBootNS int64             `json:"last_request_completion_boottime_ns,omitempty"`
	LastLogicalCompletionBootNS int64             `json:"last_logical_request_completion_boottime_ns,omitempty"`
	LatencyBuckets              []latencyBucket   `json:"latency_buckets,omitempty"`
}

var timelineClock = bootTimeNS

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:18090", "Rhiza HTTP base URL")
	requestPath := flag.String("path", "/healthz", "request path")
	body := flag.String("body", "", "JSON body; {{id}} is replaced per request")
	requests := flag.Int("n", 1000, "request count")
	concurrency := flag.Int("c", 16, "worker count")
	timeout := flag.Duration("timeout", 30*time.Second, "request timeout")
	commitUnknownRetries := flag.Int("commit-unknown-retries", 0, "maximum same-request retries after commit_unknown")
	verifyInsert := flag.Bool("verify-insert-receipt", false, "verify benchmark INSERT committed receipt and exact id")
	timelineEnabled := flag.Bool("latency-timeline", false, "record logical request latency in CLOCK_BOOTTIME second buckets")
	faultControlFile := flag.String("fault-control-file", "", "optional owned file for loopback fault overlap control URL")
	profileControlFile := flag.String("profile-control-file", "", "optional owned file for non-destructive profile overlap control URL")
	faultPID := flag.Int("fault-pid", 0, "fixed owned fault target PID, bound before requests (Linux pidfd required)")
	faultStart := flag.String("fault-start-time", "", "Linux target start-time ticks captured by the owning parent")
	flag.Parse()
	if *requests < 1 || *concurrency < 1 || *concurrency > *requests || *commitUnknownRetries < 0 {
		fmt.Fprintln(os.Stderr, "invalid request count, concurrency, or commit-unknown retry count")
		os.Exit(2)
	}
	if *timelineEnabled {
		if *requests > maxTimelineRequests {
			fmt.Fprintln(os.Stderr, "latency timeline request count exceeds maximum", maxTimelineRequests)
			os.Exit(2)
		}
		if err := checkTimelineClock(timelineClock); err != nil {
			fmt.Fprintln(os.Stderr, "latency timeline clock unavailable:", err)
			os.Exit(1)
		}
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
	var timeline *latencyTimeline
	if *timelineEnabled {
		timeline = &latencyTimeline{samples: make([]latencySample, *requests), clock: timelineClock}
	}
	ctx := context.Background()
	var progress *faultProgress
	var control *http.Server
	if (*faultControlFile == "") != (*faultPID == 0) || (*faultControlFile == "") != (*faultStart == "") {
		fmt.Fprintln(os.Stderr, "fault control file, fixed PID and start time must be supplied together")
		os.Exit(2)
	}
	if *faultControlFile != "" && *profileControlFile != "" {
		fmt.Fprintln(os.Stderr, "fault and profile controls are mutually exclusive")
		os.Exit(2)
	}
	if *faultControlFile != "" || *profileControlFile != "" {
		progress = &faultProgress{active: make(map[int]*requestInterval), epoch: time.Now(), profile: *profileControlFile != ""}
		if *faultControlFile != "" {
			target, err := openFaultTarget(*faultPID, *faultStart, os.Getppid())
			if err != nil {
				fmt.Fprintln(os.Stderr, "cannot bind fault target:", err)
				os.Exit(1)
			}
			defer target.close()
			progress.signal = target.signal
		}
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
		control = &http.Server{Handler: progress, ReadHeaderTimeout: 2 * time.Second}
		defer control.Close()
		controlFile := *faultControlFile
		if progress.profile {
			controlFile = *profileControlFile
		}
		address, err := os.OpenFile(controlFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
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
				eligible := false
				if progress != nil && progress.profile {
					eligible = progress.profileStart()
				}
				status, sample, retryCount, requestErr := doRequest(ctx, client, method, *baseURL+*requestPath, payload, *commitUnknownRetries, *verifyInsert, progress, id, timeline, begin)
				if eligible && requestErr == nil && status >= 200 && status < 300 {
					progress.profileSuccess()
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
	if control != nil {
		// Drain an already accepted /fault response before finalizing. This is
		// outside measured work; no request completion is held for injection.
		shutdown, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := control.Shutdown(shutdown)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, "fault control drain failed:", err)
			_ = control.Close()
			os.Exit(1)
		}
	}
	if timeline != nil && timeline.failed.Load() {
		fmt.Fprintln(os.Stderr, "latency timeline completion clock unavailable")
		os.Exit(1)
	}
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
	elapsedSec := duration.Seconds()
	if elapsedSec <= 0 {
		elapsedSec = 1e-9
	}
	output := result{
		Requests: *requests, Successes: len(latencies), Errors: failures.Load(), Retries: retries.Load(), DurationMS: float64(duration) / float64(time.Millisecond),
		OpsPerSec: float64(*requests) / elapsedSec, SuccessOpsPerSec: float64(len(latencies)) / elapsedSec, P50MS: quantile(latencies, .50), P95MS: quantile(latencies, .95),
		P99MS: quantile(latencies, .99), MaxMS: quantile(latencies, 1), ErrorP50MS: quantile(errorLatencies, .50),
		ErrorP95MS: quantile(errorLatencies, .95), ErrorP99MS: quantile(errorLatencies, .99), ErrorKinds: errorKinds, ErrorSamples: samples,
		FaultOverlap: progress != nil && !progress.profile,
	}
	if progress != nil && progress.profile {
		window := progress.profileResult()
		output.ProfileWindow = &window
		if !progress.completionClockFailed.Load() {
			output.LastRequestCompletionBootNS = progress.lastCompletion.Load()
		}
	} else if progress != nil {
		window := progress.window()
		output.FaultWindow = &window
	}
	if *verifyInsert {
		output.ReceiptOracle = "sql-insert-id-v1"
	}
	if timeline != nil {
		output.LastLogicalCompletionBootNS = timeline.lastCompletion()
		output.LatencyBuckets = makeLatencyBuckets(timeline.samples)
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if progress != nil && progress.profile && (progress.completionClockFailed.Load() || progress.lastCompletion.Load() <= 0) {
		fmt.Fprintln(os.Stderr, "profile completion clock unavailable")
		os.Exit(1)
	}
}

func doRequest(ctx context.Context, client *http.Client, method, url, payload string, maxRetries int, verifyInsert bool, progress *faultProgress, id int, timeline *latencyTimeline, begin time.Time) (status int, sample []byte, retries int, requestErr error) {
	if progress != nil && !progress.profile {
		var finish func(time.Time)
		ctx, finish = progress.traceRequest(ctx, id)
		defer func() { finish(time.Now()) }()
	}
	if timeline != nil {
		defer func() {
			timeline.record(id, time.Since(begin), requestErr == nil && status >= 200 && status < 300)
		}()
	}
	for retries := 0; ; retries++ {
		request, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBufferString(payload))
		if err != nil {
			if progress != nil && progress.profile {
				progress.completionClockFailed.Store(true)
			}
			return 0, nil, retries, err
		}
		if payload != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := client.Do(request)
		if err != nil {
			if progress != nil && progress.profile {
				progress.completionClockFailed.Store(true)
			}
			return 0, nil, retries, err
		}
		var body []byte
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			if verifyInsert {
				body, err = io.ReadAll(io.LimitReader(response.Body, 4097))
			} else {
				_, err = io.Copy(io.Discard, response.Body)
			}
		} else {
			limit := int64(4096)
			if progress != nil && progress.profile {
				limit++
			}
			body, err = io.ReadAll(io.LimitReader(response.Body, limit))
		}
		if progress != nil && progress.profile {
			if err != nil || len(body) > 4096 {
				progress.completionClockFailed.Store(true)
			} else {
				progress.recordCompletion()
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			if len(body) > 4096 {
				body = body[:4096]
			}
		}
		response.Body.Close()
		if err == nil && verifyInsert && response.StatusCode >= 200 && response.StatusCode < 300 {
			err = validateInsertReceipt(payload, body)
		}
		retry := err == nil && isCommitUnknown(response.StatusCode, body) && retries != maxRetries
		if !retry {
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

// Only fault lanes enable this coordinator. Retain only the current requests
// and one bounded fault snapshot, never the complete benchmark history.
type faultProgress struct {
	mu                       sync.Mutex
	token                    string
	active                   map[int]*requestInterval
	epoch                    time.Time
	signal                   func() (time.Time, time.Time, error)
	attempted                bool
	snapshot                 []*requestInterval
	before, after            time.Time
	signalOK                 bool
	reason                   string
	profile                  bool
	profileBegin, profileEnd time.Time
	profileSuccesses         uint64
	lastCompletion           atomic.Int64
	completionClockFailed    atomic.Bool
	completionClock          func() (int64, error)
}

func (p *faultProgress) recordCompletion() {
	clock := p.completionClock
	if clock == nil {
		clock = bootTimeNS
	}
	ns, err := clock()
	if err != nil || ns <= 0 {
		p.completionClockFailed.Store(true)
		return
	}
	for previous := p.lastCompletion.Load(); ns > previous; previous = p.lastCompletion.Load() {
		if p.lastCompletion.CompareAndSwap(previous, ns) {
			return
		}
	}
}

type profileWindow struct {
	Overlap    bool   `json:"overlap"`
	Incomplete bool   `json:"incomplete"`
	Successes  uint64 `json:"successes"`
	BeginNS    int64  `json:"begin_ns"`
	EndNS      int64  `json:"end_ns"`
	Reason     string `json:"reason"`
}

func (p *faultProgress) profileResult() profileWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := profileWindow{Successes: p.profileSuccesses, Incomplete: true, Reason: "bracket_incomplete"}
	if p.profileBegin.IsZero() || p.profileEnd.IsZero() {
		return r
	}
	r.BeginNS, r.EndNS = p.profileBegin.Sub(p.epoch).Nanoseconds(), p.profileEnd.Sub(p.epoch).Nanoseconds()
	r.Incomplete = false
	r.Overlap = r.Successes > 0
	r.Reason = "no_successful_request_inside_bracket"
	if r.Overlap {
		r.Reason = "qualified"
	}
	return r
}

func (p *faultProgress) profileStart() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.profileBegin.IsZero() && p.profileEnd.IsZero()
}

func (p *faultProgress) profileSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.profileEnd.IsZero() {
		p.profileSuccesses++
	}
}

type requestInterval struct {
	sent     atomic.Pointer[time.Time]
	finished atomic.Pointer[time.Time]
}

type faultWindow struct {
	Attempted         bool   `json:"attempted"`
	SignalSucceeded   bool   `json:"signal_succeeded"`
	Overlap           bool   `json:"overlap"`
	Witnesses         int    `json:"witnesses"`
	SnapshotRequests  int    `json:"snapshot_requests"`
	SignalBeforeNS    int64  `json:"signal_before_ns"`
	SignalAfterNS     int64  `json:"signal_after_ns"`
	WitnessSentNS     int64  `json:"witness_sent_ns"`
	WitnessFinishedNS int64  `json:"witness_finished_ns"`
	Reason            string `json:"reason"`
}

func (p *faultProgress) traceRequest(ctx context.Context, id int) (context.Context, func(time.Time)) {
	interval := &requestInterval{}
	p.mu.Lock()
	p.active[id] = interval
	p.mu.Unlock()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if interval.finished.Load() == nil {
			if info.Err != nil {
				return
			}
			now := time.Now()
			interval.sent.CompareAndSwap(nil, &now)
		}
	}})
	return ctx, func(at time.Time) {
		interval.finished.Store(&at)
		p.mu.Lock()
		delete(p.active, id)
		p.mu.Unlock()
	}
}

func (p *faultProgress) inject() error {
	p.mu.Lock()
	if p.attempted {
		p.mu.Unlock()
		return errors.New("fault already attempted")
	}
	p.attempted = true
	for _, interval := range p.active {
		if interval.sent.Load() != nil && interval.finished.Load() == nil {
			p.snapshot = append(p.snapshot, interval)
		}
	}
	if len(p.snapshot) == 0 {
		p.reason = "no_sent_request"
		p.mu.Unlock()
		return errors.New("no sent request at fault snapshot")
	}
	p.mu.Unlock()
	// No request-tracking/completion lock is held across the syscall.
	before, after, err := p.signal()
	p.mu.Lock()
	p.before, p.after, p.signalOK = before, after, err == nil
	if err != nil {
		p.reason = "signal_failed"
	} else {
		p.reason = "awaiting_completed_intervals"
	}
	p.mu.Unlock()
	return err
}

func coversFault(sent, finished, before, after time.Time) bool {
	return !sent.IsZero() && !finished.IsZero() && !before.IsZero() && !after.Before(before) && !sent.After(before) && !finished.Before(after)
}

func (p *faultProgress) window() faultWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := faultWindow{Attempted: p.attempted, SignalSucceeded: p.signalOK, SnapshotRequests: len(p.snapshot), Reason: p.reason}
	if !p.attempted {
		result.Reason = "not_attempted"
		return result
	}
	if !p.signalOK {
		return result
	}
	result.SignalBeforeNS, result.SignalAfterNS = p.before.Sub(p.epoch).Nanoseconds(), p.after.Sub(p.epoch).Nanoseconds()
	for _, interval := range p.snapshot {
		sent, finished := interval.sent.Load(), interval.finished.Load()
		if sent != nil && finished != nil && coversFault(*sent, *finished, p.before, p.after) {
			result.Witnesses++
			result.WitnessSentNS, result.WitnessFinishedNS = sent.Sub(p.epoch).Nanoseconds(), finished.Sub(p.epoch).Nanoseconds()
		}
	}
	result.Overlap = result.Witnesses > 0
	if result.Overlap {
		result.Reason = "qualified"
	} else {
		result.Reason = "no_completed_interval_spans_signal"
	}
	return result
}

func (p *faultProgress) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	if p.profile {
		if r.ContentLength != 0 {
			http.Error(w, "body forbidden", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		switch path {
		case "/profile/begin":
			if !p.profileBegin.IsZero() {
				p.mu.Unlock()
				http.Error(w, "bracket already begun", http.StatusConflict)
				return
			}
			p.profileBegin = time.Now()
		case "/profile/status":
			if p.profileBegin.IsZero() || !p.profileEnd.IsZero() {
				p.mu.Unlock()
				http.Error(w, "bracket not open", http.StatusConflict)
				return
			}
		case "/profile/end":
			if p.profileBegin.IsZero() || !p.profileEnd.IsZero() {
				p.mu.Unlock()
				http.Error(w, "bracket not open", http.StatusConflict)
				return
			}
			p.profileEnd = time.Now()
		default:
			p.mu.Unlock()
			http.NotFound(w, r)
			return
		}
		successes := p.profileSuccesses
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Successes uint64 `json:"successes"`
		}{successes})
		return
	}
	if path != "/fault" {
		http.NotFound(w, r)
		return
	}
	if r.ContentLength != 0 {
		http.Error(w, "fault body forbidden", http.StatusBadRequest)
		return
	}
	if err := p.inject(); err != nil {
		fmt.Fprintln(os.Stderr, "fault coordinator refusal:", p.window().Reason)
		http.Error(w, "fault not qualified: target or request window unavailable", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p.window())
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
