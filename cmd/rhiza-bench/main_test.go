package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsCommitUnknown(t *testing.T) {
	if !isCommitUnknown(http.StatusServiceUnavailable, []byte(`{"code":"commit_unknown"}`)) {
		t.Fatal("commit_unknown response was not recognized")
	}
	if isCommitUnknown(http.StatusServiceUnavailable, []byte(`{"code":"overloaded"}`)) {
		t.Fatal("unrelated 503 response was recognized")
	}
}

func TestSlowestRequestFaultCorrelation(t *testing.T) {
	epoch := time.Now()
	before, after := epoch.Add(10*time.Millisecond), epoch.Add(11*time.Millisecond)
	samples := []faultSample{
		{start: epoch.Add(time.Millisecond), end: epoch.Add(3 * time.Millisecond), measuredEnd: epoch.Add(3 * time.Millisecond)},
		{start: epoch.Add(9 * time.Millisecond), sent: epoch.Add(9 * time.Millisecond), end: epoch.Add(20 * time.Millisecond), measuredEnd: epoch.Add(21 * time.Millisecond), retries: 2, slot: 42},
		{start: epoch.Add(30 * time.Millisecond), end: epoch.Add(31 * time.Millisecond), measuredEnd: epoch.Add(31 * time.Millisecond)},
	}
	w := faultWindow{}
	w.summarizeSlowest(epoch, before, after, true, samples)
	if w.EpochUnixNS != epoch.UnixNano() || w.SlowestStartNS != int64(9*time.Millisecond) || w.SlowestSentNS == nil || *w.SlowestSentNS != int64(9*time.Millisecond) || w.SlowestEndNS != int64(20*time.Millisecond) || w.SlowestMeasuredEndNS != int64(21*time.Millisecond) || !w.SlowestOverlapsSignal || w.SlowestRetries != 2 || w.SlowestSlot != 42 || w.PostSignalMaxMS != 1 || w.PostSignalRequests != 1 {
		t.Fatalf("unexpected slowest request: %+v", w)
	}
	samples = append(samples, faultSample{start: epoch.Add(30 * time.Millisecond), end: epoch.Add(50 * time.Millisecond), measuredEnd: epoch.Add(50 * time.Millisecond)})
	w = faultWindow{}
	w.summarizeSlowest(epoch, before, after, true, samples)
	if w.SlowestOverlapsSignal || w.SlowestSentNS != nil || w.PostSignalMaxMS != 20 || w.PostSignalRequests != 2 {
		t.Fatalf("unexpected post-fault request: %+v", w)
	}
	w = faultWindow{}
	w.summarizeSlowest(epoch, before, after, true, []faultSample{{start: epoch.Add(time.Millisecond), sent: epoch.Add(2 * time.Millisecond), end: epoch.Add(9 * time.Millisecond), measuredEnd: epoch.Add(20 * time.Millisecond)}})
	if w.SlowestOverlapsSignal || w.SlowestEndNS != int64(9*time.Millisecond) || w.SlowestMeasuredEndNS != int64(20*time.Millisecond) {
		t.Fatalf("observer lock delay falsely counted as network overlap: %+v", w)
	}
}

func TestDoRequestRetriesCommitUnknownWithSamePayload(t *testing.T) {
	const payload = `{"request_id":"bench-7","sql":"INSERT INTO benchmark_writes(id) VALUES (7)"}`
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != payload {
			t.Errorf("attempt %d body = %q, err = %v", attempts, body, err)
		}
		if attempts <= 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"code":"commit_unknown"}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`)
	}))
	defer server.Close()

	status, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, payload, 3, true)
	if err != nil || status != http.StatusOK || retries != 3 || attempts != 4 {
		t.Fatalf("status=%d retries=%d attempts=%d err=%v", status, retries, attempts, err)
	}
}

func TestDoRequestStopsWhenRetriesAreExhausted(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"code":"commit_unknown"}`)
	}))
	defer server.Close()

	status, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, `{}`, 2, true)
	if err != nil || status != http.StatusServiceUnavailable || retries != 2 || attempts != 3 {
		t.Fatalf("status=%d retries=%d attempts=%d err=%v", status, retries, attempts, err)
	}
}

func TestDoRequestChecksSQLInsertReceipt(t *testing.T) {
	const payload = `{"request_id":"bench-7","sql":"INSERT INTO benchmark_writes(id) VALUES (7)"}`
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid-sql-no-applied-field", `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`, true},
		{"malformed", `{`, false},
		{"missing", `{}`, false},
		{"noncommitted", `{"slot":4,"status":"rejected","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`, false},
		{"wrong-id", `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":8,"retry_through_slot":65539}`, false},
		{"wrong-count", `{"slot":4,"status":"committed","rows_affected":2,"last_insert_id":7,"retry_through_slot":65539}`, false},
		{"error-code", `{"slot":4,"status":"committed","error_code":"execution_failed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`, false},
		{"bad-retention", `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":3}`, false},
		{"trailing", `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; _, _ = io.WriteString(w, tc.body) }))
			defer server.Close()
			status, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, payload, 3, true)
			if status != 200 || calls != 1 || retries != 0 || (err == nil) != tc.valid || (!tc.valid && !errors.Is(err, errInvalidInsertReceipt)) {
				t.Fatalf("status=%d calls=%d retries=%d err=%v", status, calls, retries, err)
			}
		})
	}
	// The public SQL encoder omits zero last_insert_id; id 0 is still valid.
	if err := validateInsertReceipt(`{"request_id":"bench-0","sql":"INSERT INTO benchmark_writes(id) VALUES (0)"}`, []byte(`{"slot":1,"status":"committed","rows_affected":1,"retry_through_slot":65536}`)); err != nil {
		t.Fatal(err)
	}
}

func TestFaultWindowIntervals(t *testing.T) {
	epoch := time.Now()
	for _, tc := range []struct {
		name                        string
		sent, finish, before, after time.Duration
		want                        bool
	}{
		{"sent-signal-finish-delayed-validation", 0, 4, 1, 2, true},
		{"completed-before-signal", 0, 1, 2, 3, false},
		{"completed-during-syscall", 0, 2, 1, 3, false},
		{"sent-after-signal-start", 2, 4, 1, 3, false},
		{"exact-boundaries", 1, 3, 1, 3, true},
		{"reversed-signal", 0, 4, 3, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := coversFault(epoch.Add(tc.sent), epoch.Add(tc.finish), epoch.Add(tc.before), epoch.Add(tc.after)); got != tc.want {
				t.Fatalf("overlap=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestFaultCoordinatorSingleAttemptAndCompletedSnapshot(t *testing.T) {
	epoch := time.Now()
	interval := &requestInterval{}
	sent := epoch
	interval.sent.Store(&sent)
	calls := 0
	p := &faultProgress{epoch: epoch, active: map[int]*requestInterval{7: interval}, token: "owned"}
	p.signal = func() (time.Time, time.Time, error) {
		calls++
		finished := epoch.Add(3 * time.Second)
		interval.finished.Store(&finished)
		return epoch.Add(time.Second), epoch.Add(2 * time.Second), nil
	}
	probe := func(path string, body io.Reader) int {
		w := httptest.NewRecorder()
		p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, body))
		return w.Code
	}
	if probe("/wrong/fault", nil) != 404 || probe("/owned/fault", strings.NewReader(`{"pid":1}`)) != 400 {
		t.Fatal("unbound control accepted")
	}
	if probe("/owned/fault", nil) != 200 {
		t.Fatal("fault refused")
	}
	if !p.window().Overlap || p.window().Witnesses != 1 {
		t.Fatal("completion before endpoint response lost historical overlap", p.window())
	}
	if probe("/owned/fault", nil) != 409 || calls != 1 {
		t.Fatal("duplicate signalled twice", calls)
	}
	t.Run("absent-witness-no-signal", func(t *testing.T) {
		calls := 0
		p := &faultProgress{active: map[int]*requestInterval{}, signal: func() (time.Time, time.Time, error) { calls++; return epoch, epoch, nil }}
		if p.inject() == nil || p.inject() == nil || calls != 0 || p.window().Overlap {
			t.Fatal("empty/repeated attempt qualified")
		}
	})
	t.Run("signal-failure", func(t *testing.T) {
		sentinel := errors.New("signal refused")
		v := &requestInterval{}
		v.sent.Store(&sent)
		p := &faultProgress{active: map[int]*requestInterval{1: v}, signal: func() (time.Time, time.Time, error) { return epoch, epoch, sentinel }}
		if !errors.Is(p.inject(), sentinel) || p.window().SignalSucceeded || p.window().Overlap {
			t.Fatal("signal failure lost")
		}
	})
}

func TestFaultCompletionTimestampPrecedesContendedLock(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval)}
	ctx, finish := p.traceRequest(deadline, 1)
	httptrace.ContextClientTrace(ctx).WroteRequest(httptrace.WroteRequestInfo{})
	interval := p.active[1]
	done := make(chan struct{})
	started := false
	t.Cleanup(func() {
		if started {
			<-done
		}
	})
	p.signal = func() (time.Time, time.Time, error) {
		p.mu.Lock()
		// Only the test delays removal. The runtime syscall never holds this lock.
		started = true
		go func() { finish(p.epoch.Add(time.Second)); close(done) }()
		for interval.finished.Load() == nil {
			select {
			case <-deadline.Done():
				p.mu.Unlock()
				return time.Time{}, time.Time{}, deadline.Err()
			default:
				runtime.Gosched()
			}
		}
		p.mu.Unlock()
		return p.epoch.Add(2 * time.Second), p.epoch.Add(3 * time.Second), nil
	}
	if err := p.inject(); err != nil {
		t.Fatal(err)
	}
	<-done
	if p.window().Overlap {
		t.Fatal("delayed tracking lock manufactured overlap", p.window())
	}
}

func TestFaultProgressActualHTTPTraceLifecycle(t *testing.T) {
	const payload = `{"request_id":"bench-7","sql":"INSERT INTO benchmark_writes(id) VALUES (7)"}`
	const receipt = `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`
	deadline, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	first, second := make(chan struct{}), make(chan struct{})
	var onceFirst, onceSecond sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != payload {
			t.Errorf("changed retry payload: %q %v", body, err)
		}
		if calls.Add(1) == 1 {
			<-first
			w.WriteHeader(503)
			_, _ = io.WriteString(w, `{"code":"commit_unknown"}`)
		} else {
			<-second
			_, _ = io.WriteString(w, receipt)
		}
	}))
	t.Cleanup(server.Close)
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval), signal: func() (time.Time, time.Time, error) { before := time.Now(); return before, time.Now(), nil }}
	wrote := make(chan struct{}, 2)
	base := httptrace.WithClientTrace(deadline, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			select {
			case wrote <- struct{}{}:
			case <-deadline.Done():
			}
		}
	}})
	ctx, finish := p.traceRequest(base, 7)
	interval := p.active[7]
	if interval.sent.Load() != nil {
		t.Fatal("unsent request marked sent")
	}
	done := make(chan struct{})
	var status, retries int
	var requestErr error
	go func() {
		defer close(done)
		status, _, retries, requestErr = doRequest(ctx, server.Client(), http.MethodPost, server.URL, payload, 1, true)
		finish(time.Now())
	}()
	t.Cleanup(func() {
		cancel()
		onceFirst.Do(func() { close(first) })
		onceSecond.Do(func() { close(second) })
		<-done
	})
	waitWrite := func() {
		t.Helper()
		select {
		case <-wrote:
		case <-deadline.Done():
			t.Fatal("HTTP write not observed", deadline.Err())
		}
	}
	waitWrite()
	originalSent := interval.sent.Load()
	if err := p.inject(); err != nil {
		t.Fatal(err)
	}
	onceFirst.Do(func() { close(first) })
	waitWrite()
	if interval.sent.Load() != originalSent {
		t.Fatal("same-ID retry replaced first sent time")
	}
	onceSecond.Do(func() { close(second) })
	<-done
	if status != 200 || retries != 1 || requestErr != nil || calls.Load() != 2 || !p.window().Overlap {
		t.Fatalf("status=%d retries=%d calls=%d err=%v window=%+v", status, retries, calls.Load(), requestErr, p.window())
	}
	if len(p.active) != 0 || len(p.snapshot) != 1 {
		t.Fatal("retention exceeds active/single snapshot")
	}
	httptrace.ContextClientTrace(ctx).WroteRequest(httptrace.WroteRequestInfo{})
	if len(p.active) != 0 || p.active[7] != nil {
		t.Fatal("late callback resurrected completed request")
	}
}

func TestFaultFastHTTPCompletionCannotQualify(t *testing.T) {
	deadline, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"slot":1,"status":"committed","rows_affected":1,"retry_through_slot":65536}`)
	}))
	defer server.Close()
	calls := 0
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval), signal: func() (time.Time, time.Time, error) { calls++; return time.Now(), time.Now(), nil }}
	ctx, finish := p.traceRequest(deadline, 0)
	status, _, _, err := doRequest(ctx, server.Client(), http.MethodPost, server.URL, `{"request_id":"bench-0","sql":"INSERT INTO benchmark_writes(id) VALUES (0)"}`, 0, true)
	finish(time.Now())
	if status != 200 || err != nil {
		t.Fatal(status, err)
	}
	httptrace.ContextClientTrace(ctx).WroteRequest(httptrace.WroteRequestInfo{})
	if p.inject() == nil || calls != 0 || len(p.active) != 0 {
		t.Fatal("completed request qualified")
	}
}
