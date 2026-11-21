package main

import (
	"context"
	"encoding/json"
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

	status, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, payload, 3, true, nil, 0)
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

	status, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, `{}`, 2, true, nil, 0)
	if err != nil || status != http.StatusServiceUnavailable || retries != 2 || attempts != 3 {
		t.Fatalf("status=%d retries=%d attempts=%d err=%v", status, retries, attempts, err)
	}
}

func TestFaultErrorResponseDoesNotWaitPast4096Bytes(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer server.Close()
	defer close(release)
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval)}
	done := make(chan error, 1)
	go func() {
		status, body, _, err := doRequest(t.Context(), server.Client(), http.MethodGet, server.URL, "", 0, false, p, 0)
		if err == nil && (status != 500 || len(body) != 4096) {
			err = errors.New("unexpected response")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fault mode waited for byte 4097")
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
			status, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, payload, 3, true, nil, 0)
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

func TestProfileCompletionClockFailure(t *testing.T) {
	p := &faultProgress{completionClock: func() (int64, error) { return 0, errors.New("clock failed") }}
	p.recordCompletion()
	if !p.completionClockFailed.Load() || p.lastCompletion.Load() != 0 {
		t.Fatal("clock failure accepted")
	}
}

func TestProfileCountsOnlySuccessfulRequestsStartedAndCompletedInside(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/pending" {
			close(entered)
			<-release
		}
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval), token: "owned", profile: true, completionClock: func() (int64, error) { return 123, nil }}
	request := func(path string) {
		eligible := p.profileStart()
		status, _, _, err := doRequest(t.Context(), server.Client(), http.MethodGet, server.URL+path, "", 0, false, p, 0)
		if eligible && err == nil && status >= 200 && status < 300 {
			p.profileSuccess()
		}
	}
	post := func(path string) (int, uint64) {
		w := httptest.NewRecorder()
		p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, nil))
		var body struct {
			Successes uint64 `json:"successes"`
		}
		if w.Code == http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
		}
		return w.Code, body.Successes
	}
	done := make(chan struct{})
	go func() { defer close(done); request("/pending") }()
	<-entered
	if code, _ := post("/wrong/profile/begin"); code != 404 {
		t.Fatal(code)
	}
	if code, n := post("/owned/profile/begin"); code != 200 || n != 0 {
		t.Fatal(code, n)
	}
	close(release)
	<-done
	request("/fail")
	if code, n := post("/owned/profile/status"); code != 200 || n != 0 {
		t.Fatal(code, n)
	}
	request("/ok")
	if code, n := post("/owned/profile/status"); code != 200 || n != 1 {
		t.Fatal(code, n)
	}
	if code, n := post("/owned/profile/end"); code != 200 || n != 1 {
		t.Fatal(code, n)
	}
	request("/ok")
	if got := p.profileResult(); !got.Overlap || got.Incomplete || got.Successes != 1 || got.Reason != "qualified" {
		t.Fatal(got)
	}
	if code, _ := post("/owned/profile/status"); code != 409 {
		t.Fatal(code)
	}
}

func TestProfileCompletionAfterEndCannotQualify(t *testing.T) {
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval), token: "owned", profile: true}
	for _, path := range []string{"begin", "end"} {
		w := httptest.NewRecorder()
		p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/owned/profile/"+path, nil))
		if w.Code != http.StatusOK {
			t.Fatal(w.Code)
		}
		if path == "begin" && !p.profileStart() {
			t.Fatal("request not marked eligible")
		}
	}
	p.profileSuccess()
	if got := p.profileResult(); got.Overlap || got.Successes != 0 {
		t.Fatal(got)
	}
}

func TestProfileSuccessfulRetryInsideBracket(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"code":"commit_unknown"}`)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	p := &faultProgress{epoch: time.Now(), active: make(map[int]*requestInterval), token: "owned", profile: true, completionClock: func() (int64, error) { return 123, nil }}
	post := func(path string) {
		w := httptest.NewRecorder()
		p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/owned/profile/"+path, nil))
		if w.Code != http.StatusOK {
			t.Fatal(path, w.Code)
		}
	}
	post("begin")
	eligible := p.profileStart()
	status, _, retries, err := doRequest(t.Context(), server.Client(), http.MethodPost, server.URL, `{}`, 1, false, p, 0)
	if eligible && err == nil && status >= 200 && status < 300 {
		p.profileSuccess()
	}
	post("end")
	if got := p.profileResult(); err != nil || status != 200 || retries != 1 || attempts.Load() != 2 || !got.Overlap || got.Successes != 1 {
		t.Fatal(status, retries, attempts.Load(), err, got)
	}
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
	done := make(chan struct{})
	var status, retries int
	var requestErr error
	go func() {
		defer close(done)
		status, _, retries, requestErr = doRequest(base, server.Client(), http.MethodPost, server.URL, payload, 1, true, p, 7)
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
	p.mu.Lock()
	firstInterval := p.active[7]
	p.mu.Unlock()
	if err := p.inject(); err != nil {
		t.Fatal(err)
	}
	onceFirst.Do(func() { close(first) })
	waitWrite()
	p.mu.Lock()
	sameInterval := p.active[7] == firstInterval
	p.mu.Unlock()
	if !sameInterval {
		t.Fatal("fault retry changed request tracking")
	}
	onceSecond.Do(func() { close(second) })
	<-done
	if status != 200 || retries != 1 || requestErr != nil || calls.Load() != 2 || !p.window().Overlap {
		t.Fatalf("status=%d retries=%d calls=%d err=%v window=%+v", status, retries, calls.Load(), requestErr, p.window())
	}
	if len(p.active) != 0 || len(p.snapshot) != 1 {
		t.Fatal("retention exceeds active/single snapshot")
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
	status, _, _, err := doRequest(deadline, server.Client(), http.MethodPost, server.URL, `{"request_id":"bench-0","sql":"INSERT INTO benchmark_writes(id) VALUES (0)"}`, 0, true, p, 0)
	if status != 200 || err != nil {
		t.Fatal(status, err)
	}
	if p.inject() == nil || calls != 0 || len(p.active) != 0 {
		t.Fatal("completed request qualified")
	}
}
