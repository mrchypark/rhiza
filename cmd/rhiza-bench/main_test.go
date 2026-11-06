package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
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

func TestFaultProgressActualHTTPTraceLifecycle(t *testing.T) {
	const payload = `{"request_id":"bench-7","sql":"INSERT INTO benchmark_writes(id) VALUES (7)"}`
	const receipt = `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`
	probe := func(p *faultProgress, route string) int {
		w := httptest.NewRecorder()
		p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, route, nil))
		return w.Code
	}
	t.Run("same-logical-request-across-commit-unknown-retry", func(t *testing.T) {
		deadline, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var calls atomic.Int32
		first, second := make(chan struct{}), make(chan struct{})
		var releaseFirst, releaseSecond sync.Once
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != payload {
				t.Errorf("changed retry payload: %q %v", body, err)
			}
			if calls.Add(1) == 1 {
				<-first
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"code":"commit_unknown"}`)
			} else {
				<-second
				_, _ = io.WriteString(w, receipt)
			}
		}))
		t.Cleanup(server.Close)
		p := &faultProgress{active: make(map[int]bool)}
		wrote := make(chan struct{}, 2)
		// The runtime trace wraps this observer: its active update precedes each
		// notification. Only test handlers hold responses; runtime never does.
		base := httptrace.WithClientTrace(deadline, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				select {
				case wrote <- struct{}{}:
				case <-deadline.Done():
				}
			}
		}})
		ctx, finish := p.traceRequest(base, 7)
		if probe(p, "/begin") != 409 {
			t.Fatal("before first write qualified")
		}
		done := make(chan struct{})
		var status, retries int
		var requestErr error
		go func() {
			defer close(done)
			status, _, retries, requestErr = doRequest(ctx, server.Client(), http.MethodPost, server.URL, payload, 1, true)
			finish()
		}()
		t.Cleanup(func() {
			cancel()
			releaseFirst.Do(func() { close(first) })
			releaseSecond.Do(func() { close(second) })
			<-done
		})
		waitWrite := func() {
			t.Helper()
			select {
			case <-wrote:
			case <-deadline.Done():
				t.Fatal("actual HTTP write not observed", deadline.Err())
			}
		}
		waitWrite()
		if probe(p, "/begin") != 200 {
			t.Fatal("sent request not active")
		}
		releaseFirst.Do(func() { close(first) })
		waitWrite()
		if probe(p, "/end") != 200 {
			t.Fatal("same logical retry lost overlap")
		}
		releaseSecond.Do(func() { close(second) })
		<-done
		if status != 200 || retries != 1 || requestErr != nil || calls.Load() != 2 {
			t.Fatalf("status=%d retries=%d calls=%d err=%v", status, retries, calls.Load(), requestErr)
		}
		if probe(p, "/end") != 409 {
			t.Fatal("completed logical request remained active")
		}
	})
	t.Run("fast-response-and-late-callback", func(t *testing.T) {
		deadline, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, receipt) }))
		defer server.Close()
		p := &faultProgress{active: make(map[int]bool)}
		ctx, finish := p.traceRequest(deadline, 7)
		status, _, retries, err := doRequest(ctx, server.Client(), http.MethodPost, server.URL, payload, 0, true)
		finish()
		if status != 200 || retries != 0 || err != nil {
			t.Fatalf("status=%d retries=%d err=%v", status, retries, err)
		}
		if probe(p, "/begin") != 409 {
			t.Fatal("fast completed request qualified")
		}
		// Exercise the exact runtime callback after completion: it cannot
		// resurrect an active request even if a transport invokes it late.
		trace := httptrace.ContextClientTrace(ctx)
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		if probe(p, "/begin") != 409 {
			t.Fatal("late callback resurrected completed request")
		}
	})
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

func TestFaultProgressRequiresSameSentUnfinishedRequest(t *testing.T) {
	probe := func(p *faultProgress, route string) int {
		w := httptest.NewRecorder()
		p.ServeHTTP(w, httptest.NewRequest(http.MethodPost, route, nil))
		return w.Code
	}
	for _, name := range []string{"before-first-request", "after-last-response"} {
		t.Run(name, func(t *testing.T) {
			p := &faultProgress{active: map[int]bool{}}
			if name == "after-last-response" {
				p.active[1] = true
				delete(p.active, 1)
			}
			if got := probe(p, "/begin"); got != http.StatusConflict {
				t.Fatalf("alive client qualified: %d", got)
			}
		})
	}
	t.Run("end-no-intersection", func(t *testing.T) {
		p := &faultProgress{active: map[int]bool{1: true}}
		if probe(p, "/begin") != 200 {
			t.Fatal("begin")
		}
		delete(p.active, 1)
		p.active[2] = true
		if probe(p, "/end") != http.StatusConflict {
			t.Fatal("different request qualified")
		}
	})
	t.Run("sent-request-spans-both", func(t *testing.T) {
		p := &faultProgress{active: map[int]bool{1: true}, token: "owned"}
		if probe(p, "/wrong/begin") != 404 || probe(p, "/owned/end") != 409 {
			t.Fatal("unowned/unarmed control accepted")
		}
		if probe(p, "/owned/begin") != 200 || probe(p, "/owned/end") != 200 {
			t.Fatal("valid overlap refused")
		}
		if probe(p, "/owned/begin") != 409 {
			t.Fatal("rearm allowed")
		}
	})
}
