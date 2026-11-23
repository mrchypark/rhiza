package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommittedSlowIngressRetryUsesSameRequest(t *testing.T) {
	const payload = `{"request_id":"bench-7","sql":"INSERT INTO benchmark_writes(id) VALUES (7)"}`
	const receipt = `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if string(b) != payload {
			t.Errorf("request changed: %s", b)
		}
		calls.Add(1)
		_, _ = io.WriteString(w, receipt)
	}))
	defer server.Close()
	slow, err := startSlowIngress(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	var fallback atomic.Bool
	var reroutes atomic.Uint64
	status, _, _, err, when := doRequestWithFallback(context.Background(), client, http.MethodPost, slow.url+"/sql/execute", server.URL, "/sql/execute", payload, 0, true, &fallback, &reroutes, nil)
	if err != nil || status != 200 || when.IsZero() || calls.Load() != 2 || slow.committed.Load() != 1 || reroutes.Load() != 1 {
		t.Fatalf("status %d err %v rerouted %v calls %d committed %d", status, err, !when.IsZero(), calls.Load(), slow.committed.Load())
	}
}

func TestCommittedSlowIngressWithoutFallbackFailsClosed(t *testing.T) {
	const payload = `{"request_id":"bench-7","sql":"INSERT INTO benchmark_writes(id) VALUES (7)"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"slot":4,"status":"committed","rows_affected":1,"last_insert_id":7,"retry_through_slot":65539}`)
	}))
	defer server.Close()
	slow, err := startSlowIngress(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	var fallback atomic.Bool
	var reroutes atomic.Uint64
	_, _, _, err, _ = doRequestWithFallback(context.Background(), &http.Client{Timeout: 100 * time.Millisecond}, http.MethodPost, slow.url+"/sql/execute", "", "/sql/execute", payload, 0, true, &fallback, &reroutes, nil)
	if err == nil || slow.committed.Load() != 1 || reroutes.Load() != 0 {
		t.Fatalf("err=%v committed=%d reroutes=%d", err, slow.committed.Load(), reroutes.Load())
	}
	if !slow.isDelayedTimeout(7, time.Time{}, err) {
		t.Fatal("missing timeout witness for delayed request")
	}
	if slow.isDelayedTimeout(8, time.Time{}, err) {
		t.Fatal("another request counted as witness")
	}
	if slow.isDelayedTimeout(7, time.Now(), err) {
		t.Fatal("rerouted request counted as no-bypass witness")
	}
	if slow.isDelayedTimeout(7, time.Time{}, io.ErrUnexpectedEOF) {
		t.Fatal("non-timeout error counted as witness")
	}
}
