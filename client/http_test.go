package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type observedBody struct {
	io.Reader
	closed *atomic.Int32
}

func (b observedBody) Close() error { b.closed.Add(1); return nil }

func TestReadRetriesSlowAndUnavailableEndpoint(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
	}))
	defer slow.Close()
	defer close(release)
	var count atomic.Int32
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer healthy.Close()
	c, err := New([]string{slow.URL, healthy.URL}, nil, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Read(context.Background(), "/sql/query", []byte(`{"sql":"SELECT 1"}`))
	if err != nil || resp.StatusCode != 200 || count.Load() != 1 {
		t.Fatalf("response=%+v error=%v count=%d", resp, err, count.Load())
	}
	select {
	case <-entered:
	default:
		t.Fatal("slow endpoint was not exercised")
	}
}

func TestMutationNeverRetriesUnknownOutcome(t *testing.T) {
	var alternate atomic.Int32
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer broken.Close()
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { alternate.Add(1) }))
	defer other.Close()
	c, err := New([]string{broken.URL, other.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Mutate(context.Background(), "/kv/put", []byte(`{"request_id":"x"}`))
	if !errors.Is(err, ErrOutcomeUnknown) || alternate.Load() != 0 {
		t.Fatalf("error=%v alternate=%d", err, alternate.Load())
	}
	if _, err := c.Read(context.Background(), "/kv/put", nil); err == nil {
		t.Fatal("mutation accepted as read")
	}
}

func TestReadDoesNotRetryCallerCancellation(t *testing.T) {
	var called atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Add(1) }))
	defer s.Close()
	c, err := New([]string{s.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Read(ctx, "/kv/get", nil)
	if !errors.Is(err, context.Canceled) || called.Load() != 0 {
		t.Fatalf("error=%v calls=%d", err, called.Load())
	}
}

func TestNoRedirectForMutation(t *testing.T) {
	var target atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { target.Add(1) }))
	defer other.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	c, err := New([]string{redirect.URL, other.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Mutate(context.Background(), "/kv/put", []byte(`{}`))
	if !errors.Is(err, ErrOutcomeUnknown) || resp.StatusCode != http.StatusTemporaryRedirect || target.Load() != 0 {
		t.Fatalf("response=%+v error=%v target=%d", resp, err, target.Load())
	}
}

func TestLastGoodEndpointServesFutureOperations(t *testing.T) {
	var badCalls, goodCalls atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badCalls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodCalls.Add(1)
		w.Write([]byte(`{"slot":1,"status":"committed","retry_through_slot":65537}`))
	}))
	defer good.Close()
	c, err := New([]string{bad.URL, good.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Read(context.Background(), "/kv/get", []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Mutate(context.Background(), "/kv/put", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if badCalls.Load() != 1 || goodCalls.Load() != 3 {
		t.Fatalf("bad=%d good=%d", badCalls.Load(), goodCalls.Load())
	}
}

func TestUnknownMutationResponseIsNotResubmitted(t *testing.T) {
	var alternate atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"code":"commit_unknown","request_id":"x"}`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alternate.Add(1)
		w.Write([]byte(`{"slot":1,"status":"committed","retry_through_slot":65537}`))
	}))
	defer second.Close()
	c, err := New([]string{first.URL, second.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Mutate(context.Background(), "/kv/put", []byte(`{"request_id":"x"}`))
	if !errors.Is(err, ErrOutcomeUnknown) || resp.StatusCode != http.StatusServiceUnavailable || alternate.Load() != 0 {
		t.Fatalf("response=%+v error=%v alternate=%d", resp, err, alternate.Load())
	}
}

func TestFailedWriteSelectsNextHostOnlyForNewRequest(t *testing.T) {
	var alternate atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alternate.Add(1)
		w.Write([]byte(`{"slot":1,"status":"committed","retry_through_slot":65537}`))
	}))
	defer second.Close()
	c, err := New([]string{first.URL, second.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutate(context.Background(), "/kv/put", []byte(`{"request_id":"one"}`)); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	if alternate.Load() != 0 {
		t.Fatal("uncertain request retried")
	}
	if _, err := c.Mutate(context.Background(), "/kv/put", []byte(`{"request_id":"two"}`)); err != nil {
		t.Fatal(err)
	}
	if alternate.Load() != 1 {
		t.Fatalf("alternate calls=%d", alternate.Load())
	}
}

func TestConcurrentReadAndPreferredSelection(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
	defer good.Close()
	c, err := New([]string{bad.URL, good.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Read(context.Background(), "/kv/get", []byte(`{}`)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestStaleSuccessDoesNotOverrideNewerPreferredHost(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			w.Write([]byte(`{}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"slot":1,"status":"committed","retry_through_slot":65537}`))
	}))
	defer second.Close()
	c, err := New([]string{first.URL, second.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.Read(context.Background(), "/kv/get", nil); done <- err }()
	<-entered
	if _, err := c.Read(context.Background(), "/kv/get", nil); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutate(context.Background(), "/kv/put", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("stale response changed preferred endpoint: calls=%d", calls.Load())
	}
}

func TestMutationAmbiguousHTTPFailuresAreUnknown(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status); w.Write([]byte(`{}`)) }))
			defer s.Close()
			c, err := New([]string{s.URL}, nil, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Mutate(context.Background(), "/kv/put", []byte(`{}`)); !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("status=%d err=%v", status, err)
			}
		})
	}
}

func TestMutationTimeoutIsBounded(t *testing.T) {
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer s.Close()
	defer close(release)
	c, err := New([]string{s.URL}, nil, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutate(context.Background(), "/kv/put", []byte(`{}`)); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatal(err)
	}
}

func TestMutationHonorsEarlierCallerDeadline(t *testing.T) {
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer s.Close()
	defer close(release)
	c, err := New([]string{s.URL}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Mutate(ctx, "/kv/put", []byte(`{}`)); !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestReadAllUnavailableReturnsErrorAndClosesEachBody(t *testing.T) {
	for _, status := range []int{502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls, closed atomic.Int32
			transport := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: observedBody{Reader: bytes.NewReader([]byte(`{"code":"not_ready"}`)), closed: &closed}}, nil
			})}
			c, err := New([]string{"http://first.invalid", "http://second.invalid"}, transport, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.Read(context.Background(), "/kv/get", []byte(`{}`))
			if !errors.Is(err, ErrReadUnavailable) || resp.StatusCode != status || calls.Load() != 2 || closed.Load() != 2 {
				t.Fatalf("response=%+v err=%v calls=%d closed=%d", resp, err, calls.Load(), closed.Load())
			}
		})
	}
}

func TestReadAllTransportFailuresPreserveCause(t *testing.T) {
	transportErr := errors.New("transport failed")
	transport := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, transportErr })}
	c, err := New([]string{"http://first.invalid", "http://second.invalid"}, transport, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Read(context.Background(), "/kv/get", nil)
	if !errors.Is(err, ErrReadUnavailable) || !errors.Is(err, transportErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestReadCancellationDuringAttemptStopsFailover(t *testing.T) {
	var calls atomic.Int32
	transport := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	c, err := New([]string{"http://first.invalid", "http://second.invalid"}, transport, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = c.Read(ctx, "/kv/get", nil)
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}
