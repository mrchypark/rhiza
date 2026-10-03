//go:build rhiza_local_testhooks

package objstore

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// S3TransportEvent contains only bounded, non-secret transport metadata. It
// deliberately excludes URLs, headers, object names, bodies, and error text.
type S3TransportEvent struct {
	Sequence   uint64        `json:"sequence"`
	Owner      string        `json:"owner"`
	Phase      string        `json:"phase"`
	Method     string        `json:"method"`
	StatusCode int           `json:"status_code,omitempty"`
	Outcome    string        `json:"outcome"`
	ErrorClass string        `json:"error_class,omitempty"`
	Elapsed    time.Duration `json:"elapsed_ns"`
}

// S3TransportAggregate summarizes error observations by phase and request
// class; it retains no per-success history.
type S3TransportAggregate struct {
	Owner        string `json:"owner"`
	Phase        string `json:"phase"`
	Method       string `json:"method"`
	StatusFamily string `json:"status_family,omitempty"`
	Outcome      string `json:"outcome"`
	ErrorClass   string `json:"error_class,omitempty"`
	Count        uint64 `json:"count"`
}

// S3TransportRequestCount aggregates all actual RoundTrip calls without
// retaining per-request success history.
type S3TransportRequestCount struct {
	Owner  string `json:"owner"`
	Phase  string `json:"phase"`
	Method string `json:"method"`
	Count  uint64 `json:"count"`
}

type transportRequestKey struct{ owner, phase, method string }

// S3TransportObserver is available only in rhiza_local_testhooks builds.
// Register one before sequentially opening the Nodes whose S3 transports are
// to be observed, then unregister it after all of them have shut down.
type S3TransportObserver struct {
	mu       sync.Mutex
	owners   []string
	phase    string
	next     int
	total    atomic.Uint64
	requests map[transportRequestKey]uint64
	groups   map[S3TransportAggregate]uint64
	first    *S3TransportEvent
}

var s3TransportObservers = struct {
	sync.Mutex
	byEndpoint map[string]*S3TransportObserver
}{byEndpoint: make(map[string]*S3TransportObserver)}

// RegisterS3TransportObserver installs an endpoint-scoped observer for tagged
// tests. Owners are assigned in transport-construction order; callers should
// open Nodes sequentially. The callback-free collector is safe for concurrent
// RoundTrip calls and retains aggregate error counts plus the first detail.
func RegisterS3TransportObserver(endpoint string, owners []string) (*S3TransportObserver, func(), error) {
	if endpoint == "" || len(owners) == 0 {
		return nil, nil, errors.New("S3 transport observer requires endpoint and owners")
	}
	observer := &S3TransportObserver{owners: append([]string(nil), owners...), phase: "setup", requests: make(map[transportRequestKey]uint64), groups: make(map[S3TransportAggregate]uint64)}
	s3TransportObservers.Lock()
	if _, exists := s3TransportObservers.byEndpoint[endpoint]; exists {
		s3TransportObservers.Unlock()
		return nil, nil, errors.New("S3 transport observer already registered for endpoint")
	}
	s3TransportObservers.byEndpoint[endpoint] = observer
	s3TransportObservers.Unlock()
	return observer, func() {
		s3TransportObservers.Lock()
		if s3TransportObservers.byEndpoint[endpoint] == observer {
			delete(s3TransportObservers.byEndpoint, endpoint)
		}
		s3TransportObservers.Unlock()
	}, nil
}

// SetPhase changes the phase label captured when each request starts.
func (o *S3TransportObserver) SetPhase(phase string) {
	o.mu.Lock()
	o.phase = phase
	o.mu.Unlock()
}

// Snapshot returns all-attempt count, request and error aggregates, and the
// first failure detail. It never stores per-success request history.
func (o *S3TransportObserver) Snapshot() (total uint64, requests []S3TransportRequestCount, aggregates []S3TransportAggregate, first *S3TransportEvent) {
	o.mu.Lock()
	for key, count := range o.requests {
		requests = append(requests, S3TransportRequestCount{Owner: key.owner, Phase: key.phase, Method: key.method, Count: count})
	}
	for key, count := range o.groups {
		key.Count = count
		aggregates = append(aggregates, key)
	}
	if o.first != nil {
		firstCopy := *o.first
		first = &firstCopy
	}
	o.mu.Unlock()
	sort.Slice(aggregates, func(i, j int) bool {
		a, b := aggregates[i], aggregates[j]
		if a.Phase != b.Phase {
			return a.Phase < b.Phase
		}
		if a.Owner != b.Owner {
			return a.Owner < b.Owner
		}
		if a.Method != b.Method {
			return a.Method < b.Method
		}
		if a.Outcome != b.Outcome {
			return a.Outcome < b.Outcome
		}
		if a.StatusFamily != b.StatusFamily {
			return a.StatusFamily < b.StatusFamily
		}
		return a.ErrorClass < b.ErrorClass
	})
	sort.Slice(requests, func(i, j int) bool {
		a, b := requests[i], requests[j]
		if a.Phase != b.Phase {
			return a.Phase < b.Phase
		}
		if a.Owner != b.Owner {
			return a.Owner < b.Owner
		}
		return a.Method < b.Method
	})
	return o.total.Load(), requests, aggregates, first
}

func wrapTestObserver(endpoint string, next http.RoundTripper) http.RoundTripper {
	s3TransportObservers.Lock()
	observer := s3TransportObservers.byEndpoint[endpoint]
	var owner string
	if observer != nil {
		observer.mu.Lock()
		if observer.next < len(observer.owners) {
			owner = observer.owners[observer.next]
			observer.next++
		}
		observer.mu.Unlock()
	}
	s3TransportObservers.Unlock()
	if observer == nil {
		return next
	}
	return &observedRoundTripper{next: next, observer: observer, owner: owner}
}

type observedRoundTripper struct {
	next     http.RoundTripper
	observer *S3TransportObserver
	owner    string
}

func (t *observedRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	started := time.Now()
	sequence := t.observer.total.Add(1)
	t.observer.mu.Lock()
	phase := t.observer.phase
	method := safeHTTPMethod(request.Method)
	t.observer.requests[transportRequestKey{owner: t.owner, phase: phase, method: method}]++
	t.observer.mu.Unlock()
	base := S3TransportEvent{Sequence: sequence, Owner: t.owner, Phase: phase, Method: method}
	response, err := t.next.RoundTrip(request)
	base.Elapsed = time.Since(started)
	if err != nil {
		if response != nil {
			base.StatusCode = response.StatusCode
		}
		base.Outcome = "round_trip_error"
		base.ErrorClass = classifyTransportError(err)
		t.observer.record(base)
		return response, err
	}
	if response == nil {
		base.Outcome = "nil_response"
		base.ErrorClass = "other"
		t.observer.record(base)
		return nil, nil
	}
	base.StatusCode = response.StatusCode
	if response.StatusCode >= 400 && !expectedHTTPControlOutcome(request, response.StatusCode) {
		base.Outcome = "http_status"
		base.ErrorClass = "http_status"
		t.observer.record(base)
	}
	if response.Body != nil {
		response.Body = &observedResponseBody{ReadCloser: response.Body, record: func(outcome string, err error) {
			event := base
			event.Elapsed = time.Since(started)
			event.Outcome = outcome
			event.ErrorClass = classifyTransportError(err)
			t.observer.record(event)
		}}
	}
	return response, nil
}

func expectedHTTPControlOutcome(request *http.Request, status int) bool {
	if status == http.StatusNotFound && expectsNotFound(request.Context()) {
		return true
	}
	return request.Method == http.MethodPut && expectsCondition(request.Context()) &&
		(request.Header.Get("If-Match") != "" || request.Header.Get("If-None-Match") != "") &&
		(status == http.StatusConflict || status == http.StatusPreconditionFailed)
}

type observedResponseBody struct {
	io.ReadCloser
	record func(string, error)
}

func (b *observedResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		b.record("response_body_read_error", err)
	}
	return n, err
}

func (b *observedResponseBody) Close() error {
	err := b.ReadCloser.Close()
	if err != nil {
		b.record("response_body_close_error", err)
	}
	return err
}

func (o *S3TransportObserver) record(event S3TransportEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := S3TransportAggregate{Owner: event.Owner, Phase: event.Phase, Method: event.Method, StatusFamily: statusFamily(event.StatusCode), Outcome: event.Outcome, ErrorClass: event.ErrorClass}
	o.groups[key]++
	if o.first == nil {
		first := event
		o.first = &first
	}
}

func statusFamily(code int) string {
	switch {
	case code >= 500 && code <= 599:
		return "5xx"
	case code >= 400 && code <= 499:
		return "4xx"
	case code >= 300 && code <= 399:
		return "3xx"
	case code >= 200 && code <= 299:
		return "2xx"
	case code >= 100 && code <= 199:
		return "1xx"
	default:
		return "none"
	}
}

func safeHTTPMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

func classifyTransportError(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context_deadline"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.ErrClosedPipe):
		return "closed_pipe"
	case errors.Is(err, net.ErrClosed):
		return "net_closed"
	case errors.Is(err, http.ErrBodyReadAfterClose):
		return "body_read_after_close"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "net_timeout"
	}
	return "other"
}
