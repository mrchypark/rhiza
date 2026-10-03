package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	kitlog "github.com/go-kit/log"
	thanosobjstore "github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/s3"
)

const testConditionalExpectContinueMinSize = 1 << 20
const maxConditionalPutTraceAttempts = 64
const maxConditionalPutTraceEvents = 64

type conditionalExpectContinueRoundTripper struct {
	next  http.RoundTripper
	trace *conditionalPutHTTPTrace
}

func (t conditionalExpectContinueRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if !isLargeConditionalPut(request) {
		return t.next.RoundTrip(request)
	}

	request = request.Clone(request.Context())
	request.Header = request.Header.Clone()
	if request.Header.Get("Expect") == "" {
		request.Header.Set("Expect", "100-continue")
	}
	var attempt *conditionalPutHTTPTraceAttempt
	if t.trace != nil {
		attempt = t.trace.beginAttempt(request.ContentLength, request.Header.Get("Expect") == "100-continue")
		if attempt != nil {
			trace := &httptrace.ClientTrace{
				WroteHeaders:         func() { attempt.record("wrote_headers", 0, "") },
				Got100Continue:       func() { attempt.record("got_100_continue", http.StatusContinue, "") },
				Got1xxResponse:       func(code int, _ textproto.MIMEHeader) error { attempt.record("got_1xx_response", code, ""); return nil },
				GotFirstResponseByte: func() { attempt.record("got_first_response_byte", 0, "") },
				WroteRequest: func(info httptrace.WroteRequestInfo) {
					attempt.record("wrote_request", 0, safeHTTPTraceErrorClass(info.Err))
				},
			}
			request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
		}
	}
	response, err := t.next.RoundTrip(request)
	if attempt != nil {
		attempt.recordRoundTrip(response, err)
	}
	return response, err
}

func isLargeConditionalPut(request *http.Request) bool {
	return request.Method == http.MethodPut && request.Body != nil && request.ContentLength >= testConditionalExpectContinueMinSize &&
		(request.Header.Get("If-Match") != "" || request.Header.Get("If-None-Match") != "")
}

type conditionalPutHTTPTraceEvent struct {
	Attempt       uint64 `json:"attempt"`
	Order         uint64 `json:"order"`
	ElapsedNS     int64  `json:"elapsed_ns"`
	Event         string `json:"event"`
	Status        int    `json:"status,omitempty"`
	ErrorClass    string `json:"error_class,omitempty"`
	ContentLength int64  `json:"content_length"`
	Expect        bool   `json:"expect_100_continue"`
	ProtoMajor    int    `json:"proto_major,omitempty"`
	ProtoMinor    int    `json:"proto_minor,omitempty"`
	ResponseClose bool   `json:"response_close,omitempty"`
}

type conditionalPutHTTPTraceSnapshot struct {
	Attempts        uint64                         `json:"attempts"`
	DroppedAttempts uint64                         `json:"dropped_attempts"`
	DroppedEvents   uint64                         `json:"dropped_events"`
	Events          []conditionalPutHTTPTraceEvent `json:"events"`
}

type conditionalPutHTTPTrace struct {
	mu              sync.Mutex
	attempts        uint64
	droppedAttempts uint64
	droppedEvents   uint64
	order           uint64
	closed          bool
	events          []conditionalPutHTTPTraceEvent
}

type conditionalPutHTTPTraceAttempt struct {
	trace         *conditionalPutHTTPTrace
	started       time.Time
	index         uint64
	contentLength int64
	expect        bool
}

func newConditionalPutHTTPTrace() *conditionalPutHTTPTrace {
	return &conditionalPutHTTPTrace{events: make([]conditionalPutHTTPTraceEvent, 0, maxConditionalPutTraceEvents)}
}

func (t *conditionalPutHTTPTrace) beginAttempt(contentLength int64, expect bool) *conditionalPutHTTPTraceAttempt {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.attempts++
	if t.attempts > maxConditionalPutTraceAttempts {
		t.droppedAttempts++
		return nil
	}
	attempt := &conditionalPutHTTPTraceAttempt{trace: t, started: time.Now(), index: t.attempts, contentLength: contentLength, expect: expect}
	t.recordLocked(attempt, "round_trip_started", 0, "", nil)
	return attempt
}

func (a *conditionalPutHTTPTraceAttempt) record(event string, status int, errorClass string) {
	t := a.trace
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordLocked(a, event, status, errorClass, nil)
}

func (a *conditionalPutHTTPTraceAttempt) recordRoundTrip(response *http.Response, err error) {
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	t := a.trace
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordLocked(a, "round_trip_result", status, safeHTTPTraceErrorClass(err), response)
}

func (t *conditionalPutHTTPTrace) recordLocked(attempt *conditionalPutHTTPTraceAttempt, event string, status int, errorClass string, response *http.Response) {
	if t.closed {
		return
	}
	if len(t.events) >= maxConditionalPutTraceEvents {
		t.droppedEvents++
		return
	}
	t.order++
	item := conditionalPutHTTPTraceEvent{
		Attempt: attempt.index, Order: t.order, ElapsedNS: time.Since(attempt.started).Nanoseconds(), Event: event,
		Status: status, ErrorClass: errorClass, ContentLength: attempt.contentLength, Expect: attempt.expect,
	}
	if response != nil {
		item.ProtoMajor = response.ProtoMajor
		item.ProtoMinor = response.ProtoMinor
		item.ResponseClose = response.Close
	}
	t.events = append(t.events, item)
}

// closeAndSnapshot bounds the trace lifetime to the completed logical SDK call.
// Any callback arriving later is safely ignored; this does not wait on transport
// goroutines or claim that all asynchronous callbacks have completed.
func (t *conditionalPutHTTPTrace) closeAndSnapshot() conditionalPutHTTPTraceSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return conditionalPutHTTPTraceSnapshot{
		Attempts: t.attempts, DroppedAttempts: t.droppedAttempts, DroppedEvents: t.droppedEvents,
		Events: append([]conditionalPutHTTPTraceEvent(nil), t.events...),
	}
}

func safeHTTPTraceErrorClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "context_deadline"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.ErrClosedPipe):
		return "closed_pipe"
	case errors.Is(err, net.ErrClosed):
		return "connection_closed"
	default:
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return "network_timeout"
		}
		return "other"
	}
}

func logConditionalPutHTTPTrace(t *testing.T, snapshot conditionalPutHTTPTraceSnapshot) {
	t.Helper()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode bounded HTTP trace: %v", err)
	}
	t.Logf("VERSITY_HTTP_TRACE %s", encoded)
}

func hasConditionalPutHTTPTraceEvent(snapshot conditionalPutHTTPTraceSnapshot, attempt uint64, event string, status int) bool {
	for _, item := range snapshot.Events {
		if item.Attempt == attempt && item.Event == event && item.Status == status {
			return true
		}
	}
	return false
}

func TestConditionalPutHTTPTraceErrorClassificationIsBounded(t *testing.T) {
	trace := newConditionalPutHTTPTrace()
	request, err := http.NewRequest(http.MethodPut, "http://s3.test/private-key", io.NopCloser(strings.NewReader("payload")))
	if err != nil {
		t.Fatal(err)
	}
	request.ContentLength = testConditionalExpectContinueMinSize
	request.Header.Set("If-None-Match", "*")
	transport := conditionalExpectContinueRoundTripper{
		trace: trace,
		next: providerTransportRoundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("private endpoint detail: %w", syscall.EPIPE)
		}),
	}
	_, gotErr := transport.RoundTrip(request)
	if !errors.Is(gotErr, syscall.EPIPE) {
		t.Fatal("diagnostic wrapper changed the underlying RoundTrip error")
	}
	snapshot := trace.closeAndSnapshot()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Attempts != 1 || len(snapshot.Events) > maxConditionalPutTraceEvents ||
		!hasConditionalPutHTTPTraceEvent(snapshot, 1, "round_trip_result", 0) || !strings.Contains(string(encoded), `"error_class":"broken_pipe"`) {
		t.Fatalf("trace did not preserve safe typed error class: %s", encoded)
	}
	if strings.Contains(string(encoded), "private-key") || strings.Contains(string(encoded), "private endpoint detail") || strings.Contains(string(encoded), "broken pipe") {
		t.Fatalf("trace leaked arbitrary error text: %s", encoded)
	}
}

func TestConditionalPutHTTPTraceBoundsAttemptsAndEvents(t *testing.T) {
	trace := newConditionalPutHTTPTrace()
	var lastAccepted *conditionalPutHTTPTraceAttempt
	for i := 0; i < maxConditionalPutTraceAttempts+1; i++ {
		attempt := trace.beginAttempt(testConditionalExpectContinueMinSize, true)
		if i < maxConditionalPutTraceAttempts && attempt == nil {
			t.Fatal("trace stopped before its attempt cap")
		}
		if i == maxConditionalPutTraceAttempts && attempt != nil {
			t.Fatal("trace retained an attempt beyond its cap")
		}
		if attempt != nil {
			lastAccepted = attempt
		}
	}
	lastAccepted.record("event_beyond_cap", 0, "")
	snapshot := trace.closeAndSnapshot()
	if snapshot.Attempts != maxConditionalPutTraceAttempts+1 || snapshot.DroppedAttempts != 1 ||
		len(snapshot.Events) != maxConditionalPutTraceEvents || snapshot.DroppedEvents != 1 {
		t.Fatalf("trace cap mismatch: attempts=%d dropped_attempts=%d events=%d dropped_events=%d", snapshot.Attempts, snapshot.DroppedAttempts, len(snapshot.Events), snapshot.DroppedEvents)
	}
}

type countingRequestBodyRoundTripper struct {
	next  http.RoundTripper
	bytes atomic.Int64
}

func (t *countingRequestBodyRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Body != nil {
		request = request.Clone(request.Context())
		request.Header = request.Header.Clone()
		request.Body = &requestBodyCounter{ReadCloser: request.Body, bytes: &t.bytes}
	}
	return t.next.RoundTrip(request)
}

type requestBodyCounter struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (r *requestBodyCounter) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes.Add(int64(n))
	return n, err
}

type providerTransportRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f providerTransportRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func newConditionalExpectVersityBucket(cfg Config, enabled bool, trace *conditionalPutHTTPTrace) (*MeteredBucket, error) {
	if !enabled {
		return NewBucket(cfg)
	}
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	metrics := &bucketMetrics{}
	bucket, err := s3.NewBucketWithConfig(kitlog.NewNopLogger(), s3.Config{
		Bucket: cfg.Bucket, Endpoint: cfg.Endpoint, Region: cfg.Region, Insecure: cfg.Insecure,
		AWSSDKAuth: cfg.AccessKey == "", AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey,
		SessionToken: cfg.SessionToken, MaxRetries: cfg.MaxRetries,
	}, "rhiza", func(next http.RoundTripper) http.RoundTripper {
		transport, ok := next.(*http.Transport)
		if !ok {
			return providerTransportRoundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("conditional Expect test requires net/http.Transport")
			})
		}
		transport = transport.Clone()
		transport.ExpectContinueTimeout = time.Second
		next = conditionalExpectContinueRoundTripper{next: transport, trace: trace}
		return metrics.transport(wrapTestObserver(cfg.Endpoint, next))
	})
	if err != nil {
		return nil, err
	}
	return newMeteredBucket(bucket, metrics), nil
}

func TestConditionalExpectContinuePolicy(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		body      bool
		length    int64
		condition string
		expect    string
		wantAdd   bool
	}{
		{name: "conditional large put", method: http.MethodPut, body: true, length: testConditionalExpectContinueMinSize, condition: "If-None-Match", wantAdd: true},
		{name: "small conditional put", method: http.MethodPut, body: true, length: testConditionalExpectContinueMinSize - 1, condition: "If-Match"},
		{name: "unconditional put", method: http.MethodPut, body: true, length: testConditionalExpectContinueMinSize, expect: ""},
		{name: "get", method: http.MethodGet, body: true, length: testConditionalExpectContinueMinSize, condition: "If-None-Match"},
		{name: "no body", method: http.MethodPut, length: testConditionalExpectContinueMinSize, condition: "If-Match"},
		{name: "existing expect preserved", method: http.MethodPut, body: true, length: testConditionalExpectContinueMinSize, condition: "If-Match", expect: "100-continue"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got *http.Request
			transport := conditionalExpectContinueRoundTripper{next: providerTransportRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
				got = request
				return nil, nil
			})}
			request, err := http.NewRequest(test.method, "http://s3.test/key", nil)
			if err != nil {
				t.Fatal(err)
			}
			if test.body {
				request.Body = io.NopCloser(strings.NewReader("payload"))
			}
			request.ContentLength = test.length
			if test.condition != "" {
				request.Header.Set(test.condition, "*")
			}
			if test.expect != "" {
				request.Header.Set("Expect", test.expect)
			}
			originalHeader := request.Header.Clone()
			if _, err := transport.RoundTrip(request); err != nil {
				t.Fatal(err)
			}
			wantExpect := test.expect
			if test.wantAdd {
				wantExpect = "100-continue"
			}
			if got.Header.Get("Expect") != wantExpect {
				t.Fatalf("transport Expect=%q, want %q", got.Header.Get("Expect"), wantExpect)
			}
			if request.Header.Get("Expect") != originalHeader.Get("Expect") {
				t.Fatalf("wrapper mutated original request: before=%q after=%q", originalHeader.Get("Expect"), request.Header.Get("Expect"))
			}
		})
	}
}

func TestS3ConditionalExpectContinueEarlyDecision(t *testing.T) {
	const payloadSize = 7 << 20
	payload := bytes.Repeat([]byte{0x5a}, payloadSize)

	for _, mode := range []string{"reject", "accept"} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int64
			var gotExpect atomic.Bool
			var gotCondition atomic.Bool
			var signedCondition atomic.Bool
			var signedExpect atomic.Bool
			var serverBodyBytes atomic.Int64
			var serverBody []byte
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				gotExpect.Store(r.Header.Get("Expect") == "100-continue")
				gotCondition.Store(r.Header.Get("If-None-Match") == "*")
				authorization := r.Header.Get("Authorization")
				if signed := strings.SplitN(authorization, "SignedHeaders=", 2); len(signed) == 2 {
					names := strings.SplitN(signed[1], ",", 2)[0]
					signedCondition.Store(strings.Contains(names, "if-none-match"))
					signedExpect.Store(strings.Contains(names, "expect"))
				}
				if mode == "reject" {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusPreconditionFailed)
					_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>`)
					return
				}

				// net/http emits 100 Continue when the handler first reads the
				// request body for a request carrying Expect: 100-continue.
				var err error
				serverBody, err = io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, "body read failed", http.StatusInternalServerError)
					return
				}
				serverBodyBytes.Store(int64(len(serverBody)))
				w.Header().Set("ETag", `"test-etag"`)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			transport, ok := server.Client().Transport.(*http.Transport)
			if !ok {
				t.Fatalf("httptest transport has unexpected type %T", server.Client().Transport)
			}
			continueTransport := transport.Clone()
			continueTransport.Proxy = nil
			continueTransport.ExpectContinueTimeout = time.Second
			httpTrace := newConditionalPutHTTPTrace()
			bodyCounter := &countingRequestBodyRoundTripper{next: conditionalExpectContinueRoundTripper{next: continueTransport, trace: httpTrace}}
			bucket, err := s3.NewBucketWithConfig(kitlog.NewNopLogger(), s3.Config{
				Bucket: "rhiza-test-bucket", Endpoint: strings.TrimPrefix(server.URL, "https://"), Region: "us-east-1",
				AccessKey: "task-test-access", SecretKey: "task-test-secret",
				BucketLookupType: s3.PathLookup, PartSize: 64 << 20, SendContentMd5: true,
				HTTPConfig: s3.HTTPConfig{Transport: bodyCounter},
			}, "test", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = bucket.Upload(ctx, "guarded-object", bytes.NewReader(payload), thanosobjstore.WithIfNotExists())
			traceSnapshot := httpTrace.closeAndSnapshot()
			if traceSnapshot.Attempts != 1 || len(traceSnapshot.Events) > maxConditionalPutTraceEvents {
				t.Fatalf("trace attempts=%d events=%d dropped_attempts=%d dropped_events=%d", traceSnapshot.Attempts, len(traceSnapshot.Events), traceSnapshot.DroppedAttempts, traceSnapshot.DroppedEvents)
			}
			if !hasConditionalPutHTTPTraceEvent(traceSnapshot, 1, "wrote_headers", 0) || !hasConditionalPutHTTPTraceEvent(traceSnapshot, 1, "got_first_response_byte", 0) {
				t.Fatalf("trace missed required request/response events: %+v", traceSnapshot)
			}
			if mode == "reject" {
				if err == nil || !bucket.IsConditionNotMetErr(err) {
					t.Fatalf("conditional rejection error=%v, condition=%t", err, bucket.IsConditionNotMetErr(err))
				}
				if got := bodyCounter.bytes.Load(); got != 0 {
					t.Fatalf("HTTP request body bytes=%d after early conditional rejection, want 0", got)
				}
				if got := serverBodyBytes.Load(); got != 0 {
					t.Fatalf("server read %d body bytes before rejecting request", got)
				}
				if !hasConditionalPutHTTPTraceEvent(traceSnapshot, 1, "round_trip_result", http.StatusPreconditionFailed) {
					t.Fatalf("trace missed rejected RoundTrip status: %+v", traceSnapshot)
				}
			} else {
				if err != nil {
					t.Fatalf("conditional accepted upload: %v", err)
				}
				if got := bodyCounter.bytes.Load(); got == 0 || got != serverBodyBytes.Load() || !bytes.Equal(serverBody, payload) {
					t.Fatalf("client body bytes=%d server body bytes=%d, want equal nonzero complete body", got, serverBodyBytes.Load())
				}
				if !hasConditionalPutHTTPTraceEvent(traceSnapshot, 1, "got_100_continue", http.StatusContinue) ||
					!hasConditionalPutHTTPTraceEvent(traceSnapshot, 1, "wrote_request", 0) ||
					!hasConditionalPutHTTPTraceEvent(traceSnapshot, 1, "round_trip_result", http.StatusOK) {
					t.Fatalf("trace missed continue/write/success events: %+v", traceSnapshot)
				}
			}
			if requests.Load() != 1 || !gotExpect.Load() || !gotCondition.Load() || !signedCondition.Load() || signedExpect.Load() {
				t.Fatalf("requests=%d expect=%t if_none_match=%t signed_condition=%t signed_expect=%t", requests.Load(), gotExpect.Load(), gotCondition.Load(), signedCondition.Load(), signedExpect.Load())
			}
		})
	}
}

func TestConditionalExpectContinuePreservesSignedCondition(t *testing.T) {
	// This guards the intended transport placement: MinIO signs the conditional
	// header first; the transport then adds the unsigned HTTP expectation field.
	request, err := http.NewRequest(http.MethodPut, "http://s3.test/key", io.NopCloser(strings.NewReader("body")))
	if err != nil {
		t.Fatal(err)
	}
	request.ContentLength = 7 << 20
	request.Header.Set("If-None-Match", "*")
	request.Header.Set("Authorization", `AWS4-HMAC-SHA256 Credential=test,SignedHeaders=host;if-none-match,Signature=redacted`)
	var got *http.Request
	transport := conditionalExpectContinueRoundTripper{next: providerTransportRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		got = request
		return nil, nil
	})}
	if _, err := transport.RoundTrip(request); err != nil {
		t.Fatal(err)
	}
	if got.Header.Get("If-None-Match") != "*" || !strings.Contains(got.Header.Get("Authorization"), "SignedHeaders=host;if-none-match") {
		t.Fatalf("condition/signature changed by wrapper: condition=%q authorization=%q", got.Header.Get("If-None-Match"), got.Header.Get("Authorization"))
	}
	if strings.Contains(got.Header.Get("Authorization"), "expect") || got.Header.Get("Expect") != "100-continue" {
		t.Fatalf("Expect must be added after signing: expect=%q authorization=%q", got.Header.Get("Expect"), got.Header.Get("Authorization"))
	}
	if request.Header.Get("Expect") != "" {
		t.Fatal("wrapper mutated original request headers")
	}
}
