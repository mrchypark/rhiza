//go:build rhiza_local_testhooks

package objstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	kitlog "github.com/go-kit/log"
	thanosobjstore "github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/s3"
)

type observerRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f observerRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type observerBody struct{ readErr, closeErr error }

func (b observerBody) Read([]byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	return 0, io.EOF
}
func (b observerBody) Close() error { return b.closeErr }

type observerTimeoutError struct{}

func (observerTimeoutError) Error() string   { return "timeout" }
func (observerTimeoutError) Timeout() bool   { return true }
func (observerTimeoutError) Temporary() bool { return true }

func TestS3TransportObserverAggregatesTypedFailuresAndKeepsFirstDetail(t *testing.T) {
	endpoint := "http://127.0.0.1:19001"
	observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	observer.SetPhase("measurement")
	transport := wrapTestObserver(endpoint, observerRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodGet:
			return &http.Response{StatusCode: http.StatusOK, Body: observerBody{readErr: errors.New("body read failed")}}, nil
		case http.MethodHead:
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("success"))}, nil
		case http.MethodPut:
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("private response"))}, nil
		case http.MethodPost:
			return nil, context.Canceled
		case http.MethodDelete:
			return nil, context.DeadlineExceeded
		case http.MethodPatch:
			return nil, observerTimeoutError{}
		default:
			return nil, errors.New("private transport error")
		}
	}))

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch, "CUSTOM-SECRET"} {
		parsed, parseErr := url.Parse(endpoint + "/private-object-name")
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		request, requestErr := http.NewRequestWithContext(context.Background(), method, parsed.String(), nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		request.Header.Set("Authorization", "secret-header")
		response, roundTripErr := transport.RoundTrip(request)
		if roundTripErr != nil {
			continue
		}
		if response != nil && response.Body != nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}

	total, requests, aggregates, first := observer.Snapshot()
	if total != 7 || len(requests) != 7 || len(aggregates) != 6 || first == nil {
		t.Fatalf("observer summary total=%d request_groups=%d error_groups=%d first=%+v", total, len(requests), len(aggregates), first)
	}
	want := map[S3TransportAggregate]uint64{
		{Owner: "node-a", Phase: "measurement", Method: http.MethodGet, StatusFamily: "2xx", Outcome: "response_body_read_error", ErrorClass: "other", ResourceClass: "other", ConditionalPresence: "none"}:        1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodPut, StatusFamily: "5xx", Outcome: "http_status", ErrorClass: "http_status", ResourceClass: "other", ConditionalPresence: "none"}:               1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodPost, StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "context_canceled", ResourceClass: "other", ConditionalPresence: "none"}:   1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodDelete, StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "context_deadline", ResourceClass: "other", ConditionalPresence: "none"}: 1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodPatch, StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "net_timeout", ResourceClass: "other", ConditionalPresence: "none"}:       1,
		{Owner: "node-a", Phase: "measurement", Method: "OTHER", StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "other", ResourceClass: "other", ConditionalPresence: "none"}:                      1,
	}
	for _, aggregate := range aggregates {
		key := aggregate
		key.Count = 0
		count, ok := want[key]
		if !ok || count != aggregate.Count {
			t.Errorf("unexpected error aggregate: %+v", aggregate)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Errorf("missing error aggregates: %+v", want)
	}
	if first.Sequence != 1 || first.Owner != "node-a" || first.Phase != "measurement" || first.Method != http.MethodGet || first.Outcome != "response_body_read_error" || first.ErrorClass != "other" || first.Elapsed < 0 {
		t.Fatalf("unexpected first failure detail: %+v", first)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"private-object-name", "secret-header", "private response", endpoint} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("observer event leaked request data %q: %s", forbidden, encoded)
		}
	}
}

func TestS3TransportObserverDoesNotRetainSuccessOrPerRequestHistory(t *testing.T) {
	endpoint := "http://127.0.0.1:19002"
	observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	transport := wrapTestObserver(endpoint, observerRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	parsed, _ := url.Parse(endpoint)
	var workers sync.WaitGroup
	for i := 0; i < 300; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			request, _ := http.NewRequest(http.MethodGet, parsed.String(), nil)
			if _, err := transport.RoundTrip(request); err != nil {
				t.Errorf("RoundTrip: %v", err)
			}
		}()
	}
	workers.Wait()
	total, requests, aggregates, first := observer.Snapshot()
	if total != 300 || len(requests) != 1 || requests[0].Count != 300 || len(aggregates) != 1 || aggregates[0].Count != 300 || first == nil || first.Sequence == 0 || first.Sequence > 300 {
		t.Fatalf("aggregate summary total=%d requests=%+v aggregates=%+v first=%+v", total, requests, aggregates, first)
	}
}

func TestS3TransportObserverExcludesExpectedControlResponses(t *testing.T) {
	endpoint := "http://127.0.0.1:19005"
	observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	transport := wrapTestObserver(endpoint, observerRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusNotFound
		switch request.Header.Get("X-Test-Response") {
		case "conflict":
			status = http.StatusConflict
		case "precondition":
			status = http.StatusPreconditionFailed
		case "server-error":
			status = http.StatusServiceUnavailable
		case "body-error":
			return &http.Response{StatusCode: http.StatusOK, Body: observerBody{readErr: io.ErrUnexpectedEOF}}, nil
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))

	request := func(method, scenario string, expectedNotFound, expectedCondition bool) {
		t.Helper()
		ctx := context.Background()
		if expectedNotFound {
			ctx = WithExpectedNotFound(ctx)
		}
		if expectedCondition {
			ctx = withExpectedCondition(ctx, thanosobjstore.WithIfNotExists())
		}
		req, reqErr := http.NewRequestWithContext(ctx, method, endpoint+"/private-key", nil)
		if reqErr != nil {
			t.Fatal(reqErr)
		}
		req.Header.Set("X-Test-Response", scenario)
		if expectedCondition {
			req.Header.Set("If-None-Match", "*")
		}
		response, roundTripErr := transport.RoundTrip(req)
		if roundTripErr != nil {
			t.Fatalf("RoundTrip: %v", roundTripErr)
		}
		if response.Body != nil {
			if scenario == "body-error" {
				_, _ = io.Copy(io.Discard, response.Body)
			} else {
				_ = response.Body.Close()
			}
		}
	}

	request(http.MethodGet, "missing", true, false)
	request(http.MethodPut, "conflict", false, true)
	request(http.MethodPut, "precondition", false, true)
	if total, _, aggregates, first := observer.Snapshot(); total != 3 || len(aggregates) != 0 || first != nil {
		t.Fatalf("expected control responses were reported as failures: total=%d aggregates=%+v first=%+v", total, aggregates, first)
	}
	request(http.MethodGet, "missing", false, false)
	request(http.MethodPut, "server-error", false, true)
	request(http.MethodGet, "body-error", false, false)
	total, _, aggregates, first := observer.Snapshot()
	if total != 6 || len(aggregates) != 3 || first == nil || first.Sequence != 4 || first.Outcome != "http_status" {
		t.Fatalf("unexpected failures after control responses: total=%d aggregates=%+v first=%+v", total, aggregates, first)
	}
	want := map[string]uint64{"http_status/4xx": 1, "http_status/5xx": 1, "response_body_read_error/2xx": 1}
	for _, aggregate := range aggregates {
		key := aggregate.Outcome + "/" + aggregate.StatusFamily
		if want[key] != aggregate.Count {
			t.Errorf("unexpected aggregate %+v", aggregate)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Errorf("missing aggregates: %v", want)
	}
}

func TestS3TransportObserverClassifiesPortableIOErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"unexpected-eof", io.ErrUnexpectedEOF, "unexpected_eof"},
		{"closed-pipe", io.ErrClosedPipe, "closed_pipe"},
		{"net-closed", net.ErrClosed, "net_closed"},
		{"body-read-after-close", http.ErrBodyReadAfterClose, "body_read_after_close"},
		{"broken-pipe", syscall.EPIPE, "broken_pipe"},
		{"connection-reset", syscall.ECONNRESET, "connection_reset"},
		{"wrapped-epipe", fmt.Errorf("write failed: %w", syscall.EPIPE), "broken_pipe"},
		{"wrapped-econnreset", fmt.Errorf("read failed: %w", syscall.ECONNRESET), "connection_reset"},
		{"os-syscall-error-epipe", &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}, "broken_pipe"},
		{"net-op-error-econnreset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, "connection_reset"},
		{"url-error-epipe", &url.Error{Op: "GET", URL: "http://private", Err: syscall.EPIPE}, "broken_pipe"},
		{"errors-join-ctx-epipe", errors.Join(context.Canceled, syscall.EPIPE), "context_canceled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := "http://127.0.0.1:19006/" + test.name
			observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			transport := wrapTestObserver(endpoint, observerRoundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, test.err
			}))
			request, err := http.NewRequest(http.MethodGet, endpoint, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := transport.RoundTrip(request); !errors.Is(err, test.err) {
				t.Fatalf("RoundTrip error = %v, want %v", err, test.err)
			}
			_, _, aggregates, first := observer.Snapshot()
			if len(aggregates) != 1 || aggregates[0].ErrorClass != test.want || first == nil || first.ErrorClass != test.want {
				t.Fatalf("error classification = aggregates %+v first %+v, want %q", aggregates, first, test.want)
			}
		})
	}
}

func TestS3TransportObserverRegistrationIsEndpointScoped(t *testing.T) {
	endpoint := "http://127.0.0.1:19003"
	observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	if _, _, err := RegisterS3TransportObserver(endpoint, []string{"node-b"}); err == nil {
		t.Fatal("duplicate endpoint registration succeeded")
	}
	other := wrapTestObserver("http://127.0.0.1:19004", observerRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unobserved")
	}))
	parsed, _ := url.Parse("http://127.0.0.1:19004")
	request, _ := http.NewRequest(http.MethodGet, parsed.String(), nil)
	_, _ = other.RoundTrip(request)
	if total, requests, aggregates, first := observer.Snapshot(); total != 0 || len(requests) != 0 || len(aggregates) != 0 || first != nil {
		t.Fatalf("other endpoint affected registration: total=%d requests=%+v aggregates=%+v first=%+v", total, requests, aggregates, first)
	}
}

var _ net.Error = observerTimeoutError{}

func TestS3TransportObserverClassifiesResourcePaths(t *testing.T) {
	const hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const generation = "00000000000000000001"
	const index = "00000000000000000000" // zero is a valid root index
	const prefix = "/my-bucket/some/prefix/"
	tests := []struct {
		name string
		path string
		want string
	}{
		{"archive-head", prefix + "archive/head.bin", "archive_head"},
		{"archive-block-legacy", prefix + "archive/blocks/" + hash + ".bin", "archive_block"},
		{"archive-block-generation", prefix + "archive/blocks/" + hash + "_" + generation + ".bin", "archive_block"},
		{"checkpoint-current", prefix + "checkpoint/CURRENT", "checkpoint_current"},
		{"checkpoint-block-legacy", prefix + "checkpoint/blocks/" + hash + ".block", "checkpoint_block"},
		{"checkpoint-block-generation", prefix + "checkpoint/blocks/" + hash + "_" + generation + ".block", "checkpoint_block"},
		{"checkpoint-root-zero-index", prefix + "checkpoint/roots/" + index + "_" + hash + ".json", "checkpoint_root"},
		{"not-archive-segment", "/my-bucket/notarchive/blocks/" + hash + ".bin", "other"},
		{"not-checkpoint-segment", "/my-bucket/notcheckpoint/blocks/" + hash + ".block", "other"},
		{"not-checkpoint-roots-segment", "/my-bucket/notcheckpoint/roots/" + index + "_" + hash + ".json", "other"},
		{"misleading-prefix", "/my-bucket/archive/blocks/invalid/checkpoint/roots/" + index + "_" + hash + ".json", "checkpoint_root"},
		{"archive-short-hash", prefix + "archive/blocks/" + hash[:63] + ".bin", "other"},
		{"archive-uppercase-hash", prefix + "archive/blocks/A" + hash[1:] + ".bin", "other"},
		{"archive-bad-extension", prefix + "archive/blocks/" + hash + ".txt", "other"},
		{"archive-extra-trailing", prefix + "archive/blocks/" + hash + ".bin/extra", "other"},
		{"archive-zero-generation", prefix + "archive/blocks/" + hash + "_00000000000000000000.bin", "other"},
		{"archive-overflow-generation", prefix + "archive/blocks/" + hash + "_18446744073709551616.bin", "other"},
		{"archive-short-generation", prefix + "archive/blocks/" + hash + "_1.bin", "other"},
		{"checkpoint-zero-generation", prefix + "checkpoint/blocks/" + hash + "_00000000000000000000.block", "other"},
		{"checkpoint-overflow-generation", prefix + "checkpoint/blocks/" + hash + "_18446744073709551616.block", "other"},
		{"checkpoint-short-generation", prefix + "checkpoint/blocks/" + hash + "_1.block", "other"},
		{"root-overflow-index", prefix + "checkpoint/roots/18446744073709551616_" + hash + ".json", "other"},
		{"root-short-index", prefix + "checkpoint/roots/1_" + hash + ".json", "other"},
		{"unknown", prefix + "other/path", "other"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := "http://127.0.0.1:19007/" + test.name
			observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			transport := wrapTestObserver(endpoint, observerRoundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, syscall.EPIPE
			}))
			request, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:19007"+test.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if request.URL.Path != test.path {
				t.Fatalf("URL.Path=%q, want %q", request.URL.Path, test.path)
			}
			if _, err := transport.RoundTrip(request); !errors.Is(err, syscall.EPIPE) {
				t.Fatalf("RoundTrip error=%v, want EPIPE", err)
			}
			_, _, aggregates, first := observer.Snapshot()
			if len(aggregates) != 1 || aggregates[0].ResourceClass != test.want || first == nil || first.ResourceClass != test.want {
				t.Fatalf("URL.Path class aggregates=%+v first=%+v, want %s", aggregates, first, test.want)
			}
		})
	}
}

func TestS3TransportObserverClassifiesConditionalPresence(t *testing.T) {
	tests := []struct {
		name        string
		ifMatch     string
		ifNoneMatch string
		want        string
	}{
		{"none", "", "", "none"},
		{"if-match", "abc", "", "if_match"},
		{"if-none-match", "", "abc", "if_none_match"},
		{"both", "abc", "def", "both"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPut, "http://127.0.0.1:19007/archive/head.bin", nil)
			if err != nil {
				t.Fatal(err)
			}
			if test.ifMatch != "" {
				request.Header.Set("If-Match", test.ifMatch)
			}
			if test.ifNoneMatch != "" {
				request.Header.Set("If-None-Match", test.ifNoneMatch)
			}
			if got := classifyConditionalPresence(request); got != test.want {
				t.Errorf("classifyConditionalPresence() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestS3TransportObserverPrivacyJSONNoPathOrHeaders(t *testing.T) {
	endpoint := "http://127.0.0.1:19008"
	observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	observer.SetPhase("measurement")
	transport := wrapTestObserver(endpoint, observerRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("private transport error: %w", syscall.EPIPE)
	}))
	request, err := http.NewRequest(http.MethodPut, endpoint+"/private-bucket/private-prefix/archive/blocks/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.bin", strings.NewReader("private body"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("If-Match", "secret-etag")
	request.Header.Set("Authorization", "secret-auth")
	if _, err := transport.RoundTrip(request); !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("RoundTrip error=%v, want EPIPE", err)
	}
	_, _, aggregates, first := observer.Snapshot()
	if len(aggregates) != 1 || first == nil || aggregates[0].ResourceClass != "archive_block" || aggregates[0].ConditionalPresence != "if_match" || aggregates[0].ErrorClass != "broken_pipe" {
		t.Fatalf("unexpected aggregate and first: %+v %+v", aggregates, first)
	}
	for _, value := range []any{first, aggregates} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"private-bucket", "private-prefix", "archive/blocks", "0123456789abcdef", "secret-etag", "secret-auth", "private body", "private transport error", endpoint} {
			if strings.Contains(string(encoded), forbidden) {
				t.Fatalf("observer JSON leaked %q: %s", forbidden, encoded)
			}
		}
	}
}

func TestS3TransportObserverGroupsDifferentLengthSameClass(t *testing.T) {
	endpoint := "http://127.0.0.1:19009"
	observer, unregister, err := RegisterS3TransportObserver(endpoint, []string{"node-a"})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	observer.SetPhase("measurement")
	transport := wrapTestObserver(endpoint, observerRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("private transport error: %w", syscall.EPIPE)
	}))
	paths := []string{
		"/private-bucket/private-prefix/archive/blocks/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.bin",
		"/private-bucket/private-prefix/archive/blocks/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef_00000000000000000001.bin",
	}
	for i, path := range paths {
		length := 11
		if i == 1 {
			length = 29
		}
		request, err := http.NewRequest(http.MethodPut, endpoint+path, strings.NewReader(strings.Repeat("x", length)))
		if err != nil {
			t.Fatal(err)
		}
		if request.ContentLength != int64(length) {
			t.Fatalf("request ContentLength=%d, want %d", request.ContentLength, length)
		}
		request.Header.Set("If-None-Match", "private-etag")
		if _, err := transport.RoundTrip(request); !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("RoundTrip error=%v, want EPIPE", err)
		}
	}
	_, _, aggregates, first := observer.Snapshot()
	if len(aggregates) != 1 || aggregates[0].Count != 2 || aggregates[0].ResourceClass != "archive_block" || aggregates[0].ConditionalPresence != "if_none_match" || aggregates[0].ErrorClass != "broken_pipe" {
		t.Fatalf("expected 1 aggregate with count 2, got %+v", aggregates)
	}
	if first == nil || first.DeclaredContentLength != 11 || first.ResourceClass != "archive_block" || first.ConditionalPresence != "if_none_match" || first.ErrorClass != "broken_pipe" {
		t.Fatalf("unexpected first: %+v", first)
	}
	encoded, err := json.Marshal(aggregates)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"declared_content_length", "private-bucket", "private-prefix", "private-etag", "0123456789abcdef", "private transport error"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("aggregate JSON leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestS3TransportObserverRealSDKRetryThenTypedCondition(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "broken_pipe", err: syscall.EPIPE, want: "broken_pipe"},
		{name: "connection_reset", err: syscall.ECONNRESET, want: "connection_reset"},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte("known immutable extent"), 32)
			hash := sha256.Sum256(payload)
			key := "cluster/archive/blocks/" + hex.EncodeToString(hash[:]) + "_00000000000000000001.bin"
			var serverPUTs atomic.Int32
			var serverGETs atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/bucket/"+key {
					http.NotFound(w, request)
					return
				}
				switch request.Method {
				case http.MethodPut:
					serverPUTs.Add(1)
					if request.Header.Get("If-None-Match") != "*" {
						http.Error(w, "missing condition", http.StatusBadRequest)
						return
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusPreconditionFailed)
					_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>`)
				case http.MethodGet:
					serverGETs.Add(1)
					w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
					_, _ = w.Write(payload)
				default:
					http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()
			observer, unregister, err := RegisterS3TransportObserver(server.URL, []string{"node-a"})
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			observer.SetPhase("cleanup_preparation")
			var putAttempts atomic.Int32
			metrics := &bucketMetrics{}
			raw, err := s3.NewBucketWithConfig(kitlog.NewNopLogger(), s3.Config{
				Bucket: "bucket", Endpoint: strings.TrimPrefix(server.URL, "http://"), Region: "us-east-1", Insecure: true,
				AccessKey: "test-access", SecretKey: "test-secret", BucketLookupType: s3.PathLookup,
				MaxRetries: 2,
			}, "test", func(next http.RoundTripper) http.RoundTripper {
				injected := observerRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodPut && putAttempts.Add(1) == 1 {
						return nil, fmt.Errorf("injected first attempt: %w", test.err)
					}
					return next.RoundTrip(request)
				})
				return metrics.transport(wrapTestObserver(server.URL, injected))
			})
			if err != nil {
				t.Fatal(err)
			}
			bucket := newMeteredBucket(raw, metrics)
			defer bucket.Close()
			uploadCtx, attribution := BeginExtentUploadAttribution(context.Background())
			err = bucket.Upload(uploadCtx, key, bytes.NewReader(payload), thanosobjstore.WithIfNotExists())
			if err == nil || !bucket.IsConditionNotMetErr(err) {
				t.Fatalf("single Upload final error=%v, want typed condition rejection", err)
			}
			stats := bucket.Stats()
			if got := putAttempts.Load(); got != 2 || serverPUTs.Load() != 1 || stats.Uploads != 1 || stats.Failures != 1 || stats.HTTPPutRequests != 2 || stats.HTTPFailures != 1 || stats.TransportFailures != 1 || stats.BytesPublished != 0 {
				t.Fatalf("single Upload attempts=%d serverPUTs=%d stats=%+v", got, serverPUTs.Load(), stats)
			}
			if stats.RetryMetadataRequests != 0 || stats.RetryMetadataUnknownRequests != 2 {
				t.Fatalf("SDK retry metadata known=%d unknown=%d, want 0/2", stats.RetryMetadataRequests, stats.RetryMetadataUnknownRequests)
			}
			total, _, aggregates, first := observer.Snapshot()
			if total != 2 || len(aggregates) != 1 || aggregates[0].Count != 1 || aggregates[0].Owner != "node-a" || aggregates[0].Phase != "cleanup_preparation" || aggregates[0].Method != http.MethodPut || aggregates[0].ErrorClass != test.want || aggregates[0].ResourceClass != "archive_block" || aggregates[0].ConditionalPresence != "if_none_match" || first == nil || first.StatusCode != 0 || first.ErrorClass != test.want || first.Outcome != "round_trip_error" {
				t.Fatalf("attempt observer total=%d aggregates=%+v first=%+v", total, aggregates, first)
			}
			body, getErr := bucket.Get(context.Background(), key)
			if getErr != nil {
				t.Fatal(getErr)
			}
			readback, readErr := io.ReadAll(io.LimitReader(body, int64(len(payload)+1)))
			closeErr := body.Close()
			if readErr != nil || closeErr != nil || serverGETs.Load() != 1 || !bytes.Equal(readback, payload) || sha256.Sum256(readback) != hash {
				t.Fatalf("known object readback bytes=%d readErr=%v closeErr=%v serverGETs=%d", len(readback), readErr, closeErr, serverGETs.Load())
			}
			// Keep the first operation's probe live: another Upload's 412
			// must not join it before either probe is completed.
			otherCtx, otherAttribution := BeginExtentUploadAttribution(context.Background())
			otherErr := bucket.Upload(otherCtx, key, bytes.NewReader(payload), thanosobjstore.WithIfNotExists())
			if otherErr == nil || !bucket.IsConditionNotMetErr(otherErr) {
				t.Fatalf("separate Upload final error=%v, want typed condition", otherErr)
			}
			otherAttribution.Complete(otherErr, true, "not_attempted")
			attribution.Complete(err, true, "verified")
			joined := observer.ExtentAttributionSnapshot()
			if len(joined) != 1 || joined[0].Operations != 1 || joined[0].PhysicalErrorAttempts != 1 || joined[0].Observed412 != 1 || joined[0].PhysicalClass != test.want || joined[0].FinalSDKClass != "typed_condition" || joined[0].GuardOutcome != "verified" {
				t.Fatalf("same-operation attribution=%+v", joined)
			}
			if got := observer.ExtentAttributionSnapshot(); len(got) != 1 || got[0].Operations != 1 || got[0].Observed412 != 1 {
				t.Fatalf("cross-operation 412 joined prior failure: %+v", got)
			}
			encoded, jsonErr := json.Marshal(joined)
			if jsonErr != nil || bytes.Contains(encoded, []byte(key)) || bytes.Contains(encoded, hash[:]) || bytes.Contains(encoded, []byte("injected first attempt")) {
				t.Fatalf("attribution leaked identity or raw error: %s, err=%v", encoded, jsonErr)
			}
		})
	}
}
