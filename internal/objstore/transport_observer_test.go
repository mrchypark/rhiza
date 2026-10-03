//go:build rhiza_local_testhooks

package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	thanosobjstore "github.com/thanos-io/objstore"
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
		{Owner: "node-a", Phase: "measurement", Method: http.MethodGet, StatusFamily: "2xx", Outcome: "response_body_read_error", ErrorClass: "other"}:        1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodPut, StatusFamily: "5xx", Outcome: "http_status", ErrorClass: "http_status"}:               1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodPost, StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "context_canceled"}:   1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodDelete, StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "context_deadline"}: 1,
		{Owner: "node-a", Phase: "measurement", Method: http.MethodPatch, StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "net_timeout"}:       1,
		{Owner: "node-a", Phase: "measurement", Method: "OTHER", StatusFamily: "none", Outcome: "round_trip_error", ErrorClass: "other"}:                      1,
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
