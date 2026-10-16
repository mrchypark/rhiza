package objstore

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	thanosobjstore "github.com/thanos-io/objstore"
)

func TestTerminalDrainJoinsLateMutationsAndRejectsAdmission(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			metrics := &bucketMetrics{}
			bucket := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				close(started)
				<-release
				return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
			}))
			go func() {
				req, _ := http.NewRequest(method, "http://store.test/object", nil)
				response, err := transport.RoundTrip(req)
				if response != nil {
					_ = response.Body.Close()
				}
				done <- err
			}()
			<-started
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := bucket.DrainMutations(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("active mutation drain=%v", err)
			}
			if err := bucket.Upload(context.Background(), "new", strings.NewReader("x")); !errors.Is(err, ErrMutationsStopped) {
				t.Fatalf("new admission=%v", err)
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := bucket.DrainMutations(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTerminalDrainRecognizesCompleteFramingBeforeDecoderClose(t *testing.T) {
	for _, format := range []string{"json", "xml"} {
		t.Run(format, func(t *testing.T) {
			metrics := &bucketMetrics{}
			bucket := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
			body := `{"name":"object"}`
			if format == "xml" {
				body = `<Result><Name>object</Name></Result>`
			}
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body))}, nil
			}))
			req, _ := http.NewRequest(http.MethodPut, "http://store.test/object", nil)
			response, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			if format == "json" {
				var value map[string]any
				err = json.NewDecoder(response.Body).Decode(&value)
			} else {
				var value struct{ Name string }
				err = xml.NewDecoder(response.Body).Decode(&value)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if err = bucket.DrainMutations(context.Background()); err != nil {
				t.Fatalf("fully framed %s was unknown: %v", format, err)
			}
		})
	}
}

func TestTerminalDrainRejectsTruncatedAndUnknownFraming(t *testing.T) {
	for _, length := range []int64{10, -1, 0} {
		metrics := &bucketMetrics{}
		bucket := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
		transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, ContentLength: length, Body: io.NopCloser(strings.NewReader("body"))}, nil
		}))
		req, _ := http.NewRequest(http.MethodPut, "http://store.test/object", nil)
		response, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		p := make([]byte, 4)
		_, _ = response.Body.Read(p)
		_ = response.Body.Close()
		if err = bucket.DrainMutations(context.Background()); !errors.Is(err, ErrMutationOutcomeUnknown) {
			t.Fatalf("length=%d drain=%v", length, err)
		}
	}
}

func TestTerminalDrainRetainsUnknownAttemptAcrossSuccessfulRetry(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			metrics := &bucketMetrics{}
			bucket := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
			attempt := 0
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				attempt++
				if attempt == 1 {
					return nil, context.DeadlineExceeded
				}
				return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
			}))
			for range 2 {
				req, _ := http.NewRequest(method, "http://store.test/object", nil)
				response, _ := transport.RoundTrip(req)
				if response != nil {
					_ = response.Body.Close()
				}
			}
			if err := bucket.DrainMutations(context.Background()); !errors.Is(err, ErrMutationOutcomeUnknown) {
				t.Fatalf("unknown attempt erased by retry: %v", err)
			}
		})
	}
}

func TestTerminalDrainRetainsLogicalMutationFailure(t *testing.T) {
	bucket := newMeteredBucket(&uploadErrorBucket{Bucket: thanosobjstore.NewInMemBucket()}, &bucketMetrics{})
	if err := bucket.Upload(context.Background(), "object", strings.NewReader("x")); err == nil {
		t.Fatal("upload unexpectedly succeeded")
	}
	if err := bucket.DrainMutations(context.Background()); !errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("drain=%v", err)
	}
}

type terminalBodyError struct{}

func (terminalBodyError) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (terminalBodyError) Close() error             { return nil }

func TestTerminalDrainRetainsSuccessfulStatusBodyFailureAfterRetry(t *testing.T) {
	metrics := &bucketMetrics{}
	bucket := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
	attempt := 0
	transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		attempt++
		var body io.ReadCloser = terminalBodyError{}
		if attempt > 1 {
			body = io.NopCloser(strings.NewReader("ok"))
		}
		return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: body}, nil
	}))
	for range 2 {
		req, _ := http.NewRequest(http.MethodPut, "http://store.test/object", nil)
		response, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	if err := bucket.DrainMutations(context.Background()); !errors.Is(err, ErrMutationOutcomeUnknown) {
		t.Fatalf("body failure erased: %v", err)
	}
}

func TestTerminalDrainWaitsForBodyAndRejectsIncompleteOrAsyncOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		consume bool
		unknown bool
	}{
		{"terminal", 200, "ok", true, false},
		{"early-close", 200, "ok", false, true},
		{"embedded-error", 200, "<Error><Code>InternalError</Code></Error>", true, true},
		{"async", 202, "accepted", true, true},
		{"condition-rejected", 412, "<Error>PreconditionFailed</Error>", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &bucketMetrics{}
			bucket := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, ContentLength: int64(len(tc.body)), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			}))
			req, _ := http.NewRequest(http.MethodPut, "http://store.test/object", nil)
			response, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			if tc.consume {
				_, _ = io.Copy(io.Discard, response.Body)
			}
			_ = response.Body.Close()
			err = bucket.DrainMutations(context.Background())
			if errors.Is(err, ErrMutationOutcomeUnknown) != tc.unknown {
				t.Fatalf("drain=%v unknown=%t", err, tc.unknown)
			}
		})
	}
}
