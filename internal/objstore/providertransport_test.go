package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kitlog "github.com/go-kit/log"
	thanosobjstore "github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/s3"
)

const testConditionalExpectContinueMinSize = 1 << 20

type conditionalExpectContinueRoundTripper struct{ next http.RoundTripper }

func (t conditionalExpectContinueRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPut || request.Body == nil || request.ContentLength < testConditionalExpectContinueMinSize ||
		(request.Header.Get("If-Match") == "" && request.Header.Get("If-None-Match") == "") || request.Header.Get("Expect") != "" {
		return t.next.RoundTrip(request)
	}

	request = request.Clone(request.Context())
	request.Header = request.Header.Clone()
	request.Header.Set("Expect", "100-continue")
	return t.next.RoundTrip(request)
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

func newConditionalExpectVersityBucket(cfg Config, enabled bool) (*MeteredBucket, error) {
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
		next = conditionalExpectContinueRoundTripper{next: transport}
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
			bodyCounter := &countingRequestBodyRoundTripper{next: conditionalExpectContinueRoundTripper{next: continueTransport}}
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
			} else {
				if err != nil {
					t.Fatalf("conditional accepted upload: %v", err)
				}
				if got := bodyCounter.bytes.Load(); got == 0 || got != serverBodyBytes.Load() || !bytes.Equal(serverBody, payload) {
					t.Fatalf("client body bytes=%d server body bytes=%d, want equal nonzero complete body", got, serverBodyBytes.Load())
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
