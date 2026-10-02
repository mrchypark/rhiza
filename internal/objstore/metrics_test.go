package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	thanosobjstore "github.com/thanos-io/objstore"
)

type sizingBucket struct {
	thanosobjstore.Bucket
	size int64
}

type contextBucket struct {
	thanosobjstore.Bucket
	ctx context.Context
}

type readErrorBucket struct {
	thanosobjstore.Bucket
}

type uploadErrorBucket struct{ thanosobjstore.Bucket }

func (b *uploadErrorBucket) Upload(_ context.Context, _ string, reader io.Reader, _ ...thanosobjstore.ObjectUploadOption) error {
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return err
	}
	return errors.New("upload failed after reading body")
}

func (b *readErrorBucket) Get(context.Context, string) (io.ReadCloser, error) {
	return &readErrorCloser{Reader: strings.NewReader("ab")}, nil
}

type readErrorCloser struct {
	*strings.Reader
}

func (r *readErrorCloser) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, errors.New("response body read failed")
	}
	return n, err
}

func (r *readErrorCloser) Close() error { return nil }

func (b *contextBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	b.ctx = ctx
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

func (b *sizingBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	var err error
	b.size, err = thanosobjstore.TryToGetSize(reader)
	if err != nil {
		return err
	}
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

func TestMeteredBucketCountsBytesAndHTTPAttempts(t *testing.T) {
	ctx := context.Background()
	metrics := &bucketMetrics{}
	bucket := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
	if err := bucket.Upload(ctx, "x", bytes.NewReader([]byte("abc"))); err != nil {
		t.Fatal(err)
	}
	r, err := bucket.Get(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: http.NoBody}, nil
	}))
	request, _ := http.NewRequest(http.MethodGet, "http://s3.test/x", nil)
	request.Header.Set("amz-sdk-request", "attempt=2; max=3")
	_, _ = transport.RoundTrip(request)
	stats := bucket.Stats()
	if stats.Uploads != 1 || stats.Gets != 1 || stats.BytesUploaded != 3 || stats.BytesDownloaded != 3 || stats.HTTPRequests != 1 || stats.HTTPFailures != 1 || stats.S3HTTPRequests != 1 || stats.S3HTTPFailures != 1 || stats.HTTPGetRequests != 1 || stats.SDKRetries != 1 || stats.RetryMetadataRequests != 1 || stats.RetryMetadataUnknownRequests != 0 || stats.HTTP5xx != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestMeteredBucketSeparatesAttemptedAndPublishedUploadBytes(t *testing.T) {
	metrics := &bucketMetrics{}
	failed := newMeteredBucket(&uploadErrorBucket{Bucket: thanosobjstore.NewInMemBucket()}, metrics)
	if err := failed.Upload(context.Background(), "failed", strings.NewReader("attempt")); err == nil {
		t.Fatal("failed upload succeeded")
	}
	succeeded := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics)
	if err := succeeded.Upload(context.Background(), "ok", strings.NewReader("published")); err != nil {
		t.Fatal(err)
	}
	stats := failed.Stats()
	if stats.BytesUploaded != uint64(len("attempt")+len("published")) || stats.BytesPublished != uint64(len("published")) {
		t.Fatalf("attempted=%d published=%d", stats.BytesUploaded, stats.BytesPublished)
	}
}

func TestMeteredBucketClassifiesHTTPMethods(t *testing.T) {
	metrics := &bucketMetrics{}
	transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	}))
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodHead, http.MethodDelete, http.MethodPost} {
		request, err := http.NewRequest(method, "http://object.test/object", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := transport.RoundTrip(request); err != nil {
			t.Fatal(err)
		}
	}
	stats := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
	if stats.HTTPRequests != 5 || stats.HTTPGetRequests != 1 || stats.HTTPPutRequests != 1 || stats.HTTPHeadRequests != 1 || stats.HTTPDeleteRequests != 1 || stats.HTTPOtherRequests != 1 {
		t.Fatalf("unexpected method stats: %+v", stats)
	}
}

func TestHTTPAttemptBodyBytesAndRetryMetadataAvailability(t *testing.T) {
	metrics := &bucketMetrics{}
	transport := metrics.transport(roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("response"))}, nil
	}))
	request, err := http.NewRequest(http.MethodPut, "http://s3.test/object", strings.NewReader("request-body"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	stats := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
	if stats.HTTPRequestBodyBytes != uint64(len("request-body")) || stats.HTTPResponseBodyBytes != uint64(len("response")) {
		t.Fatalf("consumed attempt body bytes: request=%d response=%d", stats.HTTPRequestBodyBytes, stats.HTTPResponseBodyBytes)
	}
	if stats.RetryMetadataRequests != 0 || stats.RetryMetadataUnknownRequests != 1 || stats.SDKRetries != 0 {
		t.Fatalf("missing retry metadata was not reported as unknown: %+v", stats)
	}
}

func TestAWSSDKRetry(t *testing.T) {
	for header, want := range map[string]struct{ retry, recognized bool }{
		"attempt=1; max=10":  {false, true},
		"attempt=2; max=10":  {true, true},
		"attempt=10; max=10": {true, true},
		"attempt=unknown":    {false, false},
		"attempt=0":          {false, false},
		"max=10":             {false, false},
		"":                   {false, false},
	} {
		if retry, recognized := awsSDKRetry(header); retry != want.retry || recognized != want.recognized {
			t.Errorf("awsSDKRetry(%q) = (%t, %t), want (%t, %t)", header, retry, recognized, want.retry, want.recognized)
		}
	}
}

func TestRetryMetadataStatsRequireRecognizedAttempt(t *testing.T) {
	metrics := &bucketMetrics{}
	transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	for _, header := range []string{"", "attempt=unknown", "max=10", "attempt=1; max=10", "attempt=2; max=10"} {
		request, err := http.NewRequest(http.MethodGet, "http://example.test/object", nil)
		if err != nil {
			t.Fatal(err)
		}
		if header != "" {
			request.Header.Set("amz-sdk-request", header)
		}
		response, err := transport.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
	}
	stats := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
	if stats.RetryMetadataRequests != 2 || stats.RetryMetadataUnknownRequests != 3 || stats.SDKRetries != 1 {
		t.Fatalf("retry metadata classification: known=%d unknown=%d retries=%d; want 2, 3, 1", stats.RetryMetadataRequests, stats.RetryMetadataUnknownRequests, stats.SDKRetries)
	}
}

func TestMeteredBucketCountsResponseBodyReadFailure(t *testing.T) {
	bucket := newMeteredBucket(&readErrorBucket{Bucket: thanosobjstore.NewInMemBucket()}, &bucketMetrics{})
	reader, err := bucket.Get(context.Background(), "object")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := reader.Read(make([]byte, 2)); err == nil {
			t.Fatal("Read succeeded, want response body read error")
		}
	}
	stats := bucket.Stats()
	if stats.Gets != 1 || stats.BytesDownloaded != 2 || stats.Failures != 1 || stats.HTTPRequests != 0 || stats.HTTPFailures != 0 || stats.S3HTTPRequests != 0 || stats.S3HTTPFailures != 0 || stats.TransportFailures != 0 {
		t.Fatalf("unexpected response body failure stats: %+v", stats)
	}
}

func TestHTTPResponseBodyReadFailureIsCountedOnce(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			metrics := &bucketMetrics{}
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: &readErrorCloser{Reader: strings.NewReader("ab")}}, nil
			}))
			request, err := http.NewRequest(http.MethodGet, "http://object.test/object", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := response.Body.Read(make([]byte, 2)); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := response.Body.Read(make([]byte, 2)); err == nil {
					t.Fatal("Read succeeded, want response body read error")
				}
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			stats := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
			if stats.HTTPRequests != 1 || stats.HTTPFailures != 1 || stats.S3HTTPFailures != 1 || stats.TransportFailures != 1 {
				t.Fatalf("unexpected response body failure stats: %+v", stats)
			}
		})
	}
}

func TestMeteredBucketPreservesUploadSize(t *testing.T) {
	underlying := &sizingBucket{Bucket: thanosobjstore.NewInMemBucket()}
	bucket := newMeteredBucket(underlying, &bucketMetrics{})
	if err := bucket.Upload(context.Background(), "x", bytes.NewReader([]byte("abc"))); err != nil {
		t.Fatal(err)
	}
	if underlying.size != 3 {
		t.Fatalf("upload size = %d, want 3", underlying.size)
	}
	section := io.NewSectionReader(bytes.NewReader(make([]byte, 32)), 8, 16)
	if err := bucket.Upload(context.Background(), "section", section); err != nil {
		t.Fatal(err)
	}
	if underlying.size != 16 {
		t.Fatalf("section upload size = %d, want 16", underlying.size)
	}
}

func TestConditionalHTTPOutcomeIsNotReportedAsFailure(t *testing.T) {
	for _, header := range []string{"If-Match", "If-None-Match"} {
		t.Run(header, func(t *testing.T) {
			metrics := &bucketMetrics{}
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusPreconditionFailed, Body: http.NoBody}, nil
			}))
			request, _ := http.NewRequest(http.MethodPut, "http://s3.test/head", nil)
			request.Header.Set(header, "*")
			if header == "If-Match" {
				request = request.WithContext(withExpectedCondition(request.Context(), thanosobjstore.WithIfMatch(&thanosobjstore.ObjectVersion{Type: thanosobjstore.ETag, Value: "etag"})))
			} else {
				request = request.WithContext(withExpectedCondition(request.Context(), thanosobjstore.WithIfNotExists()))
			}
			if _, err := transport.RoundTrip(request); err != nil {
				t.Fatal(err)
			}
			stats := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
			if stats.S3HTTPRequests != 1 || stats.S3HTTPFailures != 0 {
				t.Fatalf("conditional outcome stats: %+v", stats)
			}
		})
	}
}

func TestUnexpectedConditionalStatusIsReportedAsHTTPFailure(t *testing.T) {
	for _, status := range []int{http.StatusConflict, http.StatusPreconditionFailed} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			metrics := &bucketMetrics{}
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: http.NoBody}, nil
			}))
			request, _ := http.NewRequest(http.MethodPut, "http://s3.test/object", nil)
			request.Header.Set("If-Match", "*")
			if _, err := transport.RoundTrip(request); err != nil {
				t.Fatal(err)
			}
			stats := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
			if stats.S3HTTPFailures != 1 || stats.Unexpected4xx != 1 {
				t.Fatalf("unexpected conditional status stats: %+v", stats)
			}
		})
	}
}

func TestMeteredBucketMarksConditionalUpload(t *testing.T) {
	underlying := &contextBucket{Bucket: thanosobjstore.NewInMemBucket()}
	bucket := newMeteredBucket(underlying, &bucketMetrics{})
	if err := bucket.Upload(context.Background(), "conditional", bytes.NewReader([]byte("value")), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if !expectsCondition(underlying.ctx) {
		t.Fatal("conditional upload did not mark its request context")
	}
	if err := bucket.Upload(context.Background(), "plain", bytes.NewReader([]byte("value"))); err != nil {
		t.Fatal(err)
	}
	if expectsCondition(underlying.ctx) {
		t.Fatal("plain upload marked its request context")
	}
}

func TestMissingObjectProbeRequiresExpectedContext(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			metrics := &bucketMetrics{}
			transport := metrics.transport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody}, nil
			}))
			request, _ := http.NewRequest(method, "http://s3.test/missing", nil)
			if _, err := transport.RoundTrip(request); err != nil {
				t.Fatal(err)
			}
			stats := newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
			if stats.S3HTTPFailures != 1 || stats.Unexpected4xx != 1 {
				t.Fatalf("unexpected missing-object stats: %+v", stats)
			}
			request = request.WithContext(WithExpectedNotFound(request.Context()))
			if _, err := transport.RoundTrip(request); err != nil {
				t.Fatal(err)
			}
			stats = newMeteredBucket(thanosobjstore.NewInMemBucket(), metrics).Stats()
			if stats.S3HTTPRequests != 2 || stats.S3HTTPFailures != 1 || stats.Unexpected4xx != 1 {
				t.Fatalf("expected missing-object stats: %+v", stats)
			}
		})
	}
}
