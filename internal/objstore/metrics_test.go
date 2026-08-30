package objstore

import (
	"bytes"
	"context"
	"io"
	"net/http"
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
	if stats.Uploads != 1 || stats.Gets != 1 || stats.BytesUploaded != 3 || stats.BytesDownloaded != 3 || stats.S3HTTPRequests != 1 || stats.S3HTTPFailures != 1 || stats.SDKRetries != 1 || stats.HTTP5xx != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
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
