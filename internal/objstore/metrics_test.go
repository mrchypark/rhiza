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
