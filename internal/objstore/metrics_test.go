package objstore

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	thanosobjstore "github.com/thanos-io/objstore"
)

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
	_, _ = transport.RoundTrip(request)
	stats := bucket.Stats()
	if stats.Uploads != 1 || stats.Gets != 1 || stats.BytesUploaded != 3 || stats.BytesDownloaded != 3 || stats.S3HTTPRequests != 1 || stats.S3HTTPFailures != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}
