package objstore

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"

	thanosobjstore "github.com/thanos-io/objstore"
)

// Stats exposes object-store operations at both the logical bucket boundary
// and the billable S3 HTTP boundary.
type Stats struct {
	Uploads         uint64 `json:"uploads"`
	Gets            uint64 `json:"gets"`
	Lists           uint64 `json:"lists"`
	Heads           uint64 `json:"heads"`
	Deletes         uint64 `json:"deletes"`
	Failures        uint64 `json:"failures"`
	BytesUploaded   uint64 `json:"bytes_uploaded"`
	BytesDownloaded uint64 `json:"bytes_downloaded"`
	S3HTTPRequests  uint64 `json:"s3_http_requests"`
	S3HTTPFailures  uint64 `json:"s3_http_failures"`
}

type bucketMetrics struct {
	uploads, gets, lists, heads, deletes atomic.Uint64
	failures                             atomic.Uint64
	bytesUploaded, bytesDownloaded       atomic.Uint64
	httpRequests, httpFailures           atomic.Uint64
}

type MeteredBucket struct {
	thanosobjstore.Bucket
	metrics *bucketMetrics
}

func newMeteredBucket(bucket thanosobjstore.Bucket, metrics *bucketMetrics) *MeteredBucket {
	return &MeteredBucket{Bucket: bucket, metrics: metrics}
}

func (b *MeteredBucket) Stats() Stats {
	return Stats{
		Uploads: b.metrics.uploads.Load(), Gets: b.metrics.gets.Load(), Lists: b.metrics.lists.Load(),
		Heads: b.metrics.heads.Load(), Deletes: b.metrics.deletes.Load(), Failures: b.metrics.failures.Load(),
		BytesUploaded: b.metrics.bytesUploaded.Load(), BytesDownloaded: b.metrics.bytesDownloaded.Load(),
		S3HTTPRequests: b.metrics.httpRequests.Load(), S3HTTPFailures: b.metrics.httpFailures.Load(),
	}
}

func (b *MeteredBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	b.metrics.uploads.Add(1)
	counted := &countingReader{reader: reader, count: &b.metrics.bytesUploaded}
	err := b.Bucket.Upload(ctx, name, counted, opts...)
	b.record(err)
	return err
}

func (b *MeteredBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.metrics.gets.Add(1)
	reader, err := b.Bucket.Get(ctx, name)
	b.record(err)
	if err != nil {
		return nil, err
	}
	return &countingReadCloser{ReadCloser: reader, count: &b.metrics.bytesDownloaded}, nil
}

func (b *MeteredBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.metrics.gets.Add(1)
	reader, err := b.Bucket.GetRange(ctx, name, off, length)
	b.record(err)
	if err != nil {
		return nil, err
	}
	return &countingReadCloser{ReadCloser: reader, count: &b.metrics.bytesDownloaded}, nil
}

func (b *MeteredBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...thanosobjstore.IterOption) error {
	b.metrics.lists.Add(1)
	err := b.Bucket.Iter(ctx, dir, f, options...)
	b.record(err)
	return err
}

func (b *MeteredBucket) IterWithAttributes(ctx context.Context, dir string, f func(thanosobjstore.IterObjectAttributes) error, options ...thanosobjstore.IterOption) error {
	b.metrics.lists.Add(1)
	err := b.Bucket.IterWithAttributes(ctx, dir, f, options...)
	b.record(err)
	return err
}

func (b *MeteredBucket) Exists(ctx context.Context, name string) (bool, error) {
	b.metrics.heads.Add(1)
	exists, err := b.Bucket.Exists(ctx, name)
	b.record(err)
	return exists, err
}

func (b *MeteredBucket) Attributes(ctx context.Context, name string) (thanosobjstore.ObjectAttributes, error) {
	b.metrics.heads.Add(1)
	attributes, err := b.Bucket.Attributes(ctx, name)
	b.record(err)
	return attributes, err
}

func (b *MeteredBucket) Delete(ctx context.Context, name string) error {
	b.metrics.deletes.Add(1)
	err := b.Bucket.Delete(ctx, name)
	b.record(err)
	return err
}

func (b *MeteredBucket) record(err error) {
	if err != nil {
		b.metrics.failures.Add(1)
	}
}

func (m *bucketMetrics) transport(next http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		m.httpRequests.Add(1)
		response, err := next.RoundTrip(request)
		if err != nil || response.StatusCode >= http.StatusBadRequest {
			m.httpFailures.Add(1)
		}
		return response, err
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type countingReader struct {
	reader io.Reader
	count  *atomic.Uint64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.count.Add(uint64(n))
	return n, err
}

func (r *countingReader) ObjectSize() (int64, error) {
	return thanosobjstore.TryToGetSize(r.reader)
}

type countingReadCloser struct {
	io.ReadCloser
	count *atomic.Uint64
}

func (r *countingReadCloser) Read(buffer []byte) (int, error) {
	n, err := r.ReadCloser.Read(buffer)
	r.count.Add(uint64(n))
	return n, err
}
