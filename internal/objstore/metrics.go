package objstore

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	thanosobjstore "github.com/thanos-io/objstore"
)

// Stats exposes object-store operations at both the logical bucket boundary
// and the billable S3 HTTP boundary.
type Stats struct {
	Uploads            uint64 `json:"uploads"`
	Gets               uint64 `json:"gets"`
	Lists              uint64 `json:"lists"`
	Heads              uint64 `json:"heads"`
	Deletes            uint64 `json:"deletes"`
	Failures           uint64 `json:"failures"`
	BytesUploaded      uint64 `json:"bytes_uploaded"`
	BytesDownloaded    uint64 `json:"bytes_downloaded"`
	S3HTTPRequests     uint64 `json:"s3_http_requests"`
	S3HTTPFailures     uint64 `json:"s3_http_failures"`
	ConditionConflicts uint64 `json:"condition_conflicts"`
	DedupHits          uint64 `json:"dedup_hits"`
	SDKRetries         uint64 `json:"sdk_retries"`
	TransportFailures  uint64 `json:"transport_failures"`
	Unexpected4xx      uint64 `json:"http_4xx_unexpected"`
	HTTP5xx            uint64 `json:"http_5xx"`
}

type bucketMetrics struct {
	uploads, gets, lists, heads, deletes atomic.Uint64
	failures                             atomic.Uint64
	bytesUploaded, bytesDownloaded       atomic.Uint64
	httpRequests, httpFailures           atomic.Uint64
	conditionConflicts, dedupHits        atomic.Uint64
	sdkRetries, transportFailures        atomic.Uint64
	unexpected4xx, http5xx               atomic.Uint64
}

type expectedNotFoundKey struct{}
type expectedConditionKey struct{}

// WithExpectedNotFound marks a single object-store operation whose missing
// object result is part of its normal control flow. It only affects S3 HTTP
// accounting; callers still receive and handle the provider error normally.
func WithExpectedNotFound(ctx context.Context) context.Context {
	return context.WithValue(ctx, expectedNotFoundKey{}, struct{}{})
}

func expectsNotFound(ctx context.Context) bool {
	_, ok := ctx.Value(expectedNotFoundKey{}).(struct{})
	return ok
}

func withExpectedCondition(ctx context.Context, opts ...thanosobjstore.ObjectUploadOption) context.Context {
	params := thanosobjstore.ApplyObjectUploadOptions(opts...)
	if !params.IfNotExists && params.Condition == nil {
		return ctx
	}
	return context.WithValue(ctx, expectedConditionKey{}, struct{}{})
}

func expectsCondition(ctx context.Context) bool {
	_, ok := ctx.Value(expectedConditionKey{}).(struct{})
	return ok
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
		ConditionConflicts: b.metrics.conditionConflicts.Load(), DedupHits: b.metrics.dedupHits.Load(),
		SDKRetries: b.metrics.sdkRetries.Load(), TransportFailures: b.metrics.transportFailures.Load(),
		Unexpected4xx: b.metrics.unexpected4xx.Load(), HTTP5xx: b.metrics.http5xx.Load(),
	}
}

func (b *MeteredBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	b.metrics.uploads.Add(1)
	counted := &countingReader{reader: reader, count: &b.metrics.bytesUploaded}
	ctx = withExpectedCondition(ctx, opts...)
	err := b.Bucket.Upload(ctx, name, counted, opts...)
	if err != nil && b.Bucket.IsConditionNotMetErr(err) {
		if strings.Contains(name, "/blocks/") || strings.Contains(name, "/extents/") || strings.Contains(name, "/roots/") {
			b.metrics.dedupHits.Add(1)
		} else {
			b.metrics.conditionConflicts.Add(1)
		}
	}
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
		if value := request.Header.Get("amz-sdk-request"); strings.Contains(value, "attempt=") && !strings.Contains(value, "attempt=1") {
			m.sdkRetries.Add(1)
		}
		response, err := next.RoundTrip(request)
		if err != nil {
			m.transportFailures.Add(1)
			m.httpFailures.Add(1)
			return response, err
		}
		switch {
		case request.Method == http.MethodPut && expectsCondition(request.Context()) && (request.Header.Get("If-Match") != "" || request.Header.Get("If-None-Match") != "") && (response.StatusCode == http.StatusConflict || response.StatusCode == http.StatusPreconditionFailed):
			// Expected CAS and content-addressed dedup outcomes are classified by
			// the logical Upload path, not as HTTP failures.
		case response.StatusCode == http.StatusNotFound && expectsNotFound(request.Context()):
			// Missing-object probes are a normal part of first publication. The
			// logical operation still records the not-found result.
		case response.StatusCode >= 400 && response.StatusCode < 500:
			m.unexpected4xx.Add(1)
			m.httpFailures.Add(1)
		case response.StatusCode >= 500:
			m.http5xx.Add(1)
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
	size, err := thanosobjstore.TryToGetSize(r.reader)
	if err == nil {
		return size, nil
	}
	if reader, ok := r.reader.(interface{ Size() int64 }); ok {
		return reader.Size(), nil
	}
	return 0, err
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
