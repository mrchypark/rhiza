package objstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	thanosobjstore "github.com/thanos-io/objstore"
)

// Stats exposes object-store operations at both the logical bucket boundary
// and the provider-neutral HTTP boundary. S3HTTP* remains for compatibility.
// BytesUploaded counts source-reader bytes consumed by upload attempts,
// including SDK re-reads; it is not wire bytes. BytesPublished counts one
// known object size per successful Upload, falling back to source-reader bytes
// consumed when the size cannot be determined. HTTPRequestBodyBytes is the
// separate transport-body byte measure.
type Stats struct {
	Uploads         uint64 `json:"uploads"`
	Gets            uint64 `json:"gets"`
	Lists           uint64 `json:"lists"`
	Heads           uint64 `json:"heads"`
	Deletes         uint64 `json:"deletes"`
	Failures        uint64 `json:"failures"`
	BytesUploaded   uint64 `json:"bytes_uploaded"`
	BytesPublished  uint64 `json:"bytes_published"`
	BytesDownloaded uint64 `json:"bytes_downloaded"`
	HTTPRequests    uint64 `json:"http_requests"`
	// HTTP body bytes are read by the transport wrappers; they exclude protocol overhead.
	HTTPRequestBodyBytes  uint64 `json:"http_request_body_bytes"`
	HTTPResponseBodyBytes uint64 `json:"http_response_body_bytes"`
	HTTPFailures          uint64 `json:"http_failures"`
	S3HTTPRequests        uint64 `json:"s3_http_requests"`
	S3HTTPFailures        uint64 `json:"s3_http_failures"`
	HTTPGetRequests       uint64 `json:"http_get_requests"`
	HTTPPutRequests       uint64 `json:"http_put_requests"`
	HTTPHeadRequests      uint64 `json:"http_head_requests"`
	HTTPDeleteRequests    uint64 `json:"http_delete_requests"`
	HTTPOtherRequests     uint64 `json:"http_other_requests"`
	ConditionConflicts    uint64 `json:"condition_conflicts"`
	DedupHits             uint64 `json:"dedup_hits"`
	SDKRetries            uint64 `json:"sdk_retries"`
	// RetryMetadataRequests counts recognized attempt metadata; unknown counts missing or unusable metadata.
	RetryMetadataRequests        uint64 `json:"retry_metadata_requests"`
	RetryMetadataUnknownRequests uint64 `json:"retry_metadata_unknown_requests"`
	TransportFailures            uint64 `json:"transport_failures"`
	Unexpected4xx                uint64 `json:"http_4xx_unexpected"`
	HTTP5xx                      uint64 `json:"http_5xx"`
	ReplayGroupingEnabled        bool   `json:"replay_grouping_enabled"`
	// These counters describe same-identity HTTP RoundTrip repeats, not proven
	// SDK retries. Unknown requests preserve attempts that could not be grouped.
	ObservedRequestIdentities     uint64 `json:"observed_request_identities"`
	ObservedRequestRepeats        uint64 `json:"observed_request_repeats"`
	RequestGroupingUnknown        uint64 `json:"request_grouping_unknown"`
	ReplayTrackerCapacityMisses   uint64 `json:"replay_tracker_capacity_misses"`
	ReplayIdentityCapacityMisses  uint64 `json:"replay_identity_capacity_misses"`
	ReplayIncompleteOperations    uint64 `json:"replay_incomplete_operations"`
	ReplayTrackedOperationsActive uint64 `json:"replay_tracked_operations_active"`
	ReplayOpenReaders             uint64 `json:"replay_open_readers"`
}

type bucketMetrics struct {
	uploads, gets, lists, heads, deletes                            atomic.Uint64
	failures                                                        atomic.Uint64
	bytesUploaded, bytesDownloaded                                  atomic.Uint64
	bytesPublished                                                  atomic.Uint64
	httpRequests, httpRequestBodyBytes, httpResponseBodyBytes       atomic.Uint64
	httpFailures                                                    atomic.Uint64
	httpGetRequests, httpPutRequests                                atomic.Uint64
	httpHeadRequests, httpDeleteRequests                            atomic.Uint64
	httpOtherRequests                                               atomic.Uint64
	conditionConflicts, dedupHits                                   atomic.Uint64
	sdkRetries, retryMetadataRequests, retryMetadataUnknownRequests atomic.Uint64
	transportFailures                                               atomic.Uint64
	unexpected4xx, http5xx                                          atomic.Uint64
	observedRequestIdentities, observedRequestRepeats               atomic.Uint64
	requestGroupingUnknown, replayTrackerCapacityMisses             atomic.Uint64
	replayIdentityCapacityMisses, replayIncompleteOperations        atomic.Uint64
	replayTrackedOperationsActive, replayOpenReaders                atomic.Uint64
	replayGroupingEnabled                                           bool
}

// Replay grouping is deliberately bounded: at most 128 live logical
// operations, each holding at most 64 request digests (8,192 digests total).
// Multipart uploads and normal paginated reads fit comfortably; excess is
// reported as unknown rather than silently treated as no repeat.
const (
	maxReplayTrackedOperations = 128
	maxReplayIdentitiesPerOp   = 64
)

type replayOperationKey struct{}

type replayOperation struct {
	metrics          *bucketMetrics
	mu               sync.Mutex
	seen             map[[sha256.Size]byte]struct{}
	active           uint32
	done             bool
	released         bool
	incomplete       bool
	identityOverflow bool
}

func (m *bucketMetrics) beginReplayOperation(ctx context.Context) (context.Context, *replayOperation) {
	if !m.replayGroupingEnabled {
		return ctx, nil
	}
	for {
		active := m.replayTrackedOperationsActive.Load()
		if active >= maxReplayTrackedOperations {
			m.replayTrackerCapacityMisses.Add(1)
			m.replayIncompleteOperations.Add(1)
			return ctx, nil
		}
		if m.replayTrackedOperationsActive.CompareAndSwap(active, active+1) {
			break
		}
	}
	operation := &replayOperation{metrics: m}
	return context.WithValue(ctx, replayOperationKey{}, operation), operation
}

func replayOperationFromContext(ctx context.Context) *replayOperation {
	operation, _ := ctx.Value(replayOperationKey{}).(*replayOperation)
	return operation
}

func (operation *replayOperation) markIncompleteLocked() {
	if !operation.incomplete {
		operation.incomplete = true
		operation.metrics.replayIncompleteOperations.Add(1)
	}
}

// beginRequest accounts one outer RoundTrip. A repeat means only that the
// same private request digest occurred again inside this bucket operation.
func (operation *replayOperation) beginRequest(identity [sha256.Size]byte) bool {
	operation.mu.Lock()
	defer operation.mu.Unlock()
	if operation.done || operation.released {
		operation.markIncompleteLocked()
		operation.metrics.requestGroupingUnknown.Add(1)
		return false
	}
	operation.active++
	if _, ok := operation.seen[identity]; ok {
		operation.metrics.observedRequestRepeats.Add(1)
		return true
	}
	if len(operation.seen) >= maxReplayIdentitiesPerOp {
		operation.metrics.requestGroupingUnknown.Add(1)
		if !operation.identityOverflow {
			operation.identityOverflow = true
			operation.metrics.replayIdentityCapacityMisses.Add(1)
			operation.markIncompleteLocked()
		}
		return true
	}
	if operation.seen == nil {
		operation.seen = make(map[[sha256.Size]byte]struct{})
	}
	operation.seen[identity] = struct{}{}
	operation.metrics.observedRequestIdentities.Add(1)
	return true
}

func (operation *replayOperation) endRequest() {
	operation.mu.Lock()
	if operation.active > 0 {
		operation.active--
	}
	release := operation.done && operation.active == 0 && !operation.released
	if release {
		operation.released = true
	}
	operation.mu.Unlock()
	if release {
		operation.metrics.replayTrackedOperationsActive.Add(^uint64(0))
	}
}

func (operation *replayOperation) finish() {
	operation.mu.Lock()
	operation.done = true
	if operation.active > 0 {
		operation.markIncompleteLocked()
	}
	release := operation.active == 0 && !operation.released
	if release {
		operation.released = true
	}
	operation.mu.Unlock()
	if release {
		operation.metrics.replayTrackedOperationsActive.Add(^uint64(0))
	}
}

func replayIdentity(request *http.Request) [sha256.Size]byte {
	var encodedStorage [512]byte
	encoded := encodedStorage[:0]
	encoded = appendReplayField(encoded, "rhiza-request-identity-v1")
	if request.URL != nil {
		encoded = appendReplayField(encoded, request.URL.Scheme)
		encoded = appendReplayField(encoded, request.URL.Host)
		encoded = appendReplayField(encoded, request.URL.EscapedPath())
		encoded = appendReplayField(encoded, request.URL.RawQuery)
	} else {
		encoded = appendReplayField(encoded, "")
		encoded = appendReplayField(encoded, "")
		encoded = appendReplayField(encoded, "")
		encoded = appendReplayField(encoded, "")
	}
	encoded = appendReplayField(encoded, request.Method)
	for _, name := range []string{"Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		encoded = appendReplayField(encoded, request.Header.Get(name))
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(request.ContentLength))
	encoded = append(encoded, length[:]...)
	return sha256.Sum256(encoded)
}

func appendReplayField(encoded []byte, value string) []byte {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	encoded = append(encoded, length[:]...)
	return append(encoded, value...)
}

type expectedNotFoundKey struct{}
type expectedConditionKey struct{}

// WithExpectedNotFound marks a single object-store operation whose missing
// object result is part of its normal control flow. It only affects HTTP
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
	httpRequests, httpFailures := b.metrics.httpRequests.Load(), b.metrics.httpFailures.Load()
	return Stats{
		Uploads: b.metrics.uploads.Load(), Gets: b.metrics.gets.Load(), Lists: b.metrics.lists.Load(),
		Heads: b.metrics.heads.Load(), Deletes: b.metrics.deletes.Load(), Failures: b.metrics.failures.Load(),
		BytesUploaded: b.metrics.bytesUploaded.Load(), BytesDownloaded: b.metrics.bytesDownloaded.Load(),
		BytesPublished: b.metrics.bytesPublished.Load(),
		HTTPRequests:   httpRequests, HTTPRequestBodyBytes: b.metrics.httpRequestBodyBytes.Load(),
		HTTPResponseBodyBytes: b.metrics.httpResponseBodyBytes.Load(), HTTPFailures: httpFailures,
		S3HTTPRequests: httpRequests, S3HTTPFailures: httpFailures,
		HTTPGetRequests: b.metrics.httpGetRequests.Load(), HTTPPutRequests: b.metrics.httpPutRequests.Load(),
		HTTPHeadRequests: b.metrics.httpHeadRequests.Load(), HTTPDeleteRequests: b.metrics.httpDeleteRequests.Load(),
		HTTPOtherRequests:  b.metrics.httpOtherRequests.Load(),
		ConditionConflicts: b.metrics.conditionConflicts.Load(), DedupHits: b.metrics.dedupHits.Load(),
		SDKRetries: b.metrics.sdkRetries.Load(), RetryMetadataRequests: b.metrics.retryMetadataRequests.Load(),
		RetryMetadataUnknownRequests: b.metrics.retryMetadataUnknownRequests.Load(), TransportFailures: b.metrics.transportFailures.Load(),
		Unexpected4xx: b.metrics.unexpected4xx.Load(), HTTP5xx: b.metrics.http5xx.Load(),
		ReplayGroupingEnabled:         b.metrics.replayGroupingEnabled,
		ObservedRequestIdentities:     b.metrics.observedRequestIdentities.Load(),
		ObservedRequestRepeats:        b.metrics.observedRequestRepeats.Load(),
		RequestGroupingUnknown:        b.metrics.requestGroupingUnknown.Load(),
		ReplayTrackerCapacityMisses:   b.metrics.replayTrackerCapacityMisses.Load(),
		ReplayIdentityCapacityMisses:  b.metrics.replayIdentityCapacityMisses.Load(),
		ReplayIncompleteOperations:    b.metrics.replayIncompleteOperations.Load(),
		ReplayTrackedOperationsActive: b.metrics.replayTrackedOperationsActive.Load(),
		ReplayOpenReaders:             b.metrics.replayOpenReaders.Load(),
	}
}

func (b *MeteredBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	b.metrics.uploads.Add(1)
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	defer finishReplayOperation(operation)
	counted := &countingReader{reader: reader, count: &b.metrics.bytesUploaded}
	size, sizeErr := counted.ObjectSize()
	ctx = withExpectedCondition(ctx, opts...)
	err := b.Bucket.Upload(ctx, name, countingReaderWithCapabilities(counted, reader), opts...)
	if err == nil {
		if sizeErr == nil {
			b.metrics.bytesPublished.Add(uint64(size))
		} else {
			b.metrics.bytesPublished.Add(counted.read.Load())
		}
	}
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
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	reader, err := b.Bucket.Get(ctx, name)
	b.record(err)
	if err != nil {
		finishReplayOperation(operation)
		return nil, err
	}
	return b.countingReadCloser(reader, operation), nil
}

func (b *MeteredBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.metrics.gets.Add(1)
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	reader, err := b.Bucket.GetRange(ctx, name, off, length)
	b.record(err)
	if err != nil {
		finishReplayOperation(operation)
		return nil, err
	}
	return b.countingReadCloser(reader, operation), nil
}

func finishReplayOperation(operation *replayOperation) {
	if operation != nil {
		operation.finish()
	}
}

func (b *MeteredBucket) countingReadCloser(reader io.ReadCloser, operation *replayOperation) *countingReadCloser {
	counted := &countingReadCloser{ReadCloser: reader, count: &b.metrics.bytesDownloaded, onReadError: func() {
		b.metrics.failures.Add(1)
	}}
	if operation != nil {
		b.metrics.replayOpenReaders.Add(1)
		var closeOnce sync.Once
		counted.onClose = func() {
			closeOnce.Do(func() {
				b.metrics.replayOpenReaders.Add(^uint64(0))
				operation.finish()
			})
		}
	}
	return counted
}

func (b *MeteredBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...thanosobjstore.IterOption) error {
	b.metrics.lists.Add(1)
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	defer finishReplayOperation(operation)
	err := b.Bucket.Iter(ctx, dir, f, options...)
	b.record(err)
	return err
}

func (b *MeteredBucket) IterWithAttributes(ctx context.Context, dir string, f func(thanosobjstore.IterObjectAttributes) error, options ...thanosobjstore.IterOption) error {
	b.metrics.lists.Add(1)
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	defer finishReplayOperation(operation)
	err := b.Bucket.IterWithAttributes(ctx, dir, f, options...)
	b.record(err)
	return err
}

func (b *MeteredBucket) Exists(ctx context.Context, name string) (bool, error) {
	b.metrics.heads.Add(1)
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	defer finishReplayOperation(operation)
	exists, err := b.Bucket.Exists(ctx, name)
	b.record(err)
	return exists, err
}

func (b *MeteredBucket) Attributes(ctx context.Context, name string) (thanosobjstore.ObjectAttributes, error) {
	b.metrics.heads.Add(1)
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	defer finishReplayOperation(operation)
	attributes, err := b.Bucket.Attributes(ctx, name)
	b.record(err)
	return attributes, err
}

func (b *MeteredBucket) Delete(ctx context.Context, name string) error {
	b.metrics.deletes.Add(1)
	ctx, operation := b.metrics.beginReplayOperation(ctx)
	defer finishReplayOperation(operation)
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
		if m.replayGroupingEnabled {
			if operation := replayOperationFromContext(request.Context()); operation != nil {
				if operation.beginRequest(replayIdentity(request)) {
					defer operation.endRequest()
				}
			} else {
				m.requestGroupingUnknown.Add(1)
			}
		}
		switch request.Method {
		case http.MethodGet:
			m.httpGetRequests.Add(1)
		case http.MethodPut:
			m.httpPutRequests.Add(1)
		case http.MethodHead:
			m.httpHeadRequests.Add(1)
		case http.MethodDelete:
			m.httpDeleteRequests.Add(1)
		default:
			m.httpOtherRequests.Add(1)
		}
		retry, recognized := awsSDKRetry(request.Header.Get("amz-sdk-request"))
		if recognized {
			m.retryMetadataRequests.Add(1)
		} else {
			m.retryMetadataUnknownRequests.Add(1)
		}
		if retry {
			m.sdkRetries.Add(1)
		}
		if request.Body != nil {
			request.Body = &countingReadCloser{ReadCloser: request.Body, count: &m.httpRequestBodyBytes}
		}
		response, err := next.RoundTrip(request)
		if err != nil {
			m.transportFailures.Add(1)
			m.httpFailures.Add(1)
			return response, err
		}
		statusFailure := false
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
			statusFailure = true
		case response.StatusCode >= 500:
			m.http5xx.Add(1)
			m.httpFailures.Add(1)
			statusFailure = true
		}
		if response.Body != nil {
			response.Body = &countingReadCloser{ReadCloser: response.Body, count: &m.httpResponseBodyBytes, onReadError: func() {
				m.transportFailures.Add(1)
				if !statusFailure {
					m.httpFailures.Add(1)
				}
			}}
		}
		return response, err
	})
}

// awsSDKRetry recognizes the AWS SDK's documented attempt header. Other
// providers do not expose a shared retry-attempt header at this transport layer.
func awsSDKRetry(value string) (retry, recognized bool) {
	for _, part := range strings.Split(value, ";") {
		attempt, ok := strings.CutPrefix(strings.TrimSpace(part), "attempt=")
		if !ok {
			continue
		}
		n, err := strconv.Atoi(attempt)
		if err != nil || n < 1 {
			return false, false
		}
		return n > 1, true
	}
	return false, false
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type countingReader struct {
	reader io.Reader
	count  *atomic.Uint64
	read   atomic.Uint64
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	r.count.Add(uint64(n))
	r.read.Add(uint64(n))
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

func countingReaderWithCapabilities(counted *countingReader, reader io.Reader) io.Reader {
	readerAt, hasReaderAt := reader.(io.ReaderAt)
	seeker, hasSeeker := reader.(io.Seeker)
	switch {
	case hasReaderAt && hasSeeker:
		return &countingReaderAtSeeker{countingReader: counted, readerAt: readerAt, seeker: seeker}
	case hasReaderAt:
		return &countingReaderAt{countingReader: counted, readerAt: readerAt}
	case hasSeeker:
		return &countingReaderSeeker{countingReader: counted, seeker: seeker}
	default:
		return counted
	}
}

type countingReaderAt struct {
	*countingReader
	readerAt io.ReaderAt
}

func (r *countingReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := r.readerAt.ReadAt(buffer, offset)
	r.count.Add(uint64(n))
	r.read.Add(uint64(n))
	return n, err
}

type countingReaderSeeker struct {
	*countingReader
	seeker io.Seeker
}

func (r *countingReaderSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.seeker.Seek(offset, whence)
}

type countingReaderAtSeeker struct {
	*countingReader
	readerAt io.ReaderAt
	seeker   io.Seeker
}

func (r *countingReaderAtSeeker) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := r.readerAt.ReadAt(buffer, offset)
	r.count.Add(uint64(n))
	r.read.Add(uint64(n))
	return n, err
}

func (r *countingReaderAtSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.seeker.Seek(offset, whence)
}

type countingReadCloser struct {
	io.ReadCloser
	count       *atomic.Uint64
	onReadError func()
	onClose     func()
	failed      atomic.Bool
}

func (r *countingReadCloser) Read(buffer []byte) (int, error) {
	n, err := r.ReadCloser.Read(buffer)
	if r.count != nil {
		r.count.Add(uint64(n))
	}
	if err != nil && err != io.EOF && r.failed.CompareAndSwap(false, true) && r.onReadError != nil {
		r.onReadError()
	}
	return n, err
}

func (r *countingReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.onClose != nil {
		r.onClose()
	}
	return err
}
