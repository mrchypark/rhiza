package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

type readerAtOnly struct{ reader *bytes.Reader }

func (r readerAtOnly) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r readerAtOnly) ReadAt(p []byte, off int64) (int, error) {
	return r.reader.ReadAt(p, off)
}
func (r readerAtOnly) Size() int64 { return r.reader.Size() }

type seekerOnly struct{ reader *bytes.Reader }

func (r seekerOnly) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r seekerOnly) Seek(off int64, whence int) (int64, error) {
	return r.reader.Seek(off, whence)
}

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

func TestCountingReaderPreservesOnlyOriginalCapabilities(t *testing.T) {
	tests := []struct {
		name      string
		reader    io.Reader
		readerAt  bool
		seeker    bool
		workBytes uint64
	}{
		{name: "reader_at_and_seeker", reader: bytes.NewReader([]byte("abcdef")), readerAt: true, seeker: true, workBytes: 5},
		{name: "reader_at_only", reader: readerAtOnly{bytes.NewReader([]byte("abcdef"))}, readerAt: true, workBytes: 5},
		{name: "seeker_only", reader: seekerOnly{bytes.NewReader([]byte("abcdef"))}, seeker: true, workBytes: 2},
		{name: "neither", reader: io.LimitReader(bytes.NewReader([]byte("abcdef")), 6), workBytes: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var total atomic.Uint64
			counted := &countingReader{reader: test.reader, count: &total}
			wrapped := countingReaderWithCapabilities(counted, test.reader)
			_, hasReaderAt := wrapped.(io.ReaderAt)
			_, hasSeeker := wrapped.(io.Seeker)
			if hasReaderAt != test.readerAt || hasSeeker != test.seeker {
				t.Fatalf("capabilities ReaderAt=%t Seeker=%t, want ReaderAt=%t Seeker=%t", hasReaderAt, hasSeeker, test.readerAt, test.seeker)
			}
			if test.readerAt {
				buffer := make([]byte, 3)
				if n, err := wrapped.(io.ReaderAt).ReadAt(buffer, 0); err != nil || n != len(buffer) {
					t.Fatalf("ReadAt = (%d, %v), want (%d, nil)", n, err, len(buffer))
				}
			}
			if test.seeker {
				if _, err := wrapped.(io.Seeker).Seek(0, io.SeekStart); err != nil {
					t.Fatalf("Seek: %v", err)
				}
			}
			buffer := make([]byte, 2)
			if n, err := wrapped.Read(buffer); err != nil || n != len(buffer) {
				t.Fatalf("Read = (%d, %v), want (%d, nil)", n, err, len(buffer))
			}
			if got := total.Load(); got != test.workBytes {
				t.Fatalf("counted reader work bytes = %d, want %d", got, test.workBytes)
			}
		})
	}
}

func TestMeteredUploadReaderPreservesS3RetryBody(t *testing.T) {
	payload := []byte("replay-the-entire-upload-body")
	for _, test := range []struct {
		name    string
		metered bool
	}{
		{name: "direct_seekable_reader"},
		{name: "metered_reader", metered: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var bodies [][]byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				contentEncoding := r.Header.Get("Content-Encoding")
				streamingSignature := strings.Contains(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, "read failed", http.StatusInternalServerError)
					return
				}
				if strings.Contains(contentEncoding, "aws-chunked") || streamingSignature {
					body, err = io.ReadAll(httputil.NewChunkedReader(bytes.NewReader(body)))
					if err != nil {
						http.Error(w, "decode failed", http.StatusInternalServerError)
						return
					}
				}
				mu.Lock()
				bodies = append(bodies, body)
				attempt := len(bodies)
				mu.Unlock()
				if attempt == 1 {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `<Error><Code>SlowDown</Code><Message>retry</Message><RequestId>local</RequestId></Error>`)
					return
				}
				w.Header().Set("ETag", `"local-etag"`)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			bucket, err := NewBucket(Config{
				Provider: ProviderS3, Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "retry-test", Region: "us-east-1",
				Insecure: true, AccessKey: "test-access-key", SecretKey: "test-secret-key", MaxRetries: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if test.metered {
				err = bucket.Upload(ctx, "body", bytes.NewReader(payload))
			} else {
				err = bucket.Bucket.Upload(ctx, "body", bytes.NewReader(payload))
			}
			mu.Lock()
			gotBodies := append([][]byte(nil), bodies...)
			mu.Unlock()
			if err != nil {
				t.Fatalf("S3 upload: %v (attempt bodies=%d)", err, len(gotBodies))
			}
			if len(gotBodies) != 2 {
				t.Fatalf("S3 retry attempts = %d, want 2", len(gotBodies))
			}
			for i, got := range gotBodies {
				if !bytes.Equal(got, payload) {
					t.Errorf("attempt %d decoded body differs from original: got_len=%d want_len=%d", i+1, len(got), len(payload))
				}
			}
			if test.metered {
				stats := bucket.Stats()
				if stats.BytesUploaded < 2*uint64(len(payload)) || stats.BytesPublished != uint64(len(payload)) {
					t.Fatalf("upload byte semantics: attempted_reader_bytes=%d published_object_bytes=%d want published=%d", stats.BytesUploaded, stats.BytesPublished, len(payload))
				}
				if stats.HTTPRequests != 2 || stats.HTTPFailures != 1 {
					t.Fatalf("HTTP attempt accounting: requests=%d failures=%d, want 2 and 1", stats.HTTPRequests, stats.HTTPFailures)
				}
				t.Logf("retry byte evidence: reader_work_bytes=%d acknowledged_object_bytes=%d transport_body_bytes=%d", stats.BytesUploaded, stats.BytesPublished, stats.HTTPRequestBodyBytes)
			}
		})
	}
}

func TestMeteredS3ConditionalUploadStatusAttemptsAndPublishedBytes(t *testing.T) {
	payload := []byte("conditional-upload-payload")
	for _, test := range []struct {
		name   string
		status int
		code   string
		option thanosobjstore.ObjectUploadOption
	}{
		{name: "conditional_conflict", status: http.StatusConflict, code: "ConditionalRequestConflict", option: thanosobjstore.WithIfNotExists()},
		{name: "precondition_failed", status: http.StatusPreconditionFailed, code: "PreconditionFailed", option: thanosobjstore.WithIfMatch(&thanosobjstore.ObjectVersion{Type: thanosobjstore.ETag, Value: "etag"})},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requestCount atomic.Uint64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, `<Error><Code>`+test.code+`</Code><Message>condition rejected</Message><RequestId>local</RequestId></Error>`)
			}))
			defer server.Close()

			bucket, err := NewBucket(Config{
				Provider: ProviderS3, Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "condition-test", Region: "us-east-1",
				Insecure: true, AccessKey: "test-access-key", SecretKey: "test-secret-key", MaxRetries: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = bucket.Upload(ctx, "conditional", bytes.NewReader(payload), test.option)
			if err == nil {
				t.Fatal("conditional upload succeeded after S3 rejection")
			}
			actualRequests := requestCount.Load()
			stats := bucket.Stats()
			if actualRequests != 1 || stats.HTTPRequests != 1 {
				t.Fatalf("conditional response attempts: client=%d server=%d, want exactly 1 each", stats.HTTPRequests, actualRequests)
			}
			if stats.BytesPublished != 0 {
				t.Fatalf("rejected conditional upload published %d bytes", stats.BytesPublished)
			}
			if test.status == http.StatusPreconditionFailed && !bucket.Bucket.IsConditionNotMetErr(err) {
				t.Fatalf("S3 %s error was not recognized as a condition conflict: %v", test.code, err)
			}
			t.Logf("conditional S3 response: status=%d code=%s client_http_attempts=%d condition_error=%t published_bytes=%d", test.status, test.code, actualRequests, bucket.Bucket.IsConditionNotMetErr(err), stats.BytesPublished)
		})
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
