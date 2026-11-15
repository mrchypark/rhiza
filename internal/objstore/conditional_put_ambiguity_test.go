package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	kitlog "github.com/go-kit/log"
	thanosobjstore "github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/s3"
)

type observedConditionalResponse struct {
	next               http.RoundTripper
	responseSeen       chan struct{}
	responseSeenOnce   sync.Once
	putResponses       atomic.Int64
	put412             atomic.Int64
	putTransportErrors atomic.Int64
}

type signalOnReadBody struct {
	io.ReadCloser
	once    *sync.Once
	started chan struct{}
}

func (r *signalOnReadBody) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	return r.ReadCloser.Read(p)
}

func TestS3ConditionalExpectNoContinueActualSDK(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	var received atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do not read the body (which would send 100) until the client starts
		// sending it due to its ExpectContinueTimeout.
		select {
		case <-started:
		case <-r.Context().Done():
			return
		}
		n, err := io.Copy(io.Discard, r.Body)
		received.Store(n)
		if err != nil {
			return
		}
		w.Header().Set("ETag", `"fixture-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ExpectContinueTimeout = 50 * time.Millisecond
	defer transport.CloseIdleConnections()
	signal := providerTransportRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Body = &signalOnReadBody{ReadCloser: r.Body, once: &once, started: started}
		return transport.RoundTrip(r)
	})
	metrics := &bucketMetrics{}
	adapter, err := s3.NewBucketWithConfig(kitlog.NewNopLogger(), s3.Config{
		Bucket: "rhiza-test-bucket", Endpoint: strings.TrimPrefix(server.URL, "https://"), Region: "us-east-1",
		AccessKey: "test-access", SecretKey: "test-secret", BucketLookupType: s3.PathLookup, PartSize: 64 << 20,
		MaxRetries: 1, SendContentMd5: true,
		HTTPConfig: s3.HTTPConfig{Transport: metrics.transport(conditionalExpectContinueRoundTripper{next: signal})},
	}, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	bucket := newMeteredBucket(adapter, metrics)
	defer bucket.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte{0x5a}, 7<<20)
	if err := bucket.Upload(ctx, "fresh", bytes.NewReader(payload), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if got := received.Load(); got != int64(len(payload)) {
		t.Fatalf("received=%d", got)
	}
}

func (t *observedConditionalResponse) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.next.RoundTrip(r)
	if r.Method == http.MethodPut {
		if response != nil {
			t.putResponses.Add(1)
			if response.StatusCode == http.StatusPreconditionFailed {
				t.put412.Add(1)
				if t.responseSeen != nil {
					t.responseSeenOnce.Do(func() { close(t.responseSeen) })
				}
			}
		}
		if err != nil {
			t.putTransportErrors.Add(1)
		}
	}
	return response, err
}

func TestS3ConditionalPutOutcomeAmbiguityActualSDK(t *testing.T) {
	for _, mode := range []string{"early-412", "early-412-close", "continued-then-412", "truncated-412", "partial-body-reset", "committed-response-lost", "committed-loss-then-412"} {
		t.Run(mode, func(t *testing.T) {
			old := bytes.Repeat([]byte("old-value-"), 200)
			attempted := bytes.Repeat([]byte{0x5a}, 7<<20)
			objects := map[string][]byte{"existing": bytes.Clone(old)}
			var mu sync.Mutex
			var putCount, bodyRead atomic.Int64
			var attemptedBodyMatches atomic.Bool
			responseSeen := make(chan struct{})
			truncatedServerDone := make(chan error, 1)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				key := strings.TrimPrefix(r.URL.Path, "/rhiza-test-bucket/")
				if r.Method == http.MethodGet {
					mu.Lock()
					data, found := objects[key]
					data = bytes.Clone(data)
					mu.Unlock()
					if !found {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					w.Header().Set("ETag", `"fixture-etag"`)
					w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
					_, _ = w.Write(data)
					return
				}
				if r.Method != http.MethodPut || r.Header.Get("If-None-Match") != "*" {
					http.Error(w, "unexpected request", 400)
					return
				}
				attempt := putCount.Add(1)
				if mode == "continued-then-412" {
					// Reading an Expect request emits a real 100 Continue. Consume
					// the body fully before the final 412 so this is not a reset
					// or partial-response test.
					body, err := io.ReadAll(r.Body)
					bodyRead.Store(int64(len(body)))
					attemptedBodyMatches.Store(bytes.Equal(body, attempted))
					if err != nil {
						return
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusPreconditionFailed)
					_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>`)
					return
				}
				if mode == "truncated-412" {
					conn, _, hijackErr := w.(http.Hijacker).Hijack()
					if hijackErr != nil {
						truncatedServerDone <- hijackErr
						return
					}
					defer conn.Close()
					if _, err := io.WriteString(conn, "HTTP/1.1 412 Precondition Failed\r\nContent-Type: application/xml\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n<Error>"); err != nil {
						truncatedServerDone <- err
						return
					}
					// The server must not close the TLS connection before the actual
					// client transport has accepted the 412 headers. Otherwise an
					// OS-dependent TCP reset can erase the response under test.
					select {
					case <-responseSeen:
						truncatedServerDone <- nil
					case <-time.After(5 * time.Second):
						truncatedServerDone <- errors.New("client did not receive truncated response headers")
					}
					return
				}
				if mode == "early-412" || mode == "early-412-close" || (mode == "committed-loss-then-412" && attempt > 1) {
					w.Header().Set("Content-Type", "application/xml")
					if mode == "early-412-close" {
						w.Header().Set("Connection", "close")
					}
					w.WriteHeader(http.StatusPreconditionFailed)
					_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>`)
					return
				}
				if mode == "partial-body-reset" {
					buffer := make([]byte, 4096)
					n, err := io.ReadFull(r.Body, buffer)
					bodyRead.Add(int64(n))
					if err != nil {
						return
					}
				} else {
					body, err := io.ReadAll(r.Body)
					bodyRead.Add(int64(len(body)))
					if err != nil {
						return
					}
					mu.Lock()
					objects[key] = bytes.Clone(body)
					mu.Unlock()
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					return
				}
				if tcp, ok := conn.(*net.TCPConn); ok {
					_ = tcp.SetLinger(0)
				}
				_ = conn.Close()
			}))
			defer server.Close()
			transport := server.Client().Transport.(*http.Transport).Clone()
			transport.Proxy = nil
			defer transport.CloseIdleConnections()
			var next http.RoundTripper = transport
			trace := newConditionalPutHTTPTrace()
			if strings.HasPrefix(mode, "early-412") || mode == "continued-then-412" || mode == "truncated-412" || mode == "committed-loss-then-412" {
				next = conditionalExpectContinueRoundTripper{next: transport, trace: trace}
			}
			observed := &observedConditionalResponse{next: next}
			if mode == "truncated-412" {
				observed.responseSeen = responseSeen
			}
			metrics := &bucketMetrics{}
			maxRetries := 1
			if mode == "committed-loss-then-412" {
				maxRetries = 0
			}
			adapter, err := s3.NewBucketWithConfig(kitlog.NewNopLogger(), s3.Config{
				Bucket: "rhiza-test-bucket", Endpoint: strings.TrimPrefix(server.URL, "https://"), Region: "us-east-1", Insecure: false,
				AccessKey: "test-access", SecretKey: "test-secret", BucketLookupType: s3.PathLookup, PartSize: 64 << 20,
				MaxRetries: maxRetries, SendContentMd5: true, HTTPConfig: s3.HTTPConfig{Transport: metrics.transport(observed)},
			}, "test", nil)
			if err != nil {
				t.Fatal(err)
			}
			bucket := newMeteredBucket(adapter, metrics)
			defer bucket.Close()
			key := "existing"
			if mode == "committed-response-lost" || mode == "committed-loss-then-412" {
				key = "fresh"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = bucket.Upload(ctx, key, bytes.NewReader(attempted), thanosobjstore.WithIfNotExists())
			if mode == "truncated-412" {
				select {
				case serverErr := <-truncatedServerDone:
					if serverErr != nil {
						t.Fatalf("truncated 412 server: %v", serverErr)
					}
				case <-ctx.Done():
					t.Fatalf("truncated 412 server did not finish: %v", ctx.Err())
				}
			}
			condition := bucket.IsConditionNotMetErr(err)
			traceSnapshot := trace.closeAndSnapshot()
			if mode != "committed-loss-then-412" && putCount.Load() != 1 {
				t.Fatalf("PUT attempts=%d", putCount.Load())
			}
			if mode == "committed-loss-then-412" {
				if err == nil || !condition || putCount.Load() != 2 || observed.putResponses.Load() != 1 || observed.put412.Load() != 1 || observed.putTransportErrors.Load() != 1 || bodyRead.Load() != int64(len(attempted)) {
					t.Fatalf("lost success then 412: attempts=%d response=%d typed=%v transport=%d body=%d error=%v", putCount.Load(), observed.putResponses.Load(), condition, observed.putTransportErrors.Load(), bodyRead.Load(), err)
				}
			} else if strings.HasPrefix(mode, "early-412") {
				if err == nil || !condition || observed.putResponses.Load() != 1 || observed.put412.Load() != 1 || observed.putTransportErrors.Load() != 0 || bodyRead.Load() != 0 {
					t.Fatalf("early 412 condition=%v response=%d status412=%d transport=%d body=%d err=%v", condition, observed.putResponses.Load(), observed.put412.Load(), observed.putTransportErrors.Load(), bodyRead.Load(), err)
				}
			} else if mode == "continued-then-412" {
				if err == nil || !condition || observed.putResponses.Load() != 1 || observed.put412.Load() != 1 || observed.putTransportErrors.Load() != 0 || bodyRead.Load() != int64(len(attempted)) || !attemptedBodyMatches.Load() || !hasConditionalPutHTTPTraceEvent(traceSnapshot, 1, "got_100_continue", http.StatusContinue) {
					t.Fatalf("100 then 412 condition=%v response=%d status412=%d transport=%d body=%d body_matches=%v trace=%+v err=%v", condition, observed.putResponses.Load(), observed.put412.Load(), observed.putTransportErrors.Load(), bodyRead.Load(), attemptedBodyMatches.Load(), traceSnapshot, err)
				}
			} else if mode == "truncated-412" {
				if err == nil || condition || observed.putResponses.Load() != 1 || observed.put412.Load() != 1 || observed.putTransportErrors.Load() != 0 {
					t.Fatalf("truncated 412 condition=%v response=%d status412=%d transport=%d err=%v", condition, observed.putResponses.Load(), observed.put412.Load(), observed.putTransportErrors.Load(), err)
				}
			} else {
				if err == nil || condition || observed.putResponses.Load() != 0 || observed.putTransportErrors.Load() != 1 {
					t.Fatalf("ambiguous condition=%v response=%d transport=%d err=%v", condition, observed.putResponses.Load(), observed.putTransportErrors.Load(), err)
				}
				if mode == "partial-body-reset" && (bodyRead.Load() != 4096 || bodyRead.Load() >= int64(len(attempted))) {
					t.Fatalf("partial body bytes=%d", bodyRead.Load())
				}
				if mode == "committed-response-lost" && bodyRead.Load() != int64(len(attempted)) {
					t.Fatalf("committed body bytes=%d", bodyRead.Load())
				}
			}
			t.Logf("mode=%s error_type=%T condition=%v client_response=%d status412=%d transport_error=%d epipe=%v reset=%v eof=%v body_read=%d", mode, err, condition, observed.putResponses.Load(), observed.put412.Load(), observed.putTransportErrors.Load(), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNRESET), errors.Is(err, io.EOF), bodyRead.Load())
			for name, expected := range map[string][]byte{"existing": old, key: func() []byte {
				if key == "fresh" {
					return attempted
				}
				return old
			}()} {
				reader, getErr := bucket.Get(ctx, name)
				if getErr != nil {
					t.Fatal(getErr)
				}
				got, readErr := io.ReadAll(reader)
				closeErr := reader.Close()
				if readErr != nil || closeErr != nil || !bytes.Equal(got, expected) {
					t.Fatalf("SDK readback %s len=%d want=%d read=%v close=%v", name, len(got), len(expected), readErr, closeErr)
				}
			}
			if strings.HasPrefix(mode, "early-412") || mode == "continued-then-412" {
				if err := bucket.DrainMutations(ctx); err != nil {
					t.Fatalf("observed rejection drain=%v", err)
				}
			} else {
				if err := bucket.DrainMutations(ctx); !errors.Is(err, ErrMutationOutcomeUnknown) {
					t.Fatalf("ambiguous mutation drain=%v", err)
				}
			}
		})
	}
}

func TestS3ConditionalExpectCancellationBeforeContinueActualSDK(t *testing.T) {
	headerReceived := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.Header.Get("Expect") != "100-continue" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		close(headerReceived)
		// No response and no body read: the server never emits 100 Continue.
		select {
		case <-r.Context().Done():
		case <-releaseHandler:
		}
	}))
	defer server.Close()
	defer close(releaseHandler)
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ExpectContinueTimeout = 5 * time.Second
	defer transport.CloseIdleConnections()
	metrics := &bucketMetrics{}
	adapter, err := s3.NewBucketWithConfig(kitlog.NewNopLogger(), s3.Config{
		Bucket: "rhiza-test-bucket", Endpoint: strings.TrimPrefix(server.URL, "https://"), Region: "us-east-1",
		AccessKey: "test-access", SecretKey: "test-secret", BucketLookupType: s3.PathLookup, PartSize: 64 << 20,
		MaxRetries: 1, SendContentMd5: true,
		HTTPConfig: s3.HTTPConfig{Transport: metrics.transport(conditionalExpectContinueRoundTripper{next: transport})},
	}, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	bucket := newMeteredBucket(adapter, metrics)
	defer bucket.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- bucket.Upload(ctx, "existing", bytes.NewReader(bytes.Repeat([]byte{0x5a}, 7<<20)), thanosobjstore.WithIfNotExists())
	}()
	select {
	case <-headerReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("SDK did not send headers")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not stop after cancellation")
	}
}
