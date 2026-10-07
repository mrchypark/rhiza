//go:build rhiza_local_testhooks

package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/thanos-io/objstore"
)

// archiveAttributionFinalFault changes only the final result after a real SDK
// conditional Upload. It is a synthetic final fault, not a transport replay.
type archiveAttributionFinalFault struct {
	objstore.Bucket
	fault  error
	cancel context.CancelFunc
}

func (b *archiveAttributionFinalFault) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	err := b.Bucket.Upload(ctx, name, r, opts...)
	if err == nil || !b.Bucket.IsConditionNotMetErr(err) {
		return err
	}
	if b.cancel != nil {
		b.cancel()
	}
	if b.fault != nil {
		return fmt.Errorf("synthetic final fault after typed 412: %w", b.fault)
	}
	return err
}

func TestArchiveExtentAttributionActualGuard(t *testing.T) {
	for _, tc := range []struct {
		name, readback, guard, final string
		fault                        error
		cancel                       bool
		wantSuccess                  bool
		wantGETs                     int32
	}{
		{name: "exact", readback: "exact", guard: "verified", final: "typed_condition", wantSuccess: true, wantGETs: 1},
		{name: "missing", readback: "missing", guard: "unverified", final: "typed_condition", wantGETs: 1},
		{name: "corrupt", readback: "corrupt", guard: "unverified", final: "typed_condition", wantGETs: 1},
		{name: "canceled_before_guard", readback: "exact", guard: "context_done", final: "typed_condition", cancel: true},
		{name: "synthetic_final_reset", readback: "exact", guard: "not_attempted", final: "connection_reset", fault: syscall.ECONNRESET},
		{name: "synthetic_final_epipe", readback: "exact", guard: "verified", final: "broken_pipe", fault: syscall.EPIPE, wantSuccess: true, wantGETs: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var puts, gets atomic.Int32
			var key string
			var exact []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/bucket/"+key {
					http.NotFound(w, r)
					return
				}
				switch r.Method {
				case http.MethodPut:
					if r.Header.Get("If-None-Match") != "*" {
						http.Error(w, "missing condition", http.StatusBadRequest)
						return
					}
					if puts.Add(1) == 1 {
						_, _ = io.Copy(io.Discard, r.Body)
						connection, _, err := w.(http.Hijacker).Hijack()
						if err == nil {
							_ = connection.Close() // controlled lost response, platform errno unspecified
						}
						return
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusPreconditionFailed)
					_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>`)
				case http.MethodGet:
					gets.Add(1)
					if tc.readback == "missing" {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
					if tc.readback == "corrupt" {
						_, _ = io.WriteString(w, "corrupt")
					} else {
						_, _ = w.Write(exact)
					}
				default:
					http.Error(w, "unsupported", http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()
			endpoint := strings.TrimPrefix(server.URL, "http://")
			observer, unregister, err := objmetrics.RegisterS3TransportObserver(endpoint, []string{"archive"})
			if err != nil {
				t.Fatal(err)
			}
			defer unregister()
			observer.SetPhase("attribution_test")
			bucket, err := objmetrics.NewBucketWithContext(objmetrics.WithReplayObservation(context.Background()), objmetrics.Config{
				Provider: objmetrics.ProviderS3, Endpoint: endpoint, Bucket: "bucket",
				Region: "us-east-1", Insecure: true, AccessKey: "test-access", SecretKey: "test-secret", MaxRetries: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var archiveBucket objstore.Bucket = bucket
			if tc.fault != nil || tc.cancel {
				archiveBucket = &archiveAttributionFinalFault{Bucket: bucket, fault: tc.fault}
				if tc.cancel {
					archiveBucket.(*archiveAttributionFinalFault).cancel = cancel
				}
			}
			manager := NewManager(archiveBucket, "cluster", 1)
			defer manager.Close()
			if !manager.cas {
				t.Fatal("S3 bucket lacks required conditional upload support")
			}
			hash, data := testArchiveExtentBytes(t, manager)
			exact = data
			key = manager.key(extentObjectKey(hash, 1))
			err = manager.uploadExtent(ctx, hash, data, 1)
			if tc.wantSuccess && err != nil || !tc.wantSuccess && err == nil {
				t.Fatalf("uploadExtent error=%v, want success=%t", err, tc.wantSuccess)
			}
			if tc.fault != nil && !errors.Is(err, tc.fault) && !tc.wantSuccess {
				t.Fatalf("final fault error=%v, want original cause=%v", err, tc.fault)
			}
			if !tc.wantSuccess && tc.fault == nil && !archiveBucket.IsConditionNotMetErr(err) {
				t.Fatalf("unverified guard returned error=%v, want original typed condition", err)
			}
			if got := gets.Load(); got != tc.wantGETs {
				t.Fatalf("readback GETs=%d, want %d", got, tc.wantGETs)
			}
			if got := puts.Load(); got != 2 {
				t.Fatalf("server PUTs=%d, want controlled lost response then 412", got)
			}
			joined := observer.ExtentAttributionSnapshot()
			if len(joined) != 1 || joined[0].Operations != 1 || joined[0].PhysicalErrorAttempts != 1 || joined[0].Observed412 != 1 || joined[0].FinalSDKClass != tc.final || joined[0].GuardOutcome != tc.guard {
				total, counts, failures, first := observer.Snapshot()
				t.Fatalf("actual archive hook attribution=%+v transport_total=%d counts=%+v failures=%+v first=%+v, want final=%s guard=%s", joined, total, counts, failures, first, tc.final, tc.guard)
			}
		})
	}
}
