package objstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	thanosobjstore "github.com/thanos-io/objstore"
)

// TestVersityGatewayCostEvidence is an opt-in local S3 measurement, not
// provider qualification. The access log independently counts server calls.
func TestVersityGatewayCostEvidence(t *testing.T) {
	bin := os.Getenv("RHIZA_VERSITYGW_BIN")
	if bin == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to a pinned local Versity Gateway binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	server, err := versityfixture.Start(ctx, bin, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := server.Close(stopCtx); err != nil {
			t.Errorf("stop local Versity child: %v", err)
		}
		records, err := server.AccessRecords()
		if err != nil {
			t.Errorf("read Versity evidence: %v", err)
			return
		}
		for _, record := range records {
			t.Logf("versity_access_record=%s", record)
		}
		t.Logf("versity_access_log_records=%d versity_access_log_requests=%d", len(records), versityfixture.RequestCount(records))
	}()

	setupClient, setupCounts, err := server.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setupClient.ListBuckets(ctx); err != nil {
		t.Fatalf("authenticated S3 readiness: %v", err)
	}
	token := strings.TrimPrefix(server.AccessKey, "versity-")
	bucketName := "rhiza-cost-" + token[:16]
	if err := setupClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create task-owned bucket: %v", err)
	}
	setupAttemptsAfter, setupResponsesAfter := setupCounts.Snapshot()

	bucket, err := NewBucket(Config{
		Provider: ProviderS3, Endpoint: strings.TrimPrefix(server.Endpoint, "http://"),
		Bucket: bucketName, Region: "us-east-1", Insecure: true,
		AccessKey: server.AccessKey, SecretKey: server.SecretKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	workCtx, cancelWork := context.WithTimeout(ctx, 30*time.Second)
	defer cancelWork()
	key := "issue185/versity/" + token + "/object"
	payload := []byte("Versity-backed request accounting")
	if err := bucket.Upload(workCtx, key, bytes.NewReader(payload)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	conditionalErr := bucket.Upload(workCtx, key, strings.NewReader("loser"), thanosobjstore.WithIfNotExists())
	if conditionalErr == nil || !bucket.IsConditionNotMetErr(conditionalErr) {
		t.Fatalf("conditional loser error=%v, want recognized condition-not-met conflict", conditionalErr)
	}
	if exists, err := bucket.Exists(workCtx, key); err != nil || !exists {
		t.Fatalf("exists=%t err=%v", exists, err)
	}
	got, err := bucket.Get(workCtx, key)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(got)
	closeErr := got.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(body, payload) {
		t.Fatalf("readback body=%q read_err=%v close_err=%v", body, readErr, closeErr)
	}
	var listed int
	if err := bucket.Iter(workCtx, "issue185/versity/", func(string) error { listed++; return nil }, thanosobjstore.WithRecursiveIter()); err != nil {
		t.Fatal(err)
	}
	if listed != 1 {
		t.Fatalf("list yielded %d objects, want 1", listed)
	}
	if err := bucket.Delete(workCtx, key); err != nil {
		t.Fatal(err)
	}
	if exists, err := bucket.Exists(WithExpectedNotFound(workCtx), key); err != nil || exists {
		t.Fatalf("deleted object exists=%t err=%v", exists, err)
	}

	stats := bucket.Stats()
	if stats.ConditionConflicts != 1 || stats.HTTPFailures != 0 || stats.Unexpected4xx != 0 || stats.HTTP5xx != 0 || stats.TransportFailures != 0 {
		t.Fatalf("conditional loser was not classified as the sole expected conflict: %+v", stats)
	}
	stopCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
	if err := server.Close(stopCtx); err != nil {
		stop()
		t.Fatalf("stop Versity before reading flushed access log: %v", err)
	}
	stop()
	records, err := server.AccessRecords()
	if err != nil {
		t.Fatal(err)
	}
	serverCalls := versityfixture.RequestCount(records)
	if serverCalls != stats.HTTPRequests+setupResponsesAfter {
		t.Fatalf("independent server call count=%d; client workload attempts=%d + setup responses=%d (setup attempts=%d)", serverCalls, stats.HTTPRequests, setupResponsesAfter, setupAttemptsAfter)
	}
	if stats.HTTPRequests == 0 || stats.Uploads != 2 || stats.BytesPublished != uint64(len(payload)) || stats.BytesUploaded < uint64(len(payload)+len("loser")) {
		t.Fatalf("unexpected logical/transport stats: %+v", stats)
	}
	retryEvidence := fmt.Sprintf("unknown for %d requests (MinIO SDK attempt metadata absent or unrecognized); observed retries=%d", stats.RetryMetadataUnknownRequests, stats.SDKRetries)
	if stats.RetryMetadataUnknownRequests == 0 {
		retryEvidence = fmt.Sprintf("%d (recognized attempt headers)", stats.SDKRetries)
	}
	t.Logf("methodology=MeteredBucket -> Thanos S3 -> MinIO -> local Versity POSIX; provider_qualification=false; versity_version=%q; versity_binary_sha256=%s; endpoint=%s; bucket=%s", server.Version, server.BinarySHA256, server.Endpoint, bucketName)
	t.Logf("phase=setup client_attempts=%d responses=%d", setupAttemptsAfter, setupResponsesAfter)
	t.Logf("phase=workload logical_uploads=%d logical_failures=%d conflicts=%d attempted_upload_bytes=%d acknowledged_upload_bytes=%d transport_attempts=%d GET=%d PUT=%d HEAD=%d DELETE=%d OTHER=%d request_body_bytes=%d response_body_bytes=%d HTTP_failures=%d retry_metadata_known=%d retry_metadata_unknown=%d sdk_retries=%s", stats.Uploads, stats.Failures, stats.ConditionConflicts, stats.BytesUploaded, stats.BytesPublished, stats.HTTPRequests, stats.HTTPGetRequests, stats.HTTPPutRequests, stats.HTTPHeadRequests, stats.HTTPDeleteRequests, stats.HTTPOtherRequests, stats.HTTPRequestBodyBytes, stats.HTTPResponseBodyBytes, stats.HTTPFailures, stats.RetryMetadataRequests, stats.RetryMetadataUnknownRequests, retryEvidence)
	t.Logf("phase=server independent_requests=%d client_workload_attempts=%d setup_responses=%d reconciliation=exact", serverCalls, stats.HTTPRequests, setupResponsesAfter)
}

// TestVersitySDKRetryEvidence injects one transient failure before the actual
// local S3 backend and distinguishes client retries from backend requests.
func TestVersitySDKRetryEvidence(t *testing.T) {
	bin := os.Getenv("RHIZA_VERSITYGW_BIN")
	if bin == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN to a pinned local Versity Gateway binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server, err := versityfixture.Start(ctx, bin, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
		defer stop()
		if err := server.Close(stopCtx); err != nil {
			t.Errorf("stop local Versity child: %v", err)
		}
		records, err := server.AccessRecords()
		if err != nil {
			t.Errorf("read Versity access records: %v", err)
			return
		}
		for _, record := range records {
			t.Logf("versity_access_record=%s", record)
		}
		t.Logf("versity_access_log_records=%d versity_access_log_requests=%d", len(records), versityfixture.RequestCount(records))
	}()
	setup, setupCounts, err := server.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.ListBuckets(ctx); err != nil {
		t.Fatalf("authenticated Versity readiness: %v", err)
	}
	bucketName := "rhiza-retry-" + server.RunID[:16]
	if err := setup.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create task-owned bucket: %v", err)
	}
	const key = "issue185/retry/injected-once"
	payload := []byte("deterministic retry evidence")
	if _, err := setup.PutObject(ctx, bucketName, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{}); err != nil {
		t.Fatalf("seed retry read object: %v", err)
	}
	setupAttempts, setupResponses := setupCounts.Snapshot()
	setupServerCalls := versityfixture.RequestCount(mustAccessRecords(t, server))
	if setupServerCalls != setupResponses {
		t.Fatalf("setup requests: server=%d client_responses=%d client_attempts=%d", setupServerCalls, setupResponses, setupAttempts)
	}

	backend, err := url.Parse(server.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	forwarder := httputil.NewSingleHostReverseProxy(backend)
	var proxyAttempts, injectedFailures atomic.Uint64
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		proxyAttempts.Add(1)
		if request.Method == http.MethodGet && strings.Contains(request.URL.Path, key) && injectedFailures.CompareAndSwap(0, 1) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `<Error><Code>SlowDown</Code><Message>deterministic measurement fault</Message></Error>`)
			return
		}
		forwarder.ServeHTTP(w, request)
	}))
	defer proxyServer.Close()
	bucket, err := NewBucket(Config{
		Provider: ProviderS3, Endpoint: strings.TrimPrefix(proxyServer.URL, "http://"),
		Bucket: bucketName, Region: "us-east-1", Insecure: true,
		AccessKey: server.AccessKey, SecretKey: server.SecretKey, MaxRetries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	backendBefore := versityfixture.RequestCount(mustAccessRecords(t, server))
	backendGetsBefore := versityOperationCount(mustAccessRecords(t, server), "s3_GetObject")
	before := bucket.Stats()
	got, err := bucket.Get(ctx, key)
	if err != nil {
		t.Fatalf("GET after one injected 503: %v", err)
	}
	body, readErr := io.ReadAll(got)
	closeErr := got.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(body, payload) {
		t.Fatalf("GET body=%q read_err=%v close_err=%v; want seeded payload", body, readErr, closeErr)
	}
	stats := subtractStats(before, bucket.Stats())
	stopCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
	if err := server.Close(stopCtx); err != nil {
		stop()
		t.Fatalf("stop Versity before reading flushed access log: %v", err)
	}
	stop()
	backendRecords := mustAccessRecords(t, server)
	backendCalls := versityfixture.RequestCount(backendRecords) - backendBefore
	backendGets := versityOperationCount(backendRecords, "s3_GetObject") - backendGetsBefore
	if proxyAttempts.Load() != 2 || injectedFailures.Load() != 1 {
		t.Fatalf("proxy attempts=%d injected failures=%d; want two attempts and one injected failure", proxyAttempts.Load(), injectedFailures.Load())
	}
	if backendCalls != 1 || backendGets != 1 || stats.HTTPRequests != 2 || stats.HTTPGetRequests != 2 || stats.HTTP5xx != 1 || stats.HTTPFailures != 1 {
		t.Fatalf("actual backend requests=%d GETs=%d client attempts=%d GETs=%d HTTP5xx=%d HTTPFailures=%d; want 1 backend GET after 2 client GET attempts (one injected 503)", backendCalls, backendGets, stats.HTTPRequests, stats.HTTPGetRequests, stats.HTTP5xx, stats.HTTPFailures)
	}
	if stats.Gets != 1 || stats.Uploads != 0 || stats.Failures != 0 || stats.BytesDownloaded != uint64(len(payload)) || stats.BytesUploaded != 0 || stats.BytesPublished != 0 {
		t.Fatalf("logical retry outcome is unexpected: %+v", stats)
	}
	if stats.HTTPRequestBodyBytes != 0 || stats.HTTPResponseBodyBytes <= stats.BytesDownloaded {
		t.Fatalf("wire body accounting=%+v; want no GET request body and response bytes including the injected error body", stats)
	}
	if stats.RetryMetadataRequests != 0 || stats.RetryMetadataUnknownRequests != 2 || stats.SDKRetries != 0 {
		t.Fatalf("MinIO attempt metadata=%+v; want both attempts classified unknown (proxy counter is retry evidence)", stats)
	}
	t.Logf("VERSITY_RETRY provider=local-versity-s3 qualification=false version=%q binary_sha256=%s setup_attempts=%d setup_responses=%d setup_backend_requests=%d injected_failures=%d proxy_client_attempts=%d actual_backend_get_requests=%d logical_gets=%d downloaded_payload_bytes=%d request_body_bytes=%d response_body_bytes=%d controlled_proxy_proved_retries=1 sdk_retry_metadata=unknown retry_metadata_known=%d retry_metadata_unknown=%d sdk_retries_metric=%d", server.Version, server.BinarySHA256, setupAttempts, setupResponses, setupServerCalls, injectedFailures.Load(), proxyAttempts.Load(), backendGets, stats.Gets, stats.BytesDownloaded, stats.HTTPRequestBodyBytes, stats.HTTPResponseBodyBytes, stats.RetryMetadataRequests, stats.RetryMetadataUnknownRequests, stats.SDKRetries)
}

func mustAccessRecords(t *testing.T, server *versityfixture.Server) []string {
	t.Helper()
	records, err := server.AccessRecords()
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func versityOperationCount(records []string, operation string) uint64 {
	var count uint64
	for _, record := range records {
		if strings.Contains(record, " "+operation+" ") {
			count++
		}
	}
	return count
}

func subtractStats(before, after Stats) Stats {
	return Stats{
		Uploads: after.Uploads - before.Uploads, Gets: after.Gets - before.Gets, Failures: after.Failures - before.Failures,
		BytesUploaded:                after.BytesUploaded - before.BytesUploaded,
		BytesPublished:               after.BytesPublished - before.BytesPublished,
		BytesDownloaded:              after.BytesDownloaded - before.BytesDownloaded,
		HTTPRequests:                 after.HTTPRequests - before.HTTPRequests,
		HTTPGetRequests:              after.HTTPGetRequests - before.HTTPGetRequests,
		HTTP5xx:                      after.HTTP5xx - before.HTTP5xx,
		HTTPFailures:                 after.HTTPFailures - before.HTTPFailures,
		HTTPRequestBodyBytes:         after.HTTPRequestBodyBytes - before.HTTPRequestBodyBytes,
		HTTPResponseBodyBytes:        after.HTTPResponseBodyBytes - before.HTTPResponseBodyBytes,
		SDKRetries:                   after.SDKRetries - before.SDKRetries,
		RetryMetadataRequests:        after.RetryMetadataRequests - before.RetryMetadataRequests,
		RetryMetadataUnknownRequests: after.RetryMetadataUnknownRequests - before.RetryMetadataUnknownRequests,
	}
}
