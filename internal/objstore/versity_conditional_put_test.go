package objstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	thanosobjstore "github.com/thanos-io/objstore"
)

// TestVersityConditionalLargePutEvidence diagnoses whether a losing large
// If-None-Match PUT reaches the caller as a typed condition conflict or as a
// request-body transport error. It is opt-in and uses only a fresh local
// Versity fixture and task-owned bucket.
func TestVersityConditionalLargePutEvidence(t *testing.T) {
	runVersityConditionalLargePutEvidence(t, false)
}

// TestVersityConditionalLargePutExpectEvidence is a separately selectable
// local diagnostic of the 100-continue prototype. Set
// RHIZA_VERSITY_EXPECT_CONTINUE=1 to opt into this second S3 run.
func TestVersityConditionalLargePutExpectEvidence(t *testing.T) {
	if os.Getenv("RHIZA_VERSITY_EXPECT_CONTINUE") != "1" {
		t.Skip("set RHIZA_VERSITY_EXPECT_CONTINUE=1 to run the separate Expect prototype")
	}
	runVersityConditionalLargePutEvidence(t, true)
}

func runVersityConditionalLargePutEvidence(t *testing.T, expectContinue bool) {
	t.Helper()
	traceEnabled := expectContinue && os.Getenv("RHIZA_VERSITY_HTTP_TRACE") == "1"
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
	serverClosed := false
	var bucket *MeteredBucket
	t.Cleanup(func() {
		if bucket != nil {
			bucket.Close()
		}
		if !serverClosed {
			stopCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
			defer stop()
			if err := server.Close(stopCtx); err != nil {
				t.Errorf("stop local Versity child: %v", err)
			}
		}
	})

	setup, setupCounts, err := server.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setup.ListBuckets(ctx); err != nil {
		t.Fatalf("authenticated S3 readiness: %v", err)
	}
	bucketName := "rhiza-cond-put-" + server.RunID[:16]
	if err := setup.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create task-owned bucket: %v", err)
	}
	const payloadSize = 7 << 20
	payload := bytes.Repeat([]byte{0x5a}, payloadSize)
	payloadHash := sha256.Sum256(payload)
	key := "issue185/conditional-put/" + server.RunID + "/archive/blocks/" + hex.EncodeToString(payloadHash[:]) + ".bin"
	if _, err := setup.PutObject(ctx, bucketName, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{}); err != nil {
		t.Fatalf("seed task-owned object: %v", err)
	}
	seed, err := setup.GetObject(ctx, bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("read seeded object: %v", err)
	}
	seedBody, seedReadErr := io.ReadAll(io.LimitReader(seed, payloadSize+1))
	seedCloseErr := seed.Close()
	if seedReadErr != nil || seedCloseErr != nil || !bytes.Equal(seedBody, payload) {
		t.Fatalf("seed readback valid=%t read_error=%t close_error=%t", bytes.Equal(seedBody, payload), seedReadErr != nil, seedCloseErr != nil)
	}
	seedClientAttempts, seedClientResponses := setupCounts.Snapshot()

	var httpTrace *conditionalPutHTTPTrace
	if traceEnabled {
		httpTrace = newConditionalPutHTTPTrace()
	}
	bucket, err = newConditionalExpectVersityBucket(Config{
		Provider: ProviderS3, Endpoint: strings.TrimPrefix(server.Endpoint, "http://"),
		Bucket: bucketName, Region: "us-east-1", Insecure: true,
		AccessKey: server.AccessKey, SecretKey: server.SecretKey,
	}, expectContinue, httpTrace)
	if err != nil {
		t.Fatal(err)
	}
	workCtx, cancelWork := context.WithTimeout(ctx, 20*time.Second)
	conditionalErr := bucket.Upload(workCtx, key, bytes.NewReader(payload), thanosobjstore.WithIfNotExists())
	if httpTrace != nil {
		logConditionalPutHTTPTrace(t, httpTrace.closeAndSnapshot())
	}
	callStats := bucket.Stats()
	cancelWork()

	readbackCtx, cancelReadback := context.WithTimeout(ctx, 5*time.Second)
	defer cancelReadback()
	readback, readbackErr := bucket.Get(readbackCtx, key)
	readbackOK := false
	readbackSize := 0
	var readbackHash [32]byte
	if readbackErr == nil {
		readbackBody, readErr := io.ReadAll(io.LimitReader(readback, payloadSize+1))
		closeErr := readback.Close()
		readbackSize = len(readbackBody)
		readbackHash = sha256.Sum256(readbackBody)
		readbackOK = readErr == nil && closeErr == nil && readbackSize == payloadSize && bytes.Equal(readbackBody, payload)
	}
	readbackStats := bucket.Stats()
	bucket.Close()
	stopCtx, stop := context.WithTimeout(context.Background(), 7*time.Second)
	closeErr := server.Close(stopCtx)
	stop()
	serverClosed = closeErr == nil
	if closeErr != nil {
		t.Fatalf("stop Versity before reading flushed access log: %v", closeErr)
	}
	allRecords, err := server.AccessRecords()
	if err != nil {
		t.Fatal(err)
	}
	allPutRecords := countConditionalPutAccessRecords(allRecords, key)
	putRecords := allPutRecords

	epipe := errors.Is(conditionalErr, syscall.EPIPE)
	canceled := errors.Is(conditionalErr, context.Canceled)
	deadline := errors.Is(conditionalErr, context.DeadlineExceeded)
	condition := bucket.IsConditionNotMetErr(conditionalErr)
	serverCalls := versityfixture.RequestCount(allRecords)
	setupAttempts, setupResponses := setupCounts.Snapshot()
	seedHash := sha256.Sum256(seedBody)
	t.Logf("VERSITY_CONDITIONAL_PUT phase=conditional_put provider=local-versity-s3 qualification=false expect_continue_prototype=%t http_trace_enabled=%t version=%q binary_sha256=%s payload_size=%d payload_sha256=%x seed_readback_size=%d seed_readback_sha256=%x logical_uploads=%d logical_failures=%d error_type=%T condition_error=%t syscall_epipe=%t context_canceled=%t context_deadline=%t client_attempts=%d HTTP_failures=%d transport_failures=%d request_body_bytes=%d response_body_bytes=%d retry_metadata_known=%d retry_metadata_unknown=%d sdk_retries_metric=%d published_bytes=%d server_put_records=%d status_200=%d status_412=%d other_status=%d server_request_count=%d readback_ok=%t readback_size=%d readback_sha256=%x setup_attempts=%d setup_responses=%d", expectContinue, traceEnabled, server.Version, server.BinarySHA256, payloadSize, payloadHash, len(seedBody), seedHash, callStats.Uploads, callStats.Failures, conditionalErr, condition, epipe, canceled, deadline, callStats.HTTPRequests, callStats.HTTPFailures, callStats.TransportFailures, callStats.HTTPRequestBodyBytes, callStats.HTTPResponseBodyBytes, callStats.RetryMetadataRequests, callStats.RetryMetadataUnknownRequests, callStats.SDKRetries, callStats.BytesPublished, putRecords.requests, putRecords.status200, putRecords.status412, putRecords.otherStatus, serverCalls, readbackOK, readbackSize, readbackHash, setupAttempts, setupResponses)
	if seedClientAttempts != seedClientResponses {
		t.Fatalf("seed setup client attempts=%d responses=%d", seedClientAttempts, seedClientResponses)
	}
	if conditionalErr == nil || !condition {
		t.Fatalf("conditional PUT returned error_type=%T condition_error=%t syscall_epipe=%t canceled=%t deadline=%t; want a typed condition-not-met error", conditionalErr, condition, epipe, canceled, deadline)
	}
	if callStats.Uploads != 1 || callStats.Failures != 1 || callStats.DedupHits != 1 || callStats.ConditionConflicts != 0 || callStats.BytesPublished != 0 {
		t.Fatalf("conditional upload accounting: uploads=%d failures=%d dedup_hits=%d condition_conflicts=%d published_bytes=%d", callStats.Uploads, callStats.Failures, callStats.DedupHits, callStats.ConditionConflicts, callStats.BytesPublished)
	}
	if callStats.HTTPFailures != 0 || callStats.TransportFailures != 0 || callStats.Unexpected4xx != 0 || callStats.HTTP5xx != 0 {
		t.Fatalf("expected condition conflict counted as transport/HTTP failure: HTTP=%d transport=%d unexpected4xx=%d 5xx=%d", callStats.HTTPFailures, callStats.TransportFailures, callStats.Unexpected4xx, callStats.HTTP5xx)
	}
	if putRecords.requests < 2 || putRecords.status200 != 1 || putRecords.status412 == 0 {
		t.Fatalf("server per-key PUT records=%d seed_status200=%d conditional_status412=%d other_status=%d", putRecords.requests, putRecords.status200, putRecords.status412, putRecords.otherStatus)
	}
	if expectContinue && (callStats.HTTPRequests != 1 || callStats.HTTPRequestBodyBytes != 0 || putRecords.requests != 2 || putRecords.status412 != 1) {
		t.Fatalf("Expect prototype did not reject before body: attempts=%d client_body_bytes=%d per_key_puts=%d status412=%d", callStats.HTTPRequests, callStats.HTTPRequestBodyBytes, putRecords.requests, putRecords.status412)
	}
	readbackHTTPRequests := readbackStats.HTTPRequests - callStats.HTTPRequests
	if !readbackOK || readbackErr != nil || readbackStats.Gets-callStats.Gets != 1 {
		t.Fatalf("seeded object readback valid=%t error_type=%T GETs=%d", readbackOK, readbackErr, readbackStats.Gets-callStats.Gets)
	}
	if serverCalls != setupResponses+callStats.HTTPRequests+readbackHTTPRequests {
		t.Fatalf("independent server request count=%d; setup responses=%d + conditional PUT attempts=%d + readback requests=%d", serverCalls, setupResponses, callStats.HTTPRequests, readbackHTTPRequests)
	}
}

type conditionalPutAccessCounts struct {
	requests    uint64
	status200   uint64
	status412   uint64
	otherStatus uint64
}

func countConditionalPutAccessRecords(records []string, key string) conditionalPutAccessCounts {
	var counts conditionalPutAccessCounts
	for _, record := range records {
		fields := strings.Fields(record)
		for i, field := range fields {
			if field != "s3_PutObject" || i+3 >= len(fields) || fields[i+1] != key {
				continue
			}
			counts.requests++
			status, err := strconv.Atoi(fields[i+3])
			if err != nil {
				counts.otherStatus++
				break
			}
			switch status {
			case http.StatusOK:
				counts.status200++
			case http.StatusPreconditionFailed:
				counts.status412++
			default:
				counts.otherStatus++
			}
			break
		}
	}
	return counts
}
