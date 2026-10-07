package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/objstore/versityfixture"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// TestArchiveGCCost is an opt-in local S3 measurement, not a cloud benchmark.
// Run against a disposable bucket; each case owns and removes a unique prefix.
func TestArchiveGCCost(t *testing.T) {
	bin := os.Getenv("RHIZA_VERSITYGW_BIN")
	if bin == "" {
		t.Skip("set RHIZA_VERSITYGW_BIN for a local Versity S3 measurement")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	setupClient, setupCounts, err := server.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := setupClient.ListBuckets(ctx); err != nil {
		t.Fatalf("authenticated Versity readiness: %v", err)
	}
	bucketName := "rhiza-gc-" + server.RunID[:16]
	if err := setupClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create task-owned bucket: %v", err)
	}
	setupAttempts, setupResponses := setupCounts.Snapshot()
	setupServerRequests := archiveAccessRequestCount(t, server)
	if setupServerRequests != setupResponses {
		t.Fatalf("setup server requests=%d, client responses=%d (attempts=%d)", setupServerRequests, setupResponses, setupAttempts)
	}
	bucket, err := objmetrics.NewBucket(objmetrics.Config{
		Provider: objmetrics.ProviderS3, Endpoint: strings.TrimPrefix(server.Endpoint, "http://"),
		Bucket: bucketName, Region: "us-east-1", Insecure: true,
		AccessKey: server.AccessKey, SecretKey: server.SecretKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	t.Logf("ARCHIVE_GC_SETUP versity_version=%q binary_sha256=%s bucket=%s setup_attempts=%d setup_responses=%d setup_server_requests=%d provider_qualification=false", server.Version, server.BinarySHA256, bucketName, setupAttempts, setupResponses, setupServerRequests)
	for _, count := range []int{32, 256} {
		t.Run(fmt.Sprintf("objects_%d", count), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			prefix := fmt.Sprintf("rhiza-gc-cost/%d-%d", time.Now().UnixNano(), count)
			manager := NewManager(bucket, prefix, 1)
			defer manager.Close()
			defer func() {
				cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				before, serverBefore := bucket.Stats(), archiveAccessRequestCount(t, server)
				var names []string
				if err := bucket.Iter(cleanupCtx, prefix+"/", func(name string) error { names = append(names, name); return nil }, objstore.WithRecursiveIter()); err != nil {
					t.Error(err)
					return
				}
				for _, name := range names {
					if err := bucket.Delete(cleanupCtx, name); err != nil {
						t.Error(err)
					}
				}
				delta := gcStatsDelta(before, bucket.Stats())
				serverCalls := archiveAccessRequestCount(t, server) - serverBefore
				if serverCalls != delta["http_requests"] {
					t.Errorf("cleanup server requests=%d, client attempts=%d", serverCalls, delta["http_requests"])
				}
				t.Logf("ARCHIVE_GC_CLEANUP objects=%d server_requests=%d stats=%v", count, serverCalls, delta)
			}()
			value := bytes.Repeat([]byte("v"), 1024)
			hash := sha256.Sum256(value)
			source := archiveBenchmarkSource{
				decisions: []quepaxa.DecidedValue{{Slot: 1, Hash: hash, Value: value, Certificate: []byte("certificate")}},
				prefixes:  [][32]byte{{}, quepaxa.AdvancePrefixHash([32]byte{}, 1, hash)},
			}
			before, serverBefore := bucket.Stats(), archiveAccessRequestCount(t, server)
			if err := manager.syncNow(ctx, source, 1); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				data := bytes.Repeat([]byte{byte(i)}, 1024)
				copy(data, fmt.Sprintf("%08d", i))
				h := sha256.Sum256(data)
				if err := bucket.Upload(ctx, manager.key(extentObjectKey(h, 1)), bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}
			fixtureStats := gcStatsDelta(before, bucket.Stats())
			fixtureServerRequests := archiveAccessRequestCount(t, server) - serverBefore
			if fixtureServerRequests != fixtureStats["http_requests"] {
				t.Fatalf("fixture server requests=%d, client attempts=%d", fixtureServerRequests, fixtureStats["http_requests"])
			}
			t.Logf("ARCHIVE_GC_FIXTURE objects=%d server_requests=%d stats=%v", count, fixtureServerRequests, fixtureStats)
			for _, phase := range []string{"mark", "delete"} {
				before, serverBefore := bucket.Stats(), archiveAccessRequestCount(t, server)
				start := time.Now()
				if err := manager.Cleanup(ctx, 0); err != nil {
					t.Fatal(err)
				}
				elapsed := time.Since(start)
				delta := gcStatsDelta(before, bucket.Stats())
				serverCalls := archiveAccessRequestCount(t, server) - serverBefore
				if serverCalls != delta["http_requests"] {
					t.Fatalf("%s phase server requests=%d, client attempts=%d", phase, serverCalls, delta["http_requests"])
				}
				record, err := json.Marshal(map[string]any{"provider": "local-versity-s3", "qualification": "local-measurement-not-provider-qualification", "versity_version": server.Version, "versity_binary_sha256": server.BinarySHA256, "objects": count, "object_bytes": 1024, "phase": phase, "duration_ms": float64(elapsed) / float64(time.Millisecond), "server_requests": serverCalls, "stats": delta})
				if err != nil {
					t.Fatal(err)
				}
				t.Log(string(record))
				readbackBefore, readbackServerBefore := bucket.Stats(), archiveAccessRequestCount(t, server)
				blocks, markers := 0, 0
				if err := bucket.Iter(ctx, manager.key("archive/blocks"), func(string) error { blocks++; return nil }); err != nil {
					t.Fatal(err)
				}
				if err := bucket.Iter(ctx, manager.key("archive/gc-candidates"), func(string) error { markers++; return nil }); err != nil {
					t.Fatal(err)
				}
				if phase == "mark" && (blocks != count+1 || markers != count) {
					t.Fatalf("after marking: blocks=%d markers=%d", blocks, markers)
				}
				if phase == "delete" && (blocks != 1 || markers != 0) {
					t.Fatalf("after deletion: blocks=%d markers=%d", blocks, markers)
				}
				readbackStats := gcStatsDelta(readbackBefore, bucket.Stats())
				readbackRequests := archiveAccessRequestCount(t, server) - readbackServerBefore
				if readbackRequests != readbackStats["http_requests"] {
					t.Fatalf("%s readback server requests=%d, client attempts=%d", phase, readbackRequests, readbackStats["http_requests"])
				}
				t.Logf("ARCHIVE_GC_READBACK objects=%d phase=%s server_requests=%d stats=%v", count, phase, readbackRequests, readbackStats)
			}
			beforeLoad, serverBeforeLoad := bucket.Stats(), archiveAccessRequestCount(t, server)
			if err := manager.Load(ctx); err != nil {
				t.Fatal(err)
			}
			loadStats := gcStatsDelta(beforeLoad, bucket.Stats())
			loadRequests := archiveAccessRequestCount(t, server) - serverBeforeLoad
			if loadRequests != loadStats["http_requests"] {
				t.Fatalf("final archive load server requests=%d, client attempts=%d", loadRequests, loadStats["http_requests"])
			}
			t.Logf("ARCHIVE_GC_FINAL_LOAD objects=%d server_requests=%d stats=%v", count, loadRequests, loadStats)
			if manager.Tip() != 1 {
				t.Fatalf("live archive tip=%d", manager.Tip())
			}
		})
	}
}

func gcStatsDelta(before, after objmetrics.Stats) map[string]uint64 {
	decode := func(stats objmetrics.Stats) map[string]uint64 {
		data, _ := json.Marshal(stats)
		var result map[string]uint64
		_ = json.Unmarshal(data, &result)
		return result
	}
	first, result := decode(before), decode(after)
	for key := range result {
		result[key] -= first[key]
	}
	return result
}

func archiveAccessRequestCount(t *testing.T, server *versityfixture.Server) uint64 {
	t.Helper()
	records, err := server.AccessRecords()
	if err != nil {
		t.Fatalf("read Versity access records: %v", err)
	}
	return versityfixture.RequestCount(records)
}
