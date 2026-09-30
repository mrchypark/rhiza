package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/thanos-io/objstore"
)

// TestCheckpointCost is an opt-in S3-compatible measurement, not cloud
// qualification. Its 128 MiB graph file exercises multiple SectionReader uploads.
func TestCheckpointCost(t *testing.T) {
	endpoint := os.Getenv("RHIZA_CHECKPOINT_BENCH_ENDPOINT")
	if endpoint == "" {
		t.Skip("set RHIZA_CHECKPOINT_BENCH_ENDPOINT for an explicit S3 measurement")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	bucket, err := objmetrics.NewBucket(objmetrics.Config{
		Provider: objmetrics.ProviderS3, Endpoint: endpoint,
		Bucket: os.Getenv("RHIZA_CHECKPOINT_BENCH_BUCKET"), Region: "us-east-1", Insecure: true,
		AccessKey: os.Getenv("RHIZA_CHECKPOINT_BENCH_ACCESS_KEY"), SecretKey: os.Getenv("RHIZA_CHECKPOINT_BENCH_SECRET_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	prefix := fmt.Sprintf("rhiza-checkpoint-cost/%d", time.Now().UnixNano())
	manager := NewManager(bucket, prefix, t.TempDir(), 1)
	sources := []Source{source(t, RoleSQLite, "checkpoint-cost"), checkpointCostGraphSource(t, 2*blockSize)}

	cleanup := func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
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
	}
	defer cleanup()

	claim, err := manager.AcquirePublisherClaim(ctx, "checkpoint-cost", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before := bucket.Stats()
	started := time.Now()
	root, err := manager.CreateFiles(ctx, claim, sources, 1)
	createTime := time.Since(started)
	createStats := checkpointCostDelta(before, bucket.Stats())
	if err != nil {
		t.Fatal(err)
	}
	before = bucket.Stats()
	started = time.Now()
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	publishTime := time.Since(started)
	publishStats := checkpointCostDelta(before, bucket.Stats())
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}

	loaded := NewManager(bucket, prefix, t.TempDir(), 1)
	before = bucket.Stats()
	started = time.Now()
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	current := loaded.Latest()
	if current == nil {
		t.Fatal("checkpoint CURRENT did not load")
	}
	if _, err := loaded.DownloadAndVerifyRootFiles(ctx, current, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	restoreTime := time.Since(started)
	restoreStats := checkpointCostDelta(before, bucket.Stats())

	result, err := json.Marshal(map[string]any{
		"provider": "s3-compatible", "qualification": "local-or-explicit-endpoint-only",
		"source_bytes": root.Size, "block_count": len(root.Files[1].Blocks),
		"create_and_scan_ms": createTime.Milliseconds(), "current_publish_ms": publishTime.Milliseconds(),
		"restore_ms":   restoreTime.Milliseconds(),
		"create_stats": createStats, "publish_stats": publishStats, "restore_stats": restoreStats,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CHECKPOINT_COST %s", result)
}

func TestCheckpointCostPublisherClaimLifetime(t *testing.T) {
	ctx := context.Background()
	sources := []Source{source(t, RoleSQLite, "checkpoint-cost"), source(t, RoleGraphData, "graph")}

	t.Run("release before promotion is fenced", func(t *testing.T) {
		manager := NewManager(objstore.NewInMemBucket(), "checkpoint-cost-early-release", t.TempDir(), 1)
		claim, err := manager.AcquirePublisherClaim(ctx, "checkpoint-cost", 0, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		root, err := manager.CreateFiles(ctx, claim, sources, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if err := manager.PromoteCertifiedCurrent(ctx, root); !errors.Is(err, ErrPublisherFenced) {
			t.Fatalf("promotion after releasing claim=%v, want ErrPublisherFenced", err)
		}
	})

	t.Run("promote before release succeeds", func(t *testing.T) {
		manager := NewManager(objstore.NewInMemBucket(), "checkpoint-cost-valid-lifetime", t.TempDir(), 1)
		claim, err := manager.AcquirePublisherClaim(ctx, "checkpoint-cost", 0, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		root, err := manager.CreateFiles(ctx, claim, sources, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
			t.Fatalf("promote with active claim: %v", err)
		}
		if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Fatalf("release after promotion: %v", err)
		}
	})
}

func checkpointCostGraphSource(t *testing.T, size int64) Source {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "graph-data-*.db")
	if err != nil {
		t.Fatal(err)
	}
	chunk := bytes.Repeat([]byte("rhiza-checkpoint-cost\n"), 4096)
	for written := int64(0); written < size; {
		part := chunk
		if int64(len(part)) > size-written {
			part = part[:size-written]
		}
		n, err := file.Write(part)
		written += int64(n)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return Source{Role: RoleGraphData, Path: file.Name()}
}

type checkpointCostStats struct {
	Uploads         uint64 `json:"uploads"`
	Gets            uint64 `json:"gets"`
	Lists           uint64 `json:"lists"`
	Heads           uint64 `json:"heads"`
	Deletes         uint64 `json:"deletes"`
	Failures        uint64 `json:"failures"`
	HTTPRequests    uint64 `json:"http_requests"`
	HTTPFailures    uint64 `json:"http_failures"`
	HTTPGet         uint64 `json:"http_get_requests"`
	HTTPPut         uint64 `json:"http_put_requests"`
	HTTPHead        uint64 `json:"http_head_requests"`
	HTTPDelete      uint64 `json:"http_delete_requests"`
	AttemptedBytes  uint64 `json:"attempted_bytes"`
	PublishedBytes  uint64 `json:"published_bytes"`
	DownloadedBytes uint64 `json:"downloaded_bytes"`
	Retries         uint64 `json:"retries"`
}

func checkpointCostDelta(before, after objmetrics.Stats) checkpointCostStats {
	return checkpointCostStats{
		Uploads: after.Uploads - before.Uploads, Gets: after.Gets - before.Gets,
		Lists: after.Lists - before.Lists, Heads: after.Heads - before.Heads,
		Deletes: after.Deletes - before.Deletes, Failures: after.Failures - before.Failures,
		HTTPRequests: after.HTTPRequests - before.HTTPRequests, HTTPFailures: after.HTTPFailures - before.HTTPFailures,
		HTTPGet: after.HTTPGetRequests - before.HTTPGetRequests, HTTPPut: after.HTTPPutRequests - before.HTTPPutRequests,
		HTTPHead: after.HTTPHeadRequests - before.HTTPHeadRequests, HTTPDelete: after.HTTPDeleteRequests - before.HTTPDeleteRequests,
		AttemptedBytes: after.BytesUploaded - before.BytesUploaded, PublishedBytes: after.BytesPublished - before.BytesPublished,
		DownloadedBytes: after.BytesDownloaded - before.BytesDownloaded, Retries: after.SDKRetries - before.SDKRetries,
	}
}
