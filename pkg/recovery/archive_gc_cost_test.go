package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// TestArchiveGCCost is an opt-in local S3 measurement, not a cloud benchmark.
// Run against a disposable bucket; each case owns and removes a unique prefix.
func TestArchiveGCCost(t *testing.T) {
	endpoint := os.Getenv("RHIZA_GC_BENCH_ENDPOINT")
	if endpoint == "" {
		t.Skip("set RHIZA_GC_BENCH_ENDPOINT for an explicit S3 measurement")
	}
	for _, count := range []int{32, 256} {
		t.Run(fmt.Sprintf("objects_%d", count), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			bucket, err := objmetrics.NewBucket(objmetrics.Config{
				Provider: objmetrics.ProviderS3, Endpoint: endpoint,
				Bucket: os.Getenv("RHIZA_GC_BENCH_BUCKET"), Region: "us-east-1", Insecure: true,
				AccessKey: os.Getenv("RHIZA_GC_BENCH_ACCESS_KEY"), SecretKey: os.Getenv("RHIZA_GC_BENCH_SECRET_KEY"),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()
			prefix := fmt.Sprintf("rhiza-gc-cost/%d-%d", time.Now().UnixNano(), count)
			manager := NewManager(bucket, prefix, 1)
			defer manager.Close()
			defer func() {
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
			}()
			value := bytes.Repeat([]byte("v"), 1024)
			hash := sha256.Sum256(value)
			source := archiveBenchmarkSource{
				decisions: []quepaxa.DecidedValue{{Slot: 1, Hash: hash, Value: value, Certificate: []byte("certificate")}},
				prefixes:  [][32]byte{{}, quepaxa.AdvancePrefixHash([32]byte{}, 1, hash)},
			}
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
			for _, phase := range []string{"mark", "delete"} {
				before := bucket.Stats()
				start := time.Now()
				if err := manager.Cleanup(ctx, 0); err != nil {
					t.Fatal(err)
				}
				elapsed := time.Since(start)
				delta := gcStatsDelta(before, bucket.Stats())
				record, err := json.Marshal(map[string]any{"objects": count, "object_bytes": 1024, "phase": phase, "duration_ms": float64(elapsed) / float64(time.Millisecond), "stats": delta})
				if err != nil {
					t.Fatal(err)
				}
				t.Log(string(record))
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
			}
			if err := manager.Load(ctx); err != nil {
				t.Fatal(err)
			}
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
