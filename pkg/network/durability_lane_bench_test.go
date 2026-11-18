package network

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// BenchmarkDurabilityLane keeps the consensus fixture identical while comparing
// the three public mutation lanes. Before-ACK adds the real archive barrier.
func BenchmarkDurabilityLane(b *testing.B) {
	for _, durability := range []types.ObjectStoreDurability{types.ObjectStoreDurabilityAsync, types.ObjectStoreDurabilityBeforeAck} {
		for _, workload := range []string{"sql", "kv", "graph"} {
			b.Run(string(durability)+"/"+workload, func(b *testing.B) {
				benchmarkDurabilityLane(b, durability, workload)
			})
		}
	}
}

func benchmarkDurabilityLane(b *testing.B, durability types.ObjectStoreDurability, workload string) {
	ctx := context.Background()
	cluster := newInMemoryThreePeerCluster(b, false)
	cluster.transport.benchmarkQuorum = true
	server := cluster.servers["n1"]

	var bucket *objmetrics.MeteredBucket
	var archive *recovery.Manager
	if durability == types.ObjectStoreDurabilityBeforeAck {
		var err error
		bucket, err = objmetrics.NewBucket(objmetrics.Config{
			Provider: objmetrics.ProviderFilesystem, FilesystemDir: b.TempDir() + "/archive-objects",
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = bucket.Close() })
		archive = recovery.NewManager(bucket, "durability-lane/archive", 1)
		if err := archive.Load(ctx); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(archive.Close)
		server.SetDurabilityBarrier(func(barrierCtx context.Context, slot quepaxa.Slot) error {
			if err := cluster.cores["n1"].EnsureDurable(slot); err != nil {
				return err
			}
			return archive.SyncThrough(barrierCtx, cluster.cores["n1"], slot)
		})
	}

	var run func(context.Context, uint64) error
	switch workload {
	case "sql":
		if _, err := server.Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE lane (id INTEGER PRIMARY KEY)"}); err != nil {
			b.Fatal(err)
		}
		run = func(callCtx context.Context, id uint64) error {
			_, err := server.Execute(callCtx, ExecuteRequest{
				RequestID: fmt.Sprintf("sql-%d", id), SQL: "INSERT INTO lane(id) VALUES (?)", Args: []any{int64(id)},
			})
			return err
		}
	case "kv":
		run = func(callCtx context.Context, id uint64) error {
			_, err := server.KVPut(callCtx, KVMutationRequest{
				RequestID: fmt.Sprintf("kv-%d", id), Key: "lane-" + strconv.FormatUint(id, 10), Value: []byte("value"),
			})
			return err
		}
	case "graph":
		run = func(callCtx context.Context, id uint64) error {
			_, err := server.GraphExecute(callCtx, types.GraphCommand{
				RequestID: fmt.Sprintf("graph-%d", id), Cypher: "CREATE (:Lane {id: $id})", Args: map[string]any{"id": float64(id)},
			})
			return err
		}
	default:
		b.Fatalf("unknown workload %q", workload)
	}

	beforeTip := cluster.cores["n1"].Tip()
	beforeRecordCalls := cluster.transport.recordCalls.Load()
	beforeDecisionCalls := cluster.transport.decisionCalls.Load()
	var beforeSyncs uint64
	for _, wal := range cluster.wals {
		beforeSyncs += wal.SyncCount()
	}
	var beforeStats objmetrics.Stats
	if bucket != nil {
		beforeStats = bucket.Stats()
	}

	const workers = 16
	var sequence atomic.Uint64
	var latenciesMu sync.Mutex
	latencies := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id := sequence.Add(1)
				if id > uint64(b.N) {
					return
				}
				started := time.Now()
				err := run(ctx, id)
				latency := time.Since(started)
				latenciesMu.Lock()
				latencies = append(latencies, latency)
				latenciesMu.Unlock()
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	close(errs)
	for err := range errs {
		b.Fatal(err)
	}

	slots := cluster.cores["n1"].Tip() - beforeTip
	if slots > 0 {
		b.ReportMetric(float64(b.N)/float64(slots), "commands/slot")
	}
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		percentile := func(p float64) float64 {
			index := int(float64(len(latencies)-1) * p)
			return float64(latencies[index].Microseconds()) / 1000
		}
		b.ReportMetric(percentile(0.50), "p50-ms")
		b.ReportMetric(percentile(0.95), "p95-ms")
		b.ReportMetric(percentile(0.99), "p99-ms")
	}
	if b.N > 0 {
		b.ReportMetric(float64(cluster.transport.recordCalls.Load()-beforeRecordCalls)/float64(b.N), "record-rpc/op")
		b.ReportMetric(float64(cluster.transport.decisionCalls.Load()-beforeDecisionCalls)/float64(b.N), "learned-rpc/op")
		var afterSyncs uint64
		for _, wal := range cluster.wals {
			afterSyncs += wal.SyncCount()
		}
		b.ReportMetric(float64(afterSyncs-beforeSyncs)/float64(b.N), "fsync/op")
	}
	if bucket != nil {
		afterStats := bucket.Stats()
		b.ReportMetric(float64(afterStats.S3HTTPRequests-beforeStats.S3HTTPRequests)/float64(b.N), "s3-http/op")
		b.ReportMetric(float64(afterStats.Uploads-beforeStats.Uploads)/float64(b.N), "uploads/op")
		b.ReportMetric(float64(afterStats.Heads-beforeStats.Heads)/float64(b.N), "heads/op")
		b.ReportMetric(float64(afterStats.Gets-beforeStats.Gets)/float64(b.N), "gets/op")
	}
}
