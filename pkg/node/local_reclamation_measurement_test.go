package node

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

// This emits bounded workload measurements, not a latency threshold. Fixed
// WAL-cap and exactly-once assertions remain mandatory.
func TestLocalReclamationMeasurement(t *testing.T) {
	const (
		cycles   = 20
		walLimit = int64(2 << 20)
		payloadN = 64 << 10
		maxPairs = 24
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir(), MaxWALBytes: walLimit}
	n := New(&config)
	if err := n.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			_ = n.Shutdown()
		}
	}()
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: "reclaim-measurement-schema", SQL: "CREATE TABLE local_reclaim_measurement (id TEXT PRIMARY KEY, payload TEXT NOT NULL)"}); err != nil {
		t.Fatal(err)
	}
	if n.wal.Capacity().Limit != walLimit {
		t.Fatalf("initial WAL limit=%d want %d", n.wal.Capacity().Limit, walLimit)
	}

	payload := strings.Repeat("x", payloadN)
	var sqlIDs, graphIDs []string
	var sqlSlots, graphSlots []uint64
	var latencies, maintenanceLatencies, reopenLatencies []time.Duration
	var maxWAL, maxDisk int64
	var maxHeapAlloc, maxHeapSys uint64
	var returnedErrors, returnedAdmissionDenials, inferredReclaimTriggers int
	started := time.Now()
	for cycle := 0; cycle < cycles; cycle++ {
		floorBefore := n.core.CompactionFloor()
		pairs := 0
		for pairs < maxPairs && n.core.CompactionFloor() <= floorBefore {
			id := fmt.Sprintf("cycle-%02d-item-%02d", cycle, pairs)
			floorAtOp := n.core.CompactionFloor()
			sqlID := "sql-" + id
			opStart := time.Now()
			sqlReceipt, err := n.server.Execute(ctx, network.ExecuteRequest{RequestID: sqlID, SQL: "INSERT INTO local_reclaim_measurement (id, payload) VALUES (?, ?)", Args: []any{id, payload}})
			sqlElapsed := time.Since(opStart)
			latencies = append(latencies, sqlElapsed)
			if err != nil {
				returnedErrors++
				if strings.Contains(strings.ToLower(err.Error()), "admission") || strings.Contains(strings.ToLower(err.Error()), "capacity") {
					returnedAdmissionDenials++
				}
				t.Fatalf("SQL %s failed (floor=%d WAL=%d/%d): %v", sqlID, n.core.CompactionFloor(), n.wal.Bytes(), walLimit, err)
			}
			if sqlReceipt.Status != types.MutationCommitted || sqlReceipt.Slot == 0 {
				t.Fatalf("SQL receipt %s=%+v", sqlID, sqlReceipt.MutationReceipt)
			}
			sqlIDs, sqlSlots = append(sqlIDs, sqlID), append(sqlSlots, sqlReceipt.Slot)
			if n.core.CompactionFloor() > floorAtOp {
				inferredReclaimTriggers++
				maintenanceLatencies = append(maintenanceLatencies, sqlElapsed)
			}
			maxWAL = max(maxWAL, n.wal.Bytes())
			measureLocalReclamationResources(t, config.DataDir, &maxDisk, &maxHeapAlloc, &maxHeapSys)

			floorAtOp = n.core.CompactionFloor()
			graphID := "graph-" + id
			opStart = time.Now()
			graphReceipt, err := n.server.GraphExecute(ctx, types.GraphCommand{RequestID: graphID, Cypher: fmt.Sprintf("CREATE (:LocalReclaimMeasurement {id: '%s'})", id), Events: []types.GraphStreamEvent{{Stream: "local-reclaim-measurement", Kind: "created", Payload: id}}})
			graphElapsed := time.Since(opStart)
			latencies = append(latencies, graphElapsed)
			if err != nil {
				returnedErrors++
				if strings.Contains(strings.ToLower(err.Error()), "admission") || strings.Contains(strings.ToLower(err.Error()), "capacity") {
					returnedAdmissionDenials++
				}
				t.Fatalf("graph %s failed (floor=%d WAL=%d/%d): %v", graphID, n.core.CompactionFloor(), n.wal.Bytes(), walLimit, err)
			}
			if graphReceipt.Status != types.MutationCommitted || graphReceipt.Slot == 0 {
				t.Fatalf("graph receipt %s=%+v", graphID, graphReceipt.MutationReceipt)
			}
			graphIDs, graphSlots = append(graphIDs, graphID), append(graphSlots, graphReceipt.Slot)
			if n.core.CompactionFloor() > floorAtOp {
				inferredReclaimTriggers++
				maintenanceLatencies = append(maintenanceLatencies, graphElapsed)
			}
			pairs++
			maxWAL = max(maxWAL, n.wal.Bytes())
			measureLocalReclamationResources(t, config.DataDir, &maxDisk, &maxHeapAlloc, &maxHeapSys)
			if cap := n.wal.Capacity(); cap.Limit != walLimit || cap.Used > walLimit {
				t.Fatalf("cycle %d WAL bound violated: %+v", cycle, cap)
			}
		}
		floorAfter := n.core.CompactionFloor()
		if pairs == 0 || floorAfter <= floorBefore {
			t.Fatalf("cycle %d no reclamation progress: floor=%d=>%d pairs=%d", cycle, floorBefore, floorAfter, pairs)
		}
		measureLocalReclamationResources(t, config.DataDir, &maxDisk, &maxHeapAlloc, &maxHeapSys)
		if err := n.Shutdown(); err != nil {
			t.Fatalf("cycle %d shutdown: %v", cycle, err)
		}
		reopenStart := time.Now()
		n = New(&config)
		if err := n.Open(ctx); err != nil {
			t.Fatalf("cycle %d reopen: %v", cycle, err)
		}
		reopenLatencies = append(reopenLatencies, time.Since(reopenStart))
		if cap := n.wal.Capacity(); cap.Limit != walLimit || cap.Used > walLimit || n.core.CompactionFloor() < floorAfter {
			t.Fatalf("cycle %d reopen WAL/floor bounds: wal=%+v floor=%d want>=%d", cycle, cap, n.core.CompactionFloor(), floorAfter)
		}
		maxWAL = max(maxWAL, n.wal.Bytes())
		measureLocalReclamationResources(t, config.DataDir, &maxDisk, &maxHeapAlloc, &maxHeapSys)
	}

	rows, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id, length(payload) FROM local_reclaim_measurement ORDER BY id"})
	if err != nil || len(rows.Rows) != len(sqlIDs) {
		t.Fatalf("SQL rows=%d want=%d err=%v", len(rows.Rows), len(sqlIDs), err)
	}
	sqlCounts := make(map[string]int, len(sqlIDs))
	for _, row := range rows.Rows {
		if len(row) != 2 || row[1] != int64(payloadN) {
			t.Fatalf("unexpected SQL row %v", row)
		}
		sqlCounts[fmt.Sprint(row[0])]++
	}
	graph, err := n.server.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:LocalReclaimMeasurement) RETURN n.id"})
	if err != nil || len(graph.Rows) != len(graphIDs) {
		t.Fatalf("graph rows=%d want=%d err=%v", len(graph.Rows), len(graphIDs), err)
	}
	graphCounts := make(map[string]int, len(graphIDs))
	for _, row := range graph.Rows {
		if len(row) != 1 {
			t.Fatalf("unexpected graph row %v", row)
		}
		graphCounts[fmt.Sprint(row[0])]++
	}
	stream, err := n.server.GraphStreamRead(ctx, network.GraphStreamReadRequest{Stream: "local-reclaim-measurement", Limit: uint(len(graphIDs))})
	if err != nil || len(stream.Records) != len(graphIDs) {
		t.Fatalf("event records=%d want=%d err=%v", len(stream.Records), len(graphIDs), err)
	}
	eventCounts := make(map[string]int, len(graphIDs))
	for _, record := range stream.Records {
		eventCounts[fmt.Sprint(record.Payload)]++
	}
	for i := range sqlIDs {
		id := strings.TrimPrefix(graphIDs[i], "graph-")
		if sqlCounts[id] != 1 || graphCounts[id] != 1 || eventCounts[id] != 1 {
			t.Fatalf("effect counts for %s: SQL=%d graph=%d events=%d", id, sqlCounts[id], graphCounts[id], eventCounts[id])
		}
		for _, item := range []struct {
			kind, requestID string
			slot            uint64
		}{{"sql", sqlIDs[i], sqlSlots[i]}, {"graph", graphIDs[i], graphSlots[i]}} {
			status, statusErr := n.server.RequestStatus(ctx, network.RequestStatusRequest{Kind: item.kind, RequestID: item.requestID})
			if statusErr != nil || status.Receipt == nil || status.State != string(types.MutationCommitted) || status.Receipt.Slot != item.slot {
				t.Fatalf("%s receipt %s: %+v err=%v", item.kind, item.requestID, status, statusErr)
			}
		}
	}
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)
	t.Logf("reclamation_measurement cycles=%d pairs=%d operations=%d successful=%d returned_errors=%d returned_admission_denials=%d inferred_reclaim_triggers=%d throughput_ops_per_sec=%.2f wall=%s latency_p50=%s latency_p95=%s latency_p99=%s latency_max=%s maintenance_trigger_operation_max_proxy=%s reopen_count=%d reopen_p50=%s reopen_p95=%s reopen_max=%s max_observed_wal_bytes=%d wal_limit_bytes=%d max_observed_logical_file_bytes=%d max_heap_alloc_bytes=%d max_heap_sys_bytes=%d final_heap_alloc_bytes=%d total_alloc_bytes=%d snapshot_rewrite_sync_metrics=unavailable", cycles, len(sqlIDs), len(latencies), len(latencies), returnedErrors, returnedAdmissionDenials, inferredReclaimTriggers, float64(len(latencies))/time.Since(started).Seconds(), time.Since(started), durationQuantile(latencies, .50), durationQuantile(latencies, .95), durationQuantile(latencies, .99), durationQuantile(latencies, 1), durationQuantile(maintenanceLatencies, 1), len(reopenLatencies), durationQuantile(reopenLatencies, .50), durationQuantile(reopenLatencies, .95), durationQuantile(reopenLatencies, 1), maxWAL, walLimit, maxDisk, maxHeapAlloc, maxHeapSys, finalMem.HeapAlloc, finalMem.TotalAlloc)
	if len(maintenanceLatencies) == 0 {
		t.Fatal("no operation exposed a reclamation-trigger latency sample")
	}
}

func measureLocalReclamationResources(t *testing.T, dataDir string, maxDisk *int64, maxHeapAlloc, maxHeapSys *uint64) {
	t.Helper()
	disk, err := localReclamationTestDiskBytes(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	*maxDisk = max(*maxDisk, disk)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	*maxHeapAlloc, *maxHeapSys = max(*maxHeapAlloc, mem.HeapAlloc), max(*maxHeapSys, mem.HeapSys)
}

func durationQuantile(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(quantile*float64(len(ordered)))) - 1
	index = max(0, min(index, len(ordered)-1))
	return ordered[index]
}
