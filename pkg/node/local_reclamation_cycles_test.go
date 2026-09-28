package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestLocalAutomaticReclamationMakesProgressAcrossCycles(t *testing.T) {
	const (
		cycleCount = 3
		walLimit   = int64(2 << 20)
		payloadLen = 64 << 10
		maxPairs   = 24
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	if _, err := n.server.Execute(ctx, network.ExecuteRequest{
		RequestID: "reclaim-schema",
		SQL:       "CREATE TABLE local_reclaim_rows (id TEXT PRIMARY KEY, payload TEXT NOT NULL)",
	}); err != nil {
		t.Fatalf("create SQL workload table: %v", err)
	}
	if got := n.wal.Capacity().Limit; got != walLimit {
		t.Fatalf("initial WAL limit=%d, want fixed limit %d", got, walLimit)
	}

	var sqlIDs, graphIDs []string
	var sqlSlots, graphSlots []uint64
	payload := strings.Repeat("x", payloadLen)
	for cycle := range cycleCount {
		cycleStarted := time.Now()
		floorBefore := n.core.CompactionFloor()
		pairs := 0
		for pairs < maxPairs && n.core.CompactionFloor() <= floorBefore {
			id := fmt.Sprintf("cycle-%d-item-%d", cycle, pairs)
			sqlID := "sql-" + id
			sqlStarted := time.Now()
			sqlReceipt, err := n.server.Execute(ctx, network.ExecuteRequest{
				RequestID: sqlID,
				SQL:       "INSERT INTO local_reclaim_rows (id, payload) VALUES (?, ?)",
				Args:      []any{id, payload},
			})
			if err != nil {
				t.Fatalf("cycle %d SQL pair %d: %v (floor=%d WAL=%d/%d)", cycle, pairs, err, n.core.CompactionFloor(), n.wal.Bytes(), n.wal.Capacity().Limit)
			}
			if sqlReceipt.Status != types.MutationCommitted || sqlReceipt.Slot == 0 {
				t.Fatalf("SQL receipt %q=%+v, want committed and nonzero slot", sqlID, sqlReceipt.MutationReceipt)
			}
			sqlElapsed := time.Since(sqlStarted)
			sqlIDs = append(sqlIDs, sqlID)
			sqlSlots = append(sqlSlots, sqlReceipt.Slot)

			graphID := "graph-" + id
			graphStarted := time.Now()
			graphReceipt, err := n.server.GraphExecute(ctx, types.GraphCommand{
				RequestID: graphID,
				Cypher:    fmt.Sprintf("CREATE (:LocalReclaimCycle {id: '%s'})", id),
				Events:    []types.GraphStreamEvent{{Stream: "local-reclaim-events", Kind: "created", Payload: id}},
			})
			if err != nil {
				t.Fatalf("cycle %d graph pair %d: %v (floor=%d WAL=%d/%d)", cycle, pairs, err, n.core.CompactionFloor(), n.wal.Bytes(), n.wal.Capacity().Limit)
			}
			if graphReceipt.Status != types.MutationCommitted || graphReceipt.Slot == 0 {
				t.Fatalf("graph receipt %q=%+v, want committed and nonzero slot", graphID, graphReceipt.MutationReceipt)
			}
			graphElapsed := time.Since(graphStarted)
			graphIDs = append(graphIDs, graphID)
			graphSlots = append(graphSlots, graphReceipt.Slot)
			pairs++
			t.Logf("cycle=%d pair=%d sql_elapsed=%s graph_elapsed=%s floor=%d wal_bytes=%d wal_limit=%d", cycle, pairs, sqlElapsed, graphElapsed, n.core.CompactionFloor(), n.wal.Bytes(), n.wal.Capacity().Limit)
		}
		floorAfter := n.core.CompactionFloor()
		if pairs == 0 || floorAfter <= floorBefore {
			t.Fatalf("cycle %d made no automatic reclamation progress after %d SQL+graph pairs: floor %d=>%d (WAL=%d/%d)", cycle, pairs, floorBefore, floorAfter, n.wal.Bytes(), n.wal.Capacity().Limit)
		}
		if used, limit := n.wal.Bytes(), n.wal.Capacity().Limit; used > limit || limit != walLimit {
			t.Fatalf("cycle %d WAL=%d/%d, want used<=fixed limit %d", cycle, used, limit, walLimit)
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		diskBytes, err := localReclamationTestDiskBytes(config.DataDir)
		if err != nil {
			t.Fatalf("cycle %d measure data directory: %v", cycle, err)
		}
		t.Logf("cycle=%d progress floor=%d=>%d pairs=%d elapsed=%s wal_bytes=%d disk_bytes=%d heap_alloc=%d heap_sys=%d total_alloc=%d", cycle, floorBefore, floorAfter, pairs, time.Since(cycleStarted), n.wal.Bytes(), diskBytes, mem.HeapAlloc, mem.HeapSys, mem.TotalAlloc)

		for index := len(sqlIDs) - pairs; index < len(sqlIDs); index++ {
			status, err := n.server.RequestStatus(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: sqlIDs[index]})
			if err != nil || status.State != string(types.MutationCommitted) || status.Receipt == nil || status.Receipt.Slot != sqlSlots[index] {
				t.Fatalf("cycle %d SQL receipt %q status=%+v err=%v", cycle, sqlIDs[index], status, err)
			}
			status, err = n.server.RequestStatus(ctx, network.RequestStatusRequest{Kind: "graph", RequestID: graphIDs[index]})
			if err != nil || status.State != string(types.MutationCommitted) || status.Receipt == nil || status.Receipt.Slot != graphSlots[index] {
				t.Fatalf("cycle %d graph receipt %q status=%+v err=%v", cycle, graphIDs[index], status, err)
			}
		}

		// A process-style close/open between cycles verifies the checkpointed
		// materialized state and retained request receipts, without changing cap.
		if err := n.Shutdown(); err != nil {
			t.Fatalf("cycle %d shutdown: %v", cycle, err)
		}
		n = New(&config)
		if err := n.Open(ctx); err != nil {
			t.Fatalf("cycle %d reopen at fixed WAL limit %d: %v", cycle, walLimit, err)
		}
		if got := n.wal.Capacity().Limit; got != walLimit {
			t.Fatalf("cycle %d reopened WAL limit=%d, want unchanged %d", cycle, got, walLimit)
		}
		if n.core.CompactionFloor() < floorAfter || n.wal.Bytes() > walLimit {
			t.Fatalf("cycle %d reopen floor=%d (want >=%d), WAL=%d/%d", cycle, n.core.CompactionFloor(), floorAfter, n.wal.Bytes(), n.wal.Capacity().Limit)
		}
	}

	sqlResult, err := n.server.Query(ctx, network.QueryRequest{SQL: "SELECT id, length(payload) FROM local_reclaim_rows ORDER BY id"})
	if err != nil {
		t.Fatalf("query SQL effects after cycles: %v", err)
	}
	if len(sqlResult.Rows) != len(sqlIDs) {
		t.Fatalf("SQL row count=%d, want %d unique effects", len(sqlResult.Rows), len(sqlIDs))
	}
	sqlCounts := make(map[string]int, len(sqlIDs))
	for _, row := range sqlResult.Rows {
		if len(row) != 2 || row[1] != int64(payloadLen) {
			t.Fatalf("SQL row=%v, want one id and payload length %d", row, payloadLen)
		}
		sqlCounts[fmt.Sprint(row[0])]++
	}
	graphResult, err := n.server.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:LocalReclaimCycle) RETURN n.id"})
	if err != nil {
		t.Fatalf("query graph effects after cycles: %v", err)
	}
	if len(graphResult.Rows) != len(graphIDs) {
		t.Fatalf("graph row count=%d, want %d unique effects", len(graphResult.Rows), len(graphIDs))
	}
	graphCounts := make(map[string]int, len(graphIDs))
	for _, row := range graphResult.Rows {
		if len(row) != 1 {
			t.Fatalf("graph row=%v, want one id column", row)
		}
		graphCounts[fmt.Sprint(row[0])]++
	}
	stream, err := n.server.GraphStreamRead(ctx, network.GraphStreamReadRequest{Stream: "local-reclaim-events", Limit: uint(len(graphIDs))})
	if err != nil {
		t.Fatalf("read graph events after cycles: %v", err)
	}
	if len(stream.Records) != len(graphIDs) {
		t.Fatalf("graph event count=%d, want %d unique events", len(stream.Records), len(graphIDs))
	}
	eventCounts := make(map[string]int, len(graphIDs))
	for _, record := range stream.Records {
		eventCounts[fmt.Sprint(record.Payload)]++
	}
	for index := range sqlIDs {
		id := strings.TrimPrefix(graphIDs[index], "graph-")
		if sqlCounts[id] != 1 || graphCounts[id] != 1 || eventCounts[id] != 1 {
			t.Fatalf("effect multiplicity for %q: SQL=%d graph=%d events=%d; want exactly once", id, sqlCounts[id], graphCounts[id], eventCounts[id])
		}
		for _, receipt := range []struct {
			kind, requestID string
			slot            uint64
		}{{"sql", sqlIDs[index], sqlSlots[index]}, {"graph", graphIDs[index], graphSlots[index]}} {
			status, err := n.server.RequestStatus(ctx, network.RequestStatusRequest{Kind: receipt.kind, RequestID: receipt.requestID})
			if err != nil || status.Receipt == nil || status.State != string(types.MutationCommitted) || status.Receipt.Slot != receipt.slot {
				t.Fatalf("retained %s receipt %q status=%+v err=%v", receipt.kind, receipt.requestID, status, err)
			}
		}
	}
	t.Logf("complete cycles=%d pairs=%d final_floor=%d core_tip=%d materializer_tip=%d wal_bytes=%d wal_limit=%d", cycleCount, len(sqlIDs), n.core.CompactionFloor(), n.core.Tip(), n.material.Tip(), n.wal.Bytes(), n.wal.Capacity().Limit)
}

func localReclamationTestDiskBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
