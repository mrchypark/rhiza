package node

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	localOverwriteWALLimit = int64(2 << 20)
	localOverwritePayload  = 64 << 10
	localOverwriteItems    = 4
	localOverwritePairs    = 6
	localOverwriteCycles   = 3
)

type localOverwriteRequest struct {
	DataDir   string
	Cycle     int
	ReadyFile string
}

type localOverwriteReceipt struct {
	Kind      string
	RequestID string
	Slot      uint64
	Item      int
	Payload   string
}

type localOverwriteMarker struct {
	Cycle      int
	FloorStart quepaxa.Slot
	FloorEnd   quepaxa.Slot
	Pairs      int
	Writes     []localOverwriteReceipt
}

func TestLocalReclamationPreservesFixedCardinalityOverwritesAcrossHardKills(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	latestSQL := make(map[int]string, localOverwriteItems)
	latestGraph := make(map[int]string, localOverwriteItems)
	allWrites := make([]localOverwriteReceipt, 0, localOverwriteCycles*localOverwritePairs*2)
	expectedGraphEvents := localOverwriteItems // the phase-zero seed nodes each publish one event.
	var previousFloor quepaxa.Slot
	var cumulativeValueBytes int64
	var retainedRetrySQL, retainedRetryGraph localOverwriteReceipt

	for cycle := 0; cycle < localOverwriteCycles; cycle++ {
		readyFile := filepath.Join(t.TempDir(), "acknowledged-overwrites.json")
		request, err := json.Marshal(localOverwriteRequest{DataDir: dataDir, Cycle: cycle, ReadyFile: readyFile})
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestLocalReclamationOverwriteHelper$")
		cmd.Env = append(os.Environ(), "RHIZA_LOCAL_RECLAMATION_OVERWRITE="+string(request))
		startLocalNodeCrashChild(t, cmd, readyFile)
		waitForLocalNodeCrashBoundary(t, cmd, readyFile)

		markerBytes, err := os.ReadFile(readyFile)
		if err != nil {
			t.Fatalf("read phase %d overwrite marker: %v", cycle, err)
		}
		var marker localOverwriteMarker
		if err := json.Unmarshal(markerBytes, &marker); err != nil {
			t.Fatalf("decode phase %d overwrite marker: %v", cycle, err)
		}
		if marker.Cycle != cycle || marker.Pairs < localOverwritePairs || marker.FloorEnd <= marker.FloorStart {
			t.Fatalf("phase %d marker=%+v, want acknowledged overwrites and floor progress", cycle, marker)
		}

		for _, write := range marker.Writes {
			if write.Slot == 0 || write.Item < 0 || write.Item >= localOverwriteItems || len(write.Payload) != localOverwritePayload {
				t.Fatalf("phase %d invalid acknowledged write: %+v", cycle, write)
			}
			if write.Kind == "sql" {
				latestSQL[write.Item] = write.Payload
			} else if write.Kind == "graph" {
				latestGraph[write.Item] = write.Payload
				expectedGraphEvents++
			} else {
				t.Fatalf("phase %d unknown receipt kind %q", cycle, write.Kind)
			}
			cumulativeValueBytes += int64(len(write.Payload))
			allWrites = append(allWrites, write)
		}
		if len(marker.Writes) != marker.Pairs*2 {
			t.Fatalf("phase %d writes=%d for %d SQL+graph pairs", cycle, len(marker.Writes), marker.Pairs)
		}
		if cycle == 0 {
			for _, write := range marker.Writes {
				if write.Kind == "sql" && strings.HasPrefix(write.RequestID, "overwrite-sql-0-0-") {
					retainedRetrySQL = write
				}
				if write.Kind == "graph" && strings.HasPrefix(write.RequestID, "overwrite-graph-0-0-") {
					retainedRetryGraph = write
				}
			}
			if retainedRetrySQL.Slot == 0 || retainedRetryGraph.Slot == 0 {
				t.Fatalf("phase 0 missing immutable retry receipts: SQL=%+v graph=%+v", retainedRetrySQL, retainedRetryGraph)
			}
		}

		config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: dataDir, MaxWALBytes: localOverwriteWALLimit}
		n := New(&config)
		if err := n.Open(ctx); err != nil {
			t.Fatalf("phase %d reopen after hard kill: %v", cycle, err)
		}
		if got := n.wal.Capacity().Limit; got != localOverwriteWALLimit {
			_ = n.Shutdown()
			t.Fatalf("phase %d reopened WAL limit=%d, want unchanged %d", cycle, got, localOverwriteWALLimit)
		}
		if got := n.core.CompactionFloor(); got < marker.FloorEnd || got <= previousFloor {
			_ = n.Shutdown()
			t.Fatalf("phase %d recovered floor=%d, marker floor=%d previous floor=%d", cycle, got, marker.FloorEnd, previousFloor)
		}
		if used := n.wal.Bytes(); used > localOverwriteWALLimit {
			_ = n.Shutdown()
			t.Fatalf("phase %d recovered WAL bytes=%d exceed fixed limit %d", cycle, used, localOverwriteWALLimit)
		}
		previousFloor = n.core.CompactionFloor()

		assertLocalOverwriteState(t, n.server, latestSQL, latestGraph)
		for _, write := range allWrites {
			status, err := n.server.RequestStatus(ctx, network.RequestStatusRequest{Kind: write.Kind, RequestID: write.RequestID})
			if err != nil || status.State != string(types.MutationCommitted) || status.Receipt == nil || status.Receipt.Slot != write.Slot {
				_ = n.Shutdown()
				t.Fatalf("phase %d recovered %s acknowledgement %q=%+v err=%v, want slot %d", cycle, write.Kind, write.RequestID, status, err, write.Slot)
			}
		}

		// Replaying the same immutable successful requests must return their
		// original receipts without appending another application effect/event.
		if cycle > 0 {
			beforeEvents := localOverwriteEventCount(t, n.server, expectedGraphEvents)
			sqlRetry, err := n.server.Execute(ctx, localOverwriteSQLRequest(retainedRetrySQL))
			if err != nil || sqlRetry.Slot != retainedRetrySQL.Slot {
				_ = n.Shutdown()
				t.Fatalf("cycle %d exact SQL retry=%+v err=%v, want original slot %d", cycle, sqlRetry.MutationReceipt, err, retainedRetrySQL.Slot)
			}
			graphRetry, err := n.server.GraphExecute(ctx, localOverwriteGraphRequest(retainedRetryGraph))
			if err != nil || graphRetry.Slot != retainedRetryGraph.Slot {
				_ = n.Shutdown()
				t.Fatalf("cycle %d exact graph retry=%+v err=%v, want original slot %d", cycle, graphRetry.MutationReceipt, err, retainedRetryGraph.Slot)
			}
			if got := localOverwriteEventCount(t, n.server, expectedGraphEvents); got != beforeEvents {
				_ = n.Shutdown()
				t.Fatalf("cycle %d exact graph retry changed event count %d=>%d", cycle, beforeEvents, got)
			}
			assertLocalOverwriteState(t, n.server, latestSQL, latestGraph)
		}
		walBytes := n.wal.Bytes()
		if err := n.Shutdown(); err != nil {
			t.Fatalf("phase %d close recovered node: %v", cycle, err)
		}
		t.Logf("cycle=%d hard_kill=signal floor=%d wal_bytes=%d/%d acknowledged_pairs=%d", cycle, previousFloor, walBytes, localOverwriteWALLimit, marker.Pairs)
	}

	if cumulativeValueBytes <= localOverwriteWALLimit {
		t.Fatalf("cumulative overwritten value bytes=%d, want more than fixed WAL cap %d", cumulativeValueBytes, localOverwriteWALLimit)
	}
	t.Logf("complete cycles=%d fixed_sql_rows=%d fixed_graph_nodes=%d overwrite_pairs=%d cumulative_value_bytes=%d fixed_wal_limit=%d graph_events=%d final_floor=%d", localOverwriteCycles, localOverwriteItems, localOverwriteItems, len(allWrites)/2, cumulativeValueBytes, localOverwriteWALLimit, expectedGraphEvents, previousFloor)
}

func TestLocalReclamationOverwriteHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_RECLAMATION_OVERWRITE")
	if data == "" {
		return
	}
	var request localOverwriteRequest
	if err := json.Unmarshal([]byte(data), &request); err != nil {
		t.Fatal(err)
	}
	config := types.ExecutionConfig{Local: true, NodeID: "local", DataDir: request.DataDir, MaxWALBytes: localOverwriteWALLimit}
	n := New(&config)
	if err := n.Open(context.Background()); err != nil {
		t.Fatalf("open overwrite phase %d: %v", request.Cycle, err)
	}
	if request.Cycle == 0 {
		if _, err := n.server.Execute(context.Background(), network.ExecuteRequest{RequestID: "overwrite-schema", SQL: "CREATE TABLE overwrite_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"}); err != nil {
			t.Fatalf("create overwrite table: %v", err)
		}
		for item := 0; item < localOverwriteItems; item++ {
			payload := strings.Repeat("s", localOverwritePayload)
			if _, err := n.server.Execute(context.Background(), network.ExecuteRequest{RequestID: fmt.Sprintf("overwrite-seed-sql-%d", item), SQL: "INSERT INTO overwrite_rows(id, payload) VALUES(?, ?)", Args: []any{item, payload}}); err != nil {
				t.Fatalf("seed SQL row %d: %v", item, err)
			}
			if _, err := n.server.GraphExecute(context.Background(), types.GraphCommand{RequestID: fmt.Sprintf("overwrite-seed-graph-%d", item), Cypher: "CREATE (:LocalReclaimOverwrite {id: $id, payload: $payload})", Args: map[string]any{"id": int64(item), "payload": payload}, Events: []types.GraphStreamEvent{{Stream: "local-reclaim-overwrite", Kind: "created", Payload: fmt.Sprintf("seed-%d", item)}}}); err != nil {
				t.Fatalf("seed graph node %d: %v", item, err)
			}
		}
	}
	floorStart := n.core.CompactionFloor()
	marker := localOverwriteMarker{Cycle: request.Cycle, FloorStart: floorStart}
	for pair := 0; pair < localOverwritePairs; pair++ {
		item := pair % localOverwriteItems
		payload := localOverwriteValue(request.Cycle, pair, item)
		sqlWrite := localOverwriteReceipt{Kind: "sql", RequestID: fmt.Sprintf("overwrite-sql-%d-%d-%d", request.Cycle, pair, item), Item: item, Payload: payload}
		sqlReceipt, err := n.server.Execute(context.Background(), localOverwriteSQLRequest(sqlWrite))
		if err != nil || sqlReceipt.Status != types.MutationCommitted || sqlReceipt.Slot == 0 {
			t.Fatalf("cycle %d SQL overwrite pair %d receipt=%+v err=%v", request.Cycle, pair, sqlReceipt.MutationReceipt, err)
		}
		sqlWrite.Slot = sqlReceipt.Slot
		marker.Writes = append(marker.Writes, sqlWrite)

		graphWrite := localOverwriteReceipt{Kind: "graph", RequestID: fmt.Sprintf("overwrite-graph-%d-%d-%d", request.Cycle, pair, item), Item: item, Payload: payload}
		graphReceipt, err := n.server.GraphExecute(context.Background(), localOverwriteGraphRequest(graphWrite))
		if err != nil || graphReceipt.Status != types.MutationCommitted || graphReceipt.Slot == 0 {
			t.Fatalf("cycle %d graph overwrite pair %d receipt=%+v err=%v", request.Cycle, pair, graphReceipt.MutationReceipt, err)
		}
		graphWrite.Slot = graphReceipt.Slot
		marker.Writes = append(marker.Writes, graphWrite)
		marker.Pairs++
	}
	marker.FloorEnd = n.core.CompactionFloor()
	if marker.FloorEnd <= floorStart {
		t.Fatalf("cycle %d acknowledged %d overwrite pairs without floor progress: %d=>%d WAL=%d/%d", request.Cycle, marker.Pairs, floorStart, marker.FloorEnd, n.wal.Bytes(), n.wal.Capacity().Limit)
	}
	markerBytes, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	markerTemp := request.ReadyFile + ".tmp"
	if err := os.WriteFile(markerTemp, markerBytes, 0o600); err != nil {
		t.Fatalf("write acknowledged overwrite marker: %v", err)
	}
	if err := os.Rename(markerTemp, request.ReadyFile); err != nil {
		t.Fatalf("publish acknowledged overwrite marker: %v", err)
	}
	select {}
}

func localOverwriteValue(cycle, pair, item int) string {
	prefix := fmt.Sprintf("cycle=%d,pair=%d,item=%d;", cycle, pair, item)
	return prefix + strings.Repeat("v", localOverwritePayload-len(prefix))
}

func localOverwriteSQLRequest(write localOverwriteReceipt) network.ExecuteRequest {
	return network.ExecuteRequest{RequestID: write.RequestID, SQL: "UPDATE overwrite_rows SET payload = ? WHERE id = ?", Args: []any{write.Payload, write.Item}}
}

func localOverwriteGraphRequest(write localOverwriteReceipt) types.GraphCommand {
	return types.GraphCommand{
		RequestID: write.RequestID,
		Cypher:    "MATCH (n:LocalReclaimOverwrite {id: $id}) SET n.payload = $payload",
		Args:      map[string]any{"id": int64(write.Item), "payload": write.Payload},
		Events:    []types.GraphStreamEvent{{Stream: "local-reclaim-overwrite", Kind: "updated", Payload: write.RequestID}},
	}
}

func assertLocalOverwriteState(t *testing.T, server *network.Server, sqlValues, graphValues map[int]string) {
	t.Helper()
	sqlResult, err := server.Query(context.Background(), network.QueryRequest{SQL: "SELECT id, payload FROM overwrite_rows ORDER BY id"})
	if err != nil {
		t.Fatalf("query recovered fixed SQL rows: %v", err)
	}
	if len(sqlResult.Rows) != localOverwriteItems {
		t.Fatalf("SQL row count=%d, want fixed cardinality %d", len(sqlResult.Rows), localOverwriteItems)
	}
	seenSQL := make(map[int]bool, localOverwriteItems)
	for _, row := range sqlResult.Rows {
		if len(row) != 2 {
			t.Fatalf("SQL row=%v, want id and payload", row)
		}
		id := int(row[0].(int64))
		payload, ok := row[1].(string)
		if !ok || payload != sqlValues[id] || seenSQL[id] {
			t.Fatalf("SQL row id=%d payload_match=%t duplicate=%t", id, ok && payload == sqlValues[id], seenSQL[id])
		}
		seenSQL[id] = true
	}
	for id := 0; id < localOverwriteItems; id++ {
		if !seenSQL[id] || sqlValues[id] == "" {
			t.Fatalf("SQL fixed row %d missing expected latest value", id)
		}
	}

	graphResult, err := server.GraphQuery(context.Background(), network.GraphQueryRequest{Cypher: "MATCH (n:LocalReclaimOverwrite) RETURN n.id, n.payload ORDER BY n.id"})
	if err != nil {
		t.Fatalf("query recovered fixed graph nodes: %v", err)
	}
	if len(graphResult.Rows) != localOverwriteItems {
		t.Fatalf("graph node count=%d, want fixed cardinality %d", len(graphResult.Rows), localOverwriteItems)
	}
	seenGraph := make(map[int]bool, localOverwriteItems)
	for _, row := range graphResult.Rows {
		if len(row) != 2 {
			t.Fatalf("graph row=%v, want id and payload", row)
		}
		var id int
		switch value := row[0].(type) {
		case int64:
			id = int(value)
		case float64:
			id = int(value)
		default:
			t.Fatalf("graph id type=%T value=%v, want numeric ID", row[0], row[0])
		}
		payload, ok := row[1].(string)
		if !ok || payload != graphValues[id] || seenGraph[id] {
			t.Fatalf("graph node id=%d payload_match=%t duplicate=%t", id, ok && payload == graphValues[id], seenGraph[id])
		}
		seenGraph[id] = true
	}
	for id := 0; id < localOverwriteItems; id++ {
		if !seenGraph[id] || graphValues[id] == "" {
			t.Fatalf("graph fixed node %d missing expected latest value", id)
		}
	}
}

func localOverwriteEventCount(t *testing.T, server *network.Server, want int) int {
	t.Helper()
	stream, err := server.GraphStreamRead(context.Background(), network.GraphStreamReadRequest{Stream: "local-reclaim-overwrite", Limit: uint(want + 10)})
	if err != nil {
		t.Fatalf("read overwrite event stream: %v", err)
	}
	if len(stream.Records) != want {
		t.Fatalf("graph event count=%d, want %d successful graph writes", len(stream.Records), want)
	}
	return len(stream.Records)
}
