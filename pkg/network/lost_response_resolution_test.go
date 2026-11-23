package network

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A successful real execution can lose its response before the caller sees it.
func TestLostResponseResolvedOnAnotherVoter(t *testing.T) {
	cluster := newInMemoryThreePeerCluster(t, false)
	ctx := context.Background()
	if _, err := cluster.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE lost_response (n INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	cluster.applyAll(t)

	req := ExecuteRequest{RequestID: "lost-response", SQL: "INSERT INTO lost_response VALUES (1)"}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan int, 1)
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorded := httptest.NewRecorder()
		cluster.servers["n1"].ServeHTTP(recorded, r)
		completed <- recorded.Code
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(host.Close)
	response, err := host.Client().Post(host.URL+"/sql/execute", "application/json", bytes.NewReader(body))
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("expected response to be lost")
	}
	if code := <-completed; code != http.StatusOK {
		t.Fatalf("server execution status=%d", code)
	}

	cluster.applyAll(t)
	before := cluster.cores["n2"].Tip()
	result, err := cluster.servers["n2"].ResolveExecute(ctx, req)
	if err != nil || result.State != ResolveExecuteRetained || result.Receipt == nil {
		t.Fatalf("resolve via alternate voter: %+v, %v", result, err)
	}
	conflict := req
	conflict.SQL = "INSERT INTO lost_response VALUES (2)"
	if _, err := cluster.servers["n2"].ResolveExecute(ctx, conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("different payload: %v", err)
	}
	missing := req
	missing.RequestID = "not-submitted"
	unknown, err := cluster.servers["n2"].ResolveExecute(ctx, missing)
	if err != nil || unknown.State != ResolveExecuteUnknownOrExpired || unknown.Receipt != nil {
		t.Fatalf("missing request: %+v, %v", unknown, err)
	}
	if tip := cluster.cores["n2"].Tip(); tip != before {
		t.Fatalf("resolve submitted mutation: tip %d -> %d", before, tip)
	}
	rows, err := cluster.servers["n2"].Query(ctx, QueryRequest{SQL: "SELECT COUNT(*) FROM lost_response", Consistency: "local"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
		t.Fatalf("rows=%v err=%v", rows.Rows, err)
	}
}

func TestCommitUnknownResolvedOnAnotherVoter(t *testing.T) {
	cluster := newInMemoryThreePeerCluster(t, false)
	ctx := context.Background()
	if _, err := cluster.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE unknown_result (n INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "seed", SQL: "INSERT INTO unknown_result VALUES (0)"}); err != nil {
		t.Fatal(err)
	}
	cluster.applyAll(t)
	server := cluster.servers["n1"]
	original := server.sqlBatcher.propose
	var submitted quepaxa.Slot
	server.sqlBatcher.propose = func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
		slot, err := original(ctx, value)
		if err == nil && slot != 0 {
			submitted = slot
			if closeErr := cluster.material["n1"].Close(); closeErr != nil {
				return slot, closeErr
			}
		}
		return slot, err
	}
	req := ExecuteRequest{RequestID: "unknown-original", SQL: "UPDATE unknown_result SET n=n+1 RETURNING n", WantRows: true}
	_, err := server.ExecuteReturning(ctx, req)
	if !errors.Is(err, ErrCommitUnknown) || submitted == 0 {
		t.Fatalf("want post-submit unknown; slot=%d err=%v", submitted, err)
	}
	other := cluster.servers["n2"]
	if err := other.applyDecisions(ctx, submitted); err != nil {
		t.Fatalf("catch up alternate voter: %v", err)
	}
	before := cluster.cores["n2"].Tip()
	resolved, err := other.ResolveExecute(ctx, req)
	if err != nil || resolved.State != ResolveExecuteRetained || resolved.Receipt == nil || resolved.Receipt.Slot != uint64(submitted) {
		t.Fatalf("resolve original: %+v, %v", resolved, err)
	}
	if len(resolved.Statements) != 1 || len(resolved.Statements[0].Rows) != 1 || len(resolved.Statements[0].Rows[0]) != 1 || resolved.Statements[0].Rows[0][0] != int64(1) {
		t.Fatalf("original RETURNING result: %+v", resolved.Statements)
	}
	// The receipt remains inside the retention window in this controlled fixture.
	replay, err := other.ExecuteReturning(ctx, req)
	if err != nil || replay.MutationReceipt != *resolved.Receipt || !reflect.DeepEqual(replay.Statements, resolved.Statements) {
		t.Fatalf("retained replay=%+v resolution=%+v err=%v", replay, resolved, err)
	}
	conflict := req
	conflict.SQL = "INSERT INTO unknown_result VALUES (2)"
	if _, err := other.ResolveExecute(ctx, conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if got := cluster.cores["n2"].Tip(); got != before {
		t.Fatalf("resolution submitted mutation: %d -> %d", before, got)
	}
	rows, err := other.Query(ctx, QueryRequest{SQL: "SELECT n FROM unknown_result", Consistency: "local"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
		t.Fatalf("rows=%v err=%v", rows.Rows, err)
	}
}
