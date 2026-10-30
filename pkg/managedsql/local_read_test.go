package managedsql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestManagedSQLLocalReadsRemainReadyWithoutQuorum(t *testing.T) {
	configs, hosts, fixture := openSQLQuorum(t)
	h := hosts[0]
	ctx := context.Background()
	if _, err := h.Execute(ctx, ExecuteRequest{RequestID: "local-read-create", SQL: "CREATE TABLE local_read(value)"}); err != nil {
		t.Fatal(err)
	}
	if err := hosts[1].Close(); err != nil {
		t.Fatal(err)
	}
	checkLocal := func() {
		t.Helper()
		if err := h.Ready(ctx); err != nil {
			t.Fatalf("quorum loss removed local readiness: %v", err)
		}
		for _, consistency := range []string{"", "local"} {
			response, err := h.Query(ctx, QueryRequest{SQL: "SELECT COUNT(*) FROM local_read", Consistency: consistency})
			if err != nil || len(response.Rows) != 1 || len(response.Rows[0]) != 1 || fmt.Sprint(response.Rows[0][0]) != "0" || response.AppliedSlot > response.ConsensusTip {
				t.Fatalf("local consistency %q: %+v %v", consistency, response, err)
			}
		}
	}
	checkLocal()
	if _, err := h.Query(ctx, QueryRequest{SQL: "SELECT 1", Consistency: "unknown"}); err == nil {
		t.Fatal("unknown consistency downgraded to local")
	}
	linearCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	_, err := h.Query(linearCtx, QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"})
	cancel()
	if err == nil {
		t.Fatal("linearizable read succeeded without quorum")
	}
	writeCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	_, err = h.Execute(writeCtx, ExecuteRequest{RequestID: "must-not-reserve", SQL: "INSERT INTO local_read VALUES (1)"})
	cancel()
	if err == nil {
		t.Fatal("write succeeded without quorum")
	}
	checkLocal() // Failed write admission must not poison local reads.
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := a.Pending(); pending {
		t.Fatal("quorum failure reserved a write")
	}
	rejectLocal := func(reason string) {
		t.Helper()
		if h.Ready(ctx) == nil {
			t.Fatalf("%s allowed readiness", reason)
		}
		if _, err := h.Query(ctx, QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err == nil {
			t.Fatalf("%s allowed local SQL", reason)
		}
	}
	h.uncertain.Store(true)
	beforeRead, err := h.node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	checkLocal()
	afterRead, err := h.node.ManagedState()
	if err != nil || beforeRead != afterRead {
		t.Fatalf("local snapshot reads changed runtime state: before=%+v after=%+v err=%v", beforeRead, afterRead, err)
	}
	if !h.uncertain.Load() {
		t.Fatal("local reads cleared unknown outcome")
	}
	h.uncertain.Store(false)
	fixture.mu.Lock()
	original := append(json.RawMessage(nil), fixture.obj["status"]...)
	var status map[string]any
	if err := json.Unmarshal(original, &status); err != nil {
		fixture.mu.Unlock()
		t.Fatal(err)
	}
	status["authority"].(map[string]any)["evidence"] = map[string]any{"format": EvidenceFormat, "digest": strings.Repeat("0", 64)}
	fixture.obj["status"], err = json.Marshal(status)
	fixture.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	checkLocal() // Newer canonical evidence cannot invalidate the old snapshot.
	fixture.mu.Lock()
	fixture.obj["status"] = original
	fixture.mu.Unlock()
	checkLocal()
	// Simulate actual local corruption through a second SQLite connection;
	// unlike canonical evidence drift, an invalid internal schema must close reads.
	db, err := sql.Open("sqlite3", filepath.Join(configs[0].Runtime.DataDir, "sqlite.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := db.ExecContext(ctx, "CREATE TABLE _rhiza_corrupt_internal(value)"); err != nil {
		t.Fatal(err)
	}
	rejectLocal("corrupted local internal schema")
	if _, err := db.ExecContext(ctx, "DROP TABLE _rhiza_corrupt_internal"); err != nil {
		t.Fatal(err)
	}
	checkLocal()
	fixture.mu.Lock()
	if err := json.Unmarshal(original, &status); err != nil {
		fixture.mu.Unlock()
		t.Fatal(err)
	}
	status["authority"].(map[string]any)["binding"].(map[string]any)["storageID"] = "successor-storage"
	fixture.obj["status"], err = json.Marshal(status)
	fixture.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	rejectLocal("stale storage binding")
	fixture.mu.Lock()
	fixture.obj["status"] = original
	fixture.mu.Unlock()
	checkLocal()
	a, err = operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	reserved, outcome, err := operator.ReserveV1Alpha2SQLWrite(ctx, configs[0].Store, a, a.Authority(), a.Evidence(), ExecuteRequest{RequestID: "pending-local-negative", SQL: "INSERT INTO local_read VALUES (2)"})
	if err != nil || outcome != operator.V1Alpha2CASApplied {
		t.Fatalf("real pending reservation: %v %v", outcome, err)
	}
	pending, ok := reserved.Pending()
	if !ok {
		t.Fatal("reservation missing")
	}
	h.gateMu.Lock()
	h.active = &pending
	h.gateMu.Unlock()
	fixture.mu.Lock()
	pendingStatus := append(json.RawMessage(nil), fixture.obj["status"]...)
	fixture.mu.Unlock()
	h.uncertain.Store(true)
	checkLocal()
	fixture.mu.Lock()
	unchanged := bytes.Equal(pendingStatus, fixture.obj["status"])
	fixture.mu.Unlock()
	if !unchanged || !h.uncertain.Load() {
		t.Fatal("local reads mutated pending/evidence/unknown state")
	}
	if _, err := h.node.API(); err == nil {
		t.Fatal("raw managed API exposed")
	}
}
