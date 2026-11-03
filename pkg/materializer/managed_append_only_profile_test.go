package materializer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedAppendOnlyRequestMaterializerProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "request.sqlite")
	m, err := OpenManagedSQLKVWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if m.MaterializationPolicy() != sqlpolicy.ManagedRequestMaterializationPolicy || m.managedProtocol != sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol || m.graph != nil {
		t.Fatalf("request append-only profile: policy=%d protocol=%q graph=%v", m.MaterializationPolicy(), m.managedProtocol, m.graph != nil)
	}
	command := types.SQLCommand{RequestID: "append-only-request", SQL: "CREATE TABLE request_profile(id INTEGER PRIMARY KEY)"}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{command})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	wantItem, err := types.EncodeSQLBatchItem(command)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := m.RetainedRequestOriginal(context.Background(), types.MutationSQL, command.RequestID)
	if err != nil || !found || got.Slot != 1 || !bytes.Equal(got.ExecutionItem, wantItem) {
		t.Fatalf("retained request changed: found=%v original=%+v err=%v", found, got, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := InspectManagedAppendOnlyRequestFile(context.Background(), path, 1024); err != nil {
		t.Fatalf("inspect exact append-only request profile: %v", err)
	}
	if _, _, err := InspectManagedRequestFile(context.Background(), path, 1024); err == nil {
		t.Fatal("old request protocol accepted append-only materializer bytes")
	}
	if _, err := OpenManagedSQLKVWithOriginals(path, 1, 1024); err == nil {
		t.Fatal("old request constructor accepted append-only materializer bytes")
	}
	reopened, err := OpenManagedSQLKVWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, found, err := reopened.RetainedRequestOriginal(context.Background(), types.MutationSQL, command.RequestID)
	if err != nil || !found || !bytes.Equal(again.ExecutionItem, wantItem) || again.Slot != got.Slot {
		t.Fatalf("reopened original changed: found=%v original=%+v err=%v", found, again, err)
	}
}

func TestManagedAppendOnlyGraphMaterializerProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.sqlite")
	m, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if m.MaterializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy || m.managedProtocol != sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol || m.graph == nil {
		t.Fatalf("Graph append-only profile: policy=%d protocol=%q graph=%v", m.MaterializationPolicy(), m.managedProtocol, m.graph != nil)
	}
	command := types.GraphCommand{RequestID: "append-only-graph", Cypher: "CREATE (:AppendOnlyProfile {id: 1})"}
	value, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	rows, err := m.GraphQuery(context.Background(), "MATCH (n:AppendOnlyProfile) RETURN n.id", nil)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("append-only Graph query failed: rows=%v err=%v", rows.Rows, err)
	}
	original, found, err := m.RetainedRequestOriginal(context.Background(), types.MutationGraph, command.RequestID)
	if err != nil || !found || original.Graph == nil || !bytes.Equal(original.ExecutionItem, value) {
		t.Fatalf("retained Graph original changed: found=%v original=%+v err=%v", found, original, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := InspectManagedAppendOnlyGraphSQLFile(context.Background(), path, 1024); err != nil {
		t.Fatalf("inspect exact append-only Graph profile: %v", err)
	}
	if _, _, err := InspectManagedGraphSQLFile(context.Background(), path, 1024); err == nil {
		t.Fatal("old Graph protocol accepted append-only materializer bytes")
	}
	if _, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024); err == nil {
		t.Fatal("old Graph constructor accepted append-only materializer bytes")
	}
	reopened, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, found, err := reopened.RetainedRequestOriginal(context.Background(), types.MutationGraph, command.RequestID)
	if err != nil || !found || again.Graph == nil || !bytes.Equal(again.ExecutionItem, value) {
		t.Fatalf("reopened Graph original changed: found=%v original=%+v err=%v", found, again, err)
	}
}

func TestManagedAppendOnlyMaterializerRejectsUnknownAndInvalidWindows(t *testing.T) {
	for _, window := range []uint64{1023, 1_048_577} {
		t.Run("window", func(t *testing.T) {
			if _, err := OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "bad.sqlite"), 1, window); err == nil {
				t.Fatalf("invalid receipt window %d accepted", window)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "unknown.sqlite")
	if _, err := openWithProtocol(path, 1, "rhiza-managed-request-append-only/v999", 1024); err == nil {
		t.Fatal("unknown managed protocol accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unknown protocol changed the target: stat err=%v", err)
	}
	if _, _, err := InspectManagedAppendOnlyRequestFile(context.Background(), filepath.Join(t.TempDir(), "missing.sqlite"), 1024); err == nil {
		t.Fatal("missing append-only request file accepted")
	}
	maximum, err := OpenManagedSQLKVWithAppendOnlyOriginals(path, 1, 1_048_576)
	if err != nil {
		t.Fatalf("maximum supported request window rejected: %v", err)
	}
	if maximum.idempotencyWindow != 1_048_576 {
		t.Fatalf("maximum request window changed: got=%d", maximum.idempotencyWindow)
	}
	if err := maximum.Close(); err != nil {
		t.Fatal(err)
	}
	legacyPath := filepath.Join(t.TempDir(), "legacy.sqlite")
	legacy, err := OpenManagedSQLKV(legacyPath, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenManagedSQLKVWithAppendOnlyOriginals(legacyPath, 1, 1024); err == nil {
		t.Fatal("append-only profile adopted a different managed profile")
	}
	graphPath := filepath.Join(t.TempDir(), "mixed.sqlite")
	graph, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(graphPath, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenManagedSQLKVWithAppendOnlyOriginals(graphPath, 1, 1024); err == nil {
		t.Fatal("request append-only profile accepted existing Graph artifacts")
	}
}
