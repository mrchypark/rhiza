//go:build cgo

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func managedFFIGraphContractCommand(run string) types.GraphCommand {
	return types.GraphCommand{RequestID: string([]byte{0xff}) + run, Cypher: "CREATE (:ManagedFFIItem {run: $run, value: 'ffi-graph-live'})", Args: map[string]any{"run": run}}
}

func managedFFIGraphContractQuery(run string) network.GraphQueryRequest {
	return network.GraphQueryRequest{Cypher: "MATCH (n:ManagedFFIItem {run: $run}) RETURN n.run,n.value", Args: map[string]any{"run": run}, Consistency: "linearizable"}
}

// This contract runs as the replacement Pod's main process, not beside a live
// owner of its WAL. The existing identity/await init containers supply the real
// files; surviving managed voters perform the actual certified membership work.
func TestManagedFFIInClusterSQLLifecycle(t *testing.T) {
	runManagedFFIInClusterLifecycle(t, false)
}

func TestManagedFFIInClusterMixedLifecycle(t *testing.T) {
	runManagedFFIInClusterLifecycle(t, true)
}

func TestManagedFFIInClusterGraphLifecycle(t *testing.T) {
	runManagedFFIInClusterLifecycle(t, false, true)
}

func runManagedFFIInClusterLifecycle(t *testing.T, mixed bool, graphMode ...bool) {
	if os.Getenv("RHIZA_FFI_IN_CLUSTER_CONTRACT") != "1" {
		t.Skip("requires the actual in-cluster managed replacement fixture; no local Host substitute")
	}
	paths := []string{"/var/lib/rhiza/identity.json", "/var/lib/rhiza/bootstrap.json", "/var/lib/rhiza/repair.json"}
	before := make(map[string][32]byte)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("required current replacement input unavailable: %s", filepath.Base(path))
		}
		before[path] = sha256.Sum256(data)
	}
	// This is the production FFI opener: actual NewInCluster, shared validation,
	// SDK source verifier, managedsql.Open, registration, WAL, and promotion gate.
	var opened struct {
		Data struct {
			Handle uint64 `json:"handle"`
		} `json:"data"`
		Error *ffiError `json:"error"`
	}
	if err := json.Unmarshal(goOpenManaged([]byte(`{}`)), &opened); err != nil {
		t.Fatal("invalid FFI open response")
	}
	if opened.Error != nil {
		t.Fatalf("real managed FFI open failed, code=%s message=%s", opened.Error.Code, opened.Error.Message)
	}
	handle := opened.Data.Handle
	if handle == 0 {
		t.Fatal("real managed FFI open returned no handle")
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = goClose(handle)
		}
	})
	invoke := func(operation string, request any) json.RawMessage {
		t.Helper()
		wire, err := json.Marshal(map[string]any{"operation": operation, "request": request})
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Data  json.RawMessage `json:"data"`
			Error *ffiError       `json:"error"`
		}
		if err := json.Unmarshal(goCall(handle, wire, 30000), &result); err != nil {
			t.Fatal("invalid FFI call response")
		}
		if result.Error != nil {
			t.Fatalf("real managed FFI %s failed, code=%s (no mutation retry)", operation, result.Error.Code)
		}
		return result.Data
	}
	deadline := time.Now().Add(75 * time.Second)
	kube, err := operator.NewInCluster(os.Getenv("POD_NAMESPACE"))
	if err != nil {
		t.Fatal("actual in-cluster status client unavailable")
	}
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	ref := operator.V1Alpha2ClusterRef{Name: os.Getenv("RHIZA_CLUSTER_NAME"), Namespace: os.Getenv("POD_NAMESPACE"), UID: os.Getenv("RHIZA_CLUSTER_UID")}
	for {
		var ready bool
		if err := json.Unmarshal(invoke("ready", struct{}{}), &ready); err != nil {
			t.Fatal("invalid managed readiness response")
		}
		statusCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		admission, statusErr := operator.LoadV1Alpha2Admission(statusCtx, store, ref)
		cancel()
		if statusErr != nil {
			t.Fatal("actual repair status could not be verified")
		}
		repair, exists := admission.Repair()
		if !exists || repair.Plan.Replacement.PodUID != os.Getenv("POD_UID") {
			t.Fatal("actual closed repair candidate changed during FFI admission")
		}
		if ready && repair.State == "Completed" && !admission.SQLReservationsClosed() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actual replacement never reached managed application admission")
		}
		time.Sleep(100 * time.Millisecond) // Read-only readiness polling, not mutation retry.
	}
	entry, release, err := acquire(handle)
	if err != nil {
		t.Fatal(err)
	}
	if entry.managed == nil || entry.db != nil {
		release()
		t.Fatal("FFI open did not own an actual managed Host")
	}
	release()
	identityPath := "/var/lib/rhiza/data/qlog/voter-identity.json"
	registered, err := os.ReadFile(identityPath)
	if err != nil {
		t.Fatal("actual registered voter identity missing after admission")
	}
	var registration struct {
		Registered bool   `json:"registered"`
		Nonce      string `json:"nonce"`
	}
	if json.Unmarshal(registered, &registration) != nil || !registration.Registered || len(registration.Nonce) != 64 {
		t.Fatal("actual voter registration is invalid")
	}
	requestID := fmt.Sprintf("ffi-contract-%d", time.Now().UnixNano())
	if !committedFFIReceipt(invoke("execute", map[string]any{"request_id": requestID + "-schema", "sql": "CREATE TABLE IF NOT EXISTS managed_ffi_contract (id TEXT PRIMARY KEY, value TEXT NOT NULL)"})) {
		t.Fatal("schema mutation did not return a committed receipt; no retry")
	}
	if !committedFFIReceipt(invoke("execute", map[string]any{"request_id": requestID + "-insert", "sql": "INSERT INTO managed_ffi_contract (id, value) VALUES (?, ?)", "args": []any{requestID, "ffi-live"}})) {
		t.Fatal("insert mutation did not return a committed receipt; no retry")
	}
	var query struct {
		Rows [][]string `json:"rows"`
	}
	if json.Unmarshal(invoke("query", map[string]any{"sql": "SELECT id, value FROM managed_ffi_contract WHERE id = ?", "args": []any{requestID}, "consistency": "linearizable"}), &query) != nil || len(query.Rows) != 1 || len(query.Rows[0]) != 2 || query.Rows[0][0] != requestID || query.Rows[0][1] != "ffi-live" {
		t.Fatal("real FFI SQL ACK readback mismatch")
	}
	if mixed {
		original := types.KVCommand{RequestID: string([]byte{0xfd}) + requestID, Operation: "put", Key: string([]byte{0xfc}) + requestID, Value: []byte("ffi-live"), Expected: []byte{}, TTLMS: 0}
		raw, _, err := types.EncodeManagedKVOriginal(original)
		if err != nil {
			t.Fatal("invalid mixed FFI original")
		}
		var receipt managedKVReceipt
		if json.Unmarshal(invoke("kv_put", raw), &receipt) != nil || receipt.Slot == 0 || receipt.Status != types.MutationCommitted || !receipt.Applied {
			t.Fatal("mixed FFI ACK unavailable")
		}
		var found managedKVRead
		if json.Unmarshal(invoke("kv_get", map[string]any{"key_b64": []byte(original.Key), "consistency": "linearizable"}), &found) != nil || !found.Found || !bytes.Equal(found.Value, original.Value) || found.AppliedSlot < receipt.Slot {
			t.Fatal("mixed FFI ACK readback mismatch")
		}
	}
	graph := len(graphMode) == 1 && graphMode[0]
	if graph {
		original := managedFFIGraphContractCommand(requestID)
		frame, err := types.EncodeManagedGraphExecution(original)
		if err != nil {
			t.Fatal(err)
		}
		var receipt managedKVReceipt
		if json.Unmarshal(invoke("graph_execute", map[string]any{"frame_b64": frame}), &receipt) != nil || receipt.Slot == 0 || receipt.Status != types.MutationCommitted || !receipt.Applied {
			t.Fatal("actual Graph FFI ACK unavailable; no mutation retry")
		}
		var rows struct {
			Rows [][]any `json:"rows"`
		}
		if json.Unmarshal(invoke("graph_query", managedFFIGraphContractQuery(requestID)), &rows) != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 2 || rows.Rows[0][0] != requestID || rows.Rows[0][1] != "ffi-graph-live" {
			t.Fatal("actual Graph FFI ACK readback mismatch")
		}
		selector := map[string]any{"kind": "graph", "request_id_b64": []byte(original.RequestID)}
		var status struct {
			State   string            `json:"state"`
			Receipt *managedKVReceipt `json:"receipt"`
		}
		if json.Unmarshal(invoke("request_status", selector), &status) != nil || status.State != string(types.MutationCommitted) || status.Receipt == nil || *status.Receipt != receipt {
			t.Fatal("actual Graph FFI status changed original receipt")
		}
		var recovered struct {
			Kind      string            `json:"kind"`
			RequestID []byte            `json:"request_id_b64"`
			Graph     *managedKVReceipt `json:"graph"`
		}
		if json.Unmarshal(invoke("recover_pending_detailed", selector), &recovered) != nil || recovered.Kind != "graph" || !bytes.Equal(recovered.RequestID, []byte(original.RequestID)) || recovered.Graph == nil || *recovered.Graph != receipt {
			t.Fatal("actual Graph FFI recovery changed raw ID or receipt")
		}
	}
	var closeResult struct {
		Error *ffiError `json:"error"`
	}
	if json.Unmarshal(goClose(handle), &closeResult) != nil || closeResult.Error != nil {
		t.Fatal("real managed FFI close failed")
	}
	closed = true
	if code := responseErrorCode(t, goCall(handle, []byte(`{"operation":"ready","request":{}}`), 1000)); code != "invalid_handle" {
		t.Fatal("closed managed FFI handle remained usable")
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil || sha256.Sum256(data) != before[path] {
			t.Fatalf("FFI startup/SQL/close changed immutable input: %s", filepath.Base(path))
		}
	}
	after, err := os.ReadFile(identityPath)
	if err != nil || !bytes.Equal(registered, after) {
		t.Fatal("FFI SQL/close replaced the actual voter registration")
	}
	if graph {
		t.Log("actual in-cluster Go FFI -> Graph managed Host -> raw original ACK/readback/status/recovery -> close passed")
	} else if mixed {
		t.Log("actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed")
	} else {
		t.Log("actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed; no Rust or physical-fencing claim")
	}
}

// This exercises the same two command/query builders against actual certified
// Core/WAL/native Graph state, not a substitute Pod or in-cluster Host.
func TestManagedFFIGraphContractTwoRunsRetainOwnRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "one", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "one"}}}, WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mat.Close() })
	server, err := network.NewServerForPolicy(core, mat, "graph-contract", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	runs := []string{"ffi-contract-first", "ffi-contract-restart"}
	receipts := make(map[string]types.MutationReceipt)
	for _, run := range runs {
		command := managedFFIGraphContractCommand(run)
		response, err := server.GraphExecute(ctx, command)
		if err != nil || response.Status != types.MutationCommitted || response.Slot == 0 || !response.Applied {
			t.Fatalf("actual run receipt=%+v error=%v", response, err)
		}
		receipts[command.RequestID] = response.MutationReceipt
	}
	for _, run := range runs {
		rows, err := server.GraphQuery(ctx, managedFFIGraphContractQuery(run))
		if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 2 || rows.Rows[0][0] != run || rows.Rows[0][1] != "ffi-graph-live" {
			t.Fatalf("run-specific readback changed: rows=%+v error=%v", rows, err)
		}
		id := managedFFIGraphContractCommand(run).RequestID
		receipt, found, err := mat.GraphMutationReceipt(ctx, id)
		if err != nil || !found || receipt != receipts[id] {
			t.Fatalf("original raw receipt changed: found=%t receipt=%+v error=%v", found, receipt, err)
		}
	}
	all, err := server.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:ManagedFFIItem) RETURN n.run", Consistency: "linearizable"})
	if err != nil || len(all.Rows) != 2 || receipts["\xff"+runs[0]].Slot == receipts["\xff"+runs[1]].Slot {
		t.Fatalf("earlier run was removed or duplicated: rows=%+v error=%v", all, err)
	}
}

func committedFFIReceipt(raw json.RawMessage) bool {
	var receipt types.MutationReceipt
	return json.Unmarshal(raw, &receipt) == nil && receipt.Status == types.MutationCommitted && receipt.Slot != 0 && receipt.ErrorCode == ""
}

func TestManagedContractReceiptOracle(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"status":"rejected","slot":1}`, `{"status":"committed","slot":0}`, `{"status":"committed","slot":1,"error_code":"commit_unknown"}`, `{"status":"pending","slot":1}`} {
		if committedFFIReceipt(json.RawMessage(raw)) {
			t.Fatal("noncommitted receipt counted as ACK")
		}
	}
	if !committedFFIReceipt(json.RawMessage(`{"status":"committed","slot":1}`)) {
		t.Fatal("committed receipt was rejected")
	}
}
