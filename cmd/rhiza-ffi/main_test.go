//go:build cgo

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

func openTestDB(t *testing.T) uint64 {
	t.Helper()
	input, err := json.Marshal(map[string]any{"NodeID": "ffi-test", "DataDir": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Data struct {
			Handle uint64 `json:"handle"`
		} `json:"data"`
		Error *ffiError `json:"error"`
	}
	if err := json.Unmarshal(goOpen(input), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error != nil || response.Data.Handle == 0 {
		t.Fatalf("open response: %#v", response)
	}
	t.Cleanup(func() { _ = goClose(response.Data.Handle) })
	return response.Data.Handle
}

func call(t *testing.T, handle uint64, operation string, request any) map[string]json.RawMessage {
	t.Helper()
	input, err := json.Marshal(map[string]any{"operation": operation, "request": request})
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(goCall(handle, input, 0), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func requireData(t *testing.T, response map[string]json.RawMessage) json.RawMessage {
	t.Helper()
	if errData, ok := response["error"]; ok {
		t.Fatalf("ffi error: %s", errData)
	}
	data, ok := response["data"]
	if !ok {
		t.Fatalf("missing data: %#v", response)
	}
	return data
}

func responseErrorCode(t *testing.T, response []byte) string {
	t.Helper()
	var env struct {
		Error *ffiError `json:"error"`
	}
	if err := json.Unmarshal(response, &env); err != nil {
		t.Fatal(err)
	}
	if env.Error == nil {
		t.Fatalf("expected error: %s", response)
	}
	return env.Error.Code
}

func TestGoEntryBasicPublicAPI(t *testing.T) {
	h := openTestDB(t)
	requireData(t, call(t, h, "execute", map[string]any{"request_id": "schema", "sql": "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}))
	requireData(t, call(t, h, "execute", map[string]any{"request_id": "insert", "sql": "INSERT INTO items VALUES (?, ?)", "args": []any{int64(9007199254740993), "tea"}}))
	query := requireData(t, call(t, h, "query", map[string]any{"sql": "SELECT name FROM items WHERE id = ?", "args": []any{json.Number("9007199254740993")}}))
	if string(query) == "null" {
		t.Fatal("query returned null")
	}
	requireData(t, call(t, h, "kv_put", map[string]any{"request_id": "kv", "key": "key", "value": "dmFsdWU="}))
	kv := requireData(t, call(t, h, "kv_get", map[string]any{"key": "key"}))
	if !bytes.Contains(kv, []byte("dmFsdWU=")) {
		t.Fatalf("KV response: %s", kv)
	}
	requireData(t, call(t, h, "graph_execute", map[string]any{"request_id": "graph", "cypher": "CREATE (:Item {name: 'graph'})"}))
	graph := requireData(t, call(t, h, "graph_query", map[string]any{"cypher": "MATCH (n:Item) RETURN n.name"}))
	if !bytes.Contains(graph, []byte("graph")) {
		t.Fatalf("graph response: %s", graph)
	}
	ready := requireData(t, call(t, h, "ready", map[string]any{}))
	if string(ready) != "true" {
		t.Fatalf("ready: %s", ready)
	}
}

func TestHandleLifetimeAndInputValidation(t *testing.T) {
	h := openTestDB(t)
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}} trailing`), 0)); code != "invalid_request" {
		t.Fatalf("trailing code=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, make([]byte, maxInputBytes+1), 0)); code != "invalid_request" {
		t.Fatalf("oversize code=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":null}`), 0)); code != "invalid_request" {
		t.Fatalf("null code=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}}`), math.MaxUint64)); code != "invalid_request" {
		t.Fatalf("overflow code=%q", code)
	}
	streamCall := []byte(`{"operation":"graph_stream_read","request":{"stream":"empty","wait_ms":1000}}`)
	if code := responseErrorCode(t, goCall(h, streamCall, 1)); code != "timeout" {
		t.Fatalf("long-poll timeout code=%q", code)
	}
	var closeOK map[string]json.RawMessage
	if err := json.Unmarshal(goClose(h), &closeOK); err != nil || string(closeOK["data"]) != "null" {
		t.Fatalf("close response: %#v err=%v", closeOK, err)
	}
	if code := responseErrorCode(t, goClose(h)); code != "invalid_handle" {
		t.Fatalf("double close=%q", code)
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}}`), 0)); code != "invalid_handle" {
		t.Fatalf("stale call=%q", code)
	}
}

func TestConcurrentCallAndClose(t *testing.T) {
	h := openTestDB(t)
	entry, release, err := acquire(h)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan []byte, 1)
	go func() { closed <- goClose(h) }()
	select {
	case <-entry.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("close did not cancel active call")
	}
	select {
	case result := <-closed:
		t.Fatalf("close completed before active call drained: %s", result)
	default:
	}
	release()
	select {
	case result := <-closed:
		var response map[string]json.RawMessage
		if err := json.Unmarshal(result, &response); err != nil || string(response["data"]) != "null" {
			t.Fatalf("close=%s err=%v", result, err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not drain")
	}
	if code := responseErrorCode(t, goCall(h, []byte(`{"operation":"ready","request":{}}`), 0)); code != "invalid_handle" {
		t.Fatalf("stale code=%q", code)
	}
}

func TestErrorJSONAndCommitUnknownPriority(t *testing.T) {
	if !json.Valid(marshalError("bad\x00code", "line\n\x01")) {
		t.Fatalf("error JSON is invalid: %q", marshalError("bad\x00code", "line\n\x01"))
	}
	if code := errorCode(fmt.Errorf("%w while %w", rhiza.ErrCommitUnknown, context.Canceled)); code != "commit_unknown" {
		t.Fatalf("commit unknown code=%q", code)
	}
}

func Example_build() { fmt.Println("go build -buildmode=c-archive -o rhiza_ffi.a ./cmd/rhiza-ffi") }
