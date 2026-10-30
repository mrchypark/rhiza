package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func initTestPlan(t *testing.T) V1Alpha2ApplicationInitPlan {
	t.Helper()
	p, err := NewV1Alpha2ApplicationInitPlan(phase2Ref, "logical", "deployment", "assignment", strings.Repeat("a", 64), strings.Repeat("b", 64), "app/v1")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestApplicationInitRetainsOriginalSQL(t *testing.T) {
	_, store, snap := authorityTestSetup(t)
	req := network.ExecuteRequest{RequestID: "write", SQL: "INSERT INTO t VALUES (?)", Args: []any{"value"}}
	reserved, _, err := ReserveV1Alpha2SQLWrite(context.Background(), store, snap, snap.Authority(), authorityTestEvidence, req)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reserved.PendingSQL()
	if err != nil || !ok || got.RequestID != req.RequestID || got.SQL != req.SQL || len(got.Args) != 1 || got.Args[0] != "value" {
		t.Fatalf("pending SQL: %+v %v %v", got, ok, err)
	}
	if _, _, err := FinalizeV1Alpha2Write(context.Background(), store, reserved, snap.Authority(), V1Alpha2WriteRequest{Kind: "sql", ID: "write", Fingerprint: "wrong"}, V1Alpha2WriteCommitted, authorityTestEvidence); err == nil {
		t.Fatal("mismatched request finalized")
	}
}

func TestApplicationInitRetainsSQLBlob(t *testing.T) {
	_, store, snap := authorityTestSetup(t)
	req := network.ExecuteRequest{RequestID: "blob", SQL: "INSERT INTO t VALUES (?)", Args: []any{[]byte{0, 1, 255}}}
	reserved, _, err := ReserveV1Alpha2SQLWrite(context.Background(), store, snap, snap.Authority(), authorityTestEvidence, req)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := reserved.PendingSQL()
	if err != nil || !ok || len(got.Args) != 1 {
		t.Fatalf("pending: %v %v", ok, err)
	}
	blob, ok := got.Args[0].([]byte)
	if !ok || !bytes.Equal(blob, req.Args[0].([]byte)) {
		t.Fatalf("blob changed: %#v", got.Args[0])
	}
}

func TestApplicationInitPendingSQLSizeBoundary(t *testing.T) {
	const limit = 32 << 10
	makeRequest := func(size int) network.ExecuteRequest {
		req := network.ExecuteRequest{RequestID: "boundary", SQL: "SELECT ?", Args: []any{""}}
		encoded, err := types.EncodeSQLBatchItem(types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args})
		if err != nil || len(encoded) >= size {
			t.Fatalf("encode base request: size=%d err=%v", len(encoded), err)
		}
		req.Args[0] = strings.Repeat("x", size-len(encoded))
		encoded, err = types.EncodeSQLBatchItem(types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args})
		if err != nil || len(encoded) != size {
			t.Fatalf("encoded size=%d want=%d err=%v", len(encoded), size, err)
		}
		return req
	}
	server, store, old := authorityTestSetup(t)
	tooLarge := makeRequest(limit + 1)
	before := server.cluster
	if _, _, err := ReserveV1Alpha2SQLWrite(context.Background(), store, old, old.Authority(), authorityTestEvidence, tooLarge); err == nil || !strings.Contains(err.Error(), "invalid pending SQL size") {
		t.Fatalf("oversize request: %v", err)
	}
	if server.puts != 0 || server.cluster != before {
		t.Fatal("oversize request changed status")
	}
	reserved, _, err := ReserveV1Alpha2SQLWrite(context.Background(), store, old, old.Authority(), authorityTestEvidence, makeRequest(limit))
	if err != nil || server.puts != 1 {
		t.Fatalf("boundary reservation: puts=%d err=%v", server.puts, err)
	}
	before = server.cluster
	if _, _, err := ReserveV1Alpha2SQLWrite(context.Background(), store, reserved, reserved.Authority(), authorityTestEvidence, tooLarge); err == nil {
		t.Fatal("oversize request replaced pending write")
	}
	if server.puts != 1 || server.cluster != before {
		t.Fatal("oversize request changed pending status")
	}
	if _, ok, err := reserved.PendingSQL(); err != nil || !ok {
		t.Fatalf("boundary pending request: present=%v err=%v", ok, err)
	}
}

func TestApplicationInitRetainsSQLCanonicalArguments(t *testing.T) {
	cases := []network.ExecuteRequest{
		{RequestID: "mixed", SQL: "INSERT INTO t VALUES (?, ?, ?, ?)", Args: []any{[]byte{0, 255}, json.Number("9007199254740993"), nil, 1.25}},
		{RequestID: "multi", Statements: []types.SQLStatement{{SQL: "INSERT INTO t VALUES (?, ?)", Args: []any{[]byte{1, 2}, nil}}, {SQL: "INSERT INTO t VALUES (?, ?)", Args: []any{int64(42), "text"}}}},
	}
	for _, req := range cases {
		t.Run(req.RequestID, func(t *testing.T) {
			_, store, old := authorityTestSetup(t)
			before, err := sqlRequestFingerprint(req)
			if err != nil {
				t.Fatal(err)
			}
			reserved, _, err := ReserveV1Alpha2SQLWrite(context.Background(), store, old, old.Authority(), authorityTestEvidence, req)
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := reserved.PendingSQL()
			if err != nil || !ok {
				t.Fatalf("pending: %v %v", ok, err)
			}
			after, err := sqlRequestFingerprint(got)
			if err != nil || before != after {
				t.Fatalf("fingerprint mismatch: %v", err)
			}
			var args []any
			if len(got.Statements) > 0 {
				args = got.Statements[0].Args
			} else {
				args = got.Args
			}
			if _, ok := args[0].([]byte); !ok || args[1] == nil && req.RequestID == "mixed" || req.RequestID == "multi" && args[1] != nil {
				t.Fatalf("decoded arguments: %#v", args)
			}
		})
	}
}

func TestApplicationInitGenesisAndCommit(t *testing.T) {
	ctx := context.Background()
	server, store, name := phase8Setup(t, phase8Cluster, "")
	plan := initTestPlan(t)
	if _, err := InitializeV1Alpha2Authority(ctx, store, phase2Ref, name); err == nil || server.puts != 0 {
		t.Fatal("unplanned genesis was accepted")
	}
	if _, err := InitializeV1Alpha2AuthorityWithPlan(ctx, store, phase2Ref, name, plan); err != nil {
		t.Fatal(err)
	}
	if server.puts != 1 || !strings.Contains(server.cluster, `"state":"Unused"`) || !strings.Contains(server.cluster, `"unrelated":{"keep":true}`) {
		t.Fatal("genesis did not atomically preserve assignment and status")
	}
	if _, err := LoadV1Alpha2Admission(ctx, store, phase2Ref); err == nil {
		t.Fatal("ordinary admission accepted absent evidence")
	}
	initial, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref)
	if err != nil || initial.Assignment().State != "Unused" {
		t.Fatalf("load: %v", err)
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	initial, err = LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	consumed, observation, err := CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, json.RawMessage(`{"schema":"empty"}`))
	if err != nil || observation != V1Alpha2CASApplied || consumed.Assignment().State != "Consumed" || !validInitDigest(consumed.Assignment().InitialEvidenceDigest) {
		t.Fatalf("commit: %s %v", observation, err)
	}
	if _, err := LoadV1Alpha2Admission(ctx, store, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if _, _, err := CommitV1Alpha2ApplicationInit(ctx, store, consumed, plan, json.RawMessage(`{"schema":"empty"}`)); err == nil {
		t.Fatal("consumed assignment rearmed")
	}
}

func TestApplicationInitOldGenesisNeverBackfilled(t *testing.T) {
	ctx := context.Background()
	server, store, name := phase8Setup(t, phase8Cluster, "")
	if _, err := InitializeV1Alpha2AuthorityWithPlan(ctx, store, phase2Ref, name, initTestPlan(t)); err != nil {
		t.Fatal(err)
	}
	var obj map[string]json.RawMessage
	_ = json.Unmarshal([]byte(server.cluster), &obj)
	status, _ := jsonObject(obj["status"])
	delete(status, "applicationInit")
	obj["status"], _ = json.Marshal(status)
	b, _ := json.Marshal(obj)
	server.cluster = string(b)
	puts := server.puts
	if _, err := InitializeV1Alpha2AuthorityWithPlan(ctx, store, phase2Ref, name, initTestPlan(t)); err != nil {
		t.Fatal(err)
	}
	if server.puts != puts {
		t.Fatal("old genesis was rewritten")
	}
	if _, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref); err == nil {
		t.Fatal("missing assignment accepted")
	}
}

func TestApplicationInitLostCommitRemainsUnknown(t *testing.T) {
	ctx := context.Background()
	server, store, name := phase8Setup(t, phase8Cluster, "")
	plan := initTestPlan(t)
	if _, err := InitializeV1Alpha2AuthorityWithPlan(ctx, store, phase2Ref, name, plan); err != nil {
		t.Fatal(err)
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	old, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	server.timeout = true
	result, observation, err := CommitV1Alpha2ApplicationInit(ctx, store, old, plan, json.RawMessage(`{"schema":"empty"}`))
	if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) || observation != V1Alpha2CASMatchingReadback || result.Assignment().State != "Consumed" {
		t.Fatalf("lost response: %s %v", observation, err)
	}
}

func TestApplicationInitLostResponseWithAdvancedEvidence(t *testing.T) {
	ctx := context.Background()
	server, _, name := phase8Setup(t, phase8Cluster, "")
	advanced := false
	store := phase2Store(t, func(req *http.Request) (*http.Response, error) {
		resp, err := server.roundTrip(req)
		if req.Method == http.MethodPut && server.timeout && err != nil && !advanced {
			server.cluster = strings.Replace(server.cluster, `"schema":"empty"`, `"schema":"advanced"`, 1)
			server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"49"`, 1)
			advanced = true
		}
		return resp, err
	})
	plan := initTestPlan(t)
	if _, err := InitializeV1Alpha2AuthorityWithPlan(ctx, store, phase2Ref, name, plan); err != nil {
		t.Fatal(err)
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	old, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	server.timeout = true
	result, observation, err := CommitV1Alpha2ApplicationInit(ctx, store, old, plan, json.RawMessage(`{"schema":"empty"}`))
	if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) || observation != V1Alpha2CASMatchingReadback || result.Assignment().State != "Consumed" || !strings.Contains(server.cluster, `"schema":"advanced"`) {
		t.Fatalf("advanced readback: %s %v", observation, err)
	}
}

func TestApplicationInitStaleCASCannotConsume(t *testing.T) {
	ctx := context.Background()
	server, store, name := phase8Setup(t, phase8Cluster, "")
	plan := initTestPlan(t)
	if _, err := InitializeV1Alpha2AuthorityWithPlan(ctx, store, phase2Ref, name, plan); err != nil {
		t.Fatal(err)
	}
	old, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"49"`, 1)
	_, _, err = CommitV1Alpha2ApplicationInit(ctx, store, old, plan, json.RawMessage(`{"schema":"empty"}`))
	if !errors.Is(err, ErrV1Alpha2StatusConflict) || !strings.Contains(server.cluster, `"state":"Unused"`) {
		t.Fatalf("stale commit: %v", err)
	}
}
