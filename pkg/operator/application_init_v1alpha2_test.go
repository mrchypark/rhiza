package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestApplicationInitExplicitProtocolBoundary(t *testing.T) {
	for _, protocol := range []string{"", types.ManagedApplicationProtocolSQLKV, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestApplicationProtocol, sqlpolicy.ManagedGraphRequestApplicationProtocol, "rhiza-managed-request/v2", "rhiza-managed-graph/v1", "unknown"} {
		p := initTestPlan(t)
		p.ApplicationProtocol = protocol
		if protocol != "" {
			p.EvidenceFormat = protocol
		}
		g := v1alpha2Genesis{OwnerUID: p.ClusterUID, DeploymentUID: p.DeploymentUID, StorageID: p.StorageID, IntentHash: p.IntentHash}
		valid := protocol == "" || protocol == types.ManagedApplicationProtocolSQLKV || protocol == sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol || protocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
		if err := validateV1Alpha2ApplicationInitPlan(p, phase2Ref, g); (err == nil) != valid {
			t.Fatalf("protocol=%q valid=%t error=%v", protocol, valid, err)
		}
		if valid && protocol != "" {
			p.EvidenceFormat = V1Alpha2ManagedSQLEvidenceFormat
			if err := ValidateV1Alpha2ApplicationProfile(protocol, p.EvidenceFormat); err == nil {
				t.Fatal("nonlegacy protocol accepted legacy evidence marker")
			}
		}
	}
}

func initTestPlan(t *testing.T) V1Alpha2ApplicationInitPlan {
	t.Helper()
	b := initTestBootstrap(t)
	members := make([]quepaxa.Member, len(b.Members))
	for i, member := range b.Members {
		members[i] = member.Member
	}
	digest, err := types.ManagedBootstrapPlanDigestForProtocol(string(b.ClusterID), members, b.PhysicalBinding, b.EvidenceFormat, b.Durability, b.Reconfiguration, b.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewV1Alpha2ApplicationInitPlan(phase2Ref, "logical", "deployment", "assignment", b.PhysicalBinding, digest, b.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	p.ApplicationProtocol = b.EvidenceFormat
	return p
}

func initTestBootstrap(t *testing.T) V1Alpha2BootstrapPlan {
	t.Helper()
	b, _, err := testBootstrapPlan(phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	b.EvidenceFormat = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	return b
}

func TestApplicationInitRetainsOriginalSQL(t *testing.T) {
	server, store, snap := authorityTestSetup(t)
	req := network.ExecuteRequest{RequestID: "write", SQL: "INSERT INTO t VALUES (?)", Args: []any{"value"}}
	reserved := historicalPhase8Pending(t, server, store, snap, req)
	if snap.EverApplicationReserved() || !reserved.EverApplicationReserved() {
		t.Fatal("SQL reservation history was not atomically recorded")
	}
	got, ok, err := reserved.PendingSQL()
	if err != nil || !ok || got.RequestID != req.RequestID || got.SQL != req.SQL || len(got.Args) != 1 || got.Args[0] != "value" {
		t.Fatalf("pending SQL: %+v %v %v", got, ok, err)
	}

}

func TestApplicationInitRetainsSQLBlob(t *testing.T) {
	server, store, snap := authorityTestSetup(t)
	req := network.ExecuteRequest{RequestID: "blob", SQL: "INSERT INTO t VALUES (?)", Args: []any{[]byte{0, 1, 255}}}
	reserved := historicalPhase8Pending(t, server, store, snap, req)
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
	for _, size := range []int{limit, limit + 1} {
		req := makeRequest(size)
		raw, err := types.EncodeSQLBatchItem(types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args})
		if err != nil {
			t.Fatal(err)
		}
		fingerprint, err := sqlRequestFingerprint(req)
		if err != nil {
			t.Fatal(err)
		}
		request := V1Alpha2WriteRequest{Kind: "sql", ID: req.RequestID, Fingerprint: fmt.Sprintf("%x", fingerprint)}
		_, err = decodePendingSQL(raw, request)
		if (err == nil) != (size == limit) {
			t.Fatalf("historical SQL size=%d: %v", size, err)
		}
	}
}

func TestApplicationInitRetainsSQLCanonicalArguments(t *testing.T) {
	cases := []network.ExecuteRequest{
		{RequestID: "mixed", SQL: "INSERT INTO t VALUES (?, ?, ?, ?)", Args: []any{[]byte{0, 255}, json.Number("9007199254740993"), nil, 1.25}},
		{RequestID: "multi", Statements: []types.SQLStatement{{SQL: "INSERT INTO t VALUES (?, ?)", Args: []any{[]byte{1, 2}, nil}}, {SQL: "INSERT INTO t VALUES (?, ?)", Args: []any{int64(42), "text"}}}},
	}
	for _, req := range cases {
		t.Run(req.RequestID, func(t *testing.T) {
			server, store, old := authorityTestSetup(t)
			before, err := sqlRequestFingerprint(req)
			if err != nil {
				t.Fatal(err)
			}
			reserved := historicalPhase8Pending(t, server, store, old, req)
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
	server, store, name := phase8Setup(t, phase9CurrentManagedCluster(), "")
	plan := initTestPlan(t)
	if _, err := InitializeV1Alpha2Authority(ctx, store, phase2Ref, name); err == nil || server.puts != 0 {
		t.Fatal("unplanned genesis was accepted")
	}
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, plan, initTestBootstrap(t)); err != nil {
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
	server, store, name := phase8Setup(t, phase9CurrentManagedCluster(), "")
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, initTestPlan(t), initTestBootstrap(t)); err != nil {
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
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, initTestPlan(t), initTestBootstrap(t)); err == nil {
		t.Fatal("incomplete old genesis accepted")
	}
	if server.puts != puts {
		t.Fatal("old genesis was rewritten")
	}
	if _, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref); err == nil {
		t.Fatal("missing assignment accepted")
	}
}

func TestApplicationInitLostCommitRemainsUnknown(t *testing.T) {
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		t.Run(protocol, func(t *testing.T) {
			ctx := context.Background()
			server, store, name := phase8Setup(t, phase9ClusterWithProtocol(protocol), "")
			bootstrap := initTestBootstrap(t)
			bootstrap.EvidenceFormat = protocol
			members := make([]quepaxa.Member, len(bootstrap.Members))
			for i, member := range bootstrap.Members {
				members[i] = member.Member
			}
			digest, err := types.ManagedBootstrapPlanDigestForProtocol(string(bootstrap.ClusterID), members, bootstrap.PhysicalBinding, protocol, bootstrap.Durability, bootstrap.Reconfiguration, protocol)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := NewV1Alpha2ApplicationInitPlan(phase2Ref, "logical", "deployment", "assignment", bootstrap.PhysicalBinding, digest, protocol)
			if err != nil {
				t.Fatal(err)
			}
			plan.ApplicationProtocol = protocol
			if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, plan, bootstrap); err != nil {
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
				t.Fatalf("lost E0 response protocol=%s: %s %v", protocol, observation, err)
			}
		})
	}
}

func TestApplicationInitLostResponseWithAdvancedEvidence(t *testing.T) {
	ctx := context.Background()
	server, _, name := phase8Setup(t, phase9CurrentManagedCluster(), "")
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
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, plan, initTestBootstrap(t)); err != nil {
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
	server, store, name := phase8Setup(t, phase9CurrentManagedCluster(), "")
	plan := initTestPlan(t)
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, plan, initTestBootstrap(t)); err != nil {
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

func TestApplicationInitHistoricalUnusedCannotActivateRetiredProfile(t *testing.T) {
	for _, protocol := range []string{"", types.ManagedApplicationProtocolSQLKV} {
		t.Run(protocol, func(t *testing.T) {
			server, store, _ := phase8Setup(t, phase9ClusterWithProtocol(protocol), "")
			seeded := historicalUnusedFixture(t, server, store, protocol)
			plan := seeded.Assignment().Plan
			// An unresolved retained transition must not be cleared by refusal.
			var object map[string]json.RawMessage
			_ = json.Unmarshal([]byte(server.cluster), &object)
			status, _ := jsonObject(object["status"])
			auth, _ := jsonObject(status["authority"])
			auth["transition"] = json.RawMessage(`{"historical":"unresolved"}`)
			status["authority"], _ = json.Marshal(auth)
			object["status"], _ = json.Marshal(status)
			raw, _ := json.Marshal(object)
			server.cluster = string(raw)
			old, err := LoadV1Alpha2ApplicationInit(context.Background(), store, phase2Ref)
			if err != nil || old.Assignment().State != "Unused" || old.Assignment().Plan != plan {
				t.Fatalf("historical Unused must remain inspectable: %v", err)
			}
			before, puts := server.cluster, server.puts
			if _, _, err := CommitV1Alpha2ApplicationInit(context.Background(), store, old, plan, json.RawMessage(`{"must":"not-activate"}`)); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") {
				t.Fatalf("retired E0 commit not explicitly refused: %v", err)
			}
			if server.puts != puts || server.cluster != before {
				t.Fatal("retired commit changed retained pending/original/evidence/Transition bytes")
			}
		})
	}
}
