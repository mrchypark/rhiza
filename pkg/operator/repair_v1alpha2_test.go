package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"gopkg.in/yaml.v3"
)

type repairFixture struct {
	cluster               map[string]json.RawMessage
	store                 *V1Alpha2StatusStore
	plan                  V1Alpha2RepairPlan
	puts                  int
	apply, lose, readFail bool
	pod                   map[string]any
}

func repairTestSetup(t *testing.T) (*repairFixture, V1Alpha2AdmissionSnapshot) {
	t.Helper()
	ctx := context.Background()
	server, kube := phase9Setup(t, phase8Cluster)
	seed, err := ReconcileV1Alpha2ManagedInitialization(ctx, kube, phase2Ref, phase9RuntimeProfile())
	if err != nil {
		t.Fatal(err)
	}
	f := &repairFixture{apply: true, cluster: seed.object}
	var dep map[string]any
	_ = json.Unmarshal([]byte(server.deployment), &dep)
	podSpec := dep["spec"].(map[string]any)["template"].(map[string]any)["spec"]
	f.pod = managedFixturePods(phase2Ref, "rs", "rsuid", mustManagedJSON(podSpec))[0].(map[string]any)
	meta := f.pod["metadata"].(map[string]any)
	meta["name"] = "replacement"
	meta["uid"] = "replacement-uid"
	f.store = phase2Store(t, func(r *http.Request) (*http.Response, error) {
		respond := func(v any) (*http.Response, error) { return phase2Response(200, string(mustManagedJSON(v))), nil }
		if strings.Contains(r.URL.Path, "/pods/") {
			return respond(f.pod)
		}
		if strings.Contains(r.URL.Path, "/replicasets/") || strings.Contains(r.URL.Path, "/deployments/") {
			return server.roundTrip(r)
		}
		if r.Method == http.MethodGet {
			if f.readFail {
				return phase2Response(503, `{}`), nil
			}
			return respond(f.cluster)
		}
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, "/status") {
			return phase2Response(405, `{}`), nil
		}
		f.puts++
		var next map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			return nil, err
		}
		if !sameJSON(next["metadata"], f.cluster["metadata"]) {
			return phase2Response(409, `{}`), nil
		}
		if f.apply {
			var m map[string]any
			_ = json.Unmarshal(next["metadata"], &m)
			rv, _ := strconv.Atoi(m["resourceVersion"].(string))
			m["resourceVersion"] = strconv.Itoa(rv + 1)
			next["metadata"] = mustManagedJSON(m)
			f.cluster = next
		}
		if f.lose {
			return nil, errors.New("lost status response")
		}
		return respond(f.cluster)
	})
	initial, err := LoadV1Alpha2ApplicationInit(ctx, f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = CommitV1Alpha2ApplicationInit(ctx, f.store, initial, initial.Assignment().Plan, json.RawMessage(`{"schema":"empty"}`)); err != nil {
		t.Fatal(err)
	}
	old, err := LoadV1Alpha2Admission(ctx, f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := old.status.BootstrapPlan()
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := types.ManagedNodeID("replacement-uid")
	if err != nil {
		t.Fatal(err)
	}
	message := f.pod["status"].(map[string]any)["initContainerStatuses"].([]any)[0].(map[string]any)["state"].(map[string]any)["terminated"].(map[string]any)["message"].(string)
	key, err := DecodeV1Alpha2BootstrapReport(message)
	if err != nil {
		t.Fatal(err)
	}
	f.plan = V1Alpha2RepairPlan{OperationID: "repair-1", ClusterUID: phase2Ref.UID, Generation: old.Authority().Generation, DeploymentUID: old.Authority().Binding.DeploymentUID, StorageID: old.Authority().Binding.StorageID, ExpectedConfigID: 1, ExpectedAbortSlot: 0, OldNodeID: bootstrap.Members[0].Member.ID, OldWALIdentity: "actual-registered-wal-identity-from-trusted-caller", Replacement: V1Alpha2RepairMember{PodName: "replacement", PodUID: "replacement-uid", NodeID: nodeID, PublicKey: key, PeerURL: "quic://10.0.0.1:9090"}, BootstrapPlanDigest: initial.Assignment().Plan.BootstrapPlanDigest}
	f.plan.FinalVoterIDs = []types.NodeID{bootstrap.Members[1].Member.ID, bootstrap.Members[2].Member.ID, nodeID}
	slices.Sort(f.plan.FinalVoterIDs)
	f.puts = 0
	return f, old
}

// These completion fixtures represent trusted caller assertions, NOT a
// fabricated certificate or proof that membership/learner startup occurred.
func repairCompletion(plan V1Alpha2RepairPlan, kind string) V1Alpha2RepairCompletion {
	c := V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: kind, ConfigID: plan.ExpectedConfigID + 2, Slot: plan.ExpectedAbortSlot + 10, Hash: strings.Repeat("a", 64), FinalVoterIDs: slices.Clone(plan.FinalVoterIDs)}
	if kind == "Abort" {
		c.ConfigID = plan.ExpectedConfigID
		c.FinalVoterIDs = nil
	}
	return c
}

func TestRepairV1Alpha2GateAndABA(t *testing.T) {
	f, old := repairTestSetup(t)
	ctx := context.Background()
	if old.SQLReservationsClosed() {
		t.Fatal("absent repair must remain compatible/open")
	}
	closed, obs, err := BeginV1Alpha2Repair(ctx, f.store, old, old.Authority(), f.plan)
	if err != nil || obs != V1Alpha2CASApplied || !closed.SQLReservationsClosed() {
		t.Fatalf("close: %v %s", err, obs)
	}
	if !sameJSON(closed.status.authorityRaw, old.status.authorityRaw) {
		t.Fatal("closing changed application authority")
	}
	if _, _, err := ReserveV1Alpha2SQLWrite(ctx, f.store, closed, closed.Authority(), closed.Evidence(), network.ExecuteRequest{RequestID: "blocked", SQL: "CREATE TABLE t (v TEXT)"}); err == nil {
		t.Fatal("closed gate reserved SQL")
	}
	if _, _, err := ReserveV1Alpha2Write(ctx, f.store, closed, closed.Authority(), closed.Evidence(), authorityTestRequest); err == nil {
		t.Fatal("generic mutation bypassed repair gate")
	}
	for _, kind := range []string{"remove-only", "wrong op", "wrong digest", "wrong target"} {
		c := repairCompletion(f.plan, "Terminal")
		switch kind {
		case "remove-only":
			c.ConfigID--
			c.FinalVoterIDs = c.FinalVoterIDs[:2]
		case "wrong op":
			c.OperationID = "other"
		case "wrong digest":
			c.PlanDigest = strings.Repeat("b", 64)
		case "wrong target":
			c.FinalVoterIDs = []types.NodeID{"other", "voter"}
		}
		if _, _, err := FinishV1Alpha2Repair(ctx, f.store, closed, closed.Authority(), c); err == nil {
			t.Fatalf("accepted completion %s", kind)
		}
	}
	opened, _, err := FinishV1Alpha2Repair(ctx, f.store, closed, closed.Authority(), repairCompletion(f.plan, "Terminal"))
	if err != nil || opened.SQLReservationsClosed() {
		t.Fatalf("finish: %v", err)
	}
	if !sameJSON(opened.status.authorityRaw, old.status.authorityRaw) {
		t.Fatal("finishing changed pending/evidence")
	}
	// The quorum decision is imagined between this old snapshot and the two
	// repair CASes. Reusing its RV must conflict even though the gate is open.
	if _, _, err := ReserveV1Alpha2SQLWrite(ctx, f.store, old, old.Authority(), old.Evidence(), network.ExecuteRequest{RequestID: "stale", SQL: "CREATE TABLE t (v TEXT)"}); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("ABA reservation: %v", err)
	}
	if _, _, err := ReserveV1Alpha2SQLWrite(ctx, f.store, opened, opened.Authority(), opened.Evidence(), network.ExecuteRequest{RequestID: "new", SQL: "CREATE TABLE t (v TEXT)"}); err != nil {
		t.Fatalf("fresh pre-quorum snapshot reservation: %v", err)
	}
}

func TestRepairV1Alpha2PendingFinalizationPreserved(t *testing.T) {
	f, old := repairTestSetup(t)
	ctx := context.Background()
	req := network.ExecuteRequest{RequestID: "existing", SQL: "INSERT INTO t VALUES (?)", Args: []any{"value"}}
	pending, _, err := ReserveV1Alpha2SQLWrite(ctx, f.store, old, old.Authority(), old.Evidence(), req)
	if err != nil {
		t.Fatal(err)
	}
	closed, _, err := BeginV1Alpha2Repair(ctx, f.store, pending, pending.Authority(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(pending.status.authorityRaw, closed.status.authorityRaw) {
		t.Fatal("close overwrote pending/original SQL/evidence")
	}
	got, ok, err := closed.PendingSQL()
	if err != nil || !ok || got.SQL != req.SQL || got.RequestID != req.RequestID {
		t.Fatalf("original SQL lost: %v", err)
	}
	request, _ := closed.Pending()
	final, _, err := FinalizeV1Alpha2Write(ctx, f.store, closed, closed.Authority(), request, V1Alpha2WriteCommitted, json.RawMessage(`{"tip":8}`))
	if err != nil {
		t.Fatal(err)
	}
	if !final.SQLReservationsClosed() || !final.EverApplicationReserved() {
		t.Fatal("finalization opened gate or erased history")
	}
	if _, ok := final.Pending(); ok {
		t.Fatal("exact pending finalization did not clear pending")
	}
}

func TestRepairV1Alpha2CompetingAndBoundedRecords(t *testing.T) {
	f, old := repairTestSetup(t)
	ctx := context.Background()
	closed, _, err := BeginV1Alpha2Repair(ctx, f.store, old, old.Authority(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	other := f.plan
	other.OperationID = "competitor"
	if _, _, err := BeginV1Alpha2Repair(ctx, f.store, old, old.Authority(), other); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("competing snapshot: %v", err)
	}
	if _, _, err := BeginV1Alpha2Repair(ctx, f.store, closed, closed.Authority(), other); err == nil {
		t.Fatal("overlapped active repair")
	}
	aborted, _, err := FinishV1Alpha2Repair(ctx, f.store, closed, closed.Authority(), repairCompletion(f.plan, "Abort"))
	if err != nil {
		t.Fatal(err)
	}
	if aborted.SQLReservationsClosed() {
		t.Fatal("exact abort did not reopen")
	}
	if _, _, err := BeginV1Alpha2Repair(ctx, f.store, aborted, aborted.Authority(), other); err == nil {
		t.Fatal("new ID reset operation position")
	}
	other.ExpectedAbortSlot = 10
	next, _, err := BeginV1Alpha2Repair(ctx, f.store, aborted, aborted.Authority(), other)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := next.Repair()
	if !ok || r.Plan.OperationID != "competitor" || r.Completion != nil {
		t.Fatal("record was not replaced boundedly")
	}
	r.Plan.FinalVoterIDs[0] = "mutated-copy"
	again, _ := next.Repair()
	if slices.Contains(again.Plan.FinalVoterIDs, "mutated-copy") {
		t.Fatal("repair accessor aliases mutable voters")
	}
	status, _ := jsonObject(next.status.object["status"])
	if strings.Contains(string(status["repair"]), `"history"`) {
		t.Fatal("unbounded repair ledger")
	}
}

func TestRepairV1Alpha2InvalidBindingsAndTransition(t *testing.T) {
	for _, name := range []string{"cluster", "generation", "deployment", "storage", "old WAL", "node", "key", "endpoint", "target old", "target replacement", "bootstrap", "live Pod UID", "transition"} {
		t.Run(name, func(t *testing.T) {
			f, old := repairTestSetup(t)
			p := f.plan
			p.FinalVoterIDs = slices.Clone(p.FinalVoterIDs)
			switch name {
			case "cluster":
				p.ClusterUID = "other"
			case "generation":
				p.Generation++
			case "deployment":
				p.DeploymentUID = "other"
			case "storage":
				p.StorageID = "other"
			case "old WAL":
				p.OldWALIdentity = ""
			case "node":
				p.Replacement.NodeID = "other"
			case "key":
				p.Replacement.PublicKey[0]++
			case "endpoint":
				p.Replacement.PeerURL = "quic://10.0.0.9:9090"
			case "target old":
				p.FinalVoterIDs = append(p.FinalVoterIDs, p.OldNodeID)
				slices.Sort(p.FinalVoterIDs)
			case "target replacement":
				p.FinalVoterIDs = []types.NodeID{"a", "b"}
			case "bootstrap":
				p.BootstrapPlanDigest = strings.Repeat("b", 64)
			case "live Pod UID":
				f.pod["metadata"].(map[string]any)["uid"] = "replaced"
			case "transition":
				status, _ := jsonObject(f.cluster["status"])
				auth, _ := jsonObject(status["authority"])
				auth["transition"] = json.RawMessage(`{"id":"generation-transition"}`)
				status["authority"] = mustManagedJSON(auth)
				f.cluster["status"] = mustManagedJSON(status)
				var err error
				old, err = LoadV1Alpha2Admission(context.Background(), f.store, phase2Ref)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), p); err == nil || f.puts != 0 {
				t.Fatalf("invalid %s wrote: %v puts=%d", name, err, f.puts)
			}
		})
	}
}

func TestRepairV1Alpha2UnknownCASAndMalformedRead(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%v", applied), func(t *testing.T) {
			f, old := repairTestSetup(t)
			f.apply, f.lose = applied, true
			got, obs, err := BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), f.plan)
			if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) {
				t.Fatalf("unknown outcome became ownership: %v", err)
			}
			if applied && (obs != V1Alpha2CASMatchingReadback || !got.SQLReservationsClosed()) || !applied && obs != V1Alpha2CASUnknown {
				t.Fatalf("wrong uncertainty classification: %s", obs)
			}
		})
	}
	f, _ := repairTestSetup(t)
	status, _ := jsonObject(f.cluster["status"])
	status["repair"] = json.RawMessage(`null`)
	f.cluster["status"] = mustManagedJSON(status)
	if _, err := LoadV1Alpha2Admission(context.Background(), f.store, phase2Ref); err == nil {
		t.Fatal("malformed repair defaulted open")
	}
}

func TestRepairV1Alpha2UnknownFinishAndTransitionRead(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("finish-applied=%v", applied), func(t *testing.T) {
			f, old := repairTestSetup(t)
			closed, _, err := BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), f.plan)
			if err != nil {
				t.Fatal(err)
			}
			f.apply, f.lose = applied, true
			got, obs, err := FinishV1Alpha2Repair(context.Background(), f.store, closed, closed.Authority(), repairCompletion(f.plan, "Terminal"))
			if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) {
				t.Fatalf("finish claimed ownership: %v", err)
			}
			if applied && (obs != V1Alpha2CASMatchingReadback || got.SQLReservationsClosed()) || !applied && obs != V1Alpha2CASUnknown {
				t.Fatalf("finish uncertainty: %s", obs)
			}
		})
	}
	f, old := repairTestSetup(t)
	closed, _, err := BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	status, _ := jsonObject(f.cluster["status"])
	auth, _ := jsonObject(status["authority"])
	auth["transition"] = json.RawMessage(`{"id":"conflicting"}`)
	status["authority"] = mustManagedJSON(auth)
	f.cluster["status"] = mustManagedJSON(status)
	if _, err := LoadV1Alpha2Admission(context.Background(), f.store, phase2Ref); err == nil {
		t.Fatal("reader accepted active repair plus generation transition")
	}
	if !closed.SQLReservationsClosed() {
		t.Fatal("old closed snapshot changed")
	}
}

func TestRepairV1Alpha2SchemaIsBoundedIntentNotHandoff(t *testing.T) {
	b, err := os.ReadFile("../../deploy/operator/cluster-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	versions := doc["spec"].(map[string]any)["versions"].([]any)
	root := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)
	status := root["properties"].(map[string]any)["status"].(map[string]any)
	repair := status["properties"].(map[string]any)["repair"].(map[string]any)
	props := repair["properties"].(map[string]any)
	plan := props["plan"].(map[string]any)
	fields := plan["properties"].(map[string]any)
	for _, name := range []string{"headHash", "headVersion", "storageNamespace", "physicalBinding"} {
		if fields[name] != nil {
			t.Fatalf("deferred handoff field required by intent: %s", name)
		}
	}
	for _, name := range []string{"operationID", "clusterUID", "generation", "deploymentUID", "storageID", "expectedConfigID", "expectedAbortSlot", "oldNodeID", "oldWALIdentity", "replacement", "bootstrapPlanDigest", "finalVoterIDs"} {
		if fields[name] == nil {
			t.Fatalf("intent omitted %s", name)
		}
	}
	voters := fields["finalVoterIDs"].(map[string]any)
	if voters["maxItems"] != 64 || voters["x-kubernetes-list-type"] != "set" {
		t.Fatal("unbounded or duplicate final voters")
	}
	if props["history"] != nil || props["handoff"] != nil {
		t.Fatal("unexpected ledger or prepared handoff")
	}
	rules := string(b)
	for _, message := range []string{"repair record cannot be removed", "repair intent is immutable per operation", "terminal repair is immutable per operation", "replacement repair requires a terminal predecessor and advanced position", "closed repair forbids new application reservations", "REMOVE alone is not final repair completion", "closed repair must match authority and cannot overlap a transition"} {
		if !strings.Contains(rules, message) {
			t.Fatalf("missing CEL invariant %s", message)
		}
	}
}
