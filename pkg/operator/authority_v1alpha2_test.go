package operator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

var authorityTestRequest = V1Alpha2WriteRequest{Kind: "sql", ID: "request", Fingerprint: "fingerprint"}
var authorityTestEvidence = json.RawMessage(`{"tip":7}`)

func authorityTestSetup(t *testing.T) (*phase8Server, *V1Alpha2StatusStore, V1Alpha2AdmissionSnapshot) {
	t.Helper()
	cluster := strings.Replace(phase2Object, `"pendingWrite":{"keep":1},"transition":{"keep":2}`, `"pendingWrite":false,"evidence":{"tip":7}`, 1)
	cluster = strings.Replace(cluster, `"resourceVersion":"1"`, `"resourceVersion":"47"`, 1)
	s := &phase8Server{t: t, cluster: cluster, apply: true}
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		second := r.Method == http.MethodPut && strings.Contains(s.cluster, `"resourceVersion":"48"`)
		response, err := s.roundTrip(r)
		if second && err == nil && response.StatusCode == http.StatusOK {
			s.cluster = strings.Replace(s.cluster, `"resourceVersion":"48"`, `"resourceVersion":"49"`, 1)
			_ = response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(s.cluster))
		}
		return response, err
	})
	snap, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return s, store, snap
}

// Historical fixtures describe already stored UNKNOWN state. They never call a
// reservation/finalization API, perform CAS, or manufacture execution authority.
func historicalPendingSnapshot(t *testing.T, old V1Alpha2AdmissionSnapshot, original any) V1Alpha2AdmissionSnapshot {
	t.Helper()
	raw, err := json.Marshal(old.status.object)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	status, err := jsonObject(object["status"])
	if err != nil {
		t.Fatal(err)
	}
	auth, err := jsonObject(status["authority"])
	if err != nil {
		t.Fatal(err)
	}
	pending := v1alpha2PendingRequest{PriorEvidence: old.Evidence()}
	switch value := original.(type) {
	case V1Alpha2WriteRequest:
		pending.V1Alpha2WriteRequest = value
	case network.ExecuteRequest:
		fingerprint, err := sqlRequestFingerprint(value)
		if err != nil {
			t.Fatal(err)
		}
		pending.V1Alpha2WriteRequest = V1Alpha2WriteRequest{Kind: "sql", ID: value.RequestID, Fingerprint: fmt.Sprintf("%x", fingerprint)}
		pending.OriginalSQL, err = types.EncodeSQLBatchItem(types.SQLCommand{RequestID: value.RequestID, SQL: value.SQL, Args: value.Args, WantRows: value.WantRows, RequireOne: value.RequireOne, Statements: value.Statements})
		if err != nil {
			t.Fatal(err)
		}
	case types.KVCommand:
		pending.OriginalKV, _, err = types.EncodeManagedKVOriginal(value)
		if err != nil {
			t.Fatal(err)
		}
		id, err := types.ManagedKVPendingWireID(value.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		fingerprint, err := types.ManagedKVFingerprint(value)
		if err != nil {
			t.Fatal(err)
		}
		pending.V1Alpha2WriteRequest = V1Alpha2WriteRequest{Kind: "kv", ID: id, Fingerprint: fmt.Sprintf("%x", fingerprint)}
	default:
		t.Fatalf("unsupported historical original %T", original)
	}
	protocol, err := mixedApplicationProtocol(object)
	if err != nil {
		t.Fatal(err)
	}
	if protocol != "" {
		view := old.Authority()
		pending.Reservation = &v1alpha2ReservationContext{Namespace: view.Owner.Namespace, Name: view.Owner.Name, UID: view.Owner.UID, LogicalID: view.Owner.LogicalID, Generation: view.Generation, DeploymentUID: view.Binding.DeploymentUID, StorageID: view.Binding.StorageID}
	}
	auth["pendingWrite"] = json.RawMessage("true")
	auth["everApplicationReserved"] = json.RawMessage("true")
	auth["pendingRequest"], _ = json.Marshal(pending)
	status["authority"], _ = json.Marshal(auth)
	object["status"], _ = json.Marshal(status)
	snapshot := old.status
	snapshot.object = object
	snapshot.authorityRaw = status["authority"]
	snapshot.everApplicationReserved = true
	result, err := parseV1Alpha2Admission(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func historicalPhase8Pending(t *testing.T, server *phase8Server, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, original any) V1Alpha2AdmissionSnapshot {
	t.Helper()
	snapshot := historicalPendingSnapshot(t, old, original)
	raw, err := json.Marshal(snapshot.status.object)
	if err != nil {
		t.Fatal(err)
	}
	server.cluster = string(raw)
	loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func historicalRepairPending(t *testing.T, fixture *repairFixture, old V1Alpha2AdmissionSnapshot, original any) V1Alpha2AdmissionSnapshot {
	t.Helper()
	snapshot := historicalPendingSnapshot(t, old, original)
	fixture.cluster = snapshot.status.object
	loaded, err := LoadV1Alpha2Admission(context.Background(), fixture.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func TestAuthorityV1Alpha2HistoricalUnknownRemainsReadOnly(t *testing.T) {
	server, store, initial := authorityTestSetup(t)
	stored := historicalPhase8Pending(t, server, store, initial, authorityTestRequest)
	before, puts := server.cluster, server.puts
	for range 2 {
		loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
		request, pending := loaded.Pending()
		if err != nil || !pending || request != authorityTestRequest || !loaded.EverApplicationReserved() || !sameJSON(loaded.Evidence(), stored.Evidence()) {
			t.Fatalf("historical UNKNOWN lost: request=%+v pending=%t error=%v", request, pending, err)
		}
	}
	if server.cluster != before || server.puts != puts {
		t.Fatal("read-only historical observation mutated or completed UNKNOWN")
	}
}

// Seed historical Unused bytes directly; retired genesis/commit APIs must not
// be used to construct a fixture that claims those execution paths still work.
func historicalUnusedFixture(t *testing.T, server *phase8Server, store *V1Alpha2StatusStore, protocol string) V1Alpha2ApplicationInitSnapshot {
	t.Helper()
	bootstrap, _, err := testBootstrapPlan(phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	format := V1Alpha2ManagedSQLEvidenceFormat
	if protocol != "" {
		format = protocol
	}
	bootstrap.EvidenceFormat = format
	members := make([]quepaxa.Member, len(bootstrap.Members))
	for i, member := range bootstrap.Members {
		members[i] = member.Member
	}
	digest, err := types.ManagedBootstrapPlanDigestForProtocol(string(bootstrap.ClusterID), members, bootstrap.PhysicalBinding, format, bootstrap.Durability, bootstrap.Reconfiguration, protocol)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewV1Alpha2ApplicationInitPlan(phase2Ref, "logical", "deployment", "assignment", bootstrap.PhysicalBinding, digest, format)
	if err != nil {
		t.Fatal(err)
	}
	plan.ApplicationProtocol = protocol
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(phase9ClusterWithProtocol(protocol)), &object); err != nil {
		t.Fatal(err)
	}
	status, _ := jsonObject(object["status"])
	status["genesis"], _ = json.Marshal(v1alpha2Genesis{OwnerUID: phase2Ref.UID, LogicalID: "logical", DeploymentUID: plan.DeploymentUID, StorageID: plan.StorageID, IntentHash: plan.IntentHash})
	status["bootstrapPlan"], _ = json.Marshal(bootstrap)
	status["applicationInit"], _ = json.Marshal(V1Alpha2ApplicationInitAssignment{Plan: plan, State: "Unused"})
	status["authority"], _ = json.Marshal(map[string]any{
		"owner":      map[string]any{"namespace": phase2Ref.Namespace, "name": phase2Ref.Name, "uid": phase2Ref.UID, "logicalID": "logical"},
		"generation": 0, "binding": map[string]any{"deploymentUID": plan.DeploymentUID, "storageID": plan.StorageID},
		"pendingWrite": false, "everApplicationReserved": false,
	})
	object["status"], _ = json.Marshal(status)
	raw, _ := json.Marshal(object)
	server.cluster = string(raw)
	loaded, err := LoadV1Alpha2ApplicationInit(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return loaded
}

func historicalConsumedFixture(t *testing.T, protocol string) (*phase8Server, *V1Alpha2StatusStore, V1Alpha2AdmissionSnapshot) {
	t.Helper()
	server, store, _ := phase8Setup(t, phase9ClusterWithProtocol(protocol), "")
	initial := historicalUnusedFixture(t, server, store, protocol)
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(server.cluster), &object); err != nil {
		t.Fatal(err)
	}
	status, _ := jsonObject(object["status"])
	assignment := initial.Assignment()
	assignment.State, assignment.InitialEvidenceDigest = "Consumed", fmt.Sprintf("%x", sha256.Sum256(authorityTestEvidence))
	status["applicationInit"], _ = json.Marshal(assignment)
	auth, _ := jsonObject(status["authority"])
	auth["evidence"] = authorityTestEvidence
	status["authority"], _ = json.Marshal(auth)
	object["status"], _ = json.Marshal(status)
	raw, _ := json.Marshal(object)
	server.cluster = string(raw)
	loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return server, store, loaded
}
