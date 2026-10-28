package operator

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"gopkg.in/yaml.v3"
)

const fenceEndpoint = "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizafences"

func independentRequest() FenceRequest {
	return FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "auto-1", Scope: ScopeVoter, Targets: []FenceTarget{
		{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-0", WALIdentity: "wal-0", NodeName: "node-a", ContainerID: "containerd://c0", ImageID: "sha256:img-0"},
	}}
}

func independentIdentity() fencing.Identity {
	return fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-0", RunID: "run-0", WALIdentity: "wal-0"}
}

func independentPod(name, uid string) object {
	return object{
		"metadata": object{"name": name, "uid": uid, "annotations": object{fencingProtocolAnnotation: "1"}, "ownerReferences": []any{object{"uid": "sts-uid", "kind": "StatefulSet"}}},
		"spec": object{"nodeName": "node-a", "containers": []any{object{
			"name":            "rhiza",
			"command":         []any{"/usr/local/bin/rhiza-fencer"},
			"args":            []any{"supervise", "--", "/usr/local/bin/rhiza"},
			"securityContext": object{"allowPrivilegeEscalation": false, "capabilities": object{"drop": []any{"ALL"}}},
		}}},
		"status": object{"containerStatuses": []any{object{"containerID": "containerd://c0", "imageID": "sha256:img-0"}}},
	}
}

func independentStatefulSet(provider string) object {
	return object{
		"metadata": object{"name": "rhiza", "uid": "sts-uid"},
		"spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": []any{
			object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": provider},
			object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "rhiza-archive"},
			object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
		}}}}}},
	}
}

// independentLedger is a durable ledger whose runs no supervisor ever reported
// terminal. Closing it cannot complete cooperatively.
func independentLedger(runs ...fencing.Run) object {
	state := fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{}}}
	for _, run := range runs {
		state.Ledger.Runs[run.Identity.RunID] = run
	}
	return object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "ledger", "namespace": "default", "resourceVersion": "1"}, "data": object{"ledger.json": string(jsonBytes(state))}}
}

func closedLedger(request FenceRequest, hash string) object {
	identity := independentIdentity()
	stop := fencing.StopRequest{OperationID: request.OperationID, RequestHash: hash, Identity: identity}
	return independentLedger(fencing.Run{Identity: identity, Exited: true, Drain: &fencing.DrainReceipt{Request: stop, WritersJoined: true}})
}

type attestorAPI struct {
	mu         sync.Mutex
	sts        object
	pods       []object
	ledger     object
	payload    *FenceProof
	statusPuts int
	server     *httptest.Server
}

func (a *attestorAPI) Close() { a.server.Close() }

func (a *attestorAPI) serve(w http.ResponseWriter, q *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch {
	case q.URL.Path == "/apis/apps/v1/namespaces/default/statefulsets/rhiza" && q.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(a.sts)
	case q.URL.Path == "/api/v1/namespaces/default/pods" && q.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(podList{Items: a.pods})
	case q.URL.Path == "/api/v1/namespaces/default/configmaps" && q.Method == http.MethodPost:
		var next object
		if err := json.NewDecoder(q.Body).Decode(&next); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		a.ledger = next
		w.WriteHeader(http.StatusCreated)
	case strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/configmaps/rhiza-fence-ledger-"):
		if q.Method == http.MethodGet {
			if a.ledger == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(a.ledger)
			return
		}
		var next object
		if err := json.NewDecoder(q.Body).Decode(&next); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		a.ledger = next
	case strings.HasPrefix(q.URL.Path, fenceEndpoint+"/") && strings.HasSuffix(q.URL.Path, "/status") && q.Method == http.MethodPut:
		var record fenceResource
		if err := json.NewDecoder(q.Body).Decode(&record); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		a.payload, a.statusPuts = record.Status.Proof, a.statusPuts+1
	default:
		http.NotFound(w, q)
	}
}

func (a *attestorAPI) closedState(t *testing.T) fenceLedger {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	var state fenceLedger
	if err := json.Unmarshal([]byte(str(nested(a.ledger, "data", "ledger.json"))), &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func (a *attestorAPI) addPod(pod object) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pods = append(a.pods, pod)
}

func (a *attestorAPI) replaceLedger(ledger object) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ledger = ledger
}

func newAttestedExecutor(t *testing.T, attestor FenceAttestor) (*FencingExecutor, *attestorAPI, *fenceResource) {
	t.Helper()
	api := &attestorAPI{sts: independentStatefulSet("gcs"), pods: []object{independentPod("rhiza-0", "pod-0")}}
	api.server = httptest.NewTLSServer(http.HandlerFunc(api.serve))
	t.Cleanup(api.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := independentRequest()
	record := &fenceResource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaFence", Metadata: object{"name": "fence-x", "namespace": "default"}, Spec: request}
	kube := &Kubernetes{BaseURL: api.server.URL, Namespace: "default", TokenFile: token, Client: api.server.Client()}
	return &FencingExecutor{Kube: kube, Attestor: attestor}, api, record
}

func TestAttestedFenceNeverReplacesTheCooperativeLedger(t *testing.T) {
	attestor := &stubAttestor{fn: func(req AttestationRequest) (*AttestationResponse, error) { return independentEvidenceFor(req), nil }}
	executor, api, record := newAttestedExecutor(t, attestor)
	hash, err := requestHash(&record.Spec)
	if err != nil {
		t.Fatal(err)
	}
	api.ledger = closedLedger(record.Spec, hash)
	if err := executor.reconcileFence(t.Context(), fenceEndpoint, record); err != nil {
		t.Fatalf("cooperative fence failed: %v", err)
	}
	if attestor.count() != 0 {
		t.Fatal("a finished cooperative ledger still consulted the attestor")
	}
	if api.statusPuts != 1 || record.Status.Proof == nil || !strings.HasPrefix(record.Status.Proof.ProofID, "cooperative-") || record.Status.Proof.Independent != nil {
		t.Fatalf("cooperative proof changed: %+v", record.Status.Proof)
	}
}

func TestUnresolvedRepairInitHistoryBlocksCooperativeAndIndependentFenceCompletion(t *testing.T) {
	attestor := &stubAttestor{fn: func(req AttestationRequest) (*AttestationResponse, error) { return independentEvidenceFor(req), nil }}
	executor, api, record := newAttestedExecutor(t, attestor)
	hash, err := requestHash(&record.Spec)
	if err != nil {
		t.Fatal(err)
	}
	closed := closedLedger(record.Spec, hash)
	var state fenceLedger
	if err := json.Unmarshal([]byte(str(nested(closed, "data", "ledger.json"))), &state); err != nil {
		t.Fatal(err)
	}
	state.Ledger.MembershipRepairHistory = map[string]fencing.MembershipRepairRecord{
		"old-repair": {Progress: fencing.MembershipRepairProgress{Candidates: []fencing.MembershipRepairCandidate{{
			Assignment: fencing.BootstrapAssignment{PodUID: "candidate-pod", NodeID: "rhiza-0"},
		}}}},
	}
	api.ledger = object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "ledger", "namespace": "default", "resourceVersion": "1"}, "data": object{"ledger.json": string(jsonBytes(state))}}
	if err := executor.reconcileFence(t.Context(), fenceEndpoint, record); !errors.Is(err, ErrFencePending) {
		t.Fatalf("unresolved historical repair init did not keep fence pending: %v", err)
	}
	if attestor.count() != 0 || api.statusPuts != 0 {
		t.Fatalf("unresolved init history permitted independent/cooperative completion: attestations=%d statusWrites=%d", attestor.count(), api.statusPuts)
	}
}

func TestAttestedFenceWritesBoundIndependentProof(t *testing.T) {
	attestor := &stubAttestor{fn: func(req AttestationRequest) (*AttestationResponse, error) { return independentEvidenceFor(req), nil }}
	executor, api, record := newAttestedExecutor(t, attestor)
	// Another voter's unresolved run is outside a voter fence and must not block
	// evidence for the exact slot this operation closed.
	api.ledger = independentLedger(
		fencing.Run{Identity: independentIdentity()},
		fencing.Run{Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "pod-1", RunID: "run-1", WALIdentity: "wal-1"}},
	)
	hash, err := requestHash(&record.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.reconcileFence(t.Context(), fenceEndpoint, record); err != nil {
		t.Fatalf("independent fence failed: %v", err)
	}
	if attestor.count() != 1 || api.statusPuts != 1 {
		t.Fatalf("attestor calls=%d status writes=%d", attestor.count(), api.statusPuts)
	}
	proof := record.Status.Proof
	if proof == nil || proof.Independent == nil || proof.ProofID != "attested-proof" || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		t.Fatalf("independent proof is incomplete: %+v", proof)
	}
	if proof.Independent.LedgerHash == "" || proof.Independent.RequestHash != hash || proof.Independent.Storage.Prefix != "root/source" {
		t.Fatalf("independent proof lost its bindings: %+v", proof.Independent)
	}
	if len(proof.Independent.Runtimes) != 1 || proof.Independent.Runtimes[0].PodUID != "pod-0" || proof.Independent.Runtimes[0].ContainerIDs[0] != "containerd://c0" {
		t.Fatalf("independent proof lost the captured incarnation: %+v", proof.Independent.Runtimes)
	}
	if closed := api.closedState(t).Ledger.ClosedNodes["rhiza-0"]; closed.OperationID != record.Spec.OperationID || closed.RequestHash != hash {
		t.Fatalf("closed voter slot does not bind the operation: %+v", closed)
	}
	if api.payload == nil || api.payload.ProofID != proof.ProofID {
		t.Fatal("the persisted status differs from the validated proof")
	}
}

func TestAttestedFenceStaysPendingWithoutVerifiableEvidence(t *testing.T) {
	unavailable := func(AttestationRequest) (*AttestationResponse, error) { return nil, errors.New("attestor unavailable") }
	pending := func(AttestationRequest) (*AttestationResponse, error) { return nil, ErrFencePending }
	absent := func(req AttestationRequest) (*AttestationResponse, error) {
		response := independentEvidenceFor(req)
		response.Independent.Runtimes[0].State = "absent"
		return response, nil
	}
	stale := func(req AttestationRequest) (*AttestationResponse, error) {
		response := independentEvidenceFor(req)
		response.Independent.LedgerHash = strings.Repeat("f", 64)
		return response, nil
	}
	for name, build := range map[string]func(*attestorAPI) FenceAttestor{
		"no attestor configured": func(*attestorAPI) FenceAttestor { return nil },
		"attestor unavailable":   func(*attestorAPI) FenceAttestor { return &stubAttestor{fn: unavailable} },
		"attestor not ready":     func(*attestorAPI) FenceAttestor { return &stubAttestor{fn: pending} },
		"pod absence is not proof": func(*attestorAPI) FenceAttestor {
			return &stubAttestor{fn: absent}
		},
		"stale ledger evidence": func(*attestorAPI) FenceAttestor { return &stubAttestor{fn: stale} },
		"changed pod inventory during attestation": func(api *attestorAPI) FenceAttestor {
			return &stubAttestor{fn: func(req AttestationRequest) (*AttestationResponse, error) {
				api.addPod(independentPod("rhiza-0", "pod-recreated"))
				return independentEvidenceFor(req), nil
			}}
		},
		"changed ledger during attestation": func(api *attestorAPI) FenceAttestor {
			return &stubAttestor{fn: func(req AttestationRequest) (*AttestationResponse, error) {
				api.replaceLedger(independentLedger(fencing.Run{Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-0", RunID: "run-9", WALIdentity: "wal-9"}}))
				return independentEvidenceFor(req), nil
			}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			executor, api, record := newAttestedExecutor(t, nil)
			executor.Attestor = build(api)
			api.ledger = independentLedger(fencing.Run{Identity: independentIdentity()})
			if err := executor.reconcileFence(t.Context(), fenceEndpoint, record); err == nil {
				t.Fatal("fence completed without verifiable evidence")
			}
			if api.statusPuts != 0 || record.Status.Proof != nil {
				t.Fatalf("a proof was written without verifiable evidence: %+v", record.Status.Proof)
			}
		})
	}
	t.Run("storage scope is not a shared store", func(t *testing.T) {
		executor, api, record := newAttestedExecutor(t, &stubAttestor{fn: func(req AttestationRequest) (*AttestationResponse, error) { return independentEvidenceFor(req), nil }})
		api.sts = independentStatefulSet("filesystem")
		api.ledger = independentLedger(fencing.Run{Identity: independentIdentity()})
		if err := executor.reconcileFence(t.Context(), fenceEndpoint, record); err == nil {
			t.Fatal("fenced without a verifiable storage scope")
		}
		if api.statusPuts != 0 {
			t.Fatal("a proof was written without a verifiable storage scope")
		}
	})
}

// A restart or replay keeps the same closed operation and never writes a proof
// it could not verify.
func TestAttestedFenceReplayKeepsTheSamePendingOperation(t *testing.T) {
	executor, api, record := newAttestedExecutor(t, &stubAttestor{fn: func(AttestationRequest) (*AttestationResponse, error) { return nil, ErrFencePending }})
	api.ledger = independentLedger(fencing.Run{Identity: independentIdentity()})
	hash, err := requestHash(&record.Spec)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := executor.reconcileFence(t.Context(), fenceEndpoint, record); !errors.Is(err, ErrFencePending) {
			t.Fatalf("replay did not stay pending: %v", err)
		}
	}
	if api.statusPuts != 0 || record.Status.Proof != nil {
		t.Fatal("a replay wrote a proof")
	}
	state := api.closedState(t)
	closed := state.Ledger.ClosedNodes["rhiza-0"]
	if closed.OperationID != record.Spec.OperationID || closed.RequestHash != hash || state.StatefulSetUID != record.Spec.StatefulSetUID || len(state.Ledger.ClosedNodes) != 1 {
		t.Fatalf("replay changed the closed operation: %+v", state)
	}
}

// The CRD and the Go evidence contract must describe the same additive fields.
func TestFenceCRDCarriesTheIndependentContract(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/fence-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	schema, _ := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)
	targets, _ := schema["spec"].(map[string]any)["properties"].(map[string]any)["targets"].(map[string]any)
	fields, _ := targets["items"].(map[string]any)["properties"].(map[string]any)
	for _, field := range []string{"nodeName", "nodeUID", "containerID", "imageID"} {
		if fields[field] == nil {
			t.Fatalf("RhizaFence target has no %q identity field", field)
		}
	}
	if targets["maxItems"] != fenceTargetCapacity {
		t.Fatalf("target capacity changed: %v", targets["maxItems"])
	}
	proof, _ := schema["status"].(map[string]any)["properties"].(map[string]any)["proof"].(map[string]any)
	independent, _ := proof["properties"].(map[string]any)["independent"].(map[string]any)
	if independent == nil {
		t.Fatal("RhizaFence status proof has no independent evidence schema")
	}
	_, scope := independentFixture(t)
	for _, key := range []string{"version", "provider", "mode", "observedAt", "requestHash", "ledgerHash", "barrierID", "barrier", "runtimes", "storage"} {
		if _, ok := independent["properties"].(map[string]any)[key]; !ok {
			t.Fatalf("independent evidence schema is missing %q", key)
		}
	}
	raw, err := json.Marshal(independentEvidenceFor(scope.attestationRequest()).Independent)
	if err != nil {
		t.Fatal(err)
	}
	encoded := map[string]any{}
	if err := json.Unmarshal(raw, &encoded); err != nil {
		t.Fatal(err)
	}
	properties := independent["properties"].(map[string]any)
	for key := range encoded {
		if properties[key] == nil {
			t.Fatalf("evidence field %q is not declared in the CRD", key)
		}
	}
	for key := range properties {
		if _, ok := encoded[key]; !ok {
			t.Fatalf("CRD declares evidence field %q the authority never writes", key)
		}
	}
}

// The published schema may not describe evidence the transport refuses: every
// object the CRD accepts has to fit the response the authority actually reads.
func TestFenceCRDSchemaFitsTheAttestationTransport(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/fence-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	schema, _ := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)
	proof, _ := schema["status"].(map[string]any)["properties"].(map[string]any)["proof"].(map[string]any)
	independent, _ := proof["properties"].(map[string]any)["independent"].(map[string]any)
	worst := schemaWorstBytes(independent)
	if worst == 0 {
		t.Fatal("the independent evidence schema describes no payload")
	}
	if worst >= maxAttestationBytes {
		t.Fatalf("the schema allows %d bytes of evidence but the transport reads %d", worst, maxAttestationBytes)
	}
}

// schemaWorstBytes is the largest string capacity one schema node can describe.
// It ignores object and array punctuation, so the comparison keeps a margin.
func schemaWorstBytes(node map[string]any) int {
	switch node["type"] {
	case "string":
		return schemaInt(node["maxLength"])
	case "array":
		count := schemaInt(node["maxItems"])
		if count == 0 {
			count = 1
		}
		return count * schemaWorstBytes(asObject(node["items"]))
	case "object":
		total := 0
		for _, property := range asObject(node["properties"]) {
			total += schemaWorstBytes(asObject(property))
		}
		return total
	}
	return 0
}

func schemaInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	}
	return 0
}
