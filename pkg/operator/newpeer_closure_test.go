package operator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"gopkg.in/yaml.v3"
)

func TestPeerClosureOperationIDBindsRoundButNotCallerOrTarget(t *testing.T) {
	base := peerClosureOperationID("root-uid", "workload-uid", "source", 9, 12)
	if base == "" || base != peerClosureOperationID("root-uid", "workload-uid", "source", 9, 12) {
		t.Fatal("operation ID is not deterministic")
	}
	for _, changed := range []string{
		peerClosureOperationID("other-root", "workload-uid", "source", 9, 12),
		peerClosureOperationID("root-uid", "other-workload", "source", 9, 12),
		peerClosureOperationID("root-uid", "workload-uid", "other-source", 9, 12),
		peerClosureOperationID("root-uid", "workload-uid", "source", 10, 12),
		peerClosureOperationID("root-uid", "workload-uid", "source", 9, 13),
	} {
		if changed == base {
			t.Fatal("operation ID omitted a binding or round field")
		}
	}
	// Caller and target are intentionally not inputs: callers in the same source
	// round contend on one immutable operation, whose full FenceRequest hash
	// binds the target.
}

func TestPeerClosureObservationRequiresExactFreshPositiveTuple(t *testing.T) {
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", WALIdentity: "wal-0"}
	request := fencing.PeerClosureRequest{ExpectedConfigID: 4, ExpectedAbortSlot: peerClosureUint64(8), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	valid := network.VoterRecoveryStatus{ClusterID: "source", NodeID: "rhiza-0", WALIdentity: "wal-0", ConfigID: 4, AbortSlot: 8, Voters: []string{"rhiza-0", "rhiza-1"}, Voting: true, QuorumKnown: true, Quorum: true, QuorumConfigID: 4, ReadIndexSlot: 20, CertifiedTip: 20}
	if !peerClosureObservationValid(valid, quorumProbeAvailable, caller, request) {
		t.Fatal("valid signed fresh survivor tuple rejected")
	}
	cases := map[string]func(*network.VoterRecoveryStatus){
		"unknown":               func(s *network.VoterRecoveryStatus) { s.QuorumKnown = false },
		"unavailable":           func(s *network.VoterRecoveryStatus) { s.QuorumUnavailable = true },
		"wrong config":          func(s *network.VoterRecoveryStatus) { s.ConfigID++ },
		"wrong abort slot":      func(s *network.VoterRecoveryStatus) { s.AbortSlot++ },
		"wrong source":          func(s *network.VoterRecoveryStatus) { s.ClusterID = "other" },
		"wrong caller WAL":      func(s *network.VoterRecoveryStatus) { s.WALIdentity = "other" },
		"caller not a voter":    func(s *network.VoterRecoveryStatus) { s.Voters = []string{"rhiza-1", "rhiza-2"} },
		"target not a voter":    func(s *network.VoterRecoveryStatus) { s.Voters = []string{"rhiza-0", "rhiza-2"} },
		"pending":               func(s *network.VoterRecoveryStatus) { s.Pending = true },
		"not voting":            func(s *network.VoterRecoveryStatus) { s.Voting = false },
		"stale certified tip":   func(s *network.VoterRecoveryStatus) { s.CertifiedTip-- },
		"config tuple mismatch": func(s *network.VoterRecoveryStatus) { s.QuorumConfigID++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			status := valid
			status.Voters = append([]string(nil), valid.Voters...)
			mutate(&status)
			if peerClosureObservationValid(status, quorumProbeAvailable, caller, request) {
				t.Fatal("invalid survivor observation authorized closure")
			}
		})
	}
	if peerClosureObservationValid(valid, quorumProbeUnknown, caller, request) {
		t.Fatal("unknown observation authorized closure")
	}
}

func TestPeerClosureStatusMissingDoesNotCreateFence(t *testing.T) {
	var posts int
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/test/pods/rhiza-0":
			_ = json.NewEncoder(w).Encode(peerClosureCallerPod("8080"))
		case "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(peerClosureStatefulSet("8080"))
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{peerClosureRoot()}})
		case (&FencingExecutor{Kube: &Kubernetes{Namespace: "test"}}).ledgerPath("workload-uid", "source"):
			state := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
				"run-0": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "run-0", WALIdentity: "wal-0"}},
			}}}
			_ = json.NewEncoder(w).Encode(object{"data": object{"ledger.json": string(jsonBytes(state))}})
		default:
			if r.Method == http.MethodPost {
				posts++
			}
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	token := t.TempDir() + "/token"
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "run-0", WALIdentity: "wal-0"}
	closure := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	_, err := e.readPeerClosure(context.Background(), caller, closure, "workload-uid")
	if !errors.Is(err, fencing.ErrNotFound) {
		t.Fatalf("missing status error = %v, want ErrNotFound", err)
	}
	if posts != 0 {
		t.Fatalf("status lookup created %d Kubernetes resource(s)", posts)
	}
}

func TestPeerClosureRejectsMissingAbortSlotBeforeKubernetesRead(t *testing.T) {
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "run-0", WALIdentity: "wal-0"}
	closure := fencing.PeerClosureRequest{ExpectedConfigID: 1, TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	e := &FencingExecutor{}
	if _, err := e.requestPeerClosure(context.Background(), caller, closure, "workload-uid"); err == nil {
		t.Fatal("create accepted an absent abort slot")
	}
	if _, err := e.readPeerClosure(context.Background(), caller, closure, "workload-uid"); err == nil {
		t.Fatal("status accepted an absent abort slot")
	}
}

func TestPeerClosureDoesNotTrustRootAnnotationOnNonControllerPod(t *testing.T) {
	pod := peerClosureCallerPod("8080")
	asObject(nested(pod, "metadata", "annotations"))[fencingRootAnnotation] = "root-uid"
	owner := asObject(list(nested(pod, "metadata", "ownerReferences"))[0])
	owner["controller"] = false
	var statefulSetReads int
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/test/pods/rhiza-0" {
			_ = json.NewEncoder(w).Encode(pod)
			return
		}
		if r.URL.Path == "/apis/apps/v1/namespaces/test/statefulsets/rhiza" {
			statefulSetReads++
			_ = json.NewEncoder(w).Encode(peerClosureStatefulSet("8080"))
			return
		}
		http.NotFound(w, r)
	}))
	defer api.Close()
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", Client: api.Client()}}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "run-0", WALIdentity: "wal-0"}
	if _, err := e.peerClosureBinding(context.Background(), caller, "workload-uid"); err == nil {
		t.Fatal("standalone Pod with a root annotation acquired StatefulSet authority")
	}
	if statefulSetReads != 0 {
		t.Fatal("non-controller owner was treated as a workload binding")
	}
}

func TestPeerClosureRequestCreatesExactVoterFenceFromAuthenticatedObservation(t *testing.T) {
	const adminToken = "signed-probe-token"
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/recovery/probe" || r.URL.Query().Get("version") != "1" || r.URL.Query().Get("exclude") != "rhiza-1" {
			http.Error(w, "unexpected probe", http.StatusBadRequest)
			return
		}
		observation := network.RecoveryProbe{Version: network.RecoveryProbeProtocolVersion, Excluded: "rhiza-1", Nonce: r.URL.Query().Get("nonce"), Status: network.VoterRecoveryStatus{
			NodeID: "rhiza-0", ClusterID: "source", ConfigID: 3, Voters: []string{"rhiza-0", "rhiza-1", "rhiza-2"}, WALIdentity: "wal-0",
			Voting: true, AbortSlot: 7, Quorum: true, QuorumKnown: true, QuorumConfigID: 3, ReadIndexSlot: 12, CertifiedTip: 12,
		}}
		observation.MAC = peerClosureProbeMAC(observation, adminToken)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(observation)
	}))
	defer probe.Close()
	probeURL, err := url.Parse(probe.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(probeURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	var created fenceResource
	var mu sync.Mutex
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews" && r.Method == http.MethodPost:
			_ = json.NewEncoder(w).Encode(object{"status": object{"authenticated": true, "audiences": []any{"rhiza-fencer"}, "user": object{
				"username": "system:serviceaccount:test:default",
				"extra":    object{"authentication.kubernetes.io/pod-uid": []any{"caller-uid"}, "authentication.kubernetes.io/pod-name": []any{"rhiza-0"}},
			}}})
		case r.URL.Path == "/api/v1/namespaces/test/pods/rhiza-0":
			_ = json.NewEncoder(w).Encode(peerClosureCallerPod(port))
		case r.URL.Path == "/api/v1/namespaces/test/pods/rhiza-1":
			http.NotFound(w, r)
		case r.URL.Path == "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(peerClosureStatefulSet(port))
		case r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{peerClosureRoot()}})
		case r.URL.Path == (&FencingExecutor{Kube: &Kubernetes{Namespace: "test"}}).ledgerPath("workload-uid", "source"):
			state := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
				"caller-run": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-0"}},
				"target-run": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "target-uid", RunID: "target-run", WALIdentity: "wal-1"}},
			}}}
			_ = json.NewEncoder(w).Encode(object{"data": object{"ledger.json": string(jsonBytes(state))}})
		case strings.HasPrefix(r.URL.Path, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences/fence-") && r.Method == http.MethodGet:
			http.NotFound(w, r)
		case r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences" && r.Method == http.MethodPost:
			mu.Lock()
			defer mu.Unlock()
			if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
				http.Error(w, "invalid fence", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := t.TempDir() + "/token"
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-0"}
	closure := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	_, err = e.requestPeerClosure(context.Background(), caller, closure, "workload-uid")
	if !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("request result = %v, want accepted pending fence", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if created.Spec.Scope != ScopeVoter || created.Spec.OperationID != peerClosureOperationID("root-uid", "workload-uid", "source", 3, 7) || created.Spec.SourceClusterID != "source" || created.Spec.BindingUID != "root-uid" || created.Spec.StatefulSetUID != "workload-uid" || len(created.Spec.Targets) != 1 {
		t.Fatalf("created immutable request has wrong scope/binding: %+v", created.Spec)
	}
	target := created.Spec.Targets[0]
	if target.NodeID != "rhiza-1" || target.Pod != "rhiza-1" || target.PodUID != "target-uid" || target.WALIdentity != "wal-1" || target.ContainerID != "" {
		t.Fatalf("target evidence did not preserve missing-Pod runtime uncertainty: %+v", target)
	}
}

func TestPeerClosureAuthorityRBACCreateIsFenceOnly(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/fencing/authority.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var documents []map[string]any
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		if len(doc) > 0 {
			documents = append(documents, doc)
		}
	}
	if len(documents) < 2 || documents[1]["kind"] != "Role" || nested(asObject(documents[1]["metadata"]), "namespace") != "rhiza" {
		t.Fatal("expected namespaced fencing authority Role")
	}
	roleBindingOK := false
	for _, document := range documents {
		if document["kind"] == "RoleBinding" && nested(asObject(document["metadata"]), "namespace") == "rhiza" && nested(asObject(document["roleRef"]), "kind") == "Role" {
			roleBindingOK = true
		}
	}
	if !roleBindingOK {
		t.Fatal("expected namespace-scoped RoleBinding to the namespaced Role")
	}
	rules := list(nested(asObject(documents[1]), "rules"))
	createFence, readFence, createCluster := false, false, false
	createFenceVerbs := []string(nil)
	for _, raw := range rules {
		rule := asObject(raw)
		resources := stringSlice(list(rule["resources"]))
		verbs := stringSlice(list(rule["verbs"]))
		hasCreate := containsString(verbs, "create")
		if containsString(resources, "rhizafences") && hasCreate {
			createFence = true
			createFenceVerbs = verbs
		}
		if containsString(resources, "rhizaclusters") && hasCreate {
			createCluster = true
		}
		if containsString(resources, "rhizafences") && containsString(verbs, "get") && containsString(verbs, "list") {
			readFence = true
		}
	}
	if !createFence || !readFence || createCluster || len(createFenceVerbs) != 1 || createFenceVerbs[0] != "create" {
		t.Fatalf("unexpected Role resource scope: fence create=%t read=%t create verbs=%v cluster create=%t", createFence, readFence, createFenceVerbs, createCluster)
	}
}

func peerClosureCallerPod(port string) object {
	return object{
		"metadata": object{"name": "rhiza-0", "uid": "caller-uid", "annotations": object{fencingProtocolAnnotation: "1"}, "ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "workload-uid", "controller": true}}},
		"spec":     object{"containers": []any{object{"name": "rhiza", "command": []any{"/usr/local/bin/rhiza-fencer"}, "args": []any{"supervise", "--", "/usr/local/bin/rhiza"}, "env": []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}, "ports": []any{object{"name": "recovery", "containerPort": portNumber(port)}}, "securityContext": object{"allowPrivilegeEscalation": false, "capabilities": object{"drop": []any{"ALL"}}}}}},
		"status":   object{"podIP": "127.0.0.1"},
	}
}

func peerClosureStatefulSet(port string) object {
	return object{
		"metadata": object{"name": "rhiza", "uid": "workload-uid"},
		"spec":     object{"template": object{"metadata": object{"annotations": object{fencingProtocolAnnotation: "1"}}, "spec": object{"containers": []any{object{"name": "rhiza", "env": []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}, object{"name": "RHIZA_ADMIN_TOKEN", "value": "signed-probe-token"}}, "ports": []any{object{"name": "recovery", "containerPort": portNumber(port)}}}}}}},
	}
}

func portNumber(port string) int {
	value, _ := strconv.Atoi(port)
	return value
}

func peerClosureProbeMAC(probe network.RecoveryProbe, token string) string {
	probe.MAC = ""
	data, _ := json.Marshal(probe)
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func peerClosureRoot() object {
	return object{"metadata": object{"uid": "root-uid", "name": "root", "namespace": "test"}, "spec": ClusterSpec{LogicalID: "logical", StatefulSet: "rhiza", Automatic: true}, "status": ClusterStatus{ActiveClusterID: "source"}}
}

func stringSlice(values []any) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, str(value))
	}
	return result
}

func peerClosureUint64(value uint64) *uint64 { return &value }
