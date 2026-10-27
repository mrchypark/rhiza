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
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
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
	root := peerClosureRoot()
	bucket := objstore.NewInMemBucket()
	rootConfig := rootResource(root)
	seedPeerClosureAutoBinding(t, bucket, "root", root, "workload-uid", "workload-uid", autoBinding(&rootConfig))
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/test/pods/rhiza-0":
			_ = json.NewEncoder(w).Encode(peerClosureCallerPod("8080"))
		case "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(peerClosureStatefulSet("8080"))
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
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
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}, Bucket: bucket, Prefix: "root"}
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
	token := t.TempDir() + "/token"
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "run-0", WALIdentity: "wal-0"}
	if _, err := e.peerClosureBinding(context.Background(), caller, "workload-uid"); err == nil {
		t.Fatal("standalone Pod with a root annotation acquired StatefulSet authority")
	}
	if statefulSetReads != 0 {
		t.Fatal("non-controller owner was treated as a workload binding")
	}
}

func TestPeerClosureRequiresExistingAutomaticBinding(t *testing.T) {
	for _, test := range []struct {
		name               string
		controlWorkloadUID string
		bindingHash        string
		wantErr            string
	}{
		{name: "missing record", controlWorkloadUID: "workload-uid", wantErr: "durable automatic control unavailable"},
		{name: "old durable StatefulSet UID", controlWorkloadUID: "old-workload-uid", bindingHash: "current-hash", wantErr: "durable automatic binding differs"},
		{name: "mismatched workload hash", controlWorkloadUID: "workload-uid", bindingHash: "stale-hash", wantErr: "durable workload binding unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := peerClosureRoot()
			rootConfig := rootResource(root)
			bucket := objstore.NewInMemBucket()
			if test.name != "missing record" {
				bindingHash := test.bindingHash
				if bindingHash == "current-hash" {
					bindingHash = autoBinding(&rootConfig)
				}
				seedPeerClosureAutoBinding(t, bucket, "root", root, "workload-uid", test.controlWorkloadUID, bindingHash)
			}
			sts := peerClosureStatefulSet("8080")
			container := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
			container["env"] = append(list(container["env"]), object{"name": "RHIZA_ADMIN_TOKEN", "value": "test-token"})
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/namespaces/test/pods/rhiza-0":
					_ = json.NewEncoder(w).Encode(peerClosureCallerPod("8080"))
				case "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
					_ = json.NewEncoder(w).Encode(sts)
				case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
					_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
				default:
					http.NotFound(w, r)
				}
			}))
			defer api.Close()
			token := t.TempDir() + "/token"
			if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
				t.Fatal(err)
			}
			e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}, Bucket: bucket, Prefix: "root"}
			caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "run-0", WALIdentity: "wal-0"}
			if _, err := e.peerClosureBinding(context.Background(), caller, "workload-uid"); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("peerClosureBinding error=%v, want containing %q", err, test.wantErr)
			}
			if test.name == "missing record" {
				for _, key := range []string{
					path.Join("root", "automatic", "test", "logical", "control.json"),
					path.Join("root", "automatic-workloads", "test", "workload-uid", "binding.json"),
				} {
					if exists, err := bucket.Exists(context.Background(), key); err != nil || exists {
						t.Fatalf("missing binding check mutated %q: exists=%t err=%v", key, exists, err)
					}
				}
			}
		})
	}
}

func TestPeerClosureRequestCreatesExactVoterFenceFromAuthenticatedObservation(t *testing.T) {
	const adminToken = "signed-probe-token"
	var probeRequests atomic.Int32
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probeRequests.Add(1)
		excluded := r.URL.Query().Get("exclude")
		if r.URL.Path != "/recovery/probe" || r.URL.Query().Get("version") != "1" || (excluded != "rhiza-1" && excluded != "rhiza-2") {
			http.Error(w, "unexpected probe", http.StatusBadRequest)
			return
		}
		observation := network.RecoveryProbe{Version: network.RecoveryProbeProtocolVersion, Excluded: excluded, Nonce: r.URL.Query().Get("nonce"), Status: network.VoterRecoveryStatus{
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
	root := peerClosureRoot()
	bucket := objstore.NewInMemBucket()
	rootConfig := rootResource(root)
	seedPeerClosureAutoBinding(t, bucket, "root", root, "workload-uid", "workload-uid", autoBinding(&rootConfig))
	var created fenceResource
	var fenceStored bool
	var fencePosts int
	ledgerState := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
		"caller-run":     {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-0"}},
		"target-bound":   {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "target-uid", RunID: "target-bound", WALIdentity: "wal-1"}},
		"target-unbound": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "target-uid", RunID: "target-unbound"}},
		"target2-run":    {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-2", PodUID: "target2-uid", RunID: "target2-run", WALIdentity: "wal-2"}},
	}}}
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
			_ = json.NewEncoder(w).Encode(peerClosureTargetPod("target-uid", "worker-1", "containerd://target", "sha256:target"))
		case r.URL.Path == "/api/v1/namespaces/test/pods/rhiza-2":
			http.NotFound(w, r)
		case r.URL.Path == "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(peerClosureStatefulSet(port))
		case r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
		case r.URL.Path == (&FencingExecutor{Kube: &Kubernetes{Namespace: "test"}}).ledgerPath("workload-uid", "source"):
			mu.Lock()
			defer mu.Unlock()
			if r.Method == http.MethodGet {
				_ = json.NewEncoder(w).Encode(object{"metadata": object{"resourceVersion": "1"}, "data": object{"ledger.json": string(jsonBytes(ledgerState))}})
				return
			}
			if r.Method == http.MethodPut {
				var resource object
				if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
					http.Error(w, "invalid ledger", http.StatusBadRequest)
					return
				}
				if err := json.Unmarshal([]byte(str(asObject(resource["data"])["ledger.json"])), &ledgerState); err != nil {
					http.Error(w, "invalid ledger body", http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusOK)
				return
			}
		case strings.HasPrefix(r.URL.Path, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences/fence-") && r.Method == http.MethodGet:
			mu.Lock()
			defer mu.Unlock()
			if !fenceStored {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(created)
		case r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences" && r.Method == http.MethodPost:
			mu.Lock()
			defer mu.Unlock()
			fencePosts++
			if fencePosts == 1 {
				if err := json.NewDecoder(r.Body).Decode(&created); err != nil {
					http.Error(w, "invalid fence", http.StatusBadRequest)
					return
				}
				fenceStored = true // the API persisted the CR but the response was lost
				http.Error(w, "injected create response failure", http.StatusServiceUnavailable)
				return
			}
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
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}, Bucket: bucket, Prefix: "root"}
	authority := httptest.NewTLSServer(e.Handler())
	defer authority.Close()
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: tokenPath, Client: authority.Client()}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-0"}
	closure := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	if _, err := client.RequestPeerClosure(context.Background(), caller, closure); !errors.Is(err, fencing.ErrUnavailable) {
		t.Fatalf("first fence create response loss = %v, want ErrUnavailable", err)
	}
	mu.Lock()
	firstReservation := ledgerState.Ledger.PeerClosure
	mu.Unlock()
	if firstReservation == nil || firstReservation.TargetNodeID != "rhiza-1" {
		t.Fatalf("failed create did not retain durable reservation: %+v", firstReservation)
	}
	changedTarget := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-2", TargetWALIdentity: "wal-2"}
	if _, err := client.RequestPeerClosure(context.Background(), caller, changedTarget); err == nil {
		t.Fatal("changed target was returned from the stored CR after response loss")
	}
	mu.Lock()
	unchanged := ledgerState.Ledger.PeerClosure != nil && ledgerState.Ledger.PeerClosure.OperationID == firstReservation.OperationID && ledgerState.Ledger.PeerClosure.RequestHash == firstReservation.RequestHash && ledgerState.Ledger.PeerClosure.TargetNodeID == "rhiza-1"
	mu.Unlock()
	if !unchanged {
		t.Fatal("changed-target request replaced the durable peer-closure reservation")
	}
	if _, err := client.RequestPeerClosure(context.Background(), caller, closure); !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("exact retry result = %v, want accepted pending fence", err)
	}
	if _, err := client.ReadPeerClosure(context.Background(), caller, closure); !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("status without complete R0/R1 evidence = %v, want pending", err)
	}
	mu.Lock()
	if fencePosts != 1 {
		t.Fatalf("fence POST attempts=%d, want exactly one persisted create despite response loss", fencePosts)
	}
	if created.Spec.Scope != ScopeVoter || created.Spec.OperationID != peerClosureOperationID("root-uid", "workload-uid", "source", 3, 7) || created.Spec.SourceClusterID != "source" || created.Spec.BindingUID != "root-uid" || created.Spec.StatefulSetUID != "workload-uid" || len(created.Spec.Targets) != 2 {
		t.Fatalf("created immutable request has wrong scope/binding: %+v", created.Spec)
	}
	if created.Spec.Targets[0].WALIdentity != "" || created.Spec.Targets[1].WALIdentity != "wal-1" {
		t.Fatalf("CR collapsed or rewrote retained unbound WAL history: %+v", created.Spec.Targets)
	}
	for _, target := range created.Spec.Targets {
		if target.NodeID != "rhiza-1" || target.Pod != "rhiza-1" || target.PodUID != "target-uid" || target.NodeName != "worker-1" || target.ContainerID != "containerd://target" || target.ImageID != "sha256:target" {
			t.Fatalf("target evidence did not preserve the live Pod runtime identity: %+v", target)
		}
	}
	createdHash, err := requestHash(&created.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if ledgerState.Ledger.PeerClosure == nil || ledgerState.Ledger.PeerClosure.OperationID != created.Spec.OperationID || ledgerState.Ledger.PeerClosure.RequestHash != createdHash || ledgerState.Ledger.PeerClosure.TargetNodeID != created.Spec.Targets[0].NodeID || len(ledgerState.Ledger.PeerClosure.Targets) != len(created.Spec.Targets) {
		t.Fatalf("exact retry reservation differs from immutable CR: reservation=%+v created=%+v", ledgerState.Ledger.PeerClosure, created.Spec)
	}
	closedState := ledgerState
	storage := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	closedScope, err := buildIndependentScope(closedState, created.Spec, []string{"rhiza-1"}, createdHash, e.ledgerHash(closedState), []string{"target-uid"}, storage)
	if err != nil {
		mu.Unlock()
		t.Fatalf("complete retained history did not produce evidence scope: %v", err)
	}
	if _, err := validateIndependent(closedScope, nil); err == nil {
		mu.Unlock()
		t.Fatal("missing R0/R1 runtime and storage evidence was accepted")
	}
	response := independentEvidenceFor(closedScope.attestationRequest())
	response.Independent.Runtimes = []EvidenceRuntime{{
		NodeName: "worker-1", NodeUID: "node-uid-worker-1", InstanceID: "instance-target-uid", PodName: "rhiza-1", PodUID: "target-uid",
		ContainerIDs: []string{"containerd://target"}, ImageIDs: []string{"sha256:target"},
		RunIDs: []string{"target-bound", "target-unbound"}, WALIdentities: []string{"wal-1", ""}, State: RuntimeStateTerminated, RuntimeEvidence: "instance terminated",
	}}
	proof, err := validateIndependent(closedScope, response)
	if err != nil {
		mu.Unlock()
		t.Fatalf("complete R0/R1 and stable-storage evidence rejected: %v", err)
	}
	created.Status.Proof = proof
	mu.Unlock()
	receipt, err := client.ReadPeerClosure(context.Background(), caller, closure)
	if err != nil {
		t.Fatalf("full HTTP status path rejected complete proof: %v", err)
	}
	if len(receipt.Targets) != 2 || receipt.Targets[0].WALIdentity != "" || receipt.Targets[1].WALIdentity != "wal-1" {
		t.Fatalf("full HTTP receipt dropped unbound history: %+v", receipt.Targets)
	}
	mu.Lock()
	for runID, run := range ledgerState.Ledger.Runs {
		if run.Identity.NodeID != closure.TargetNodeID {
			continue
		}
		if run.Stop == nil {
			mu.Unlock()
			t.Fatalf("completed target run %q has no durable stop", runID)
		}
		if run.Identity.WALIdentity == "" {
			if err := ledgerState.Ledger.RecordNeverBoundExit(*run.Stop); err != nil {
				mu.Unlock()
				t.Fatalf("record unbound target exit: %v", err)
			}
			continue
		}
		if err := ledgerState.Ledger.RecordDrain(fencing.DrainReceipt{Request: *run.Stop, WritersJoined: true}); err != nil {
			mu.Unlock()
			t.Fatalf("record target drain: %v", err)
		}
		if err := ledgerState.Ledger.RecordExit(*run.Stop); err != nil {
			mu.Unlock()
			t.Fatalf("record target exit: %v", err)
		}
	}
	nextClosure := fencing.PeerClosureRequest{ExpectedConfigID: 4, ExpectedAbortSlot: peerClosureUint64(8), TargetNodeID: "rhiza-2", TargetWALIdentity: "wal-2"}
	nextTargets := []fencing.PeerClosureTarget{{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "target2-uid", WALIdentity: "wal-2"}}
	nextOperationID := peerClosureOperationID("root-uid", "workload-uid", "source", nextClosure.ExpectedConfigID, *nextClosure.ExpectedAbortSlot)
	nextBinding := peerClosureBinding{root: rootResource(root), rootUID: "root-uid", namespace: "test", workloadUID: "workload-uid"}
	nextRequest := e.peerClosureFenceRequest(nextBinding, "source", nextClosure, nextOperationID, nextTargets)
	nextHash, err := requestHash(&nextRequest)
	if err != nil {
		mu.Unlock()
		t.Fatalf("hash next voter request: %v", err)
	}
	if err := ledgerState.Ledger.ReservePeerClosure(fencing.PeerClosureReservation{
		OperationID: nextOperationID, RequestHash: nextHash, ConfigID: nextClosure.ExpectedConfigID, AbortSlot: *nextClosure.ExpectedAbortSlot,
		TargetNodeID: nextClosure.TargetNodeID, TargetWAL: nextClosure.TargetWALIdentity,
		Voters: []string{"rhiza-0", "rhiza-2"}, Targets: nextTargets,
	}); err != nil {
		mu.Unlock()
		t.Fatalf("reserve next voter after completed target removal: %v", err)
	}
	mu.Unlock()
	probesBeforeHistoricalLookup := probeRequests.Load()
	mu.Lock()
	postsBeforeHistoricalLookup := fencePosts
	mu.Unlock()
	if _, err := client.RequestPeerClosure(context.Background(), caller, closure); err == nil {
		t.Fatal("request retry returned a historical receipt after a later target reservation")
	}
	receipt, err = client.ReadPeerClosure(context.Background(), caller, closure)
	if err != nil || receipt.OperationID != created.Spec.OperationID || receipt.RequestHash != createdHash {
		t.Fatalf("historical status after next target reservation = %+v, err=%v", receipt, err)
	}
	mu.Lock()
	postsAfterHistoricalLookup := fencePosts
	mu.Unlock()
	if probesAfter := probeRequests.Load(); probesAfter != probesBeforeHistoricalLookup || postsAfterHistoricalLookup != postsBeforeHistoricalLookup {
		t.Fatalf("historical lookup probed or created a fence: probes %d->%d, POSTs %d->%d", probesBeforeHistoricalLookup, probesAfter, postsBeforeHistoricalLookup, postsAfterHistoricalLookup)
	}
	mu.Lock()
	created.Spec.Targets[0].ContainerID = "containerd://changed-incarnation"
	mu.Unlock()
	if _, err := client.RequestPeerClosure(context.Background(), caller, closure); err == nil {
		t.Fatal("same-operation CR with a changed target inventory was returned")
	}
	if _, err := client.ReadPeerClosure(context.Background(), caller, closure); err == nil {
		t.Fatal("status path returned a CR whose inventory differs from the durable reservation")
	}
}

func TestPeerClosurePreservesPostProbeKubernetesUnavailableThroughClient(t *testing.T) {
	const adminToken = "signed-probe-token"
	probeCompleted := atomic.Bool{}
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observation := network.RecoveryProbe{Version: network.RecoveryProbeProtocolVersion, Excluded: "rhiza-1", Nonce: r.URL.Query().Get("nonce"), Status: network.VoterRecoveryStatus{
			NodeID: "rhiza-0", ClusterID: "source", ConfigID: 3, Voters: []string{"rhiza-0", "rhiza-1", "rhiza-2"}, WALIdentity: "wal-0",
			Voting: true, AbortSlot: 7, Quorum: true, QuorumKnown: true, QuorumConfigID: 3, ReadIndexSlot: 12, CertifiedTip: 12,
		}}
		observation.MAC = peerClosureProbeMAC(observation, adminToken)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(observation)
		probeCompleted.Store(true)
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
	root := peerClosureRoot()
	bucket := objstore.NewInMemBucket()
	rootConfig := rootResource(root)
	seedPeerClosureAutoBinding(t, bucket, "root", root, "workload-uid", "workload-uid", autoBinding(&rootConfig))
	var fencePosts, ledgerWrites atomic.Int32
	var api *httptest.Server
	api = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews" && r.Method == http.MethodPost:
			_ = json.NewEncoder(w).Encode(object{"status": object{"authenticated": true, "audiences": []any{"rhiza-fencer"}, "user": object{
				"username": "system:serviceaccount:test:default",
				"extra":    object{"authentication.kubernetes.io/pod-uid": []any{"caller-uid"}, "authentication.kubernetes.io/pod-name": []any{"rhiza-0"}},
			}}})
		case r.URL.Path == "/api/v1/namespaces/test/pods/rhiza-0":
			_ = json.NewEncoder(w).Encode(peerClosureCallerPod(port))
		case r.URL.Path == "/apis/apps/v1/namespaces/test/statefulsets/rhiza":
			if probeCompleted.Load() {
				http.Error(w, "temporary Kubernetes API outage", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(peerClosureStatefulSet(port))
		case r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
		case r.URL.Path == (&FencingExecutor{Kube: &Kubernetes{Namespace: "test"}}).ledgerPath("workload-uid", "source"):
			if r.Method != http.MethodGet {
				ledgerWrites.Add(1)
			}
			state := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
				"caller-run": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-0"}},
				"target-run": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "target-uid", RunID: "target-run", WALIdentity: "wal-1"}},
			}}}
			_ = json.NewEncoder(w).Encode(object{"data": object{"ledger.json": string(jsonBytes(state))}})
		case strings.HasPrefix(r.URL.Path, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"):
			if r.Method == http.MethodPost {
				fencePosts.Add(1)
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := t.TempDir() + "/token"
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	executor := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: tokenPath, Client: api.Client()}, Bucket: bucket, Prefix: "root"}
	authority := httptest.NewTLSServer(executor.Handler())
	defer authority.Close()
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-0"}
	closure := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-1"}
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: tokenPath, Client: authority.Client()}
	if _, err := client.RequestPeerClosure(context.Background(), caller, closure); !errors.Is(err, fencing.ErrUnavailable) {
		t.Fatalf("post-probe Kubernetes 503 = %v, want ErrUnavailable", err)
	}
	if !probeCompleted.Load() {
		t.Fatal("test did not reach post-probe binding revalidation")
	}
	if fencePosts.Load() != 0 || ledgerWrites.Load() != 0 {
		t.Fatalf("unavailable revalidation made durable/exec writes: fencePosts=%d ledgerWrites=%d", fencePosts.Load(), ledgerWrites.Load())
	}
}

func TestPeerClosureTokenReview429And503RemainUnavailableThroughClient(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var tokenReviews, otherRequests atomic.Int32
			kube := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews" {
					tokenReviews.Add(1)
					http.Error(w, "injected TokenReview outage", status)
					return
				}
				otherRequests.Add(1)
				http.NotFound(w, r)
			}))
			defer kube.Close()
			token := t.TempDir() + "/token"
			if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
				t.Fatal(err)
			}
			executor := &FencingExecutor{Kube: &Kubernetes{BaseURL: kube.URL, Namespace: "test", TokenFile: token, Client: kube.Client()}}
			authority := httptest.NewTLSServer(executor.Handler())
			defer authority.Close()
			client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: token, Client: authority.Client()}
			caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-caller"}
			closure := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-target"}
			if _, err := client.RequestPeerClosure(context.Background(), caller, closure); !errors.Is(err, fencing.ErrUnavailable) {
				t.Fatalf("TokenReview HTTP %d surfaced as %v, want ErrUnavailable", status, err)
			}
			if tokenReviews.Load() != 1 || otherRequests.Load() != 0 {
				t.Fatalf("authentication outage escaped TokenReview: calls=%d otherRequests=%d", tokenReviews.Load(), otherRequests.Load())
			}
		})
	}
}

func TestPeerClosureTargetsPreserveUnboundHistoricalRun(t *testing.T) {
	pod := object{"metadata": object{"uid": "target-uid"}, "spec": object{"nodeName": "worker-1"}, "status": object{"containerStatuses": []any{object{"containerID": "containerd://target", "imageID": "sha256:target"}}}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/test/pods/rhiza-1" {
			_ = json.NewEncoder(w).Encode(pod)
			return
		}
		http.NotFound(w, r)
	}))
	defer api.Close()
	token := t.TempDir() + "/token"
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-caller"}
	closure := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-target"}
	runs := []fencing.Run{
		{Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "target-uid", RunID: "run-unbound"}},
		{Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "target-uid", RunID: "run-bound", WALIdentity: "wal-target"}},
	}
	targets, err := e.peerClosureTargets(context.Background(), runs, closure)
	if err != nil {
		t.Fatalf("retained unbound run rejected: %v", err)
	}
	if len(targets) != 2 || targets[0].WALIdentity != "" || targets[1].WALIdentity != "wal-target" {
		t.Fatalf("target history was collapsed or rewritten: %+v", targets)
	}
	for i := range targets {
		targets[i].NodeUID = "node-uid-target"
		if targets[i].NodeName != "worker-1" || targets[i].NodeUID == "" || targets[i].ContainerID == "" || targets[i].ImageID == "" {
			t.Fatalf("captured runtime metadata was lost: %+v", targets[i])
		}
	}
	state := fenceLedger{StatefulSetUID: "workload-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
		"run-unbound": runs[0], "run-bound": runs[1],
	}}}
	request := FenceRequest{LogicalID: "logical", BindingUID: "root-uid", Namespace: "test", StatefulSet: "rhiza", StatefulSetUID: "workload-uid", SourceClusterID: "source", OperationID: "op-one", Scope: ScopeVoter, Targets: []FenceTarget{
		{NodeID: targets[0].NodeID, Pod: targets[0].Pod, PodUID: targets[0].PodUID, WALIdentity: targets[0].WALIdentity, NodeName: targets[0].NodeName, NodeUID: targets[0].NodeUID, ContainerID: targets[0].ContainerID, ImageID: targets[0].ImageID},
		{NodeID: targets[1].NodeID, Pod: targets[1].Pod, PodUID: targets[1].PodUID, WALIdentity: targets[1].WALIdentity, NodeName: targets[1].NodeName, NodeUID: targets[1].NodeUID, ContainerID: targets[1].ContainerID, ImageID: targets[1].ImageID},
	}}
	requestHash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	storage := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	if _, err := buildIndependentScope(state, request, []string{"rhiza-1"}, requestHash, "ledger", []string{"target-uid"}, storage); err != nil {
		t.Fatalf("retained unbound run not represented in independent evidence scope: %v", err)
	}
	ledger := fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
		"run-unbound": runs[0], "run-bound": runs[1],
		"run-next": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-2", PodUID: "next-uid", RunID: "run-next", WALIdentity: "wal-next"}},
	}}
	if err := ledger.ReservePeerClosure(fencing.PeerClosureReservation{
		OperationID: request.OperationID, RequestHash: requestHash, ConfigID: 3, AbortSlot: 7, TargetNodeID: "rhiza-1", TargetWAL: "wal-target",
		Voters: []string{"rhiza-0", "rhiza-1", "rhiza-2"}, Targets: targets,
	}); err != nil {
		t.Fatalf("captured target metadata did not match ledger identity coverage: %v", err)
	}
	unboundStop := *ledger.Runs["run-unbound"].Stop
	if err := ledger.RecordNeverBoundExit(unboundStop); err != nil {
		t.Fatal(err)
	}
	boundStop := *ledger.Runs["run-bound"].Stop
	if err := ledger.RecordDrain(fencing.DrainReceipt{Request: boundStop, WritersJoined: true}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.RecordExit(boundStop); err != nil {
		t.Fatal(err)
	}
	closedState := state
	closedState.Ledger = ledger
	closedScope, err := buildIndependentScope(closedState, request, []string{"rhiza-1"}, requestHash, "closed-ledger-hash", []string{"target-uid"}, storage)
	if err != nil {
		t.Fatalf("closed retained history did not produce a complete evidence scope: %v", err)
	}
	if _, err := validateIndependent(closedScope, nil); err == nil {
		t.Fatal("missing independent termination/storage evidence accepted for retained history")
	}
	response := independentEvidenceFor(closedScope.attestationRequest())
	response.Independent.Runtimes = []EvidenceRuntime{{
		NodeName: "worker-1", NodeUID: "node-uid-target", InstanceID: "instance-target-uid", PodName: "rhiza-1", PodUID: "target-uid",
		ContainerIDs: []string{"containerd://target"}, ImageIDs: []string{"sha256:target"},
		RunIDs: []string{"run-bound", "run-unbound"}, WALIdentities: []string{"wal-target", ""}, State: RuntimeStateTerminated, RuntimeEvidence: "instance terminated",
	}}
	proof, err := validateIndependent(closedScope, response)
	if err != nil {
		t.Fatalf("complete retained-history termination and storage evidence rejected: %v", err)
	}
	receipt, err := peerClosureReceipt(caller, closure, request.OperationID, request, proof)
	if err != nil || len(receipt.Targets) != 2 || receipt.Targets[0].WALIdentity != "" || receipt.Targets[1].WALIdentity != "wal-target" {
		t.Fatalf("complete retained-history receipt=%+v err=%v", receipt, err)
	}
	if err := ledger.ReservePeerClosure(fencing.PeerClosureReservation{
		OperationID: "op-next", RequestHash: "hash-next", ConfigID: 4, AbortSlot: 8, TargetNodeID: "rhiza-2", TargetWAL: "wal-next",
		Voters: []string{"rhiza-0", "rhiza-2"}, Targets: []fencing.PeerClosureTarget{{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "next-uid", WALIdentity: "wal-next", NodeName: "worker-2", NodeUID: "node-uid-2", ContainerID: "containerd://next", ImageID: "sha256:next"}},
	}); err != nil {
		t.Fatalf("fully covered empty-WAL history prevented next target after terminal proof and membership change: %v", err)
	}
	runs[1].Identity.WALIdentity = "other-wal"
	if _, err := e.peerClosureTargets(context.Background(), runs, closure); err == nil {
		t.Fatal("different nonempty historical WAL accepted")
	}
}

func TestPeerClosureServerReceiptPreservesUnboundTargetHistory(t *testing.T) {
	caller := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "caller-uid", RunID: "caller-run", WALIdentity: "wal-caller"}
	closure := fencing.PeerClosureRequest{ExpectedConfigID: 3, ExpectedAbortSlot: peerClosureUint64(7), TargetNodeID: "rhiza-1", TargetWALIdentity: "wal-target"}
	root := rootResource(peerClosureRoot())
	binding := peerClosureBinding{root: root, rootUID: "root-uid", namespace: "test", workloadUID: "workload-uid"}
	request := FenceRequest{
		LogicalID: root.Spec.LogicalID, BindingUID: binding.rootUID, Namespace: binding.namespace, StatefulSet: root.Spec.StatefulSet,
		StatefulSetUID: binding.workloadUID, SourceClusterID: caller.SourceClusterID, OperationID: "peer-op", Scope: ScopeVoter,
		Targets: []FenceTarget{
			{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid", NodeName: "worker-1", NodeUID: "node-uid-1", ContainerID: "containerd://target", ImageID: "sha256:target"},
			{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid", WALIdentity: "wal-target", NodeName: "worker-1", NodeUID: "node-uid-1", ContainerID: "containerd://target", ImageID: "sha256:target"},
		},
	}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	proof := &FenceProof{OperationID: request.OperationID, SourceClusterID: caller.SourceClusterID, BindingUID: binding.rootUID, RequestHash: hash, ProofID: "proof-id", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	name := "fence-" + hashJSON([]string{binding.rootUID, request.OperationID})[:40]
	record := fenceResource{Metadata: object{"name": name}, Spec: request}
	record.Status.Proof = proof
	reservedTargets := []fencing.PeerClosureTarget{
		{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid", NodeName: "worker-1", NodeUID: "node-uid-1", ContainerID: "containerd://target", ImageID: "sha256:target"},
		{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "target-uid", WALIdentity: "wal-target", NodeName: "worker-1", NodeUID: "node-uid-1", ContainerID: "containerd://target", ImageID: "sha256:target"},
	}
	ledger := fencing.Ledger{
		SourceClusterID: caller.SourceClusterID,
		ClosedNodes:     map[string]fencing.StopRequest{"rhiza-1": {OperationID: request.OperationID, RequestHash: hash}},
		PeerClosure:     &fencing.PeerClosureReservation{OperationID: request.OperationID, RequestHash: hash, ConfigID: closure.ExpectedConfigID, AbortSlot: *closure.ExpectedAbortSlot, TargetNodeID: closure.TargetNodeID, TargetWAL: closure.TargetWALIdentity, Voters: []string{"rhiza-0", "rhiza-1", "rhiza-2"}, Targets: reservedTargets},
	}
	receipt, err := peerClosureReceiptFromRecord(caller, closure, binding, request.OperationID, ledger, record, false)
	if err != nil {
		t.Fatalf("server rejected complete retained history receipt: %v", err)
	}
	if len(receipt.Targets) != 2 || receipt.Targets[0].WALIdentity != "" || receipt.Targets[1].WALIdentity != "wal-target" || receipt.Targets[0].NodeUID != "node-uid-1" {
		t.Fatalf("receipt discarded or changed captured target history/runtime metadata: %+v", receipt.Targets)
	}
	withoutReservation := ledger
	withoutReservation.PeerClosure = nil
	if _, err := peerClosureReceiptFromRecord(caller, closure, binding, request.OperationID, withoutReservation, record, false); err == nil {
		t.Fatal("existing fence without peer-closure reservation was returned")
	}
	tampered := record
	tampered.Spec.Targets = append([]FenceTarget(nil), request.Targets...)
	tampered.Spec.Targets[0].ContainerID = "containerd://different"
	if _, err := peerClosureReceiptFromRecord(caller, closure, binding, request.OperationID, ledger, tampered, false); err == nil {
		t.Fatal("existing fence with changed captured inventory was returned")
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

func peerClosureTargetPod(uid, nodeName, containerID, imageID string) object {
	return object{
		"metadata": object{"name": "rhiza-1", "uid": uid},
		"spec":     object{"nodeName": nodeName},
		"status":   object{"containerStatuses": []any{object{"containerID": containerID, "imageID": imageID}}},
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

func seedPeerClosureAutoBinding(t *testing.T, bucket objstore.Bucket, prefix string, root object, currentWorkloadUID, controlWorkloadUID, bindingHash string) {
	t.Helper()
	cluster := rootResource(root)
	state := autoControl{
		Version: 1, BindingUID: str(root["metadata"].(object)["uid"]), BindingHash: autoBinding(&cluster),
		BindingVersion: autoBindingVersion, StatefulSetUID: controlWorkloadUID, ActiveClusterID: "source",
		Identities: make(map[quepaxa.NodeID]FenceTarget),
	}
	objects := []struct {
		key   string
		value any
	}{
		{path.Join(prefix, "automatic", "test", cluster.Spec.LogicalID, "control.json"), state},
		{path.Join(prefix, "automatic-workloads", "test", currentWorkloadUID, "binding.json"), object{
			"logicalID": cluster.Spec.LogicalID, "bindingUID": state.BindingUID, "bindingHash": bindingHash,
		}},
	}
	for _, item := range objects {
		if err := bucket.Upload(context.Background(), item.key, bytes.NewReader(jsonBytes(item.value)), objstore.WithIfNotExists()); err != nil {
			t.Fatalf("seed peer-closure binding %q: %v", item.key, err)
		}
	}
}

func rootResource(root object) ClusterResource {
	data, _ := json.Marshal(root)
	var result ClusterResource
	_ = json.Unmarshal(data, &result)
	return result
}

func stringSlice(values []any) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, str(value))
	}
	return result
}

func peerClosureUint64(value uint64) *uint64 { return &value }
