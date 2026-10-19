package operator

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// independentEvidenceFor builds the evidence an external attestor must return
// for one bound request. Tests mutate exactly one field of this baseline.
func independentEvidenceFor(req AttestationRequest) *AttestationResponse {
	byPod := map[string][]AttestationRun{}
	for _, run := range req.Runs {
		byPod[run.PodUID] = append(byPod[run.PodUID], run)
	}
	evidence := IndependentEvidence{
		Version:     independentEvidenceVersion,
		Provider:    "fake-power-provider",
		Mode:        "infrastructure",
		ObservedAt:  "2026-10-19T00:00:00Z",
		RequestHash: req.RequestHash,
		LedgerHash:  req.LedgerHash,
		BarrierID:   "barrier-" + req.StatefulSetUID,
		Barrier: EvidenceBarrier{
			Kind:              "closed-ledger",
			ClosedOperationID: req.Request.OperationID,
			ClosedRequestHash: req.RequestHash,
			CurrentPodUIDs:    append([]string(nil), req.CurrentPodUIDs...),
			DeniedAt:          "2026-10-19T00:00:30Z",
		},
		Storage: EvidenceStorage{
			Provider:           req.StorageScope.Provider,
			Bucket:             req.StorageScope.Bucket,
			Prefix:             req.StorageScope.Prefix,
			QuietWindowSeconds: 60,
			HeadGeneration:     "100",
			CurrentGeneration:  "100",
			InventoryDigest:    "sha256:inventory",
			Stable:             true,
			ObservedFrom:       "2026-10-19T00:00:00Z",
			ObservedTo:         "2026-10-19T00:01:00Z",
		},
	}
	for _, target := range req.Request.Targets {
		nodeUID := target.NodeUID
		if nodeUID == "" {
			nodeUID = "node-uid-" + target.NodeName
		}
		var runIDs, wals []string
		for _, run := range byPod[target.PodUID] {
			runIDs, wals = append(runIDs, run.RunID), append(wals, run.WALIdentity)
		}
		evidence.Runtimes = append(evidence.Runtimes, EvidenceRuntime{
			NodeName: target.NodeName, NodeUID: nodeUID, InstanceID: "instance-" + target.PodUID,
			PodName: target.Pod, PodUID: target.PodUID,
			ContainerIDs: []string{target.ContainerID}, ImageIDs: []string{target.ImageID},
			RunIDs: runIDs, WALIdentities: wals,
			State: RuntimeStateTerminated, RuntimeEvidence: "compute instance terminated and disks detached",
		})
	}
	sort.Slice(evidence.Runtimes, func(i, j int) bool { return evidence.Runtimes[i].PodUID < evidence.Runtimes[j].PodUID })
	return &AttestationResponse{
		Proof:       FenceProof{OperationID: req.Request.OperationID, SourceClusterID: req.Request.SourceClusterID, BindingUID: req.Request.BindingUID, RequestHash: req.RequestHash, ProofID: "attested-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true},
		Independent: evidence,
	}
}

// independentFixture is one closed ledger, its exact request and the scope the
// authority would demand the evidence to restate.
func independentFixture(t *testing.T) (FenceRequest, independentScope) {
	t.Helper()
	state := fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source", ClosedBy: "op", ClosedHash: "request", Runs: map[string]fencing.Run{
		"run-0": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-0", RunID: "run-0", WALIdentity: "wal-0"}},
		"run-1": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "pod-1", RunID: "run-1", WALIdentity: "wal-1"}},
	}}}
	request := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "op", Scope: ScopeGeneration, Targets: []FenceTarget{
		{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-0", WALIdentity: "wal-0", NodeName: "node-a", NodeUID: "node-uid-a", ContainerID: "containerd://c0", ImageID: "sha256:img-0"},
		{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "pod-1", WALIdentity: "wal-1", NodeName: "node-b", ContainerID: "containerd://c1", ImageID: "sha256:img-1"},
	}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := buildIndependentScope(state, request, hash, "ledger-hash", []string{"pod-1", "pod-0"}, AttestationStoreScope{Provider: "gcs", Bucket: "rhiza-archive", Prefix: "root/source"})
	if err != nil {
		t.Fatal(err)
	}
	return request, scope
}

func TestIndependentEvidenceMustBindTheClosedScope(t *testing.T) {
	_, scope := independentFixture(t)
	proof, err := validateIndependent(scope, independentEvidenceFor(scope.attestationRequest()))
	if err != nil {
		t.Fatalf("exactly bound evidence rejected: %v", err)
	}
	if proof.Independent == nil || proof.RequestHash != scope.requestHash || proof.ProofID == "" || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		t.Fatalf("accepted proof is incomplete: %+v", proof)
	}
	for name, change := range map[string]func(*AttestationResponse){
		"stale request hash":       func(r *AttestationResponse) { r.Independent.RequestHash = strings.Repeat("f", 64) },
		"stale ledger hash":        func(r *AttestationResponse) { r.Independent.LedgerHash = "other-ledger" },
		"unknown evidence version": func(r *AttestationResponse) { r.Independent.Version = 2 },
		"missing provider":         func(r *AttestationResponse) { r.Independent.Provider = "" },
		"missing barrier id":       func(r *AttestationResponse) { r.Independent.BarrierID = "" },
		"other closed operation":   func(r *AttestationResponse) { r.Independent.Barrier.ClosedOperationID = "other" },
		"other closed request":     func(r *AttestationResponse) { r.Independent.Barrier.ClosedRequestHash = "other" },
		"changed pod inventory":    func(r *AttestationResponse) { r.Independent.Barrier.CurrentPodUIDs = []string{"pod-0"} },
		"missing barrier denial":   func(r *AttestationResponse) { r.Independent.Barrier.DeniedAt = "" },
		"foreign pod uid":          func(r *AttestationResponse) { r.Independent.Runtimes[0].PodUID = "remote-pod" },
		"foreign pod name":         func(r *AttestationResponse) { r.Independent.Runtimes[0].PodName = "rhiza-9" },
		"foreign node name":        func(r *AttestationResponse) { r.Independent.Runtimes[0].NodeName = "node-z" },
		"foreign node uid":         func(r *AttestationResponse) { r.Independent.Runtimes[0].NodeUID = "node-uid-z" },
		"missing node uid":         func(r *AttestationResponse) { r.Independent.Runtimes[0].NodeUID = "" },
		"missing instance id":      func(r *AttestationResponse) { r.Independent.Runtimes[0].InstanceID = "" },
		"foreign container":        func(r *AttestationResponse) { r.Independent.Runtimes[0].ContainerIDs = []string{"containerd://other"} },
		"extra container": func(r *AttestationResponse) {
			r.Independent.Runtimes[0].ContainerIDs = append(r.Independent.Runtimes[0].ContainerIDs, "containerd://new")
		},
		"foreign image": func(r *AttestationResponse) { r.Independent.Runtimes[0].ImageIDs = []string{"sha256:other"} },
		"missing run":   func(r *AttestationResponse) { r.Independent.Runtimes[0].RunIDs = nil },
		"extra run": func(r *AttestationResponse) {
			r.Independent.Runtimes[0].RunIDs = append(r.Independent.Runtimes[0].RunIDs, "run-9")
		},
		"foreign wal":            func(r *AttestationResponse) { r.Independent.Runtimes[0].WALIdentities = []string{"wal-1"} },
		"absent runtime":         func(r *AttestationResponse) { r.Independent.Runtimes[0].State = "absent" },
		"deleted runtime":        func(r *AttestationResponse) { r.Independent.Runtimes[0].State = "deleted" },
		"unknown runtime":        func(r *AttestationResponse) { r.Independent.Runtimes[0].State = "unknown" },
		"running runtime":        func(r *AttestationResponse) { r.Independent.Runtimes[0].State = "CONTAINER_RUNNING" },
		"empty runtime evidence": func(r *AttestationResponse) { r.Independent.Runtimes[0].RuntimeEvidence = "" },
		"missing incarnation":    func(r *AttestationResponse) { r.Independent.Runtimes = r.Independent.Runtimes[1:] },
		"foreign incarnation": func(r *AttestationResponse) {
			r.Independent.Runtimes = append(r.Independent.Runtimes, EvidenceRuntime{PodUID: "remote-pod"})
		},
		"duplicate incarnation":  func(r *AttestationResponse) { r.Independent.Runtimes[1] = r.Independent.Runtimes[0] },
		"other storage provider": func(r *AttestationResponse) { r.Independent.Storage.Provider = "azure" },
		"other bucket":           func(r *AttestationResponse) { r.Independent.Storage.Bucket = "other" },
		"other prefix":           func(r *AttestationResponse) { r.Independent.Storage.Prefix = "root/other" },
		"unstable storage":       func(r *AttestationResponse) { r.Independent.Storage.Stable = false },
		"short quiet window": func(r *AttestationResponse) {
			r.Independent.Storage.QuietWindowSeconds = minIndependentQuietWindowSeconds - 1
		},
		"window not observed":      func(r *AttestationResponse) { r.Independent.Storage.ObservedTo = r.Independent.Storage.ObservedFrom },
		"unparsable observation":   func(r *AttestationResponse) { r.Independent.Storage.ObservedFrom = "yesterday" },
		"missing head generation":  func(r *AttestationResponse) { r.Independent.Storage.HeadGeneration = "" },
		"missing inventory digest": func(r *AttestationResponse) { r.Independent.Storage.InventoryDigest = "" },
		"foreign operation":        func(r *AttestationResponse) { r.Proof.OperationID = "other" },
		"foreign binding":          func(r *AttestationResponse) { r.Proof.BindingUID = "other" },
		"foreign source cluster":   func(r *AttestationResponse) { r.Proof.SourceClusterID = "other" },
		"foreign proof request":    func(r *AttestationResponse) { r.Proof.RequestHash = "other" },
		"missing proof id":         func(r *AttestationResponse) { r.Proof.ProofID = "" },
		"unterminated process":     func(r *AttestationResponse) { r.Proof.ProcessesTerminated = false },
		"unblocked recreation":     func(r *AttestationResponse) { r.Proof.RecreationBlocked = false },
		"unquiesced storage":       func(r *AttestationResponse) { r.Proof.StorageQuiesced = false },
		"pre-attached evidence":    func(r *AttestationResponse) { evidence := r.Independent; r.Proof.Independent = &evidence },
		"empty storage location": func(r *AttestationResponse) {
			r.Independent.Storage.Provider, r.Independent.Storage.Bucket, r.Independent.Storage.Prefix = "", "", ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := independentEvidenceFor(scope.attestationRequest())
			change(candidate)
			if _, err := validateIndependent(scope, candidate); err == nil {
				t.Fatalf("accepted evidence that does not bind the closed scope: %s", name)
			}
		})
	}
	t.Run("missing response", func(t *testing.T) {
		if _, err := validateIndependent(scope, nil); err == nil {
			t.Fatal("accepted a missing attestation")
		}
	})
}

// A Pod name, or a target that never reported container identity, can never
// stand in for the captured incarnation.
func TestIndependentScopeRequiresCapturedIdentity(t *testing.T) {
	state := fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
		"run-0": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-0", RunID: "run-0", WALIdentity: "wal-0"}},
	}}}
	captured := FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-0", WALIdentity: "wal-0", NodeName: "node-a", ContainerID: "containerd://c0", ImageID: "sha256:img-0"}
	request := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "op", Scope: ScopeVoter, Targets: []FenceTarget{captured}}
	storage := AttestationStoreScope{Provider: "gcs", Bucket: "rhiza-archive", Prefix: "root/source"}
	if _, err := buildIndependentScope(state, request, "hash", "ledger", []string{"pod-0"}, storage); err != nil {
		t.Fatalf("captured identity rejected: %v", err)
	}
	for name, mutate := range map[string]func(*FenceTarget){
		"pod name only": func(t *FenceTarget) {
			*t = FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-0", WALIdentity: "wal-0"}
		},
		"missing container id": func(t *FenceTarget) { t.ContainerID = "" },
		"missing image id":     func(t *FenceTarget) { t.ImageID = "" },
		"missing node name":    func(t *FenceTarget) { t.NodeName = "" },
		"missing pod uid":      func(t *FenceTarget) { t.PodUID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			target := captured
			mutate(&target)
			candidate.Targets = []FenceTarget{target}
			if _, err := buildIndependentScope(state, candidate, "hash", "ledger", []string{"pod-0"}, storage); err == nil {
				t.Fatal("accepted a fence without captured runtime identity")
			}
		})
	}
	t.Run("run outside the captured scope", func(t *testing.T) {
		extra := state
		extra.Ledger.Runs["run-1"] = fencing.Run{Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "pod-1", RunID: "run-1", WALIdentity: "wal-1"}}
		if _, err := buildIndependentScope(extra, request, "hash", "ledger", []string{"pod-0"}, storage); err == nil {
			t.Fatal("accepted a closed run that the request never named")
		}
	})
	t.Run("empty closed ledger", func(t *testing.T) {
		empty := fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}
		if _, err := buildIndependentScope(empty, request, "hash", "ledger", []string{"pod-0"}, storage); err == nil {
			t.Fatal("accepted a ledger without runs")
		}
	})
	t.Run("never-bound incarnation keeps its empty WAL", func(t *testing.T) {
		unbound := fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source", Runs: map[string]fencing.Run{
			"run-0": {Identity: fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-0", RunID: "run-0"}},
		}}}
		unboundTarget := captured
		unboundTarget.WALIdentity = ""
		unboundRequest := request
		unboundRequest.Targets = []FenceTarget{unboundTarget}
		if _, err := buildIndependentScope(unbound, unboundRequest, "hash", "ledger", []string{"pod-0"}, storage); err != nil {
			t.Fatalf("never-bound incarnation rejected: %v", err)
		}
	})
}

func TestHTTPSAttestorRoundTripAndFailClosedResponses(t *testing.T) {
	_, scope := independentFixture(t)
	request := scope.attestationRequest()
	var seen AttestationRequest
	var idempotency, authorization string
	url, caFile := attestorTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Error(err)
		}
		idempotency, authorization = r.Header.Get("Idempotency-Key"), r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(independentEvidenceFor(seen))
	}))
	attestor, err := NewHTTPSAttestor(url, writeToken(t), caFile)
	if err != nil {
		t.Fatal(err)
	}
	response, err := attestor.Attest(t.Context(), request)
	if err != nil {
		t.Fatalf("attestation failed: %v", err)
	}
	if seen.RequestHash != request.RequestHash || seen.LedgerHash != request.LedgerHash || seen.StorageScope != request.StorageScope || len(seen.Runs) != len(request.Runs) {
		t.Fatalf("attestor received a different scope: %+v", seen)
	}
	if idempotency != request.Request.OperationID || authorization != "Bearer test-bearer-token" {
		t.Fatalf("attestation headers differ: %q %q", idempotency, authorization)
	}
	if _, err := validateIndependent(scope, response); err != nil {
		t.Fatalf("round-tripped evidence rejected: %v", err)
	}
}

func TestHTTPSAttestorRejectsWeakerOrMalformedResponses(t *testing.T) {
	_, scope := independentFixture(t)
	request := scope.attestationRequest()
	for name, handler := range map[string]http.HandlerFunc{
		"pending": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		},
		"server error": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		},
		"redirect": func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
		},
		"unknown field": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"proof":{},"independent":{},"surprise":true}`))
		},
		"trailing data": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"proof":{},"independent":{}}{}`))
		},
		"oversized": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+100)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			url, caFile := attestorTLS(t, handler)
			attestor, err := NewHTTPSAttestor(url, writeToken(t), caFile)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := attestor.Attest(t.Context(), request); err == nil {
				t.Fatal("accepted a response that proves nothing")
			}
		})
	}
	t.Run("pending is an explicit retry", func(t *testing.T) {
		url, caFile := attestorTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) }))
		attestor, err := NewHTTPSAttestor(url, writeToken(t), caFile)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := attestor.Attest(t.Context(), request); !errors.Is(err, ErrFencePending) {
			t.Fatalf("pending attestation must stay pending, got %v", err)
		}
	})
	t.Run("missing token", func(t *testing.T) {
		url, caFile := attestorTLS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		attestor, err := NewHTTPSAttestor(url, filepath.Join(t.TempDir(), "absent-token"), caFile)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := attestor.Attest(t.Context(), request); err == nil {
			t.Fatal("accepted an unauthenticated attestor call")
		}
	})
}

func TestHTTPSAttestorConfigurationFailsClosed(t *testing.T) {
	token := writeToken(t)
	caFile := filepath.Join(t.TempDir(), "empty-ca.crt")
	if err := os.WriteFile(caFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct{ url, token, ca, want string }{
		"no endpoint":      {url: "", token: token, want: "invalid endpoint"},
		"plaintext":        {url: "http://attestor.example/v1/attest", token: token, want: "https"},
		"no token file":    {url: "https://attestor.example/v1/attest", want: "token file is required"},
		"unusable ca file": {url: "https://attestor.example/v1/attest", token: token, ca: caFile, want: "CA contains no certificate"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewHTTPSAttestor(tc.url, tc.token, tc.ca)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}

// attestorTLS serves handler over HTTPS and returns the endpoint plus a CA file
// that pins the test certificate.
func attestorTLS(t *testing.T, handler http.Handler) (string, string) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return server.URL + "/v1/attest", caFile
}

type stubAttestor struct {
	mu    sync.Mutex
	calls int
	fn    func(AttestationRequest) (*AttestationResponse, error)
}

func (s *stubAttestor) Attest(_ context.Context, req AttestationRequest) (*AttestationResponse, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.fn(req)
}

func (s *stubAttestor) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}
