package operator

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestHTTPEmptyRecoveryRegistrationRequiresExactCanonicalResponse(t *testing.T) {
	token := writeHTTPFenceToken(t, "controller-token")
	request := ArchiveEmptyRecoveryRequest{
		RecoveryName: "recover-empty", StatefulSetUID: "sts-uid", TargetClusterID: "rhiza-r-target",
		OperationID: "resource-uid:recover-empty", SourceFenceOperationID: "recover-empty",
		TargetMembershipHash: hashJSON("target-membership"), TargetDurability: "before-ack",
		PolicyDigest: hashJSON("persisted-spec"),
	}
	digest := hashJSON("source-proof")
	var wrongPolicy atomic.Bool
	emptyHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/archive-genesis/empty-recovery/register" || r.Method != http.MethodPost || r.Header.Get("Idempotency-Key") != request.OperationID || r.Header.Get("Authorization") != "Bearer controller-token" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		var got ArchiveEmptyRecoveryRequest
		if json.NewDecoder(r.Body).Decode(&got) != nil || got != request {
			http.Error(w, "request mismatch", http.StatusBadRequest)
			return
		}
		authorization := fencing.ArchiveEmptyRecoveryAuthorization{
			Version: fencing.ArchiveGenesisVersion, StatefulSetUID: request.StatefulSetUID,
			TargetClusterID: request.TargetClusterID, OperationID: request.OperationID,
			SourceFenceOperationID: request.SourceFenceOperationID, ConfigID: 1,
			SourceScopeHash: digest, TargetScopeHash: hashJSON("target-scope"),
			SourceGenesisRefDigest: digest, SourceHeadDigest: digest,
			SourceHeadVersion:     objstore.ObjectVersion{Type: objstore.ETag, Value: "source-head"},
			TargetEmptyHeadDigest: authorityStateDigest(emptyHead),
			SourceRootDigest:      digest, SourceFenceDigest: digest, SourceMembershipHash: digest,
			SourceDurability: "before-ack", TargetMembershipHash: request.TargetMembershipHash,
			TargetDurability: request.TargetDurability,
			PolicyDigest:     emptyRecoveryPolicyDigest(request.PolicyDigest, "before-ack", request.TargetDurability, request.AllowDataLoss),
		}
		if wrongPolicy.Load() {
			authorization.PolicyDigest = request.PolicyDigest
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ArchiveEmptyRecoveryResponse{State: fencing.EmptyRecoveryAuthorized, Authorization: authorization})
	}))
	defer server.Close()
	client := &FencingClient{URL: server.URL, TokenFile: token, Client: server.Client()}
	response, err := client.RegisterArchiveEmptyRecovery(t.Context(), request)
	if err != nil || response.State != fencing.EmptyRecoveryAuthorized {
		t.Fatalf("registration response=%+v err=%v", response, err)
	}
	wrongPolicy.Store(true)
	if _, err := client.RegisterArchiveEmptyRecovery(t.Context(), request); err == nil || !strings.Contains(err.Error(), "empty recovery authorization differs from request") {
		t.Fatalf("client accepted an authorization with the un-derived policy digest: err=%v", err)
	}
}

type httpFenceFixture struct {
	records       map[string]fenceResource
	creates       int
	username      string
	authenticated bool
}

func TestHTTPFenceRequestUsesCanonicalCRDAndResumes(t *testing.T) {
	fixture := &httpFenceFixture{
		records:       map[string]fenceResource{},
		username:      "system:serviceaccount:test:rhiza-operator",
		authenticated: true,
	}
	apiToken := writeHTTPFenceToken(t, "api-token")
	controllerToken := writeHTTPFenceToken(t, "controller-token")
	var executor *FencingExecutor
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == httpFenceRequestPath {
			executor.Handler().ServeHTTP(w, r)
			return
		}
		fixture.serveKubernetes(w, r)
	}))
	defer server.Close()
	kube := &Kubernetes{BaseURL: server.URL, Namespace: "test", TokenFile: apiToken, Client: server.Client()}
	executor = &FencingExecutor{Kube: kube, ControllerServiceAccount: "rhiza-operator"}
	client := &FencingClient{URL: server.URL + httpFenceRequestPath, TokenFile: controllerToken, Client: server.Client()}
	request := FenceRequest{
		LogicalID: "logical", BindingUID: "binding", Namespace: "test", StatefulSet: "rhiza",
		StatefulSetUID: "sts-uid", SourceClusterID: "source", OperationID: "operation-1", Scope: ScopeGeneration,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	unknownField := append(append([]byte(nil), encoded[:len(encoded)-1]...), []byte(`,"unexpected":true}`)...)
	for _, tc := range []struct {
		name string
		body []byte
		key  string
	}{
		{name: "unknown field", body: unknownField, key: request.OperationID},
		{name: "trailing JSON", body: append(append([]byte(nil), encoded...), []byte(` {}`)...), key: request.OperationID},
		{name: "idempotency mismatch", body: encoded, key: "different-operation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			httpRequest, err := http.NewRequest(http.MethodPost, server.URL+httpFenceRequestPath, bytes.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			httpRequest.Header.Set("Authorization", "Bearer controller-token")
			httpRequest.Header.Set("Idempotency-Key", tc.key)
			response, err := server.Client().Do(httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("HTTP status=%d, want %d", response.StatusCode, http.StatusBadRequest)
			}
		})
	}
	if fixture.creates != 0 {
		t.Fatal("malformed requests created a RhizaFence")
	}

	for i := 0; i < 2; i++ {
		if _, err := client.Fence(t.Context(), request); !errors.Is(err, ErrFencePending) {
			t.Fatalf("request %d: want pending, got %v", i+1, err)
		}
	}
	if fixture.creates != 1 {
		t.Fatalf("created %d RhizaFence records, want exactly one", fixture.creates)
	}

	changed := request
	changed.Scope = ScopeVoter
	if _, err := client.Fence(t.Context(), changed); err == nil || errors.Is(err, ErrFencePending) {
		t.Fatalf("changed immutable request was not rejected: %v", err)
	}
	if fixture.creates != 1 {
		t.Fatalf("changed request created another RhizaFence: creates=%d", fixture.creates)
	}

	name := "fence-" + hashJSON([]string{request.BindingUID, request.OperationID})[:40]
	record := fixture.records[name]
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	record.Status.Proof = &FenceProof{
		OperationID: request.OperationID, SourceClusterID: request.SourceClusterID,
		BindingUID: request.BindingUID, RequestHash: hash, ProofID: "verified-test-proof",
		ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
	}
	fixture.records[name] = record // Simulates the existing executor's status reconciliation.
	proof, err := client.Fence(t.Context(), request)
	if err != nil {
		t.Fatalf("resumed request: %v", err)
	}
	if proof.ProofID != "verified-test-proof" {
		t.Fatalf("returned proof %q", proof.ProofID)
	}
}

func TestHTTPFenceRequestRejectsUntrustedController(t *testing.T) {
	fixture := &httpFenceFixture{records: map[string]fenceResource{}, username: "system:serviceaccount:test:other", authenticated: true}
	apiToken := writeHTTPFenceToken(t, "api-token")
	controllerToken := writeHTTPFenceToken(t, "controller-token")
	var executor *FencingExecutor
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == httpFenceRequestPath {
			executor.Handler().ServeHTTP(w, r)
			return
		}
		fixture.serveKubernetes(w, r)
	}))
	defer server.Close()
	executor = &FencingExecutor{
		Kube:                     &Kubernetes{BaseURL: server.URL, Namespace: "test", TokenFile: apiToken, Client: server.Client()},
		ControllerServiceAccount: "rhiza-operator",
	}
	client := &FencingClient{URL: server.URL + httpFenceRequestPath, TokenFile: controllerToken, Client: server.Client()}
	request := FenceRequest{BindingUID: "binding", Namespace: "test", OperationID: "operation-1", SourceClusterID: "source", Scope: ScopeGeneration}
	if _, err := client.Fence(t.Context(), request); err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("untrusted controller was not rejected: %v", err)
	}
	if fixture.creates != 0 || len(fixture.records) != 0 {
		t.Fatal("untrusted controller created a RhizaFence")
	}
}

func (f *httpFenceFixture) serveKubernetes(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews" && r.Method == http.MethodPost:
		var review object
		if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
			http.Error(w, "invalid review", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(object{"status": object{
			"authenticated": f.authenticated,
			"audiences":     []string{"rhiza-fencer"},
			"user":          object{"username": f.username},
		}})
	case strings.HasPrefix(r.URL.Path, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences/") && r.Method == http.MethodGet:
		name := strings.TrimPrefix(r.URL.Path, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences/")
		record, ok := f.records[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(record)
	case r.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences" && r.Method == http.MethodPost:
		var record fenceResource
		if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
			http.Error(w, "invalid object", http.StatusBadRequest)
			return
		}
		name := str(record.Metadata["name"])
		if _, exists := f.records[name]; exists {
			w.WriteHeader(http.StatusConflict)
			return
		}
		f.creates++
		f.records[name] = record
		w.WriteHeader(http.StatusCreated)
	default:
		http.NotFound(w, r)
	}
}

func writeHTTPFenceToken(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

var _ Fencer = (*FencingClient)(nil)
