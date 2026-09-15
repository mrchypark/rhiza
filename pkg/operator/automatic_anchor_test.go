package operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnchorEnvGateMismatch(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"rhiza","uid":"sts-uid-1"},"spec":{"replicas":3,"selector":{"matchLabels":{"app":"rhiza"}},"template":{"spec":{"containers":[{"name":"rhiza","env":[{"name":"RHIZA_CLUSTER_ID","value":"source"},{"name":"RHIZA_ENABLE_RECONFIGURATION","value":"true"},{"name":"RHIZA_RECOVERY_ANCHOR_ID","value":"wrong-anchor"}]}],"volumes":[{"name":"data","emptyDir":{}}]}}}}`))
	})
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	os.WriteFile(tokenFile, []byte("tok"), 0o600)
	kube := &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: tokenFile, Client: server.Client()}

	bucket := autoBucket(t)
	c := autoController(bucket)
	c.Kube = kube
	fenced := 0
	c.Fencer = &countingFencer{count: &fenced}

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	r.Spec.AnchorID = "expected-anchor"

	err := c.reconcileCluster(context.Background(), r)
	if err != nil {
		t.Fatalf("reconcileCluster: %v", err)
	}
	if r.Status.Phase != "Blocked" {
		t.Fatalf("expected Blocked, got %q: %s", r.Status.Phase, r.Status.Message)
	}
	if !strings.Contains(r.Status.Message, "does not match") {
		t.Fatalf("expected anchor mismatch message, got: %s", r.Status.Message)
	}
	if fenced != 0 {
		t.Fatal("fencer should not be called when anchor env gate fails")
	}
}

func TestAdvanceAutomaticPropagatesAnchorID(t *testing.T) {
	var posted Resource
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && strings.Contains(r.URL.Path, "/rhizarecoveries/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == "GET" {
			w.Write([]byte(`{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"rhiza","uid":"sts-uid-1"},"spec":{"replicas":3,"selector":{"matchLabels":{"app":"rhiza"}},"template":{"spec":{"containers":[{"name":"rhiza"}],"volumes":[{"name":"data","emptyDir":{}}]}}}}`))
			return
		}
		if r.Method == "POST" || r.Method == "PUT" {
			json.NewDecoder(r.Body).Decode(&posted)
			w.Write([]byte(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha1","kind":"RhizaRecovery","metadata":{"name":"auto-1","uid":"child-uid-1"}}`))
			return
		}
	})
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	tokenFile := filepath.Join(t.TempDir(), "token")
	os.WriteFile(tokenFile, []byte("tok"), 0o600)
	kube := &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: tokenFile, Client: server.Client()}

	bucket := autoBucket(t)
	c := autoController(bucket)
	c.Kube = kube
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	r.Spec.AnchorID = "auto-anchor"

	state := validAutoControl()
	request := FenceRequest{
		LogicalID:       r.Spec.LogicalID,
		BindingUID:      state.BindingUID,
		Namespace:       "default",
		StatefulSet:     r.Spec.StatefulSet,
		StatefulSetUID:  state.StatefulSetUID,
		SourceClusterID: state.ActiveClusterID,
		OperationID:     "auto-1",
		Scope:           ScopeGeneration,
		Targets:         []FenceTarget{{NodeID: "n1", Pod: "rhiza-0"}, {NodeID: "n2", Pod: "rhiza-1"}, {NodeID: "n3", Pod: "rhiza-2"}},
	}
	hash, _ := requestHash(&request)
	state.Intent = &autoIntent{
		Request:      request,
		Policy:       r.Spec.Policy,
		Durability:   "async",
		Voters:       append([]VoterPod(nil), state.Voters...),
		RecoveryName: "auto-1",
		Proof: &FenceProof{
			OperationID:         request.OperationID,
			SourceClusterID:     request.SourceClusterID,
			BindingUID:          request.BindingUID,
			RequestHash:         hash,
			ProofID:             "proof-1",
			ProcessesTerminated: true,
			RecreationBlocked:   true,
			StorageQuiesced:     true,
		},
	}

	err := c.advanceAutomatic(context.Background(), r, state, nil, object{
		"apiVersion": "apps/v1", "kind": "StatefulSet",
		"metadata": object{"name": "rhiza", "uid": "sts-uid-1"},
		"spec": object{
			"replicas": float64(3),
			"selector": object{"matchLabels": object{"app": "rhiza"}},
			"template": object{
				"spec": object{
					"containers": []any{object{"name": "rhiza"}},
					"volumes":    []any{object{"name": "data", "emptyDir": object{}}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("advanceAutomatic: %v", err)
	}
	if posted.Spec.AnchorID != "auto-anchor" {
		t.Fatalf("expected child AnchorID %q, got %q", "auto-anchor", posted.Spec.AnchorID)
	}
}

func TestAutoBindingHashStable(t *testing.T) {
	type legacyBinding struct {
		LogicalID, StatefulSet, Container, AdoptClusterID string
		Voters                                            []VoterPod
	}
	legacy := hashJSON(legacyBinding{"l1", "s1", "c1", "a1", []VoterPod{{NodeID: "n1", Pod: "p1"}}})

	r := autoClusterResource("c1", "uid-1", "l1")
	r.Spec.StatefulSet = "s1"
	r.Spec.Container = "c1"
	r.Spec.AdoptClusterID = "a1"
	r.Spec.VoterPods = []VoterPod{{NodeID: "n1", Pod: "p1"}}
	r.Spec.AnchorID = ""
	empty := autoBinding(r)
	if legacy != empty {
		t.Fatalf("empty AnchorID changed hash: legacy=%s new=%s", legacy, empty)
	}

	r.Spec.AnchorID = "my-anchor"
	withID := autoBinding(r)
	if empty == withID {
		t.Fatal("non-empty AnchorID did not change hash")
	}
}

type countingFencer struct {
	count *int
}

func (f *countingFencer) Fence(_ context.Context, req FenceRequest) (*FenceProof, error) {
	*f.count++
	h, _ := requestHash(&req)
	return &FenceProof{
		OperationID:         req.OperationID,
		SourceClusterID:     req.SourceClusterID,
		BindingUID:          req.BindingUID,
		RequestHash:         h,
		ProofID:             "proof",
		ProcessesTerminated: true,
		RecreationBlocked:   true,
		StorageQuiesced:     true,
	}, nil
}
