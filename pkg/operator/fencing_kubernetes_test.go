package operator

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestKubernetesFenceResumesImmutableOperation(t *testing.T) {
	var record *fenceResource
	creates := 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			creates++
			record = new(fenceResource)
			if err := json.NewDecoder(r.Body).Decode(record); err != nil {
				t.Error(err)
			}
			w.WriteHeader(http.StatusCreated)
		} else if record == nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			json.NewEncoder(w).Encode(record)
		}
	}))
	defer api.Close()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	kube := &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}
	req := FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "test", StatefulSet: "rhiza", StatefulSetUID: "uid", SourceClusterID: "source", OperationID: "op", Scope: ScopeGeneration}
	for range 2 {
		// A fresh backend resumes the original immutable request after restart.
		_, err := (&KubernetesFencer{Kube: kube}).Fence(t.Context(), req)
		if !errors.Is(err, ErrFencePending) {
			t.Fatalf("want pending: %v", err)
		}
	}
	if creates != 1 {
		t.Fatalf("created %d operations", creates)
	}
	changed := req
	changed.SourceClusterID = "other"
	if _, err := (&KubernetesFencer{Kube: kube}).Fence(t.Context(), changed); err == nil {
		t.Fatal("accepted changed operation")
	}
	hash, _ := requestHash(&req)
	record.Status.Proof = &FenceProof{OperationID: req.OperationID, SourceClusterID: req.SourceClusterID, BindingUID: req.BindingUID, RequestHash: hash, ProofID: "runtime-evidence", RecreationBlocked: true, StorageQuiesced: true}
	if _, err := (&KubernetesFencer{Kube: kube}).Fence(t.Context(), req); err == nil {
		t.Fatal("accepted deletion without termination")
	}
	record.Status.Proof.ProcessesTerminated = true
	if _, err := (&KubernetesFencer{Kube: kube}).Fence(t.Context(), req); err != nil {
		t.Fatal(err)
	}
}
