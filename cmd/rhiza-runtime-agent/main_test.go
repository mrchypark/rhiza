package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestTokenReviewerRequiresLivePinnedAuthorityWorkload(t *testing.T) {
	tests := []struct {
		name          string
		deploymentUID string
		audiences     []string
		wantErr       bool
	}{
		{name: "valid authority", deploymentUID: "authority-deployment-uid"},
		{name: "same service account from another workload", deploymentUID: "other-deployment-uid", wantErr: true},
		{name: "wrong audience", deploymentUID: "authority-deployment-uid", audiences: []string{"kubernetes.default.svc"}, wantErr: true},
		{name: "mixed audience", deploymentUID: "authority-deployment-uid", audiences: []string{runtimeAgentAudience, "kubernetes.default.svc"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/apis/authentication.k8s.io/v1/tokenreviews":
					audiences := tt.audiences
					if audiences == nil {
						audiences = []string{runtimeAgentAudience}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{
						"authenticated": true, "audiences": audiences,
						"user": map[string]any{"username": "system:serviceaccount:rhiza:rhiza-fencer", "extra": map[string][]string{
							"authentication.kubernetes.io/pod-uid":  {"authority-pod-uid"},
							"authentication.kubernetes.io/pod-name": {"rhiza-fencer-0"},
						}},
					}})
				case "/api/v1/namespaces/rhiza/pods/rhiza-fencer-0":
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "authority-pod-uid", "ownerReferences": []map[string]any{
						{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rhiza-fencer-abc", "uid": "authority-rs-uid", "controller": true},
					}}, "spec": map[string]any{"serviceAccountName": "rhiza-fencer"}})
				case "/apis/apps/v1/namespaces/rhiza/replicasets/rhiza-fencer-abc":
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "authority-rs-uid", "ownerReferences": []map[string]any{
						{"apiVersion": "apps/v1", "kind": "Deployment", "name": "rhiza-fencer", "uid": tt.deploymentUID, "controller": true},
					}}})
				case "/apis/apps/v1/namespaces/rhiza/deployments/rhiza-fencer":
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"uid": "authority-deployment-uid"}})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("runtime-agent-token"), 0600); err != nil {
				t.Fatal(err)
			}
			kube := &operator.Kubernetes{BaseURL: server.URL, TokenFile: token, Client: server.Client()}
			reviewer := tokenReviewer{Kube: kube, Namespace: "rhiza", ServiceAccount: "rhiza-fencer", DeploymentUID: "authority-deployment-uid"}
			principal, err := reviewer.Authenticate(context.Background(), "bound-token")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("accepted wrong workload: %+v", principal)
				}
				return
			}
			if err != nil || principal.PodUID != "authority-pod-uid" || principal.WorkloadUID != "authority-deployment-uid" {
				t.Fatalf("wrong verified principal: %+v %v", principal, err)
			}
		})
	}
}
