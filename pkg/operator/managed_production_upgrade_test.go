package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestManagedResourcesAcceptable(t *testing.T) {
	want, _ := json.Marshal(V1Alpha2ManagedResources(false))
	within, _ := json.Marshal(map[string]any{
		"requests": map[string]any{"cpu": "250m", "memory": "256Mi", "ephemeral-storage": "512Mi"},
		"limits":   map[string]any{"cpu": "1", "memory": "1Gi", "ephemeral-storage": "4Gi"},
	})
	if !managedResourcesAcceptable(within, want) {
		t.Fatal("rejected valid LimitRange-adjusted resources within guardrails")
	}
	exceeds, _ := json.Marshal(map[string]any{
		"requests": map[string]any{"cpu": "500m", "memory": "512Mi", "ephemeral-storage": "1Gi"},
		"limits":   map[string]any{"cpu": "2", "memory": "2Gi", "ephemeral-storage": "8Gi"},
	})
	if managedResourcesAcceptable(exceeds, want) {
		t.Fatal("accepted resources exceeding operator CPU limit guardrail")
	}
	if !managedResourcesAcceptable(json.RawMessage(`{}`), nil) {
		t.Fatal("rejected Kubernetes empty resources default when template omitted resources")
	}
}

func seededManagedClusterJSON(t *testing.T, deletionPolicy, image string) []byte {
	t.Helper()
	bootstrap, digest, err := testBootstrapPlan(phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewV1Alpha2ApplicationInitPlan(phase2Ref, "logical", "deployment", "assignment", bootstrap.PhysicalBinding, digest, bootstrap.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	plan.ApplicationProtocol = phase9RuntimeProfile().ApplicationProtocol
	var cluster map[string]any
	_ = json.Unmarshal([]byte(phase8Cluster), &cluster)
	spec := cluster["spec"].(map[string]any)
	spec["applicationProtocol"] = plan.ApplicationProtocol
	if deletionPolicy != "" {
		spec["deletionPolicy"] = deletionPolicy
	}
	if image != "" {
		spec["workload"].(map[string]any)["podTemplate"].(map[string]any)["spec"].(map[string]any)["containers"] = []any{
			map[string]any{"name": "sql", "image": image},
		}
	}
	evRaw := []byte(`{"slot":1}`)
	evSum := sha256.Sum256(evRaw)
	cluster["status"] = map[string]any{
		"bootstrapPlan": bootstrap,
		"genesis": v1alpha2Genesis{
			OwnerUID:      phase2Ref.UID,
			LogicalID:     "logical",
			DeploymentUID: "deployment",
			StorageID:     plan.StorageID,
			IntentHash:    plan.IntentHash,
		},
		"applicationInit": V1Alpha2ApplicationInitAssignment{Plan: plan, State: "Consumed", InitialEvidenceDigest: hex.EncodeToString(evSum[:])},
		"authority": map[string]any{
			"everApplicationReserved": true,
			"owner":                   map[string]any{"namespace": phase2Ref.Namespace, "name": phase2Ref.Name, "uid": phase2Ref.UID, "logicalID": "logical"},
			"generation":              0,
			"binding":                 map[string]any{"deploymentUID": "deployment", "storageID": plan.StorageID},
			"pendingWrite":            false,
			"evidence":                json.RawMessage(evRaw),
		},
	}
	b, err := json.Marshal(cluster)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReconcileV1Alpha2ManagedProductionPolicies(t *testing.T) {
	for _, tc := range []struct {
		name      string
		policy    string
		wantOwner bool
	}{
		{name: "orphan-default", policy: "", wantOwner: false},
		{name: "delete-owned", policy: "Delete", wantOwner: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rawCluster := seededManagedClusterJSON(t, tc.policy, "")
			var mu sync.Mutex
			var pdb map[string]any
			kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				mu.Lock()
				defer mu.Unlock()
				if strings.Contains(r.URL.Path, "/rhizaclusters/") {
					return phase2Response(200, string(rawCluster)), nil
				}
				if strings.HasSuffix(r.URL.Path, "/poddisruptionbudgets") && r.Method == http.MethodPost {
					if err := json.NewDecoder(r.Body).Decode(&pdb); err != nil {
						return phase2Response(400, err.Error()), nil
					}
					pdb["metadata"].(map[string]any)["uid"] = "pdb-uid"
					return phase2Response(201, "{}"), nil
				}
				if strings.Contains(r.URL.Path, "/poddisruptionbudgets/") && r.Method == http.MethodGet {
					b, _ := json.Marshal(pdb)
					return phase2Response(200, string(b)), nil
				}
				return phase2Response(404, ""), nil
			}).Kube
			if err := ReconcileV1Alpha2ManagedProduction(context.Background(), kube, phase2Ref); err != nil {
				t.Fatalf("ReconcileV1Alpha2ManagedProduction(%s): %v", tc.name, err)
			}
			owners, _ := pdb["metadata"].(map[string]any)["ownerReferences"].([]any)
			if (len(owners) > 0) != tc.wantOwner {
				t.Fatalf("policy=%q ownerReferences=%v wantOwner=%t", tc.policy, owners, tc.wantOwner)
			}
		})
	}
}

func TestReconcileV1Alpha2ManagedImageUpgrade(t *testing.T) {
	depName, _ := V1Alpha2GenesisDeploymentName("uid")
	profile := phase9RuntimeProfile()
	rawCluster := seededManagedClusterJSON(t, "", "example/sql:v2")

	dep := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": depName, "namespace": "ns", "uid": "deployment", "generation": 1, "resourceVersion": "10",
			"ownerReferences": []any{map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "name": "cluster", "uid": "uid", "controller": true}},
		},
		"spec": map[string]any{
			"replicas": 3,
			"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "sql", "image": "example/sql:v1"}}}},
		},
		"status": map[string]any{"observedGeneration": 1, "replicas": 3, "readyReplicas": 3, "updatedReplicas": 3},
	}

	var mu sync.Mutex
	var putCount int
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.Contains(r.URL.Path, "/rhizaclusters/") && !strings.HasSuffix(r.URL.Path, "/status"):
			return phase2Response(200, string(rawCluster)), nil
		case strings.Contains(r.URL.Path, "/deployments/"+depName):
			if r.Method == http.MethodPut {
				putCount++
				var updated map[string]any
				if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
					return phase2Response(400, err.Error()), nil
				}
				updated["metadata"].(map[string]any)["resourceVersion"] = "11"
				dep = updated
			}
			b, _ := json.Marshal(dep)
			return phase2Response(200, string(b)), nil
		}
		return phase2Response(404, ""), nil
	}).Kube

	if err := ReconcileV1Alpha2ManagedImageUpgrade(context.Background(), kube, phase2Ref, profile); err != nil {
		t.Fatalf("ReconcileV1Alpha2ManagedImageUpgrade: %v", err)
	}
	if putCount != 1 {
		t.Fatalf("expected 1 Deployment PUT for rolling image upgrade, got %d", putCount)
	}
}

func TestRunLeaderElectionAcquireAndCancel(t *testing.T) {
	var mu sync.Mutex
	var lease operatorLease
	hasLease := false
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/leases/rhiza-operator") && r.Method == http.MethodGet:
			if !hasLease {
				return phase2Response(404, `{"reason":"NotFound"}`), nil
			}
			b, _ := json.Marshal(lease)
			return phase2Response(200, string(b)), nil
		case strings.HasSuffix(r.URL.Path, "/leases") && r.Method == http.MethodPost:
			var raw map[string]any
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				return phase2Response(400, err.Error()), nil
			}
			meta, _ := raw["metadata"].(map[string]any)
			if meta["name"] != "rhiza-operator" || meta["namespace"] != "ns" {
				return phase2Response(422, "metadata.name and metadata.namespace must be lowercase"), nil
			}
			if _, hasUpper := meta["Name"]; hasUpper {
				return phase2Response(422, "unexpected uppercase Name in metadata"), nil
			}
			b, _ := json.Marshal(raw)
			if err := json.Unmarshal(b, &lease); err != nil {
				return phase2Response(400, err.Error()), nil
			}
			if _, err := time.Parse(kubernetesMicroTimeFmt, lease.Spec.AcquireTime); err != nil || len(lease.Spec.AcquireTime) != len("2006-01-02T15:04:05.000000Z") {
				return phase2Response(422, "acquireTime must be MicroTime"), nil
			}
			if _, err := time.Parse(kubernetesMicroTimeFmt, lease.Spec.RenewTime); err != nil || len(lease.Spec.RenewTime) != len("2006-01-02T15:04:05.000000Z") {
				return phase2Response(422, "renewTime must be MicroTime"), nil
			}
			lease.Metadata.ResourceVersion = "1"
			hasLease = true
			return phase2Response(201, "{}"), nil
		}
		return phase2Response(404, ""), nil
	}).Kube
	kube.Namespace = "ns"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acquired := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunLeaderElection(ctx, kube, "op-pod-1", "rhiza-operator", func(lctx context.Context) error {
			close(acquired)
			<-lctx.Done()
			return lctx.Err()
		})
	}()
	<-acquired
	cancel()
	if err := <-errCh; err != context.Canceled {
		t.Fatalf("RunLeaderElection returned %v, want context.Canceled", err)
	}
}
