package operator

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestManagedResourceRequestsCanonicalQuantities(t *testing.T) {
	for _, tc := range []struct {
		name, a, b string
		equal      bool
	}{
		{"cpu decimal", `{"requests":{"cpu":"0.1"}}`, `{"requests":{"cpu":"100m"}}`, true},
		{"memory binary", `{"requests":{"memory":"1Gi"}}`, `{"requests":{"memory":"1024Mi"}}`, true},
		{"different cpu", `{"requests":{"cpu":"0.1"}}`, `{"requests":{"cpu":"101m"}}`, false},
		{"injected limits", `{"requests":{"cpu":"100m"},"limits":{"cpu":"1"}}`, `{"requests":{"cpu":"0.1"}}`, false},
		{"invalid", `{"requests":{"cpu":"one"}}`, `{"requests":{"cpu":"one"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := managedResourceRequestsEqual(json.RawMessage(tc.a), json.RawMessage(tc.b)); got != tc.equal {
				t.Fatalf("got %v", got)
			}
		})
	}
}

func TestManagedCandidateCanonicalResources(t *testing.T) {
	want := json.RawMessage(`{"template":{"metadata":{},"spec":{"containers":[{"name":"app","image":"app:v1","resources":{"requests":{"cpu":"0.1","memory":"1Gi"}}}],"initContainers":[]}}}`)
	actual := json.RawMessage(`{"template":{"metadata":{},"spec":{"containers":[{"name":"app","image":"app:v1","resources":{"requests":{"cpu":"100m","memory":"1024Mi"}}}],"initContainers":[]}}}`)
	if !managedCandidateSpecMatches(actual, want) {
		t.Fatal("equivalent Kubernetes quantities must match")
	}
	bad := json.RawMessage(`{"template":{"metadata":{},"spec":{"containers":[{"name":"app","image":"app:v1","resources":{"requests":{"cpu":"101m","memory":"1024Mi"}}}],"initContainers":[]}}}`)
	if managedCandidateSpecMatches(bad, want) {
		t.Fatal("changed request was accepted")
	}
}

func TestEmbeddedManagedPodPreservesCommandAndGates(t *testing.T) {
	profile := V1Alpha2RuntimeProfile{InitImage: "init", ProfileSecretName: "runtime"}
	ref := V1Alpha2ClusterRef{UID: "uid", Name: "cluster"}
	containers := []V1Alpha2Container{{Name: "app", Image: "app-image", Command: []string{"/app"}, Args: []string{"serve"}, Resources: &V1Alpha2Resources{Requests: map[string]string{"cpu": "250m", "memory": "128Mi"}}}}
	pod, err := V1Alpha2ManagedSQLPodSpec(ref, profile, containers, true)
	if err != nil {
		t.Fatal(err)
	}
	main := pod["containers"].([]map[string]any)[0]
	if !reflect.DeepEqual(main["command"], containers[0].Command) || !reflect.DeepEqual(main["args"], containers[0].Args) {
		t.Fatalf("application command lost: %v", main)
	}
	if main["readinessProbe"] == nil || main["env"] == nil || len(pod["initContainers"].([]map[string]any)) != 2 {
		t.Fatal("managed gates missing")
	}
	if !reflect.DeepEqual(main["resources"], containers[0].Resources) {
		t.Fatal("CPU request lost")
	}
}
