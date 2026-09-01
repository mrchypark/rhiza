package k8s

import (
	"os"
	"strings"
	"testing"
)

func TestThreePeerBootstrapSettings(t *testing.T) {
	statefulSet, err := os.ReadFile("statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	service, err := os.ReadFile("service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(statefulSet), "podManagementPolicy: Parallel") {
		t.Fatal("three-peer StatefulSet must start peers in parallel")
	}
	if !strings.Contains(string(service), "publishNotReadyAddresses: true") {
		t.Fatal("headless service must publish peers before readiness")
	}
}

func TestReadReplicaDeploymentsSelectExplicitRoles(t *testing.T) {
	manifest, err := os.ReadFile("read-replicas.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(manifest)
	for _, required := range []string{"value: object-store", "value: learner", "path: /ready", "name: rhiza-object-store", "app.kubernetes.io/name: rhiza-read-replica"} {
		if !strings.Contains(text, required) {
			t.Fatalf("read replica manifest missing %q", required)
		}
	}
	if strings.Contains(text, "app.kubernetes.io/name: rhiza\n") || strings.Contains(text, "voter token") {
		t.Fatal("read replica manifest overlaps voter selectors or requests voter credentials")
	}
}

func TestThreePeerE2EManifestsProvideDistinctVoterTokens(t *testing.T) {
	for _, name := range []string{"sql-server-3peer-e2e.yaml", "graph-server-3peer-e2e.yaml"} {
		manifest, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(manifest)
		if strings.Count(text, `"token":"rhiza-`) != 3 {
			t.Fatalf("%s must provide one voter token per member", name)
		}
		if strings.Contains(text, `"token":"rhiza-local-e2e-peer-token"`) {
			t.Fatalf("%s reuses the admin token as a voter token", name)
		}
	}
}
