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
