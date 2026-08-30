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
