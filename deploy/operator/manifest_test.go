package operator_test

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestNoPVCBootstrapRetainsPeerCredentials(t *testing.T) {
	data, err := os.ReadFile("no-pvc/statefulset-base.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"name: rhiza-peer-credentials", "name: rhiza-object-store", "podManagementPolicy: Parallel", "type: OnDelete", "fieldPath: metadata.name"} {
		if !strings.Contains(string(data), required) {
			t.Fatalf("no-PVC bootstrap omits %q; voters must retain the fixed authenticated membership", required)
		}
	}
}

// The CRD schema accepts the supported voterCount range and no longer exposes
// the removed authoritative voter list.
func TestClusterCRDAcceptsVoterCountRange(t *testing.T) {
	data, err := os.ReadFile("cluster-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	if len(versions) != 1 {
		t.Fatalf("expected one served version, got %d", len(versions))
	}
	spec, _ := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)["spec"].(map[string]any)["properties"].(map[string]any)
	schema, ok := spec["voterCount"].(map[string]any)
	if !ok {
		t.Fatal("voterCount is missing from the RhizaCluster schema")
	}
	if min, _ := schema["minimum"].(int); min != 3 {
		t.Fatalf("voterCount minimum=%v, want 3", schema["minimum"])
	}
	if capped, _ := schema["maximum"].(int); capped != 64 {
		t.Fatalf("voterCount maximum=%v, want the 64-target fence bound", schema["maximum"])
	}
	if _, ok := spec["voterPods"]; ok {
		t.Fatal("voterPods must be removed from the RhizaCluster schema")
	}
	if bootstrap, _ := spec["bootstrap"].(map[string]any); bootstrap["default"] != false {
		t.Fatalf("bootstrap default=%v, want false", bootstrap["default"])
	}
	if domain, _ := spec["clusterDomain"].(map[string]any); domain["default"] != "cluster.local" {
		t.Fatalf("clusterDomain default=%v, want cluster.local", domain["default"])
	}
}

// The PDB must leave quorum intact for every supported voter count.
func TestNoPVCPDBToleratesOneVoterAtATTime(t *testing.T) {
	data, err := os.ReadFile("no-pvc/statefulset-base.yaml")
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	found := false
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		if doc["kind"] != "PodDisruptionBudget" {
			continue
		}
		found = true
		spec, _ := doc["spec"].(map[string]any)
		if spec["maxUnavailable"] != 1 || spec["minAvailable"] != nil {
			t.Fatalf("PodDisruptionBudget must tolerate one voter at a time: %v", spec)
		}
	}
	if !found {
		t.Fatal("no PodDisruptionBudget in the no-PVC base")
	}
}

// Generation fencing still covers every voter plus learner history, so the
// documented 64-target bound must stay in the fence CRD.
func TestFenceCRDKeepsGenerationTargetBound(t *testing.T) {
	data, err := os.ReadFile("fence-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	spec, _ := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)["spec"].(map[string]any)["properties"].(map[string]any)
	targets, _ := spec["targets"].(map[string]any)
	if targets["maxItems"] != 64 {
		t.Fatalf("fence targets maxItems=%v, want 64", targets["maxItems"])
	}
}
