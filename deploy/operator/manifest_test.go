package operator_test

import (
	"fmt"
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

// The voter count is the length of spec.voterPods, so the CRD schema must
// accept any list of three or more voters.
func TestClusterCRDAcceptsVoterListsAboveThree(t *testing.T) {
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
	schema, ok := spec["voterPods"].(map[string]any)
	if !ok {
		t.Fatal("voterPods is missing from the RhizaCluster schema")
	}
	if min, _ := schema["minItems"].(int); min != 3 {
		t.Fatalf("voterPods minItems=%v, want 3", schema["minItems"])
	}
	if capped, present := schema["maxItems"]; present {
		t.Fatalf("voterPods maxItems=%v caps the authoritative voter list", capped)
	}
	items, _ := schema["items"].(map[string]any)
	required, _ := items["required"].([]any)
	list := "voterPods:\n"
	for i := range 5 {
		list += fmt.Sprintf("  - {nodeID: rhiza-%d, pod: rhiza-%d}\n", i, i)
	}
	var manifest map[string]any
	if err := yaml.Unmarshal([]byte(list), &manifest); err != nil {
		t.Fatal(err)
	}
	voters, _ := manifest["voterPods"].([]any)
	if len(voters) != 5 {
		t.Fatalf("five-entry voter list was not parsed: %s", list)
	}
	for _, entry := range voters {
		item, _ := entry.(map[string]any)
		for _, key := range required {
			if item[key.(string)] == nil {
				t.Fatalf("voterPods entry %v omits required %v", item, key)
			}
		}
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
// documented 16-target bound must stay in the fence CRD.
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
	if targets["maxItems"] != 16 {
		t.Fatalf("fence targets maxItems=%v, want 16", targets["maxItems"])
	}
}
