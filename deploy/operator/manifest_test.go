package operator_test

import (
	"bytes"
	"os"
	"regexp"
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
}

func TestHAContractSchemas(t *testing.T) {
	for _, name := range []string{"cluster-crd.yaml", "crd.yaml"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var crd map[string]any
		if err := yaml.Unmarshal(data, &crd); err != nil {
			t.Fatal(err)
		}
		versions := crd["spec"].(map[string]any)["versions"].([]any)
		if len(versions) != 1 {
			t.Fatalf("%s: expected exactly one version", name)
		}
		v := versions[0].(map[string]any)
		if v["name"] != "v1alpha2" || v["served"] != true || v["storage"] != true {
			t.Fatalf("%s: invalid version %v", name, v)
		}
		spec := v["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)["spec"].(map[string]any)
		props := spec["properties"].(map[string]any)
		rules := spec["x-kubernetes-validations"].([]any)
		if len(rules) == 0 {
			t.Fatalf("%s: missing validation rules", name)
		}
		if name == "cluster-crd.yaml" {
			foundRules := map[string]bool{}
			for _, raw := range rules {
				foundRules[raw.(map[string]any)["rule"].(string)] = true
			}
			for _, rule := range []string{"self.voterCount == oldSelf.voterCount"} {
				if !foundRules[rule] {
					t.Fatalf("missing cluster transition rule %q", rule)
				}
			}
			if props["automaticRecovery"].(map[string]any)["default"] != "OFF" || props["deletionPolicy"].(map[string]any)["default"] != "Orphan" || props["provisionalRetentionSeconds"].(map[string]any)["default"] != 172800 {
				t.Fatal("cluster defaults changed")
			}
			if props["provisionalRetentionSeconds"].(map[string]any)["maximum"] != 9223372036 {
				t.Fatal("retention maximum must fit time.Duration")
			}
			assertPattern(t, props["logicalID"].(map[string]any), "logical", "\u00a0")
			workload := props["workload"].(map[string]any)["properties"].(map[string]any)
			template := workload["podTemplate"].(map[string]any)
			if template["x-kubernetes-preserve-unknown-fields"] != nil {
				t.Fatal("pod template must not preserve arbitrary fields")
			}
			pod := template["properties"].(map[string]any)["spec"].(map[string]any)
			podProps := pod["properties"].(map[string]any)
			if !strings.Contains(pod["x-kubernetes-validations"].([]any)[0].(map[string]any)["rule"].(string), "!has(self.volumes)") {
				t.Fatal("pod volumes must be rejected")
			}
			containers := podProps["containers"].(map[string]any)
			if containers["minItems"] != 1 || len(containers["x-kubernetes-validations"].([]any)) == 0 {
				t.Fatal("containers must be nonempty and unique")
			}
			containerRules := containers["x-kubernetes-validations"].([]any)
			if containerRules[1].(map[string]any)["rule"] != "self.map(c, [c.name, c.image]) == oldSelf.map(c, [c.name, c.image])" {
				t.Fatal("missing bounded workload transition rule")
			}
			item := containers["items"].(map[string]any)
			itemProps := item["properties"].(map[string]any)
			pattern := itemProps["image"].(map[string]any)["pattern"].(string)
			re, err := regexp.Compile(pattern)
			if err != nil {
				t.Fatalf("invalid decoded image pattern %q: %v", pattern, err)
			}
			if !re.MatchString("example/app:v1") || re.MatchString(" \t\n") || re.MatchString("\u00a0") {
				t.Fatalf("image pattern %q does not reject whitespace-only images", pattern)
			}
			if itemProps["name"] == nil || itemProps["image"] == nil || len(item["x-kubernetes-validations"].([]any)) == 0 {
				t.Fatal("container subset or rejection guard missing")
			}
		} else {
			target := props["target"].(map[string]any)["properties"].(map[string]any)
			assertPattern(t, target["name"].(map[string]any), "a.b", "a..b")
			assertPattern(t, target["uid"].(map[string]any), "uid", "\u00a0")
			if props["backoffLimit"].(map[string]any)["default"] != 6 || props["expectedGeneration"].(map[string]any)["minimum"] != 0 || props["activeDeadlineSeconds"].(map[string]any)["minimum"] != 1 || props["ttlSecondsAfterFinished"].(map[string]any)["minimum"] != 0 {
				t.Fatal("recovery defaults or bounds changed")
			}
			for _, raw := range rules {
				if strings.Contains(raw.(map[string]any)["rule"].(string), "ttlSecondsAfterFinished") {
					t.Fatal("TTL must remain mutable")
				}
			}
		}
	}
}

func assertPattern(t *testing.T, schema map[string]any, valid, invalid string) {
	t.Helper()
	re, err := regexp.Compile(schema["pattern"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if !re.MatchString(valid) || re.MatchString(invalid) {
		t.Fatalf("pattern %q: valid %q or invalid %q mismatch", re.String(), valid, invalid)
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

// The controller deletes the learner-credential Secrets it created
// (retireLearnerCredential) and the Pods it created. A Role that omits either
// verb makes learner retirement fail with HTTP 403 at runtime, so the release
// Role must cover every write the operator actually issues.
func TestOperatorRoleCoversControllerWrites(t *testing.T) {
	required := map[string][]string{
		"pods":                   {"get", "list", "create", "delete"},
		"secrets":                {"get", "create", "delete"},
		"configmaps":             {"get"},
		"services":               {"get", "create"},
		"statefulsets":           {"get", "update"},
		"rhizaclusters":          {"get", "list"},
		"rhizaclusters/status":   {"get", "update"},
		"rhizafences":            {"get", "create"},
		"rhizarecoveries":        {"get", "list", "create", "update"},
		"rhizarecoveries/status": {"get", "update"},
	}
	data, err := os.ReadFile("rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	grants := map[string]map[string]bool{}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		if doc["kind"] != "Role" {
			continue
		}
		meta, _ := doc["metadata"].(map[string]any)
		if meta["name"] != "rhiza-operator" {
			continue
		}
		for _, raw := range doc["rules"].([]any) {
			rule, _ := raw.(map[string]any)
			for _, res := range rule["resources"].([]any) {
				name, _ := res.(string)
				if grants[name] == nil {
					grants[name] = map[string]bool{}
				}
				for _, verb := range rule["verbs"].([]any) {
					grants[name][verb.(string)] = true
				}
			}
		}
	}
	for resource, verbs := range required {
		for _, verb := range verbs {
			if !grants[resource][verb] {
				t.Errorf("operator Role must grant %q on %q", verb, resource)
			}
		}
	}
}
