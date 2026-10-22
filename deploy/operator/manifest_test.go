package operator_test

import (
	"bytes"
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

func TestSupervisedSourcePodReplacesObjectStoreSecretInjection(t *testing.T) {
	data, err := os.ReadFile("fencing/supervised-statefulset.patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var patch map[string]any
	if err := yaml.Unmarshal(data, &patch); err != nil {
		t.Fatal(err)
	}
	spec, _ := patch["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := nestedManifestMap(template, "spec")
	containers, _ := podSpec["containers"].([]any)
	if len(containers) != 1 {
		t.Fatalf("supervised StatefulSet has %d containers, want exactly one", len(containers))
	}
	container, _ := containers[0].(map[string]any)
	envFrom, _ := container["envFrom"].([]any)
	if len(envFrom) != 2 {
		t.Fatalf("supervised envFrom has %d entries, want only ConfigMap and peer Secret", len(envFrom))
	}
	if !isEnvFromRef(envFrom[0], "configMapRef", "rhiza-config") || !isEnvFromRef(envFrom[1], "secretRef", "rhiza-peer-credentials") {
		t.Fatalf("supervised envFrom is not the exact public-config plus peer-credentials allowlist: %#v", envFrom)
	}
}

func nestedManifestMap(value map[string]any, key string) (map[string]any, bool) {
	nested, ok := value[key].(map[string]any)
	return nested, ok
}

func isEnvFromRef(value any, kind, name string) bool {
	ref, ok := value.(map[string]any)
	if !ok || len(ref) != 1 {
		return false
	}
	entry, ok := ref[kind].(map[string]any)
	return ok && len(entry) == 1 && entry["name"] == name
}

func TestOperatorDeploymentProjectsFencerAudienceTokenReadOnly(t *testing.T) {
	data, err := os.ReadFile("deployment.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var deployment map[string]any
	if err := yaml.Unmarshal(data, &deployment); err != nil {
		t.Fatal(err)
	}
	spec, _ := deployment["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	if podSpec["serviceAccountName"] != "rhiza-operator" {
		t.Fatalf("operator service account=%v, want rhiza-operator", podSpec["serviceAccountName"])
	}
	containers, _ := podSpec["containers"].([]any)
	if len(containers) != 1 {
		t.Fatalf("operator deployment containers=%d, want 1", len(containers))
	}
	container, _ := containers[0].(map[string]any)
	env, _ := container["env"].([]any)
	if !containsManifestNameValue(env, "RHIZA_FENCER_TOKEN_FILE", "/var/run/secrets/rhiza-fencer/token") {
		t.Fatal("operator must read the projected rhiza-fencer audience token from its configured path")
	}
	mounts, _ := container["volumeMounts"].([]any)
	if !containsReadonlyManifestMount(mounts, "fencer-token", "/var/run/secrets/rhiza-fencer") {
		t.Fatal("operator fencer token mount must be read-only at /var/run/secrets/rhiza-fencer")
	}
	volumes, _ := podSpec["volumes"].([]any)
	if !containsProjectedServiceAccountToken(volumes, "fencer-token", "rhiza-fencer", "token") {
		t.Fatal("operator must project a service-account token with audience rhiza-fencer and path token")
	}
}

func containsManifestNameValue(items []any, name, value string) bool {
	for _, item := range items {
		entry, _ := item.(map[string]any)
		if entry["name"] == name && entry["value"] == value {
			return true
		}
	}
	return false
}

func containsReadonlyManifestMount(items []any, name, mountPath string) bool {
	for _, item := range items {
		entry, _ := item.(map[string]any)
		if entry["name"] == name && entry["mountPath"] == mountPath && entry["readOnly"] == true {
			return true
		}
	}
	return false
}

func containsProjectedServiceAccountToken(items []any, name, audience, tokenPath string) bool {
	for _, item := range items {
		volume, _ := item.(map[string]any)
		if volume["name"] != name {
			continue
		}
		projected, _ := volume["projected"].(map[string]any)
		sources, _ := projected["sources"].([]any)
		for _, source := range sources {
			tokenSource, _ := source.(map[string]any)
			token, _ := tokenSource["serviceAccountToken"].(map[string]any)
			if token["audience"] == audience && token["path"] == tokenPath {
				return true
			}
		}
	}
	return false
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

func TestClusterCRDAutomaticRecoveryRequiresExplicitPolicy(t *testing.T) {
	data, err := os.ReadFile("cluster-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	spec, _ := versions[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)["spec"].(map[string]any)
	properties, _ := spec["properties"].(map[string]any)
	requiredSpec, _ := spec["required"].([]any)
	for _, field := range requiredSpec {
		if field == "automatic" || field == "policy" {
			t.Fatalf("spec.%s must be optional for manual recovery", field)
		}
	}
	if automatic, _ := properties["automatic"].(map[string]any); automatic["default"] != false {
		t.Fatalf("automatic default=%v, want false", automatic["default"])
	}
	policy, ok := properties["policy"].(map[string]any)
	if !ok {
		t.Fatal("optional policy schema is missing")
	}
	wantRequired := map[string]bool{"failureGraceSeconds": true, "cooldownSeconds": true, "maxRecoveriesPerDay": true, "allowDataLoss": true}
	required, _ := policy["required"].([]any)
	for _, field := range required {
		delete(wantRequired, field.(string))
	}
	if len(wantRequired) != 0 {
		t.Fatalf("policy omits required explicit fields: %v", wantRequired)
	}
	validations, _ := spec["x-kubernetes-validations"].([]any)
	if len(validations) != 1 || !strings.Contains(validations[0].(map[string]any)["rule"].(string), "has(self.policy)") {
		t.Fatalf("automatic policy opt-in validation missing: %v", validations)
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

func TestFencerRoleCreatesFenceRequestsWithoutClusterWrites(t *testing.T) {
	data, err := os.ReadFile("fencing/authority.yaml")
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
		if meta["name"] != "rhiza-fencer" {
			continue
		}
		for _, raw := range doc["rules"].([]any) {
			rule, _ := raw.(map[string]any)
			for _, resource := range rule["resources"].([]any) {
				name, _ := resource.(string)
				if grants[name] == nil {
					grants[name] = map[string]bool{}
				}
				for _, verb := range rule["verbs"].([]any) {
					grants[name][verb.(string)] = true
				}
			}
		}
	}
	for _, verb := range []string{"get", "list", "create"} {
		if !grants["rhizafences"][verb] {
			t.Errorf("fencer Role must grant %q on rhizafences", verb)
		}
	}
	for _, verb := range []string{"get", "list"} {
		if !grants["rhizaclusters"][verb] {
			t.Errorf("fencer Role must grant %q on rhizaclusters", verb)
		}
	}
	for _, verb := range []string{"create", "update", "patch", "delete", "deletecollection"} {
		if grants["rhizaclusters"][verb] {
			t.Errorf("fencer Role must not grant %q on rhizaclusters", verb)
		}
	}
	if !grants["rhizarecoveries"]["get"] {
		t.Error("fencer Role must grant get on rhizarecoveries for canonical recovery registration")
	}
	for _, verb := range []string{"list", "create", "update", "patch", "delete", "deletecollection"} {
		if grants["rhizarecoveries"][verb] {
			t.Errorf("fencer Role must not grant %q on rhizarecoveries", verb)
		}
	}
}
