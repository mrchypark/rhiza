package operator_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestManualRecoveryAttestationCRDIsNamespacedStrictAndImmutable(t *testing.T) {
	data, err := os.ReadFile("manual-recovery-attestation-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	if crd["kind"] != "CustomResourceDefinition" {
		t.Fatalf("kind=%v, want CustomResourceDefinition", crd["kind"])
	}
	spec, _ := nestedManifestMap(crd, "spec")
	if spec["scope"] != "Namespaced" {
		t.Fatalf("CRD scope=%v, want Namespaced", spec["scope"])
	}
	versions, _ := spec["versions"].([]any)
	if len(versions) != 1 {
		t.Fatalf("served versions=%d, want one", len(versions))
	}
	version, _ := versions[0].(map[string]any)
	schema, _ := nestedManifestMap(version, "schema")
	openAPI, _ := nestedManifestMap(schema, "openAPIV3Schema")
	rootProps, _ := nestedManifestMap(openAPI, "properties")
	specSchema, _ := nestedManifestMap(rootProps, "spec")
	validations, _ := specSchema["x-kubernetes-validations"].([]any)
	if len(validations) != 1 {
		t.Fatalf("spec immutability validations=%d, want one", len(validations))
	}
	validation, _ := validations[0].(map[string]any)
	if validation["rule"] != "self == oldSelf" {
		t.Fatalf("immutable spec rule=%v, want self == oldSelf", validation["rule"])
	}
	properties, _ := nestedManifestMap(specSchema, "properties")
	for _, field := range []string{
		"challengeDigest", "recoveryNamespace", "recoveryName", "recoveryUID",
		"recoveryID", "operationID", "sourceClusterID", "statefulSetName", "statefulSetUID", "fenceRequestHash",
		"policyDigest", "rootDigest", "ledgerDigest",
		"cohortDigest", "storageScopeDigest", "sourceArchiveDigest", "targetClusterID",
		"targetMembershipHash", "currentPodUIDs", "assertions", "attester", "evidenceRefs",
	} {
		if _, ok := properties[field]; !ok {
			t.Errorf("attestation schema is missing %q", field)
		}
	}
	assertions, _ := properties["assertions"].(map[string]any)
	assertionFields, _ := nestedManifestMap(assertions, "properties")
	for _, field := range []string{
		"allHistoricalRunsTerminated", "recreationBlocked", "oldPeerAndClientWriteAccessBlocked",
		"oldArchiveWriterAccessRevoked", "storageQuiesced",
	} {
		assertion, _ := assertionFields[field].(map[string]any)
		allowed, _ := assertion["enum"].([]any)
		if len(allowed) != 1 || allowed[0] != true {
			t.Errorf("assertion %q must accept only true, got %v", field, assertion["enum"])
		}
	}
}

func TestManualRecoveryAttestationRBACSeparatesCreationFromReading(t *testing.T) {
	data, err := os.ReadFile("fencing/manual-attestation-rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	documents := decodeManualAttestationRBAC(t, data)
	roles := map[string][]string{}
	bindings := map[string]map[string]any{}
	for _, doc := range documents {
		metadata, _ := doc["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		if doc["kind"] == "Role" {
			rules, _ := doc["rules"].([]any)
			for _, rawRule := range rules {
				rule, _ := rawRule.(map[string]any)
				for _, rawResource := range rule["resources"].([]any) {
					resource, _ := rawResource.(string)
					verbs := make([]string, 0)
					for _, rawVerb := range rule["verbs"].([]any) {
						verb, _ := rawVerb.(string)
						verbs = append(verbs, verb)
					}
					roles[name] = append(roles[name], resource+":"+strings.Join(verbs, ","))
				}
			}
		}
		if doc["kind"] == "RoleBinding" {
			bindings[name] = doc
		}
	}
	if got := strings.Join(roles["rhiza-manual-recovery-approver"], ";"); got != "rhizamanualrecoveryattestations:create" {
		t.Fatalf("admin approval Role grants=%q, want create only on attestation", got)
	}
	if got := strings.Join(roles["rhiza-fencer-manual-attestation-reader"], ";"); got != "rhizamanualrecoveryattestations:get" {
		t.Fatalf("fencer approval Role grants=%q, want get only on attestation", got)
	}
	if len(bindings) != 1 {
		t.Fatalf("manual-attestation RoleBindings=%d, want only the fencer reader binding", len(bindings))
	}
	if _, bound := bindings["rhiza-manual-recovery-approver"]; bound {
		t.Fatal("approval Role must not guess or bind an installation-specific human principal")
	}
	binding, ok := bindings["rhiza-fencer-manual-attestation-reader"]
	if !ok {
		t.Fatal("fencer read RoleBinding is missing")
	}
	subjects, _ := binding["subjects"].([]any)
	if len(subjects) != 1 {
		t.Fatalf("fencer reader subjects=%d, want only rhiza-fencer", len(subjects))
	}
	subject, _ := subjects[0].(map[string]any)
	if subject["kind"] != "ServiceAccount" || subject["name"] != "rhiza-fencer" || subject["namespace"] != "rhiza" {
		t.Fatalf("unexpected fencer reader subject: %#v", subject)
	}
	roleRef, _ := binding["roleRef"].(map[string]any)
	if roleRef["name"] != "rhiza-fencer-manual-attestation-reader" {
		t.Fatalf("fencer RoleBinding references %v", roleRef["name"])
	}
}

func TestOperatorRoleCannotWriteManualRecoveryAttestations(t *testing.T) {
	data, err := os.ReadFile("rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, doc := range decodeManualAttestationRBAC(t, data) {
		if doc["kind"] != "Role" {
			continue
		}
		metadata, _ := doc["metadata"].(map[string]any)
		if metadata["name"] != "rhiza-operator" {
			continue
		}
		found = true
		for _, rawRule := range doc["rules"].([]any) {
			rule, _ := rawRule.(map[string]any)
			for _, resource := range rule["resources"].([]any) {
				if resource == "rhizamanualrecoveryattestations" || resource == "*" {
					t.Fatalf("operator Role unexpectedly grants access to manual approvals: %#v", rule)
				}
			}
		}
	}
	if !found {
		t.Fatal("rhiza-operator Role is missing from the deployed RBAC manifest")
	}
}

func decodeManualAttestationRBAC(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	documents := make([]map[string]any, 0)
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		if len(doc) != 0 {
			documents = append(documents, doc)
		}
	}
	return documents
}
