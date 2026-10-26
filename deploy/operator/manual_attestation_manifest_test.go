package operator_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/operator"
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
		"cohortDigest", "storageScopeDigest", "sourceArchiveDigest", "targetClusterID", "targetBaseKind",
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

func TestManualRecoveryAttestationCRDMatchesSerializedSpecAndPreservesChallengeStatus(t *testing.T) {
	data, err := os.ReadFile("manual-recovery-attestation-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var attestationCRD map[string]any
	if err := yaml.Unmarshal(data, &attestationCRD); err != nil {
		t.Fatal(err)
	}
	attestationSpecSchema := crdVersionSpecSchema(t, attestationCRD)
	attestationProperties, _ := nestedManifestMap(attestationSpecSchema, "properties")
	attestationRequired := manifestStringSet(t, attestationSpecSchema["required"], "attestation spec required")

	serializedSpec, err := json.Marshal(operator.ManualRecoveryAttestationSpec{
		Version:         1,
		ChallengeDigest: strings.Repeat("a", 64),
		ManualRecoveryBinding: operator.ManualRecoveryBinding{
			EvidenceKind:         operator.ManualTerminalEvidenceKind,
			RecoveryNamespace:    "recovery-ns",
			RecoveryName:         "recovery",
			RecoveryUID:          "recovery-uid",
			RecoveryID:           "recovery-id",
			OperationID:          "operation-id",
			SourceClusterID:      "source-cluster",
			StatefulSetName:      "db",
			StatefulSetUID:       "sts-uid",
			FenceRequestHash:     strings.Repeat("b", 64),
			PolicyDigest:         strings.Repeat("c", 64),
			RootDigest:           strings.Repeat("d", 64),
			LedgerDigest:         strings.Repeat("e", 64),
			CohortDigest:         strings.Repeat("f", 64),
			StorageScopeDigest:   strings.Repeat("1", 64),
			SourceArchiveDigest:  strings.Repeat("2", 64),
			TargetClusterID:      "target-cluster",
			TargetBaseKind:       "fork",
			TargetMembershipHash: strings.Repeat("3", 64),
			CurrentPodUIDs:       []string{"pod-uid"},
		},
		Assertions: operator.ManualRecoveryAssertions{
			AllHistoricalRunsTerminated:        true,
			RecreationBlocked:                  true,
			OldPeerAndClientWriteAccessBlocked: true,
			OldArchiveWriterAccessRevoked:      true,
			StorageQuiesced:                    true,
		},
		Attester:     "incident-admin",
		EvidenceRefs: []string{"incident-record"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var serializedFields map[string]json.RawMessage
	if err := json.Unmarshal(serializedSpec, &serializedFields); err != nil {
		t.Fatal(err)
	}
	for field := range serializedFields {
		if _, ok := attestationProperties[field]; !ok {
			t.Errorf("serialized attestation field %q is pruned by the CRD schema", field)
		}
		if field != "targetMembershipHash" && !attestationRequired[field] {
			t.Errorf("serialized required attestation field %q is not required by the CRD", field)
		}
	}
	if attestationRequired["targetMembershipHash"] {
		t.Error("optional targetMembershipHash must not become a required CRD field")
	}
	targetBaseKind, _ := attestationProperties["targetBaseKind"].(map[string]any)
	allowedBaseKinds, _ := targetBaseKind["enum"].([]any)
	if targetBaseKind["type"] != "string" || len(allowedBaseKinds) != 1 || allowedBaseKinds[0] != "fork" {
		t.Errorf("targetBaseKind schema=%#v, want string enum [fork]", targetBaseKind)
	}

	assertionSchema, _ := attestationProperties["assertions"].(map[string]any)
	assertionProperties, _ := nestedManifestMap(assertionSchema, "properties")
	assertionRequired := manifestStringSet(t, assertionSchema["required"], "attestation assertions required")
	var serializedAssertions map[string]json.RawMessage
	if err := json.Unmarshal(serializedFields["assertions"], &serializedAssertions); err != nil {
		t.Fatal(err)
	}
	for field := range serializedAssertions {
		if _, ok := assertionProperties[field]; !ok || !assertionRequired[field] {
			t.Errorf("serialized assertion field %q is not both declared and required", field)
		}
	}

	recoveryCRD, err := os.ReadFile("crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var recoveryDefinition map[string]any
	if err := yaml.Unmarshal(recoveryCRD, &recoveryDefinition); err != nil {
		t.Fatal(err)
	}
	recoveryRoot := crdVersionRootSchema(t, recoveryDefinition)
	recoveryProperties, _ := nestedManifestMap(recoveryRoot, "properties")
	statusSchema, _ := recoveryProperties["status"].(map[string]any)
	if statusSchema["x-kubernetes-preserve-unknown-fields"] != true {
		t.Fatalf("RhizaRecovery status schema does not preserve the typed challenge fields: %#v", statusSchema)
	}
}

func crdVersionSpecSchema(t *testing.T, crd map[string]any) map[string]any {
	t.Helper()
	root := crdVersionRootSchema(t, crd)
	rootProperties, ok := nestedManifestMap(root, "properties")
	if !ok {
		t.Fatal("CRD root has no properties")
	}
	specSchema, ok := nestedManifestMap(rootProperties, "spec")
	if !ok {
		t.Fatal("CRD root has no spec schema")
	}
	return specSchema
}

func crdVersionRootSchema(t *testing.T, crd map[string]any) map[string]any {
	t.Helper()
	spec, ok := nestedManifestMap(crd, "spec")
	if !ok {
		t.Fatal("CRD has no spec")
	}
	versions, ok := spec["versions"].([]any)
	if !ok || len(versions) != 1 {
		t.Fatalf("CRD versions=%#v, want one", spec["versions"])
	}
	version, ok := versions[0].(map[string]any)
	if !ok {
		t.Fatal("CRD version is not a mapping")
	}
	schema, ok := nestedManifestMap(version, "schema")
	if !ok {
		t.Fatal("CRD version has no schema")
	}
	openAPI, ok := nestedManifestMap(schema, "openAPIV3Schema")
	if !ok {
		t.Fatal("CRD version has no openAPIV3Schema")
	}
	return openAPI
}

func manifestStringSet(t *testing.T, raw any, label string) map[string]bool {
	t.Helper()
	values, ok := raw.([]any)
	if !ok {
		t.Fatalf("%s=%#v, want string list", label, raw)
	}
	result := make(map[string]bool, len(values))
	for _, value := range values {
		field, ok := value.(string)
		if !ok {
			t.Fatalf("%s contains non-string %T", label, value)
		}
		result[field] = true
	}
	return result
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
			metadata, _ := doc["metadata"].(map[string]any)
			if _, namespaced := metadata["namespace"]; namespaced {
				t.Fatalf("Role %q hardcodes a namespace; install must use the selected operator namespace", metadata["name"])
			}
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
			metadata, _ := doc["metadata"].(map[string]any)
			if _, namespaced := metadata["namespace"]; namespaced {
				t.Fatalf("RoleBinding %q hardcodes a namespace; install must use the selected operator namespace", metadata["name"])
			}
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
	if subject["kind"] != "ServiceAccount" || subject["name"] != "rhiza-fencer" || subject["namespace"] != nil {
		t.Fatalf("unexpected fencer reader subject: %#v", subject)
	}
	roleRef, _ := binding["roleRef"].(map[string]any)
	if roleRef["name"] != "rhiza-fencer-manual-attestation-reader" {
		t.Fatalf("fencer RoleBinding references %v", roleRef["name"])
	}
}

func TestManualAttestationInstallInstructionsAreNamespaceScopedAndExplicit(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"kubectl apply -f deploy/operator/manual-recovery-attestation-crd.yaml",
		"kubectl -n \"$RHIZA_OPERATOR_NAMESPACE\" apply -f deploy/operator/fencing/manual-attestation-rbac.yaml",
		"kubectl -n \"$RHIZA_OPERATOR_NAMESPACE\" create rolebinding rhiza-manual-recovery-approver",
		"--group=\"$RHIZA_INCIDENT_ADMIN_GROUP\"",
	} {
		if !strings.Contains(string(data), required) {
			t.Errorf("manual-attestation install instructions are missing %q", required)
		}
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
