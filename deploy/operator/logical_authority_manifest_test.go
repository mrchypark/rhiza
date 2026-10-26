package operator_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLogicalAuthorityProfileHasNoHostRuntimeDependencies(t *testing.T) {
	authorityText := readLogicalProfileFile(t, "logical-authority/authority.yaml")
	workloadText := readLogicalProfileFile(t, "logical-authority/statefulset.yaml")
	forbidden := []string{"runtime-agent", "DaemonSet", "hostPID: true", "hostPath:", "containerd", "/proc", "cgroup", "SYS_PTRACE", "privileged: true"}
	for _, text := range []string{authorityText, workloadText} {
		for _, value := range forbidden {
			if strings.Contains(text, value) {
				t.Errorf("logical deployment manifest contains forbidden host/runtime dependency %q", value)
			}
		}
	}

	resources := readLogicalProfileDocuments(t, "logical-authority/authority.yaml")
	authoritySA := manifestByName(t, resources, "ServiceAccount", "rhiza-fencer")
	authorityAnnotations := nestedLogicalMap(t, authoritySA, "metadata", "annotations")
	if authorityAnnotations["iam.gke.io/gcp-service-account"] != "REPLACE_WITH_AUTHORITY_GSA_EMAIL" {
		t.Fatalf("authority writer identity is not a distinct replaceable identity: %#v", authorityAnnotations)
	}
	_ = manifestByName(t, resources, "ClusterRole", "rhiza-fencer-tokenreview")
	if !strings.Contains(authorityText, "resources: [tokenreviews]\n    verbs: [create]") {
		t.Fatal("logical profile must retain TokenReview create permission")
	}
	if strings.Contains(authorityText, "resources: [nodes]") {
		t.Fatal("logical authority must not request Node read permission")
	}
	if !strings.Contains(authorityText, "resourceNames: [rhiza]") {
		t.Fatal("namespace UID read must remain pinned to the target namespace")
	}

	deployment := manifestByName(t, resources, "Deployment", "rhiza-fencer")
	podSpec := nestedLogicalMap(t, deployment, "spec", "template", "spec")
	if podSpec["serviceAccountName"] != "rhiza-fencer" {
		t.Fatalf("authority service account=%v", podSpec["serviceAccountName"])
	}
	containers := logicalList(t, podSpec, "containers")
	if len(containers) != 1 {
		t.Fatalf("authority containers=%d, want one", len(containers))
	}
	authorityContainer := logicalMap(t, containers[0])
	assertUnprivilegedContainer(t, authorityContainer)
	args := logicalStringList(t, authorityContainer["args"])
	if !containsString(args, "--authority-profile=logical-writer-v1") || !containsString(args, "--source-writer-use-adc") {
		t.Fatalf("authority args do not select the supported logical profile and ADC writer: %v", args)
	}
	if image := logicalString(t, authorityContainer["image"]); !pinnedImageTemplate(image) {
		t.Fatalf("authority image is not digest-pinned as a replaceable template: %q", image)
	}
	if !strings.Contains(authorityText, "name: rhiza-object-store") {
		t.Fatal("authority must receive only public object-store coordinates, not a writer Secret")
	}
	if strings.Contains(authorityText, "secretRef:") || strings.Contains(authorityText, "source-writer-credentials-file") {
		t.Fatal("authority profile must not add a static writer-key Secret or key-file path")
	}
}

func TestLogicalAuthorityWorkloadSeparatesRestoreStageAndWriterCredentials(t *testing.T) {
	text := readLogicalProfileFile(t, "logical-authority/statefulset.yaml")
	if strings.Contains(text, "rhiza-membership") {
		t.Fatal("workload must use the controller-generated recovery membership ConfigMap, not a manual fixed-name ConfigMap")
	}
	resources := readLogicalProfileDocuments(t, "logical-authority/statefulset.yaml")
	set := manifestByName(t, resources, "StatefulSet", "rhiza")
	spec := nestedLogicalMap(t, set, "spec")
	if spec["volumeClaimTemplates"] != nil {
		t.Fatal("logical NoPVC StatefulSet must not declare volume claim templates")
	}
	podSpec := nestedLogicalMap(t, set, "spec", "template", "spec")
	if podSpec["serviceAccountName"] != "rhiza-voter-reader" || podSpec["automountServiceAccountToken"] != false {
		t.Fatalf("workload must use reader-only identity and disable implicit API token: %#v", podSpec)
	}
	workloadResources := readLogicalProfileDocuments(t, "logical-authority/statefulset.yaml")
	readerSA := manifestByName(t, workloadResources, "ServiceAccount", "rhiza-voter-reader")
	readerAnnotations := nestedLogicalMap(t, readerSA, "metadata", "annotations")
	if readerAnnotations["iam.gke.io/gcp-service-account"] != "REPLACE_WITH_READ_ONLY_GSA_EMAIL" {
		t.Fatalf("reader workload identity is not an explicit replaceable read-only identity: %#v", readerAnnotations)
	}
	if readerAnnotations["iam.gke.io/gcp-service-account"] == "REPLACE_WITH_AUTHORITY_GSA_EMAIL" {
		t.Fatal("reader and writer workload identities must be separate")
	}
	initContainers := logicalList(t, podSpec, "initContainers")
	containers := logicalList(t, podSpec, "containers")
	if len(initContainers) != 1 || len(containers) != 1 {
		t.Fatalf("init/main container counts = %d/%d, want exactly one each", len(initContainers), len(containers))
	}
	init := logicalMap(t, initContainers[0])
	main := logicalMap(t, containers[0])
	assertUnprivilegedContainer(t, init)
	assertUnprivilegedContainer(t, main)
	if init["image"] != main["image"] || !pinnedImageTemplate(logicalString(t, main["image"])) {
		t.Fatalf("init/main must use the same digest-pinned rhiza image: init=%v main=%v", init["image"], main["image"])
	}
	if !sameStringList(t, init["command"], main["command"]) || !containsString(logicalStringList(t, init["args"]), "restore-init") {
		t.Fatal("restore-init must invoke the main rhiza binary's supported restore-init subcommand")
	}
	if !sameStringList(t, main["command"], []any{"/usr/local/bin/rhiza"}) {
		t.Fatalf("main command=%v, want /usr/local/bin/rhiza", main["command"])
	}
	initEnvFrom := logicalList(t, init, "envFrom")
	if len(initEnvFrom) != 2 || !isEnvFromRef(initEnvFrom[0], "configMapRef", "rhiza-config") ||
		!isEnvFromRef(initEnvFrom[1], "configMapRef", "rhiza-object-store") {
		t.Fatalf("init must receive only non-secret cluster and object-store reader configuration: %#v", initEnvFrom)
	}
	for _, entry := range initEnvFrom {
		if _, hasSecret := logicalMap(t, entry)["secretRef"]; hasSecret {
			t.Fatal("restore-init must not receive a secretRef")
		}
	}
	if containsEnvName(t, init["env"], "RHIZA_AUTHORITY_TOKEN_FILE") || containsEnvName(t, init["env"], "RHIZA_PEER_TOKEN") ||
		containsEnvName(t, init["env"], "RHIZA_PEER_TOKENS") {
		t.Fatal("restore-init must not receive authority or peer credentials")
	}
	mainEnvFrom := logicalList(t, main, "envFrom")
	if len(mainEnvFrom) != 3 || !isEnvFromRef(mainEnvFrom[0], "configMapRef", "rhiza-config") ||
		!isEnvFromRef(mainEnvFrom[1], "configMapRef", "rhiza-object-store") ||
		!hasSecretEnvFrom(mainEnvFrom, "rhiza-peer-credentials") {
		t.Fatal("main container must retain peer credentials")
	}
	if !isEnvFromRef(mainEnvFrom[2], "secretRef", "rhiza-peer-credentials") {
		t.Fatalf("main peer credentials reference must be the only Secret envFrom: %#v", mainEnvFrom)
	}
	if !envUsesSecretKey(t, main["env"], "RHIZA_CLUSTER_MEMBERS", "rhiza-peer-credentials", "members") {
		t.Fatal("source main container must read its public membership descriptor from the existing credentials Secret")
	}
	for _, required := range []string{"RHIZA_AUTHORITY_PROFILE", "RHIZA_AUTHORITY_URL", "RHIZA_AUTHORITY_CA_FILE", "RHIZA_AUTHORITY_TOKEN_FILE", "POD_UID"} {
		if !containsEnvName(t, main["env"], required) {
			t.Errorf("main container is missing required authority environment %q", required)
		}
	}
	if !containsString(logicalStringList(t, main["command"]), "/usr/local/bin/rhiza") {
		t.Fatal("main command must use the rhiza binary")
	}

	volumes := logicalList(t, podSpec, "volumes")
	if !emptyDirVolume(volumes, "data") || !emptyDirVolume(volumes, "restore-staging") {
		t.Fatal("data and restore staging must be independent emptyDir volumes")
	}
	if !mountIs(logicalList(t, init, "volumeMounts"), "restore-staging", "/ha-restore-staging", false) ||
		mountIs(logicalList(t, init, "volumeMounts"), "data", "/data", false) {
		t.Fatal("init must write only to restore staging and never mount DataDir")
	}
	if !mountIs(logicalList(t, main, "volumeMounts"), "restore-staging", "/ha-restore-staging", true) ||
		!mountIs(logicalList(t, main, "volumeMounts"), "data", "/data", false) {
		t.Fatal("main must own DataDir and consume restore staging read-only")
	}
	if !mountIs(logicalList(t, main, "volumeMounts"), "authority-token", "/var/run/secrets/rhiza-authority", true) ||
		mountIs(logicalList(t, init, "volumeMounts"), "authority-token", "/var/run/secrets/rhiza-authority", true) {
		t.Fatal("only main may mount the projected authority token")
	}
	if !mountIs(logicalList(t, main, "volumeMounts"), "authority-ca", "/fencer-ca", true) ||
		mountIs(logicalList(t, init, "volumeMounts"), "authority-ca", "/fencer-ca", true) {
		t.Fatal("only main may mount the authority trust root")
	}
	if !projectedAudience(volumes, "authority-token", "rhiza-fencer", "token") {
		t.Fatal("main authority token must be projected for the rhiza-fencer audience")
	}
	if envValue(t, init["env"], "RHIZA_HA_INIT_RESTORE_DIR") != "/ha-restore-staging" ||
		envValue(t, main["env"], "RHIZA_HA_INIT_RESTORE_DIR") != "/ha-restore-staging" {
		t.Fatal("init/main stage path differs from the mounted restore staging directory")
	}
	if envValue(t, main["env"], "RHIZA_AUTHORITY_PROFILE") != "logical-writer-v1" ||
		envValue(t, main["env"], "RHIZA_AUTHORITY_URL") != "https://rhiza-fencer:9444/v1/supervisor" ||
		envValue(t, main["env"], "RHIZA_AUTHORITY_CA_FILE") != "/fencer-ca/ca.crt" ||
		envValue(t, main["env"], "RHIZA_AUTHORITY_TOKEN_FILE") != "/var/run/secrets/rhiza-authority/token" {
		t.Fatal("main authority settings do not match the profile, endpoint, and mounted credentials")
	}
}

func readLogicalProfileFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile("logical-authority/" + strings.TrimPrefix(name, "logical-authority/"))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func readLogicalProfileDocuments(t *testing.T, name string) []map[string]any {
	t.Helper()
	data := []byte(readLogicalProfileFile(t, name))
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var documents []map[string]any
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode %s: %v", name, err)
		}
		if len(document) > 0 {
			documents = append(documents, document)
		}
	}
	return documents
}

func manifestByName(t *testing.T, documents []map[string]any, kind, name string) map[string]any {
	t.Helper()
	for _, document := range documents {
		metadata, _ := document["metadata"].(map[string]any)
		if document["kind"] == kind && metadata["name"] == name {
			return document
		}
	}
	t.Fatalf("manifest lacks %s/%s", kind, name)
	return nil
}

func nestedLogicalMap(t *testing.T, root map[string]any, keys ...string) map[string]any {
	t.Helper()
	current := root
	for _, key := range keys {
		value, ok := current[key].(map[string]any)
		if !ok {
			t.Fatalf("manifest path %v is missing map %q", keys, key)
		}
		current = value
	}
	return current
}

func logicalList(t *testing.T, root map[string]any, key string) []any {
	t.Helper()
	value, ok := root[key].([]any)
	if !ok {
		t.Fatalf("manifest field %q is not a list", key)
	}
	return value
}

func logicalMap(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("manifest value is not an object: %T", value)
	}
	return result
}

func logicalString(t *testing.T, value any) string {
	t.Helper()
	result, ok := value.(string)
	if !ok {
		t.Fatalf("manifest value %v is not a string", value)
	}
	return result
}

func logicalStringList(t *testing.T, value any) []string {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("manifest value is not a string list: %T", value)
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		result = append(result, logicalString(t, item))
	}
	return result
}

func containsString(items []string, wanted string) bool {
	for _, item := range items {
		if item == wanted {
			return true
		}
	}
	return false
}

func sameStringList(t *testing.T, left, right any) bool {
	t.Helper()
	a, b := logicalStringList(t, left), logicalStringList(t, right)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func pinnedImageTemplate(image string) bool {
	return strings.Contains(image, "@sha256:REPLACE_WITH_64_HEX_DIGEST") && !strings.Contains(image, ":latest")
}

func containsEnvName(t *testing.T, value any, name string) bool {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if logicalMap(t, item)["name"] == name {
			return true
		}
	}
	return false
}

func envValue(t *testing.T, value any, name string) string {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		t.Fatalf("manifest env is not a list: %T", value)
	}
	for _, item := range items {
		entry := logicalMap(t, item)
		if entry["name"] == name {
			result, _ := entry["value"].(string)
			return result
		}
	}
	t.Fatalf("manifest env lacks %q", name)
	return ""
}

func envUsesSecretKey(t *testing.T, value any, envName, secretName, key string) bool {
	t.Helper()
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		entry := logicalMap(t, item)
		if entry["name"] != envName {
			continue
		}
		valueFrom, _ := entry["valueFrom"].(map[string]any)
		secret, _ := valueFrom["secretKeyRef"].(map[string]any)
		return secret["name"] == secretName && secret["key"] == key
	}
	return false
}

func assertUnprivilegedContainer(t *testing.T, container map[string]any) {
	t.Helper()
	security, ok := container["securityContext"].(map[string]any)
	if !ok || security["runAsNonRoot"] != true || security["allowPrivilegeEscalation"] != false || security["readOnlyRootFilesystem"] != true {
		t.Fatalf("container lacks non-root, no-escalation, read-only security context: %#v", security)
	}
	capabilities, _ := security["capabilities"].(map[string]any)
	dropped, _ := capabilities["drop"].([]any)
	if len(dropped) != 1 || dropped[0] != "ALL" {
		t.Fatalf("container capabilities must drop all: %#v", capabilities)
	}
}

func hasSecretEnvFrom(items []any, name string) bool {
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ref, ok := entry["secretRef"].(map[string]any)
		if ok && ref["name"] == name {
			return true
		}
	}
	return false
}

func emptyDirVolume(items []any, name string) bool {
	for _, item := range items {
		volume, ok := item.(map[string]any)
		if ok && volume["name"] == name {
			_, ok := volume["emptyDir"].(map[string]any)
			return ok
		}
	}
	return false
}

func mountIs(items []any, name, path string, readOnly bool) bool {
	for _, item := range items {
		mount, ok := item.(map[string]any)
		if !ok || mount["name"] != name || mount["mountPath"] != path {
			continue
		}
		actual, _ := mount["readOnly"].(bool)
		return actual == readOnly
	}
	return false
}

func projectedAudience(volumes []any, volumeName, audience, path string) bool {
	for _, item := range volumes {
		volume, ok := item.(map[string]any)
		if !ok || volume["name"] != volumeName {
			continue
		}
		projected, ok := volume["projected"].(map[string]any)
		if !ok {
			return false
		}
		sources, _ := projected["sources"].([]any)
		for _, source := range sources {
			entry, ok := source.(map[string]any)
			if !ok {
				continue
			}
			token, ok := entry["serviceAccountToken"].(map[string]any)
			if ok && token["audience"] == audience && token["path"] == path {
				return true
			}
		}
	}
	return false
}
