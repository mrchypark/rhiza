package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func readContractFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func assertNodeNameORTerms(t *testing.T, name, manifest string, wantSets int) {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(manifest))
	affinityCount := 0
	var inspect func(any)
	inspect = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			for key, child := range value {
				if key == "nodeAffinity" {
					affinityCount++
					assertCapturedNodeNameAffinity(t, name, child)
				}
				inspect(child)
			}
		case []any:
			for _, child := range value {
				inspect(child)
			}
		}
	}
	for {
		var document map[string]any
		err := decoder.Decode(&document)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("%s manifest is not valid YAML: %v", name, err)
		}
		inspect(document)
	}
	if affinityCount != wantSets {
		t.Fatalf("%s has %d node affinity selectors, want %d", name, affinityCount, wantSets)
	}
}

func assertCapturedNodeNameAffinity(t *testing.T, name string, value any) {
	t.Helper()
	affinity, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s nodeAffinity has type %T, want mapping", name, value)
	}
	required, ok := affinity["requiredDuringSchedulingIgnoredDuringExecution"].(map[string]any)
	if !ok {
		t.Fatalf("%s nodeAffinity lacks required scheduling rules", name)
	}
	terms, ok := required["nodeSelectorTerms"].([]any)
	if !ok || len(terms) != 3 {
		t.Fatalf("%s has %d nodeSelectorTerms, want three captured-node OR terms", name, len(terms))
	}
	want := []string{"__NODE_A__", "__NODE_B__", "__NODE_C__"}
	for i, term := range terms {
		termMap, ok := term.(map[string]any)
		if !ok || len(termMap) != 1 {
			t.Fatalf("%s nodeSelectorTerm %d must contain only matchFields, got %#v", name, i, term)
		}
		matchFields, ok := termMap["matchFields"].([]any)
		if !ok || len(matchFields) != 1 {
			t.Fatalf("%s nodeSelectorTerm %d has %d matchFields, want exactly one", name, i, len(matchFields))
		}
		field, ok := matchFields[0].(map[string]any)
		if !ok || len(field) != 3 || field["key"] != "metadata.name" || field["operator"] != "In" {
			t.Fatalf("%s nodeSelectorTerm %d must be exactly metadata.name In, got %#v", name, i, matchFields[0])
		}
		values, ok := field["values"].([]any)
		if !ok || len(values) != 1 || values[0] != want[i] {
			t.Fatalf("%s nodeSelectorTerm %d values = %#v, want only %s", name, i, field["values"], want[i])
		}
	}
}

func manifestDocumentForKind(manifest, kind string) string {
	for _, document := range strings.Split(manifest, "\n---\n") {
		if strings.Contains(document, "kind: "+kind) {
			return document
		}
	}
	return ""
}

func TestIndependentGCSChaosManifestContract(t *testing.T) {
	manifest := readContractFile(t, "../votercount_gcs_chaos.yaml.in")
	launcher := readContractFile(t, "../run-votercount-gcs-chaos.sh")
	preflight := readContractFile(t, "../gcs-iam-preflight.yaml.in")
	expiry := readContractFile(t, "../gcs-namespace-expiry.yaml.in")
	authority := readContractFile(t, "../../../deploy/operator/fencing/authority.yaml")
	passivePatch := readContractFile(t, "../../../deploy/operator/fencing/runtime-agent-passive-kernel-patch.yaml")
	fencingLauncher := readContractFile(t, "../../../e2e/fencing/run.sh")
	bootstrapNamespace := readContractFile(t, "../gcs-postrelease-auth.yaml.in")
	probe := readContractFile(t, "gcs_reader_probe.go")
	assertNodeNameORTerms(t, "expiry Job", expiry, 1)
	assertNodeNameORTerms(t, "IAM preflight Pod", preflight, 1)
	assertNodeNameORTerms(t, "voterCount workloads", manifest, 5)
	assertProbePairPlacement(t, manifest)
	statefulSet := manifestDocumentForKind(manifest, "StatefulSet")
	rhizaCluster := manifestDocumentForKind(manifest, "RhizaCluster")
	if !strings.Contains(statefulSet, "replicas: 0") ||
		!strings.Contains(statefulSet, "rhiza.mrchypark.dev/cooperative-fencing: \"1\"") {
		t.Fatal("GCS supervised bootstrap must start at zero replicas with the cooperative fencing template annotation")
	}
	if !strings.Contains(rhizaCluster, "bootstrap: true") || !strings.Contains(rhizaCluster, "automatic: true") {
		t.Fatal("GCS supervised bootstrap must explicitly opt in to bootstrap and automatic recovery")
	}
	operatorRollout := strings.Index(launcher, "step 20-operator kubectl")
	clusterApply := strings.Index(launcher, "step 25-apply-rhiza-cluster")
	if operatorRollout < 0 || clusterApply < 0 || operatorRollout > clusterApply ||
		strings.Contains(launcher, "scale statefulset rhiza-voter") {
		t.Fatal("GCS bootstrap must pin and bootstrap through the operator before any external scale")
	}
	guardDefinition := strings.Index(launcher, "require_fresh_runtime_agent_daemonset() {")
	guardCall := strings.Index(launcher, "  require_fresh_runtime_agent_daemonset\n")
	runtimeAgentApply := strings.Index(launcher, "  step 20-runtime-agent-apply kubectl --context \"$RHIZA_KUBE_CONTEXT\" apply -f \"$out/runtime-agent.yaml\"")
	if guardDefinition < 0 || guardCall < 0 || runtimeAgentApply < 0 || guardDefinition > guardCall || guardCall > runtimeAgentApply ||
		!strings.Contains(launcher, "get daemonset rhiza-runtime-agent --ignore-not-found -o name") ||
		!strings.Contains(launcher, "[ -z \"$existing_runtime_agent\" ]") ||
		!strings.Contains(launcher, "could not verify runtime-agent DaemonSet absence; refusing to reuse or overwrite it") ||
		!strings.Contains(launcher, "runtime-agent DaemonSet already exists; refusing to reuse or overwrite it") {
		t.Fatal("GCS installer must fail closed on existing or uncheckable runtime-agent DaemonSets before applying the profile")
	}

	for _, want := range []string{
		"FENCER_INDEPENDENT_MUTATION_PROFILE, value: \"true\"",
		"FENCER_SOURCE_WRITER_USE_ADC, value: \"true\"",
		"FENCER_AUTHORITY_STATE_PREFIX, value: __AUTHORITY_PREFIX__",
		"RHIZA_OBJSTORE_PREFIX: __WORKLOAD_PREFIX__",
		"RHIZA_FENCER_BACKEND, value: http",
		"audience: rhiza-fencer",
		"RHIZA_FENCER_CA_FILE",
		"iam.gke.io/gcp-service-account: __CONTROLPLANE_GSA__",
		"iam.gke.io/gcp-service-account: __READONLY_WORKLOAD_GSA__",
		"serviceAccountName: rhiza-workload",
		"resources: [rhizarecoveries], verbs: [get]",
	} {
		if !strings.Contains(manifest, want) {
			t.Errorf("manifest missing %q", want)
		}
	}
	fencerRole := manifestDocumentForKind(manifest, "ClusterRole")
	fencerBinding := manifestDocumentForKind(manifest, "ClusterRoleBinding")
	for _, want := range []string{
		"name: '__NAMESPACE__-fencer-tokenreview'",
		"chaos.rhiza.io/run: '__RUN_ID__'",
		"kind: Namespace, name: __NAMESPACE__, uid: __NAMESPACE_UID__",
		"resources: [namespaces]",
		"resourceNames: [__NAMESPACE__]",
		"verbs: [get]",
	} {
		if !strings.Contains(fencerRole, want) {
			t.Errorf("run-scoped fencer ClusterRole missing %q", want)
		}
	}
	if strings.Contains(fencerRole, "resources: [namespaces], verbs: [get, list]") ||
		strings.Contains(fencerRole, "resources: [namespaces], verbs: [get, list, watch]") ||
		strings.Contains(fencerRole, "resourceNames: [*]") {
		t.Fatal("fencer namespace permission must be get-only and pinned to the rendered run namespace")
	}
	for _, want := range []string{
		"name: '__NAMESPACE__-fencer-tokenreview'",
		"chaos.rhiza.io/run: '__RUN_ID__'",
		"namespace: __NAMESPACE__",
		"name: rhiza-fencer",
		"kind: ClusterRole, name: '__NAMESPACE__-fencer-tokenreview'",
	} {
		if !strings.Contains(fencerBinding, want) {
			t.Errorf("run-scoped fencer ClusterRoleBinding missing %q", want)
		}
	}
	for _, want := range []string{
		"resourceNames: [__NAMESPACE__]", "verbs: [get, delete]",
		"kind: Namespace, name: __NAMESPACE__, uid: __NAMESPACE_UID__",
		"metadata.resourceVersion", "deadline=__EXPIRY_DEADLINE__", "activeDeadlineSeconds: 10800",
		"kubectl delete --raw /api/v1/namespaces/__NAMESPACE__ -f -", "sleep 30",
		"DeleteOptions", "resourceVersion",
	} {
		if !strings.Contains(expiry, want) {
			t.Errorf("namespace expiry ownership/expiry contract missing %q", want)
		}
	}
	if strings.Contains(expiry, "--preconditions=") {
		t.Fatal("namespace expiry must send a raw DeleteOptions body; kubectl has no resourceVersion precondition flag")
	}
	if !strings.Contains(launcher, "namespace cleanup was not verified locally; the in-cluster expiry Job remains pending") ||
		!strings.Contains(launcher, "exit_status=1") {
		t.Fatal("unverified local namespace cleanup must make the launcher fail")
	}
	if !strings.Contains(launcher, "command -v kubectl >/dev/null && command -v date >/dev/null && command -v sleep >/dev/null") {
		t.Fatal("expiry image must be checked for every command used by its cleanup loop")
	}
	if !strings.Contains(launcher, "expiry_deadline=$((expiry_now + 7200))") ||
		!strings.Contains(launcher, "s|__EXPIRY_DEADLINE__|$expiry_deadline|g") {
		t.Fatal("expiry deadline must be computed once at render time and fixed into the Job")
	}
	if !strings.Contains(bootstrapNamespace, "pods: '12'") {
		t.Fatal("namespace Pod quota does not account for pinned agent, workload, probe, and expiry Pods")
	}
	if strings.Contains(fencerRole, "name: rhiza-fencer-tokenreview") || strings.Contains(fencerBinding, "name: rhiza-fencer-tokenreview") {
		t.Fatal("fencer RBAC must not bind a shared, non-run-unique ClusterRole")
	}
	for _, want := range []string{
		"GOOGLE_APPLICATION_CREDENTIALS",
		"CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE",
		"STORAGE_EMULATOR_HOST",
		"ReadVersion(context.Context, string, int64)",
		"func (s *gcsIAMProbeBucket) ReadVersion",
		"!bytes.Equal(body, item.expected)",
		"GCS IAM preflight %s phase failed",
	} {
		if !strings.Contains(probe, want) {
			t.Errorf("IAM preflight identity/cleanup contract missing %q", want)
		}
	}
	if strings.Contains(probe, "return runErr") {
		t.Fatal("IAM preflight must not log raw provider errors")
	}
	preflightOrder := strings.Index(launcher, "run_iam_probe \"rhiza-gcs-writer-prepare-")
	clusterOrder := strings.Index(launcher, "step 25-apply-rhiza-cluster")
	if preflightOrder < 0 || clusterOrder < 0 || preflightOrder > clusterOrder {
		t.Fatal("RhizaCluster was applied before GCS IAM identity preflight")
	}
	for _, want := range []string{
		"chaos.rhiza.io/owner: gcs-iam-preflight",
		"serviceAccountName: __PROBE_KSA__",
		"qualification-gcs-iam-probe, __PROBE_MODE__",
		"automountServiceAccountToken: false",
		"readOnlyRootFilesystem: true",
		"__PROBE_NONCE__",
		"__MANIFEST_GENERATION__",
	} {
		if !strings.Contains(preflight, want) {
			t.Errorf("IAM preflight Pod template missing %q", want)
		}
	}
	for _, forbidden := range []string{"GOOGLE_APPLICATION_CREDENTIALS", "credentials.json", "secretKeyRef:", "envFrom:", "volumes:"} {
		if strings.Contains(preflight, forbidden) {
			t.Errorf("IAM probe Pod must use only its workload identity, found %q", forbidden)
		}
	}
	for _, want := range []string{
		"--ignore-not-found -o json",
		"kubectl --context \"$RHIZA_KUBE_CONTEXT\" -n \"$RHIZA_NAMESPACE\" create -f",
		"pre-existing preflight Pod name",
		"preconditions",
		"manual UID-scoped cleanup required",
		"gcs-iam-cleanup-pending-$probe_nonce.",
		"gcs-iam-pod-cleanup-pending-$probe_nonce",
		"create response lost",
	} {
		if !strings.Contains(launcher, want) {
			t.Errorf("IAM preflight ownership/cleanup contract missing %q", want)
		}
	}
	if strings.Contains(launcher, "apply -f \"$out/$pod.yaml\"") {
		t.Fatal("IAM preflight Pod creation must not adopt an existing Pod")
	}
	for _, forbidden := range []string{
		"FENCER_SOURCE_MUTATION_RECEIPT_DIR",
		"FENCER_SOURCE_STAGE_DIR",
		"FENCER_SOURCE_WRITER_CREDENTIALS_FILE",
	} {
		if strings.Contains(manifest, forbidden) {
			t.Errorf("manifest must not contain %q", forbidden)
		}
	}

	for _, input := range []string{
		"RHIZA_NODE_A", "RHIZA_NODE_B", "RHIZA_NODE_C", "RHIZA_KUBECTL_IMAGE",
		"RHIZA_READONLY_WORKLOAD_GSA_ID",
		"RHIZA_RUNTIME_AGENT_CA_SECRET",
		"RHIZA_RUNTIME_AGENT_TLS_SECRET",
		"RHIZA_CONTAINERD_VERSION",
		"RHIZA_CONTAINERD_RUNTIME_TYPE",
		"RHIZA_CGROUP_PROFILE_ID",
		"RHIZA_KERNEL_RELEASE",
		"PINNED_|PLACEHOLDER",
	} {
		if !strings.Contains(launcher, input) {
			t.Errorf("launcher does not validate/render %q", input)
		}
	}
	for _, want := range []string{
		"requiredDuringSchedulingIgnoredDuringExecution", "matchFields", "key: metadata.name",
		"__NODE_A__", "__NODE_B__", "__NODE_C__", "capture_run_scope",
		"get node \"$node\" -o json", "a captured existing node is not Ready",
		"timeout --signal=TERM --kill-after=10m", "RHIZA_GCS_DEADLINE_WRAPPED",
		"cleanup_owned_namespace", "resourceVersion",
	} {
		if !strings.Contains(launcher+manifest+preflight, want) {
			t.Errorf("bounded placement/cleanup contract missing %q", want)
		}
	}
	for _, want := range []string{
		"kind: ClusterRole", "resources: [namespaces]", "resourceNames: [__NAMESPACE__]",
		"verbs: [get, delete]", "kind: ClusterRoleBinding", "kind: Job",
		"activeDeadlineSeconds: 10800", "deadline=__EXPIRY_DEADLINE__", "__NAMESPACE_UID__",
		`preconditions":{"uid":"%s","resourceVersion":"%s"}`, "__NODE_A__", "__NODE_B__", "__NODE_C__", "__KUBECTL_IMAGE__",
	} {
		if !strings.Contains(expiry, want) {
			t.Errorf("run-owned namespace expiry contract missing %q", want)
		}
	}
	if strings.Contains(expiry, "deadline=$(($(date +%s) + 7200))") {
		t.Fatal("namespace expiry must use the fixed render-time deadline across Pod restarts")
	}
	for _, want := range []string{
		"workload_prefix=${RHIZA_GCS_PREFIX}data/",
		"authority_prefix=${RHIZA_GCS_PREFIX}control",
		"__WORKLOAD_PREFIX__",
		"__AUTHORITY_PREFIX__",
	} {
		if !strings.Contains(launcher+manifest, want) {
			t.Errorf("run-prefix isolation contract missing %q", want)
		}
	}
	for _, want := range []string{
		"^# This profile is deliberately opt-in:",
		"hostPID: true",
		"capabilities:",
		"drop: [ALL]",
		"PINNED_RHIZA_FENCER_DEPLOYMENT_UID",
		"--runtime-agent-ca-file=/runtime-agent-ca/ca.crt",
		"RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256",
		"QUALIFIED_SANDBOX_EXECUTABLE_SHA256",
		"sandbox-executable-sha256: \"QUALIFIED_SANDBOX_EXECUTABLE_SHA256\"",
		"rhiza.dev/sandbox-executable-sha256: \"QUALIFIED_SANDBOX_EXECUTABLE_SHA256\"",
		"rhiza.dev/sandbox-executable-sha256: \\\"$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256\\\"",
		"sandbox-args-json: '[\\\"/pause\\\"]'",
		"qualified sandbox executable approval must be 64 lowercase hex characters",
		"${RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL:=no}",
		"RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL must be yes or no",
		"if [ \"$RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL\" = yes ]; then",
		"runtime-agent-passive-kernel-patch.yaml",
		"path: /sys/fs/cgroup",
		"path: /proc",
	} {
		if !strings.Contains(launcher+authority, want) {
			t.Errorf("runtime-agent rendering contract missing %q", want)
		}
	}
	for _, want := range []string{
		"namespace: rhiza",
		"rhiza.mrchypark.dev/runtime-proof-profile: passive-kernel-v1",
		"privileged: false",
		"allowPrivilegeEscalation: false",
		"drop: [ALL]",
		"add: [SYS_PTRACE]",
	} {
		if !strings.Contains(passivePatch, want) {
			t.Errorf("passive-kernel opt-in patch missing %q", want)
		}
	}
	if strings.Contains(passivePatch, "SYS_ADMIN") || strings.Contains(passivePatch, "privileged: true") || strings.Contains(authority, "add: [SYS_PTRACE]") {
		t.Fatal("passive privilege must be explicit, narrow, and absent from the base profile")
	}
	if !strings.Contains(fencingLauncher, "${RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL:-no}") ||
		!strings.Contains(fencingLauncher, "if [ \"$fencing_passive_kernel\" = yes ]; then") ||
		!strings.Contains(fencingLauncher, "runtime-agent-passive-kernel-patch.yaml") {
		t.Fatal("native fencing installer must keep passive-kernel capability default-off and explicit")
	}
}

func assertProbePairPlacement(t *testing.T, manifest string) {
	t.Helper()
	type podAffinityTerm struct {
		LabelSelector struct {
			MatchLabels map[string]string `yaml:"matchLabels"`
		} `yaml:"labelSelector"`
		TopologyKey string `yaml:"topologyKey"`
	}
	type podContract struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Spec struct {
			Affinity struct {
				NodeAffinity any `yaml:"nodeAffinity"`
				PodAffinity  *struct {
					Required []podAffinityTerm `yaml:"requiredDuringSchedulingIgnoredDuringExecution"`
				} `yaml:"podAffinity"`
			} `yaml:"affinity"`
		} `yaml:"spec"`
	}

	pods := make(map[string]podContract, 2)
	decoder := yaml.NewDecoder(strings.NewReader(manifest))
	for {
		var pod podContract
		err := decoder.Decode(&pod)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode inspector probe manifests: %v", err)
		}
		if pod.Kind == "Pod" && (pod.Metadata.Name == "rhiza-inspector" || pod.Metadata.Name == "rhiza-inspector-control") {
			if _, exists := pods[pod.Metadata.Name]; exists {
				t.Fatalf("duplicate probe Pod %q", pod.Metadata.Name)
			}
			pods[pod.Metadata.Name] = pod
		}
	}
	inspector, haveInspector := pods["rhiza-inspector"]
	control, haveControl := pods["rhiza-inspector-control"]
	if !haveInspector || !haveControl {
		t.Fatalf("inspector probe pair is incomplete: inspector=%t control=%t", haveInspector, haveControl)
	}
	assertCapturedNodeNameAffinity(t, "rhiza-inspector", inspector.Spec.Affinity.NodeAffinity)
	assertCapturedNodeNameAffinity(t, "rhiza-inspector-control", control.Spec.Affinity.NodeAffinity)
	if inspector.Spec.Affinity.PodAffinity != nil {
		t.Fatal("inspector Pod must not carry the control's co-location affinity")
	}
	if control.Spec.Affinity.PodAffinity == nil || len(control.Spec.Affinity.PodAffinity.Required) != 1 {
		t.Fatal("control Pod must require co-location with the inspector")
	}
	term := control.Spec.Affinity.PodAffinity.Required[0]
	if term.TopologyKey != "kubernetes.io/hostname" ||
		term.LabelSelector.MatchLabels["chaos.rhiza.io/run"] != "__RUN_ID__" ||
		term.LabelSelector.MatchLabels["app"] != "rhiza-inspector" ||
		len(term.LabelSelector.MatchLabels) != 2 {
		t.Fatalf("control Pod co-location selector is not run-scoped to the inspector: %#v", term)
	}
}

func TestRuntimeAgentRenderPinsNodesAndNamespaceOwnerReferences(t *testing.T) {
	launcher := readContractFile(t, "../run-votercount-gcs-chaos.sh")
	input := filepath.Join(t.TempDir(), "runtime-agent.yaml")
	if err := os.WriteFile(input, []byte("kind: DaemonSet\nmetadata:\n  name: rhiza-runtime-agent\nspec:\n  template:\n    spec:\n      serviceAccountName: rhiza-runtime-agent\n---\nkind: ClusterRole\nmetadata:\n  name: run-runtime-agent\n---\nkind: ClusterRoleBinding\nmetadata:\n  name: run-runtime-agent\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	programs := []struct {
		start, end string
		args       []string
	}{
		{
			start: "  awk -v a=",
			end:   "\n  ' \"$out/runtime-agent.yaml\" > \"$runtime_agent_pinned\"",
			args:  []string{"-v", "a=node-a", "-v", "b=node-b", "-v", "c=node-c"},
		},
		{
			start: "  awk -v ns=",
			end:   "\n  ' \"$runtime_agent_pinned\" > \"$runtime_agent_owned\"",
			args:  []string{"-v", "ns=run-ns", "-v", "uid=namespace-uid"},
		},
	}
	for i, program := range programs {
		start := strings.Index(launcher, program.start)
		if start < 0 {
			t.Fatalf("runtime-agent renderer block %d missing", i)
		}
		quote := strings.Index(launcher[start:], "'\n")
		if quote < 0 {
			t.Fatalf("runtime-agent renderer block %d has no awk program", i)
		}
		bodyStart := start + quote + 2
		end := strings.Index(launcher[bodyStart:], program.end)
		if end < 0 {
			t.Fatalf("runtime-agent renderer block %d terminator missing", i)
		}
		awkProgram := launcher[bodyStart : bodyStart+end]
		args := append([]string{}, program.args...)
		args = append(args, awkProgram, input)
		output, err := exec.Command("awk", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("runtime-agent renderer block %d failed: %v: %s", i, err, output)
		}
		if err := os.WriteFile(input, output, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rendered, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"values: [node-a, node-b, node-c]",
		"kind: ClusterRole\nmetadata:\n  ownerReferences:\n    - apiVersion: v1\n      kind: Namespace\n      name: run-ns\n      uid: namespace-uid\n      blockOwnerDeletion: false\n  name: run-runtime-agent",
		"kind: ClusterRoleBinding\nmetadata:\n  ownerReferences:\n    - apiVersion: v1\n      kind: Namespace\n      name: run-ns\n      uid: namespace-uid\n      blockOwnerDeletion: false\n  name: run-runtime-agent",
	} {
		if !strings.Contains(string(rendered), want) {
			t.Errorf("runtime-agent render missing %q", want)
		}
	}
}

func TestNamespaceExpiryRendersUIDResourceVersionDelete(t *testing.T) {
	manifest := readContractFile(t, "../gcs-namespace-expiry.yaml.in")
	for _, replacement := range [][2]string{
		{"__NAMESPACE__", "rhiza-chaos-abc12345"},
		{"__NAMESPACE_UID__", "namespace-uid-123"},
		{"__RUN_ID__", "abc12345"},
		{"__EXPIRY_DEADLINE__", "1792761600"},
		{"__NODE_A__", "node-a"}, {"__NODE_B__", "node-b"}, {"__NODE_C__", "node-c"},
		{"__KUBECTL_IMAGE__", "registry.example/kubectl@sha256:" + strings.Repeat("c", 64)},
	} {
		manifest = strings.ReplaceAll(manifest, replacement[0], replacement[1])
	}
	for _, want := range []string{
		"kubectl delete --raw /api/v1/namespaces/rhiza-chaos-abc12345 -f -",
		"deadline=1792761600",
		`body=$(printf '{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"%s","resourceVersion":"%s"}}' "$uid" "$rv");`,
		"test \"$uid\" = 'namespace-uid-123'",
		"test \"$run\" = 'abc12345'",
	} {
		if !strings.Contains(manifest, want) {
			t.Errorf("rendered namespace expiry is missing %q", want)
		}
	}
	if strings.Contains(manifest, "--preconditions=") {
		t.Fatal("rendered expiry must not use kubectl's unsupported preconditions flag")
	}
}

func TestVoterCountLauncherRunRequiresPrivilegedInstallCredential(t *testing.T) {
	out := t.TempDir()
	authState := filepath.Join(t.TempDir(), "bootstrap-state")
	command := exec.Command("/bin/sh", "../run-votercount-gcs-chaos.sh", "run")
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"RHIZA_GCS_INDEPENDENT_LAYOUT=yes",
		"RHIZA_GCS_INDEPENDENT_CONSUMER=run-votercount-gcs-chaos/v2",
		"RHIZA_RUN_ID=abc12345",
		"RHIZA_NAMESPACE=rhiza-chaos-abc12345",
		"RHIZA_KUBE_CONTEXT=test-context",
		"RHIZA_GCS_BUCKET=test-bucket",
		"RHIZA_GCS_PREFIX=test/abc12345/",
		"RHIZA_AUTH_STATE=" + authState,
		"RHIZA_CLUSTER_ID=test-cluster",
		"RHIZA_GCP_PROJECT_ID=test-project",
		"RHIZA_DATA_GSA_ID=rhiza-writer",
		"RHIZA_READONLY_WORKLOAD_GSA_ID=rhiza-reader",
		"RHIZA_RUNTIME_AGENT_CA_SECRET=runtime-agent-ca",
		"RHIZA_RUNTIME_AGENT_TLS_SECRET=runtime-agent-tls",
		"RHIZA_CONTAINERD_VERSION=containerd-1.7.1",
		"RHIZA_CONTAINERD_RUNTIME_TYPE=io.containerd.runc.v2",
		"RHIZA_CGROUP_PROFILE_ID=cgroup-v2-default",
		"RHIZA_KERNEL_RELEASE=6.1.0",
		"RHIZA_APP_IMAGE=repo/app@sha256:" + strings.Repeat("a", 64),
		"RHIZA_OPERATOR_IMAGE=repo/operator@sha256:" + strings.Repeat("b", 64),
		"RHIZA_KUBECTL_IMAGE=repo/kubectl@sha256:" + strings.Repeat("c", 64),
		"RHIZA_NODE_A=node-a",
		"RHIZA_NODE_B=node-b",
		"RHIZA_NODE_C=node-c",
		"RHIZA_OUT=" + out,
	}
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("run phase accepted a missing privileged-install credential: %s", output)
	}
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 2 {
		t.Fatalf("run phase exit = %v, want status 2; output: %s", err, output)
	}
	if !strings.Contains(string(output), "missing privileged-install credential") {
		t.Fatalf("run phase failed before the privileged-install credential gate: %s", output)
	}
}

func TestMixedPendingLauncherSharesAbsoluteBudgetAndSerializesCleanup(t *testing.T) {
	launcher := readContractFile(t, "../run-votercount-gcs-chaos.sh")
	for _, want := range []string{
		"mixed_pending=${RHIZA_GCS_MIXED_PENDING:-no}",
		"mixed_deadline_file=$out/gcs-mixed-deadlines.txt",
		"[ \"$(wc -l < \"$mixed_deadline_file\" | tr -d ' ')\" = 5 ]",
		"mixed_hard_deadline=$((mixed_start + 3300))",
		"mixed_work_deadline=$((mixed_hard_deadline - 300))",
		"mixed_input_work=${FENCING_E2E_MIXED_WORK_DEADLINE_UNIX:-}",
		"mixed_input_hard=${FENCING_E2E_MIXED_HARD_DEADLINE_UNIX:-}",
		"ln \"$mixed_deadline_tmp\" \"$mixed_deadline_file\"",
		"mixed-run deadline receipt already exists; refusing to reset its budget",
		"export FENCING_E2E_MIXED_WORK_DEADLINE_UNIX=$mixed_work_deadline",
		"export FENCING_E2E_MIXED_HARD_DEADLINE_UNIX=$mixed_hard_deadline",
		"export RHIZA_GCS_MIXED_SUPERVISED_CHILD=yes",
		"[ \"$mixed_supervised_child\" != yes ]",
		"timeout --signal=TERM --kill-after=\"${mixed_term_grace}s\" \"${mixed_work_remaining}s\"",
		"wait \"$mixed_supervisor_pid\" || mixed_supervisor_status=$?",
		"timeout --signal=TERM --kill-after=1s \"${cleanup_request_limit}s\" kubectl",
		"FENCING_E2E_MIXED_WORK_DEADLINE_UNIX=\"$mixed_work_deadline\"",
		"FENCING_E2E_MIXED_HARD_DEADLINE_UNIX=\"$mixed_hard_deadline\"",
		"RHIZA_GCS_HA_SCENARIO=mixed-pending",
		"[ \"$RHIZA_CLUSTER_ID\" != chaos-source ]",
		"mixed-pending remove gate source did not render exactly once into the voter template",
		"\"$FENCING_E2E_BIN\" run-gcs-ha-mixed-pending",
	} {
		if !strings.Contains(launcher, want) {
			t.Errorf("mixed-pending wrapper is missing %q", want)
		}
	}
	if strings.Index(launcher, "mixed_start=$(date +%s)") > strings.Index(launcher, "  verify_bootstrap_receipt || exit 2\n  capture_run_scope") {
		t.Fatal("mixed absolute deadline must be fixed before install preparation begins")
	}
	if strings.Index(launcher, "wait \"$mixed_supervisor_pid\"") > strings.Index(launcher, "cleanup_namespace=true\n  fi\n  exit \"$mixed_supervisor_status\"") {
		t.Fatal("parent namespace cleanup must begin only after the owned child is reaped")
	}
}

func TestMixedPendingTemplateGateIsDisarmedByDefaultAndPinnedForMixedSelection(t *testing.T) {
	manifest := readContractFile(t, "../votercount_gcs_chaos.yaml.in")
	launcher := readContractFile(t, "../run-votercount-gcs-chaos.sh")
	const marker = `{name: RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE, value: ""}`
	const mixedSource = `{name: RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE, value: "chaos-source"}`
	if strings.Count(manifest, marker) != 1 {
		t.Fatalf("source StatefulSet template has %d default-disabled REMOVE gate markers, want one", strings.Count(manifest, marker))
	}
	if !strings.Contains(launcher, "if [ \"$mixed_pending\" = yes ]; then") ||
		!strings.Contains(launcher, "sed -e 's|"+marker+"|"+mixedSource+"|'") ||
		!strings.Contains(launcher, "RHIZA_GCS_HA_SCENARIO=mixed-pending") {
		t.Fatal("mixed selector is not wired through template rendering and the driver scenario selector")
	}
	if got := templateRemoveGateSource(t, manifest); got != "" {
		t.Fatalf("non-mixed source template enables REMOVE gate with %q", got)
	}
	mixedManifest := strings.Replace(manifest, marker, mixedSource, 1)
	if got := templateRemoveGateSource(t, mixedManifest); got != "chaos-source" {
		t.Fatalf("mixed source template REMOVE gate source=%q, want chaos-source", got)
	}
	if strings.Count(mixedManifest, "name: RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE") != 1 {
		t.Fatal("mixed selector did not render exactly one REMOVE gate source into the StatefulSet Pod template")
	}
}

func templateRemoveGateSource(t *testing.T, manifest string) string {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(manifest))
	for {
		var document map[string]any
		err := decoder.Decode(&document)
		if err == io.EOF {
			return ""
		}
		if err != nil {
			t.Fatalf("decode voter template YAML: %v", err)
		}
		if document["kind"] != "StatefulSet" {
			continue
		}
		spec, _ := document["spec"].(map[string]any)
		podTemplate, _ := spec["template"].(map[string]any)
		podSpec, _ := podTemplate["spec"].(map[string]any)
		containers, _ := podSpec["containers"].([]any)
		for _, rawContainer := range containers {
			container, _ := rawContainer.(map[string]any)
			if container["name"] != "rhiza" {
				continue
			}
			env, _ := container["env"].([]any)
			var value string
			matches := 0
			for _, rawEntry := range env {
				entry, _ := rawEntry.(map[string]any)
				if entry["name"] == "RHIZA_QUALIFICATION_REMOVE_GATE_SOURCE" {
					value, _ = entry["value"].(string)
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("voter template has %d REMOVE gate source env entries, want one", matches)
			}
			return value
		}
		t.Fatal("StatefulSet template has no rhiza source container")
	}
}

func TestMixedPendingShellCancellationReapsBeforeOwnedCleanup(t *testing.T) {
	timeoutPath, err := exec.LookPath("timeout")
	if err != nil {
		t.Fatal("GNU timeout is required for the mixed wrapper cancellation contract")
	}
	for _, scenario := range []struct {
		name           string
		hung           bool
		expired        bool
		unsafeOut      bool
		uidDrift       bool
		deleteFail     bool
		wantDelete     bool
		externalSignal os.Signal
	}{
		{name: "term-cleanup", wantDelete: true},
		{name: "hung-child", hung: true, wantDelete: true},
		{name: "expired-work-window", expired: true, wantDelete: true},
		{name: "public-output-with-private-child", unsafeOut: true},
		{name: "namespace-uid-drift", uidDrift: true},
		{name: "namespace-delete-failure", deleteFail: true, wantDelete: true},
		{name: "external-sigint", wantDelete: true, externalSignal: os.Interrupt},
		{name: "external-sigterm", wantDelete: true, externalSignal: syscall.SIGTERM},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			root := t.TempDir()
			out := filepath.Join(root, "out")
			authState := filepath.Join(root, "auth")
			fakeBin := filepath.Join(root, "bin")
			for _, dir := range []string{out, authState, fakeBin} {
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			const runID = "abc12345"
			const namespace = "rhiza-chaos-abc12345"
			const prefix = "test/abc12345/"
			const uid = "11111111-2222-3333-4444-555555555555"
			receipt := map[string]any{
				"run": runID, "namespace": namespace, "parentPrefix": prefix, "controlPrefix": prefix + "control",
				"workload": map[string]any{
					"ksa":                     map[string]any{"name": "rhiza-workload"},
					"gsa":                     map[string]any{"email": "reader@test-project.iam.gserviceaccount.com", "role": "roles/storage.objectViewer", "managedFolder": prefix + "data/"},
					"workloadIdentityBinding": map[string]any{"members": []string{"member"}},
				},
				"writer": map[string]any{
					"gsa":                     map[string]any{"email": "writer@test-project.iam.gserviceaccount.com", "role": "projects/test-project/roles/rhiza19_abc12345_objects", "managedFolders": []string{prefix + "data/", prefix + "control/"}},
					"ksas":                    []map[string]any{{"name": "rhiza-fencer"}, {"name": "rhiza-operator"}},
					"workloadIdentityBinding": map[string]any{"members": []string{"one", "two"}},
				},
			}
			receiptJSON, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			receiptPath := filepath.Join(authState, "workload-identity.json")
			writePrivateTestFile(t, receiptPath, append(receiptJSON, '\n'))
			receiptHash := sha256.Sum256(append(receiptJSON, '\n'))
			writePrivateTestFile(t, filepath.Join(authState, "workload-identity.sha256"), []byte(hex.EncodeToString(receiptHash[:])+"\n"))
			writePrivateTestFile(t, filepath.Join(out, "admin-token"), []byte("test-only-token-value-long-enough\n"))
			writePrivateTestFile(t, filepath.Join(out, "gcs-namespace-uid"), []byte(uid+"\n"))
			writePrivateTestFile(t, filepath.Join(out, "gcs-existing-nodes"), []byte("node-a\nnode-b\nnode-c\n"))
			writePrivateTestFile(t, filepath.Join(out, "gcs-iam-preflight.ok"), []byte(runID+" "+hex.EncodeToString(receiptHash[:])+"\n"))
			if scenario.unsafeOut {
				if err := os.Mkdir(filepath.Join(out, "private-child"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(out, 0o777); err != nil {
					t.Fatal(err)
				}
			}
			deadlineStart := time.Now().Unix()
			workDeadline := deadlineStart + 8
			hardDeadline := workDeadline + 122
			if scenario.expired {
				workDeadline = deadlineStart - 1
				hardDeadline = deadlineStart + 119
			}
			writePrivateTestFile(t, filepath.Join(out, "gcs-mixed-deadlines.txt"), []byte(strconv.FormatInt(workDeadline, 10)+"\n"+strconv.FormatInt(hardDeadline, 10)+"\n"+runID+"\n"+namespace+"\n"+strconv.FormatInt(deadlineStart, 10)+"\n"))

			collectorStarted := filepath.Join(root, "collector.started")
			collectorTerminated := filepath.Join(root, "collector.term")
			collectorDeadline := filepath.Join(root, "collector.deadlines")
			collectorPID := filepath.Join(root, "collector.pid")
			kubectlDeleted := filepath.Join(root, "namespace.deleted")
			kubectlDeleteAttempt := filepath.Join(root, "namespace.delete-attempt")
			kubectlOrder := filepath.Join(root, "namespace.delete-order")
			kubectl := `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_KUBECTL_LOG"
case " $* " in
  *" get namespace $RHIZA_NAMESPACE "*)
    case " $* " in
      *" -o name "*)
        [ -e "$FAKE_NAMESPACE_DELETED" ] || printf 'namespace/%s\n' "$RHIZA_NAMESPACE"
        exit 0
        ;;
    esac
    fake_uid=11111111-2222-3333-4444-555555555555
    if [ "$FAKE_UID_DRIFT" = yes ] && [ -e "$FAKE_COLLECTOR_STARTED" ]; then fake_uid=99999999-2222-3333-4444-555555555555; fi
    printf '{"metadata":{"name":"%s","uid":"%s","resourceVersion":"17","labels":{"chaos.rhiza.io/run":"%s"}}}\n' "$RHIZA_NAMESPACE" "$fake_uid" "$RHIZA_RUN_ID"
    ;;
  *" delete --raw /api/v1/namespaces/"*)
    : > "$FAKE_DELETE_ATTEMPT"
    if [ -f "$FAKE_COLLECTOR_PIDFILE" ]; then
      child_pid=$(cat "$FAKE_COLLECTOR_PIDFILE") || exit 73
      if kill -0 "$child_pid" 2>/dev/null; then exit 74; fi
    fi
    : > "$FAKE_DELETE_ORDER"
    [ "$FAKE_DELETE_FAIL" != yes ] || exit 1
    : > "$FAKE_NAMESPACE_DELETED"
    ;;
  *) exit 90 ;;
esac
`
			kubectlPath := filepath.Join(fakeBin, "kubectl")
			writePrivateTestFile(t, kubectlPath, []byte(kubectl))
			if err := os.Chmod(kubectlPath, 0o700); err != nil {
				t.Fatal(err)
			}
			collector := `#!/bin/sh
printf '%s\n' "$$" > "$FAKE_COLLECTOR_PIDFILE"
printf '%s\n' "$FENCING_E2E_MIXED_WORK_DEADLINE_UNIX $FENCING_E2E_MIXED_HARD_DEADLINE_UNIX" > "$FAKE_COLLECTOR_DEADLINES"
if [ "$FAKE_HUNG_CHILD" = yes ]; then
  trap '' TERM INT
else
  trap ': > "$FAKE_COLLECTOR_TERM"; exit 143' TERM
  trap ': > "$FAKE_COLLECTOR_TERM"; exit 130' INT
fi
: > "$FAKE_COLLECTOR_STARTED"
while :; do sleep 1; done
`
			readyIndex := strings.Index(collector, `: > "$FAKE_COLLECTOR_STARTED"`)
			termTrapIndex := strings.Index(collector, `trap ': > "$FAKE_COLLECTOR_TERM"; exit 143' TERM`)
			intTrapIndex := strings.Index(collector, `trap ': > "$FAKE_COLLECTOR_TERM"; exit 130' INT`)
			if readyIndex < 0 || termTrapIndex < 0 || intTrapIndex < 0 || readyIndex < termTrapIndex || readyIndex < intTrapIndex {
				t.Fatal("collector readiness must be published after both signal traps are installed")
			}
			collectorPath := filepath.Join(root, "fencing-e2e")
			writePrivateTestFile(t, collectorPath, []byte(collector))
			if err := os.Chmod(collectorPath, 0o700); err != nil {
				t.Fatal(err)
			}
			kubectlLog := filepath.Join(root, "kubectl.log")
			env := map[string]string{
				"PATH":                         fakeBin + string(os.PathListSeparator) + filepath.Dir(timeoutPath) + string(os.PathListSeparator) + os.Getenv("PATH"),
				"RHIZA_GCS_INDEPENDENT_LAYOUT": "yes", "RHIZA_GCS_INDEPENDENT_CONSUMER": "run-votercount-gcs-chaos/v2",
				"RHIZA_RUN_ID": runID, "RHIZA_NAMESPACE": namespace, "RHIZA_KUBE_CONTEXT": "test-context",
				"RHIZA_GCS_BUCKET": "test-bucket", "RHIZA_GCS_PREFIX": prefix, "RHIZA_AUTH_STATE": authState,
				"RHIZA_CLUSTER_ID": "chaos-source", "RHIZA_GCP_PROJECT_ID": "test-project",
				"RHIZA_DATA_GSA_ID": "writer", "RHIZA_READONLY_WORKLOAD_GSA_ID": "reader",
				"RHIZA_RUNTIME_AGENT_CA_SECRET": "runtime-agent-ca", "RHIZA_RUNTIME_AGENT_TLS_SECRET": "runtime-agent-tls",
				"RHIZA_CONTAINERD_VERSION": "containerd-1.7.1", "RHIZA_CONTAINERD_RUNTIME_TYPE": "io.containerd.runc.v2",
				"RHIZA_CGROUP_PROFILE_ID": "cgroup-v2-default", "RHIZA_KERNEL_RELEASE": "6.1.0",
				"RHIZA_APP_IMAGE":      "repo/app@sha256:" + strings.Repeat("a", 64),
				"RHIZA_OPERATOR_IMAGE": "repo/operator@sha256:" + strings.Repeat("b", 64),
				"RHIZA_KUBECTL_IMAGE":  "repo/kubectl@sha256:" + strings.Repeat("c", 64),
				"RHIZA_NODE_A":         "node-a", "RHIZA_NODE_B": "node-b", "RHIZA_NODE_C": "node-c",
				"RHIZA_OUT": out, "RHIZA_GCS_MIXED_PENDING": "yes", "FENCING_E2E_BIN": collectorPath,
				"FAKE_KUBECTL_LOG": kubectlLog, "FAKE_NAMESPACE_DELETED": kubectlDeleted,
				"FAKE_DELETE_ATTEMPT": kubectlDeleteAttempt, "FAKE_DELETE_ORDER": kubectlOrder,
				"FAKE_COLLECTOR_STARTED": collectorStarted, "FAKE_COLLECTOR_TERM": collectorTerminated,
				"FAKE_COLLECTOR_DEADLINES": collectorDeadline, "FAKE_COLLECTOR_PIDFILE": collectorPID,
				"FAKE_HUNG_CHILD":  map[bool]string{true: "yes", false: "no"}[scenario.hung],
				"FAKE_UID_DRIFT":   map[bool]string{true: "yes", false: "no"}[scenario.uidDrift],
				"FAKE_DELETE_FAIL": map[bool]string{true: "yes", false: "no"}[scenario.deleteFail],
			}
			command := exec.Command("/bin/sh", "../run-votercount-gcs-chaos.sh", "run")
			command.Dir = "."
			command.Env = envMapToSlice(env)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitResult := make(chan error, 1)
			go func() { waitResult <- command.Wait() }()
			var runErr error
			if scenario.externalSignal != nil {
				readinessDeadline := time.Now().Add(5 * time.Second)
				for {
					if _, err := os.Stat(collectorStarted); err == nil {
						break
					}
					if time.Now().After(readinessDeadline) {
						_ = command.Process.Kill()
						<-waitResult
						t.Fatalf("collector did not start before delivering %v; output: %s", scenario.externalSignal, output.String())
					}
					time.Sleep(10 * time.Millisecond)
				}
				if err := command.Process.Signal(scenario.externalSignal); err != nil {
					_ = command.Process.Kill()
					<-waitResult
					t.Fatalf("deliver external %v to wrapper: %v", scenario.externalSignal, err)
				}
				select {
				case runErr = <-waitResult:
				case <-time.After(15 * time.Second):
					_ = command.Process.Kill()
					<-waitResult
					t.Fatalf("wrapper did not reap and clean up after external %v; output: %s", scenario.externalSignal, output.String())
				}
			} else {
				runErr = <-waitResult
			}
			if runErr == nil {
				t.Fatalf("canceled launcher unexpectedly passed: %s", output.String())
			}
			if exit, ok := runErr.(*exec.ExitError); !ok || exit.ExitCode() == 0 {
				t.Fatalf("canceled launcher exit = %v; output: %s", runErr, output.String())
			}
			assertFileExists := func(path string, want bool) {
				t.Helper()
				_, err := os.Stat(path)
				if want && err != nil {
					t.Fatalf("expected %s: %v; output: %s", path, err, output.String())
				}
				if !want && !os.IsNotExist(err) {
					t.Fatalf("unexpected %s (stat err %v); output: %s", path, err, output.String())
				}
			}
			assertFileExists(collectorStarted, !scenario.expired && !scenario.unsafeOut)
			assertFileExists(kubectlDeleteAttempt, scenario.wantDelete || scenario.deleteFail)
			assertFileExists(kubectlDeleted, scenario.wantDelete && !scenario.deleteFail)
			if scenario.uidDrift {
				assertFileExists(kubectlDeleteAttempt, false)
			}
			if scenario.wantDelete || scenario.deleteFail {
				assertFileExists(kubectlOrder, true)
			}
			if !scenario.hung && !scenario.expired && !scenario.unsafeOut {
				assertFileExists(collectorTerminated, true)
			}
			if !scenario.expired && !scenario.unsafeOut {
				deadlineBytes, err := os.ReadFile(collectorDeadline)
				if err != nil || string(deadlineBytes) != strconv.FormatInt(workDeadline, 10)+" "+strconv.FormatInt(hardDeadline, 10)+"\n" {
					t.Fatalf("collector deadline pair = %q, err %v; want original absolute receipt pair", deadlineBytes, err)
				}
				pidBytes, err := os.ReadFile(collectorPID)
				if err != nil {
					t.Fatal(err)
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
				if err != nil {
					t.Fatalf("collector pid is invalid: %v", err)
				}
				if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
					t.Fatalf("collector process %d survived launcher cleanup", pid)
				}
			}
			cleanupStatus, err := os.ReadFile(filepath.Join(out, "namespace-cleanup.status"))
			if scenario.wantDelete && !scenario.deleteFail {
				if err != nil || !strings.Contains(string(cleanupStatus), "namespace-absence-observed") {
					t.Fatalf("successful cleanup was not observed: %q, err %v", cleanupStatus, err)
				}
			} else if scenario.uidDrift || scenario.deleteFail {
				if err != nil || !strings.Contains(string(cleanupStatus), "local-delete-unavailable-or-not-observed") {
					t.Fatalf("failed cleanup was reported as verified: %q, err %v", cleanupStatus, err)
				}
			}
		})
	}
}

func writePrivateTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func envMapToSlice(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}

func TestIAMProbePodIdentityRejectsPrivilegeMutation(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Fatal("jq is required for the launcher and this exact identity-filter regression")
	}
	launcher := readContractFile(t, "../run-votercount-gcs-chaos.sh")
	start := strings.Index(launcher, "    select(.metadata.name==$pod")
	if start < 0 {
		t.Fatal("launcher Pod identity filter not found")
	}
	endOffset := strings.Index(launcher[start:], "\n  ' \"$object\"")
	if endOffset < 0 {
		t.Fatal("launcher Pod identity filter terminator not found")
	}
	filter := strings.TrimSpace(launcher[start : start+endOffset])

	base := map[string]any{
		"metadata": map[string]any{
			"name": "probe", "namespace": "run-ns", "uid": "pod-uid",
			"labels":      map[string]any{"chaos.rhiza.io/run": "1234abcd"},
			"annotations": map[string]any{"chaos.rhiza.io/owner": "gcs-iam-preflight"},
		},
		"spec": map[string]any{
			"serviceAccountName": "rhiza-workload", "automountServiceAccountToken": false,
			"restartPolicy": "Never",
			"affinity": map[string]any{"nodeAffinity": map[string]any{
				"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
					"nodeSelectorTerms": []any{map[string]any{"matchFields": []any{map[string]any{
						"key": "metadata.name", "operator": "In", "values": []any{"node-a", "node-b", "node-c"},
					}}}},
				},
			}},
			"securityContext": map[string]any{
				"runAsNonRoot": true, "runAsUser": 65532,
				"seccompProfile": map[string]any{"type": "RuntimeDefault"},
			},
			"volumes": []any{}, "initContainers": []any{},
			"containers": []any{map[string]any{
				"name": "probe", "image": "example.invalid/rhiza@sha256:abc",
				"command": []any{"/usr/local/bin/rhiza-entrypoint", "qualification-gcs-iam-probe", "reader"},
				"env": []any{
					map[string]any{"name": "RHIZA_GCS_BUCKET", "value": "bucket"},
					map[string]any{"name": "RHIZA_GCS_PREFIX", "value": "qual/1234abcd/data/"},
					map[string]any{"name": "RHIZA_RUN_ID", "value": "1234abcd"},
					map[string]any{"name": "RHIZA_GCS_IAM_PROBE_NONCE", "value": strings.Repeat("b", 32)},
					map[string]any{"name": "RHIZA_GCS_IAM_PROBE_MANIFEST_GENERATION", "value": "6"},
					map[string]any{"name": "RHIZA_GCS_IAM_PROBE_CREATE_GENERATION", "value": "0"},
					map[string]any{"name": "RHIZA_GCS_IAM_PROBE_UPDATE_GENERATION", "value": "0"},
					map[string]any{"name": "RHIZA_GCS_IAM_PROBE_EXPECTED_GSA", "value": "rhiza-reader@project.iam.gserviceaccount.com"},
				},
				"securityContext": map[string]any{
					"runAsNonRoot": true, "readOnlyRootFilesystem": true,
					"allowPrivilegeEscalation": false,
					"capabilities":             map[string]any{"drop": []any{"ALL"}},
				},
			}},
		},
	}
	args := []string{"-er", "--arg", "pod", "probe", "--arg", "ns", "run-ns", "--arg", "run", "1234abcd",
		"--arg", "node_a", "node-a", "--arg", "node_b", "node-b", "--arg", "node_c", "node-c",
		"--arg", "ksa", "rhiza-workload", "--arg", "image", "example.invalid/rhiza@sha256:abc",
		"--arg", "mode", "reader", "--arg", "bucket", "bucket", "--arg", "prefix", "qual/1234abcd/data/",
		"--arg", "nonce", strings.Repeat("b", 32), "--arg", "manifest", "6", "--arg", "create", "0",
		"--arg", "update", "0", "--arg", "gsa", "rhiza-reader@project.iam.gserviceaccount.com", filter}
	runFilter := func(pod map[string]any) error {
		data, err := json.Marshal(pod)
		if err != nil {
			return err
		}
		command := exec.Command(jq, args...)
		command.Stdin = bytes.NewReader(data)
		return command.Run()
	}
	clone := func(value map[string]any) map[string]any {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if err := runFilter(base); err != nil {
		t.Fatalf("launcher rejected expected probe Pod: %v", err)
	}
	for _, mutate := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"hostNetwork", func(p map[string]any) { p["spec"].(map[string]any)["hostNetwork"] = true }},
		{"hostPID", func(p map[string]any) { p["spec"].(map[string]any)["hostPID"] = true }},
		{"hostIPC", func(p map[string]any) { p["spec"].(map[string]any)["hostIPC"] = true }},
		{"shareProcessNamespace", func(p map[string]any) { p["spec"].(map[string]any)["shareProcessNamespace"] = true }},
		{"ephemeralContainer", func(p map[string]any) {
			p["spec"].(map[string]any)["ephemeralContainers"] = []any{map[string]any{"name": "debug"}}
		}},
		{"extraVolume", func(p map[string]any) {
			p["spec"].(map[string]any)["volumes"] = []any{map[string]any{"name": "host", "hostPath": map[string]any{"path": "/"}}}
		}},
		{"extraContainer", func(p map[string]any) {
			p["spec"].(map[string]any)["containers"] = append(p["spec"].(map[string]any)["containers"].([]any), map[string]any{"name": "sidecar"})
		}},
		{"privileged", func(p map[string]any) {
			p["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["securityContext"].(map[string]any)["privileged"] = true
		}},
		{"addedCapability", func(p map[string]any) {
			p["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["securityContext"].(map[string]any)["capabilities"].(map[string]any)["add"] = []any{"SYS_ADMIN"}
		}},
		{"missingDropAll", func(p map[string]any) {
			p["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["securityContext"].(map[string]any)["capabilities"].(map[string]any)["drop"] = []any{}
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			pod := clone(base)
			mutate.edit(pod)
			if err := runFilter(pod); err == nil {
				t.Fatal("mutated Pod retained a trusted preflight identity")
			}
		})
	}
}
