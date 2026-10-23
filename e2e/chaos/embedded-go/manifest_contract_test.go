package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func readContractFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
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
	authority := readContractFile(t, "../../../deploy/operator/fencing/authority.yaml")
	passivePatch := readContractFile(t, "../../../deploy/operator/fencing/runtime-agent-passive-kernel-patch.yaml")
	fencingLauncher := readContractFile(t, "../../../e2e/fencing/run.sh")
	probe := readContractFile(t, "gcs_reader_probe.go")
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
		"rhiza.mrchypark.dev/cooperative-fencing: \"1\"",
		"FENCER_SOURCE_MUTATION_RECEIPT_DIR",
		"FENCER_SOURCE_STAGE_DIR",
		"FENCER_SOURCE_WRITER_CREDENTIALS_FILE",
	} {
		if strings.Contains(manifest, forbidden) {
			t.Errorf("manifest must not contain %q", forbidden)
		}
	}

	for _, input := range []string{
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
		"rhiza.dev/sandbox-executable-sha256: \"$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256\"",
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
