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

func TestIndependentGCSChaosManifestContract(t *testing.T) {
	manifest := readContractFile(t, "../votercount_gcs_chaos.yaml.in")
	launcher := readContractFile(t, "../run-votercount-gcs-chaos.sh")
	preflight := readContractFile(t, "../gcs-iam-preflight.yaml.in")
	authority := readContractFile(t, "../../../deploy/operator/fencing/authority.yaml")
	probe := readContractFile(t, "gcs_reader_probe.go")

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
		"PINNED_RHIZA_FENCER_DEPLOYMENT_UID",
		"--runtime-agent-ca-file=/runtime-agent-ca/ca.crt",
		"path: /sys/fs/cgroup",
		"path: /proc",
	} {
		if !strings.Contains(launcher+authority, want) {
			t.Errorf("runtime-agent rendering contract missing %q", want)
		}
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
