package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestPublicImageEvidenceOmitsCredentialCanaries(t *testing.T) {
	const canary = "fixture-secret-canary-never-publish"
	runtime := obj{"status": obj{"id": "container-id", "imageRef": "image-ref", "labels": obj{"io.kubernetes.pod.uid": "pod-uid", "private": canary}}, "info": obj{"runtimeSpec": obj{"process": obj{"env": []string{"TOKEN=" + canary}, "args": []string{canary}}}}}
	cri := obj{"status": obj{"id": "config-id", "repoDigests": []string{"repo-digest"}}, "info": obj{"imageSpec": obj{"config": obj{"Env": []string{canary}}}}}
	if data := encoded(publicRuntime(runtime)); bytes.Contains(data, []byte(canary)) || !bytes.Contains(data, []byte("container-id")) {
		t.Fatal("runtime projection leaked or lost identity")
	}
	if data := encoded(publicCRIImage(cri)); bytes.Contains(data, []byte(canary)) || !bytes.Contains(data, []byte("config-id")) {
		t.Fatal("CRI projection leaked or lost image identity")
	}
	descriptor := publicDescriptor(encoded(obj{"config": obj{"digest": "config-digest", "Env": canary}, "annotations": obj{"secret": canary}}))
	if bytes.Contains(encoded(descriptor), []byte(canary)) {
		t.Fatal("descriptor projection leaked unknown content")
	}
	data, err := safeJSON("recovered-image-evidence.json", encoded([]obj{{"pod": "pod", "uid": "pod-uid", "container": runtime, "private": canary, "resolution": []obj{{"cri": cri, "content": canary}}}}))
	if err != nil || bytes.Contains(data, []byte(canary)) || !bytes.Contains(data, []byte("container-id")) {
		t.Fatalf("image packet projection failed: %v", err)
	}
}

func TestPublicPacketExcludesRawDiagnosticsAndSecretFields(t *testing.T) {
	const canary = "fixture-secret-canary-never-publish"
	privateAgentLog := "2026/10/22 00:00:00 runtime-agent enrollment rejected stage=cri_enroll code=containerd_verbose_schema_mismatch\n" +
		"2026/10/22 00:00:01 runtime-agent enrollment rejected stage=cri_enroll code=containerd_verbose_schema_mismatch private=" + canary + "\n"
	agentLogDigest := sha256.Sum256([]byte(privateAgentLog))
	raw := t.TempDir()
	write := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("source-head.txt", []byte(strings.Repeat("a", 40)+"\n"))
	write("actual-exit.txt", []byte("0\n"))
	write("image-ids.txt", []byte("sha256:"+strings.Repeat("b", 64)+"\n"))
	write("runtime-profile.json", encoded(runtimeProfileEvidence{
		Qualification: "runtime-agent-component-only", KindNodeImage: "kindest/node:v1@sha256:" + strings.Repeat("c", 64),
		RuntimeName: "containerd", RuntimeVersion: "test", RuntimeAPIVersion: "v1", RuntimeType: "io.containerd.runc.v2",
		KernelRelease: "test-kernel", CgroupFilesystem: "cgroup2fs", CgroupProfileID: "kind-ci-cgroup-v2-component-v1",
	}))
	write("runtime-component-probe.json", encoded(obj{
		"qualification": "runtime-agent-component-only", "outcome": "passed", "enrollmentCaptured": true,
		"containerStopAcknowledged": true, "sandboxStopAcknowledged": true, "containerExited": true,
		"sandboxNotReady": true, "cgroupEmpty": true, "receiptVerified": true,
		"agentRestarted": true, "receiptRecovered": true, "receiptSHA256": strings.Repeat("d", 64), "token": canary,
	}))
	write("fencer-diagnostics.json", encoded(obj{"context": "kind-rhiza-generic-fencing", "namespace": "rhiza-fencing-e2e", "podsCaptured": true, "eventsCaptured": true,
		"logs":         obj{"captured": true, "sha256": strings.Repeat("e", 64), "bytes": 123, "lines": 4},
		"runtimeAgent": obj{"podName": "rhiza-runtime-agent-abc", "podUID": "runtime-agent-pod-uid", "logs": obj{"captured": true, "sha256": hex.EncodeToString(agentLogDigest[:]), "bytes": len([]byte(privateAgentLog)), "lines": 2}, "rawLog": canary}, "token": canary}))
	write("fencer-pods.json", encoded(obj{"pods": []obj{{"name": "rhiza-fencer-abc", "uid": "pod-uid", "phase": "Pending", "conditions": []obj{{"type": "Ready", "status": "False", "reason": "ContainersNotReady", "message": canary}}, "containers": []obj{{"name": "fencer", "ready": false, "restartCount": 0, "state": obj{"waiting": obj{"reason": "CrashLoopBackOff", "message": canary}}, "private": canary}}}}, "spec": obj{"env": canary}}))
	write("fencer-events.json", encoded(obj{"warnings": []obj{{"type": "Warning", "reason": "FailedMount", "count": 2, "lastTimestamp": "2026-10-22T00:00:00Z", "involvedObject": obj{"kind": "Pod", "name": "rhiza-fencer-abc", "uid": canary}, "source": obj{"component": "kubelet"}, "message": canary}}}))
	write("fencer-deployment.json", encoded(obj{"name": "rhiza-fencer", "uid": "deployment-uid", "generation": 2, "observedGeneration": 1, "replicas": 1, "updatedReplicas": 0, "readyReplicas": 0, "availableReplicas": 0, "conditions": []obj{{"type": "Available", "status": "False", "reason": "MinimumReplicasUnavailable", "message": canary}}}))
	write("cleanup.log", []byte("Deleted nodes: [\"rhiza-generic-fencing-control-plane\"]\n"+canary))
	write("terminal.json", encoded(obj{"items": []obj{{"kind": "Pod", "metadata": obj{"name": "pod", "uid": "uid", "annotations": obj{"kubectl.kubernetes.io/last-applied-configuration": canary}}, "spec": obj{"containers": []obj{{"env": []obj{{"name": "TOKEN", "value": canary}}}}}, "status": obj{"phase": "Running"}}, {"kind": "Secret", "metadata": obj{"name": "credential", "uid": "credential-uid"}, "data": obj{"token": canary}, "stringData": obj{"token": canary}}}}))
	write("events.jsonl", append(encoded(obj{"event": "PASS", "sourceACKRows": 3, "oldRuntimesGone": true, "error": canary, "env": canary}), '\n'))
	write("pods.json", []byte(canary))
	write("resources.yaml", []byte(canary))
	if err := os.Mkdir(filepath.Join(raw, "kind"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(raw, "kind", "raw.log"), []byte(canary), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(raw, "private-fencer-diagnostics"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(raw, "private-fencer-diagnostics", "authority-current.log"), []byte(canary), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(raw, "private-fencer-diagnostics", "runtime-agent-current.log"), []byte(privateAgentLog), 0600); err != nil {
		t.Fatal(err)
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(raw, "public"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(raw, "public", entry.Name()))
		if err != nil || bytes.Contains(data, []byte(canary)) {
			t.Fatalf("public artifact %s leaked: %v", entry.Name(), err)
		}
	}
	if _, err := os.Stat(filepath.Join(raw, "public", "pods.json")); !os.IsNotExist(err) {
		t.Fatal("raw Pod dump entered public packet")
	}
	for _, name := range []string{"runtime-profile.json", "runtime-component-probe.json"} {
		data, err := os.ReadFile(filepath.Join(raw, "public", name))
		if err != nil || bytes.Contains(data, []byte(canary)) {
			t.Fatalf("runtime evidence projection failed or leaked: %s: %v", name, err)
		}
	}
	var diagnostics fencerDiagnosticsView
	diagnosticsData, err := os.ReadFile(filepath.Join(raw, "public", "fencer-diagnostics.json"))
	if err != nil || json.Unmarshal(diagnosticsData, &diagnostics) != nil || !diagnostics.RuntimeAgent.Logs.Captured || diagnostics.RuntimeAgent.Logs.SHA256 != hex.EncodeToString(agentLogDigest[:]) ||
		len(diagnostics.RuntimeAgent.Diagnostics) != 1 || diagnostics.RuntimeAgent.Diagnostics[0] != (runtimeAgentEnrollmentDiagnostic{Stage: "cri_enroll", Code: "containerd_verbose_schema_mismatch"}) {
		t.Fatalf("runtime-agent log hash projection failed: %v", err)
	}
	var resources []obj
	data, err := os.ReadFile(filepath.Join(raw, "public", "resources-summary.json"))
	if err != nil || json.Unmarshal(data, &resources) != nil || len(resources) != 2 || resources[0]["uid"] != "uid" {
		t.Fatal("resource summary lost audit identity")
	}
}

func TestRuntimeAgentDiagnosticsRejectUnallowlistedOrExcessivePairs(t *testing.T) {
	base := fencerDiagnosticsView{}
	base.RuntimeAgent.PodName = "rhiza-runtime-agent-abc"
	base.RuntimeAgent.PodUID = "runtime-agent-pod-uid"
	base.RuntimeAgent.Logs.Captured = true
	base.RuntimeAgent.Logs.SHA256 = strings.Repeat("a", 64)
	for _, diagnostics := range [][]runtimeAgentEnrollmentDiagnostic{
		{{Stage: "state_read", Code: "authority_mismatch"}},
		make([]runtimeAgentEnrollmentDiagnostic, maxRuntimeAgentDiagnostics+1),
	} {
		view := base
		view.RuntimeAgent.Diagnostics = diagnostics
		if _, err := safeJSON("fencer-diagnostics.json", encoded(view)); err == nil {
			t.Fatal("unallowlisted or excessive runtime-agent diagnostics were accepted")
		}
	}
}

func TestRuntimeAgentDiagnosticsAllowRuntimeProfileBindingAndCgroupPathFailures(t *testing.T) {
	view := fencerDiagnosticsView{}
	view.RuntimeAgent.PodName = "rhiza-runtime-agent-abc"
	view.RuntimeAgent.PodUID = "runtime-agent-pod-uid"
	view.RuntimeAgent.Logs.Captured = true
	view.RuntimeAgent.Logs.SHA256 = strings.Repeat("a", 64)
	for _, code := range []string{
		"runtime_api_unavailable", "runtime_binding_invalid",
		"process_cgroup_path_root", "process_cgroup_path_absolute", "process_cgroup_path_dot",
		"process_cgroup_path_noncanonical", "process_cgroup_path_traversal",
		"cgroup_namespace_all_same", "cgroup_namespace_agent_host_same_target_other",
		"cgroup_namespace_agent_target_same_host_other", "cgroup_namespace_host_target_same_agent_other",
		"cgroup_namespace_all_different", "cgroup_namespace_agent_unavailable",
		"cgroup_namespace_host_unavailable", "cgroup_namespace_target_unavailable",
		"cgroup_namespace_multiple_unavailable",
	} {
		view.RuntimeAgent.Diagnostics = []runtimeAgentEnrollmentDiagnostic{{Stage: "cri_enroll", Code: code}}
		if _, err := safeJSON("fencer-diagnostics.json", encoded(view)); err != nil {
			t.Errorf("allowlisted CRI enrollment code %q was rejected: %v", code, err)
		}
	}
}

func TestRuntimeAgentDiagnosticsAllowCapturedScopeDigestMismatch(t *testing.T) {
	view := fencerDiagnosticsView{}
	view.RuntimeAgent.PodName = "rhiza-runtime-agent-abc"
	view.RuntimeAgent.PodUID = "runtime-agent-pod-uid"
	view.RuntimeAgent.Logs.Captured = true
	view.RuntimeAgent.Logs.SHA256 = strings.Repeat("a", 64)
	view.RuntimeAgent.Diagnostics = []runtimeAgentEnrollmentDiagnostic{{Stage: "backend_result_validation", Code: "scope_digest_mismatch"}}
	if _, err := safeJSON("fencer-diagnostics.json", encoded(view)); err != nil {
		t.Fatalf("allowlisted captured-scope digest mismatch was rejected: %v", err)
	}
}

func TestFailedEarlyRunKeepsExitReceiptWithoutRuntimeProfile(t *testing.T) {
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"actual-exit.txt":      []byte("1\n"),
		"source-head.txt":      []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":        []byte("sha256:" + strings.Repeat("b", 64) + "\n"),
		"runtime-profile.json": nil,
		"terminal.json":        nil,
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("early failed run did not produce a usable public receipt: %v", err)
	}
	public := filepath.Join(raw, "public")
	if data, err := os.ReadFile(filepath.Join(public, "actual-exit.txt")); err != nil || string(data) != "1\n" {
		t.Fatalf("actual exit receipt was not preserved: %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(public, "runtime-profile.json")); !os.IsNotExist(err) {
		t.Fatalf("empty runtime profile should be omitted after early failure, got %v", err)
	}
}

func TestFailurePacketDoesNotRequireUnreachedRuntimeProbe(t *testing.T) {
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
		"actual-exit.txt": []byte("1\n"),
		"image-ids.txt":   []byte("sha256:" + strings.Repeat("b", 64) + "\n"),
		"runtime-profile.json": encoded(runtimeProfileEvidence{
			Qualification: "runtime-agent-component-only", KindNodeImage: "kindest/node:v1@sha256:" + strings.Repeat("c", 64),
			RuntimeName: "containerd", RuntimeVersion: "test", RuntimeAPIVersion: "v1", RuntimeType: "io.containerd.runc.v2",
			KernelRelease: "test-kernel", CgroupFilesystem: "cgroup2fs", CgroupProfileID: "kind-ci-cgroup-v2-component-v1",
		}),
		"fencer-diagnostics.json": encoded(fencerDiagnosticsView{Context: "kind-rhiza-generic-fencing", Namespace: "rhiza-fencing-e2e"}),
		"fencer-pods.json":        encoded(fencerPodsView{}),
		"fencer-events.json":      encoded(fencerEventsView{}),
		"fencer-deployment.json":  encoded(fencerDeploymentView{}),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("failure evidence should survive a rollout failure before runtime probe: %v", err)
	}
	if _, err := os.Stat(filepath.Join(raw, "public", "runtime-component-probe.json")); !os.IsNotExist(err) {
		t.Fatal("missing runtime probe should not be fabricated for an early failure")
	}
}

func TestRunScriptCapturesBoundedOwnedDiagnosticsBeforeDeletingKind(t *testing.T) {
	script, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	capture := strings.Index(source, "capture_fencer_diagnostics || true")
	delete := strings.Index(source, "kind delete cluster --name \"$fencing_cluster\"")
	if capture < 0 || delete < 0 || capture > delete {
		t.Fatal("owned fencer diagnostics must be captured before Kind deletion")
	}
	for _, required := range []string{
		"kubectl --context \"$fencing_context\" -n \"$fencing_namespace\"",
		"--tail=200 --limit-bytes=65536 --max-log-requests=5",
		"diagnostics_dir/authority-current.log",
		"get deployment rhiza-fencer",
		"runtime-agent-current.log",
		".kind == \"DaemonSet\" and .name == \"rhiza-runtime-agent\" and .uid == $owner",
		"logs \"$runtime_agent_pod_name\" -c runtime-agent --prefix --tail=200 --limit-bytes=65536",
		"get pod \"$runtime_agent_pod_name\" -o json",
		"runtime_agent_after_ds_uid=$(k --request-timeout=10s get daemonset rhiza-runtime-agent -o jsonpath='{.metadata.uid}'",
		"[ \"$runtime_agent_after_ds_uid\" = \"$runtime_agent_ds_uid\" ]",
		".metadata.uid == $pod and .spec.nodeName == $node",
		"runtimeAgentLogsSHA256",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("bounded owned diagnostic capture missing %q", required)
		}
	}
}

func TestPublicPacketFailsClosedOnUnexpectedReadback(t *testing.T) {
	raw := t.TempDir()
	data := append(encoded(obj{"event": "new_generation_linearizable_readback", "rows": []any{[]string{"secret-canary"}}}), '\n')
	if err := os.WriteFile(filepath.Join(raw, "events.jsonl"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := safeArtifacts(raw); err == nil {
		t.Fatal("unexpected readback admitted for publication")
	}
}

func TestAsyncEmptyPublicProjectionHashesPrivateEvidenceWithoutScopeIdentifiers(t *testing.T) {
	const privateCanary = "private-bucket-project-prefix-endpoint-poduid-token-canary"
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"actual-exit.txt":              []byte("0\n"),
		"source-head.txt":              []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":                []byte("sha256:" + strings.Repeat("b", 64) + "\n"),
		"runtime-profile.json":         encoded(runtimeProfileEvidence{}),
		"runtime-component-probe.json": encoded(runtimeComponentEvidence{}),
		gcsAsyncEmptyEventsName:        []byte(`{"event":"private","value":"` + privateCanary + `"}` + "\n"),
		gcsAsyncEmptyEvidenceName:      asyncEmptyPrivateEvidenceFixture(privateCanary),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("strict async-empty public projection failed: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", gcsAsyncEmptySummaryName))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(public, []byte(privateCanary)) || bytes.Contains(public, []byte("bucket")) || bytes.Contains(public, []byte("prefix")) || bytes.Contains(public, []byte("endpoint")) {
		t.Fatalf("public async-empty summary leaked a private identifier: %s", public)
	}
	var summary gcsAsyncEmptyPublicSummary
	if err := json.Unmarshal(public, &summary); err != nil || summary.Outcome != "passed" || !summary.SourceAckLostAtEmptyBase || !summary.EmptyBaseRecoveryCommitted || !summary.TargetAckReadbackVerified || len(summary.PrivateEvidenceSHA256) != 64 || len(summary.PrivateEventsSHA256) != 64 {
		t.Fatalf("public async-empty summary is incomplete: %+v, %v", summary, err)
	}
	for _, name := range []string{gcsAsyncEmptyEvidenceName, gcsAsyncEmptyEventsName} {
		if _, err := os.Stat(filepath.Join(raw, "public", name)); !os.IsNotExist(err) {
			t.Fatalf("private async-empty artifact %s was copied publicly", name)
		}
	}
}

func TestAsyncEmptyArtifactProjectionRejectsUnknownFieldsAndFiles(t *testing.T) {
	privateCanary := "private-scope-value"
	for _, tc := range []struct {
		name   string
		mutate func(obj) obj
		file   string
	}{
		{name: "unknown evidence key", mutate: func(value obj) obj { value["unreviewed"] = "private"; return value }},
		{name: "recovery references another proof", mutate: func(value obj) obj {
			recovery := value["recovery"].(map[string]any)
			spec := recovery["spec"].(map[string]any)
			fence := spec["fence"].(map[string]any)
			fence["evidence"] = "different-proof"
			return value
		}},
		{name: "recovery fence names another operation", mutate: func(value obj) obj {
			recovery := value["recovery"].(map[string]any)
			spec := recovery["spec"].(map[string]any)
			fence := spec["fence"].(map[string]any)
			fence["recoveryID"] = "different-operation"
			return value
		}},
		{name: "recovery fence is unconfirmed", mutate: func(value obj) obj {
			recovery := value["recovery"].(map[string]any)
			spec := recovery["spec"].(map[string]any)
			fence := spec["fence"].(map[string]any)
			fence["confirmed"] = false
			return value
		}},
		{name: "unknown async artifact file", file: "gcs-async-empty-extra.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := t.TempDir()
			if err := os.Mkdir(filepath.Join(raw, "public"), 0700); err != nil {
				t.Fatal(err)
			}
			if tc.file != "" {
				if err := os.WriteFile(filepath.Join(raw, tc.file), []byte("private"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				value := asyncEmptyPrivateEvidenceFixture(privateCanary)
				var evidence obj
				if err := json.Unmarshal(value, &evidence); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(raw, gcsAsyncEmptyEvidenceName), encoded(tc.mutate(evidence)), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(raw, gcsAsyncEmptyEventsName), []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := projectGCSAsyncEmptyEvidence(raw, filepath.Join(raw, "public"), false); err == nil {
				t.Fatal("accepted unreviewed async-empty artifact content")
			}
		})
	}
}

func TestFailedAsyncEmptyRunPublishesOnlyHashOfPartialPrivateEvents(t *testing.T) {
	const privateCanary = "private-partial-async-empty-scope"
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"actual-exit.txt":       []byte("1\n"),
		"source-head.txt":       []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":         []byte("sha256:" + strings.Repeat("b", 64) + "\n"),
		gcsAsyncEmptyEventsName: []byte(`{"event":"partial","private":"` + privateCanary + `"}` + "\n"),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("failed async-empty run did not preserve safe failure artifacts: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", gcsAsyncEmptySummaryName))
	if err != nil || bytes.Contains(public, []byte(privateCanary)) {
		t.Fatalf("failed-run projection leaked partial private event data: %v", err)
	}
	var summary gcsAsyncEmptyPublicSummary
	if err := json.Unmarshal(public, &summary); err != nil || summary.Outcome != "failed" || summary.PrivateEventsSHA256 == "" || summary.PrivateEvidenceSHA256 != "" {
		t.Fatalf("failed-run summary incorrectly claims complete private evidence: %+v, %v", summary, err)
	}
}

func TestFailedAckLossProjectionPublishesNoPrivateScopeOrIdentity(t *testing.T) {
	const privateCanary = "private-bucket-prefix-poduid-credential-canary"
	raw := t.TempDir()
	public := filepath.Join(raw, "public")
	if err := os.Mkdir(public, 0700); err != nil {
		t.Fatal(err)
	}
	events := []byte(`{"event":"ack_loss_fault_armed","time":"2026-10-23T00:00:00Z","data":{"bucket":"` + privateCanary + `","prefix":"` + privateCanary + `","podUID":"` + privateCanary + `"}}` + "\n")
	if err := os.WriteFile(filepath.Join(raw, gcsAckLossEventsName), events, 0600); err != nil {
		t.Fatal(err)
	}
	seen, err := projectGCSAckLossEvidence(raw, public, true)
	if err != nil || !seen {
		t.Fatalf("failed ACK-loss run did not produce a safe partial summary: seen=%t err=%v", seen, err)
	}
	data, err := os.ReadFile(filepath.Join(public, gcsAckLossSummaryName))
	if err != nil || bytes.Contains(data, []byte(privateCanary)) {
		t.Fatalf("ACK-loss public summary leaked private scope or identity: %v", err)
	}
	var summary gcsAckLossPublicSummary
	if err := json.Unmarshal(data, &summary); err != nil || summary.Outcome != "failed" || summary.PrivateEventsSHA256 == "" || !summary.FaultArmed || summary.FaultInjected {
		t.Fatalf("ACK-loss failure summary is incomplete or overstates outcome: %+v, %v", summary, err)
	}
}

func TestAckLossProjectorRejectsReceiptSourceAndGuardMismatch(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*gcsAckLossPrivateEvidence)
		wantErr bool
	}{
		{name: "consistent canonical packet", mutate: func(*gcsAckLossPrivateEvidence) {}},
		{name: "receipt source mismatch", mutate: func(private *gcsAckLossPrivateEvidence) {
			var first struct {
				Source gcsRecoverySource `json:"source"`
			}
			if err := decodeAckLossRecovery(private.FirstRecovery, &first); err != nil {
				t.Fatalf("decode first recovery: %v", err)
			}
			first.Source.ClusterID = "different-source"
			private.FirstRecovery["source"] = first.Source
		}, wantErr: true},
		{name: "guard tuple mismatch", mutate: func(private *gcsAckLossPrivateEvidence) {
			private.CanonicalObservation.Guarded.DispatchID = "different-dispatch"
		}, wantErr: true},
		{name: "guard digest mismatch", mutate: func(private *gcsAckLossPrivateEvidence) {
			private.CanonicalObservation.Guarded.GuardDigest = strings.Repeat("b", 64)
		}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, public := t.TempDir(), filepath.Join(t.TempDir(), "public")
			if err := os.Mkdir(public, 0700); err != nil {
				t.Fatal(err)
			}
			private := validGCSAckLossEvidenceFixture(t)
			test.mutate(&private)
			data, err := json.Marshal(private)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(raw, gcsAckLossEvidenceName), data, 0600); err != nil {
				t.Fatal(err)
			}
			var events strings.Builder
			for _, name := range []string{"ack_loss_fault_armed", "ack_loss_canonical_and_object_verified", "ack_loss_two_recoveries_complete"} {
				fmt.Fprintf(&events, `{"event":%q,"time":"2026-10-23T00:00:00Z","data":{}}`+"\n", name)
			}
			if err := os.WriteFile(filepath.Join(raw, gcsAckLossEventsName), []byte(events.String()), 0600); err != nil {
				t.Fatal(err)
			}
			seen, err := projectGCSAckLossEvidence(raw, public, false)
			if err != nil && !test.wantErr {
				t.Fatalf("consistent private ACK-loss evidence was rejected: %v", err)
			}
			if err == nil && test.wantErr {
				t.Fatal("inconsistent private ACK-loss evidence was projected as a successful qualification")
			}
			if !test.wantErr {
				data, readErr := os.ReadFile(filepath.Join(public, gcsAckLossSummaryName))
				var summary gcsAckLossPublicSummary
				if !seen || readErr != nil || json.Unmarshal(data, &summary) != nil || summary.Outcome != "passed" {
					t.Fatalf("consistent private packet did not produce a passed summary: seen=%t summary=%+v readErr=%v", seen, summary, readErr)
				}
			}
		})
	}
}

func validGCSAckLossEvidenceFixture(t *testing.T) gcsAckLossPrivateEvidence {
	t.Helper()
	identity := fencing.Identity{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", WALIdentity: "wal"}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", StatefulSetUID: "sts-uid",
		SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", NodeName: "node-name", NodeUID: "node-uid", MachineID: "machine",
		BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
	source := gcsRecoverySource{Namespace: "run-ns", NamespaceUID: runtime.NamespaceUID, BindingUID: runtime.BindingUID, StatefulSetUID: runtime.StatefulSetUID,
		ClusterID: runtime.SourceClusterID, Bucket: "bucket", Prefix: "data/source"}
	scope := gcsHAAckSourceScope(source)
	scopeHash, err := gcsHAAckScopeHash(scope)
	if err != nil {
		t.Fatal(err)
	}
	payloadDigest := strings.Repeat("a", 64)
	intent := fencing.MutationIntent{ContractVersion: 1, StoreScopeHash: scopeHash,
		OwnerStep: fencing.MutationOwnerStep{Owner: "rhiza", Workflow: "archive", Step: "extent", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: "mutation-op", Kind: "upload",
		Key: "archive/blocks/" + strings.Repeat("c", 64) + "_00000000000000000001.bin", PayloadDigest: payloadDigest, PayloadSize: 16}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true, Generation: 0}
	receipt := operator.GCSMutationAckLossReceipt{Version: 1, Injected: true, WireRequests: 1, SourceIdentity: identity, Runtime: runtime,
		ProducerUID: "producer", Sequence: 4, DispatchID: "dispatch", OperationID: intent.OperationID, Intent: intent, Preparation: preparation,
		Scope: scope, Bucket: source.Bucket, Prefix: source.Prefix, ObjectKey: intent.Key, PayloadDigest: payloadDigest, PayloadSize: intent.PayloadSize}
	receipt.TupleDigest = gcsHAAckTupleDigest(receipt)
	guarded := fencing.MutationGuardedLogicalResolution{Version: fencing.MutationGuardedLogicalResolutionVersion, Runtime: runtime, WALIdentity: identity.WALIdentity,
		ProducerUID: receipt.ProducerUID, Sequence: receipt.Sequence, Intent: intent, Preparation: preparation, PreparationHash: strings.Repeat("d", 64),
		DispatchID: receipt.DispatchID, Profile: fencing.MutationHeldExecutionProfile, GuardRootIdentity: gcsHAAuthorityRootIdentity(runtime.StatefulSetUID, runtime.SourceClusterID),
		GuardVersion: fencing.MutationGuardedLogicalResolutionVersion, GuardRef: "guard/record", GuardDigest: strings.Repeat("e", 64),
		ObjectGeneration: 101, ObjectMetageneration: 2, ObjectSize: intent.PayloadSize, PayloadDigest: payloadDigest}
	observation := gcsHAAckObservation{Runtime: runtime, ProducerUID: receipt.ProducerUID, Sequence: receipt.Sequence, DispatchID: receipt.DispatchID,
		Guarded: guarded, GuardSHA256: guarded.GuardDigest}
	object := gcsAckLossObjectEvidence{Generation: guarded.ObjectGeneration, Metageneration: guarded.ObjectMetageneration, Size: guarded.ObjectSize,
		PayloadSHA256: payloadDigest, TemporaryHold: true}
	firstFence := operator.FenceRequest{OperationID: "fence-1", SourceClusterID: "source", StatefulSetUID: source.StatefulSetUID, BindingUID: source.BindingUID}
	firstProof := operator.FenceProof{OperationID: firstFence.OperationID, SourceClusterID: firstFence.SourceClusterID, BindingUID: firstFence.BindingUID, ProofID: "proof-1"}
	firstRecovery := operator.Resource{Spec: operator.Spec{SourceClusterID: "source", RecoveryID: firstFence.OperationID,
		Fence: operator.Fence{RecoveryID: firstFence.OperationID, ClusterID: "source", StatefulSetUID: source.StatefulSetUID, Confirmed: true, Evidence: firstProof.ProofID}},
		Status: operator.Status{Phase: "Complete", Stage: "Complete", StatefulSetUID: source.StatefulSetUID, Source: "source", Target: "target-1",
			TargetIdentities: []operator.FenceTarget{{PodUID: "target-pod-1", WALIdentity: "target-wal-1"}}}}
	secondFence := operator.FenceRequest{OperationID: "fence-2", SourceClusterID: "target-1", StatefulSetUID: source.StatefulSetUID, BindingUID: source.BindingUID,
		Targets: []operator.FenceTarget{{PodUID: "new-source-pod", WALIdentity: "new-source-wal"}}}
	secondProof := operator.FenceProof{OperationID: secondFence.OperationID, SourceClusterID: secondFence.SourceClusterID, BindingUID: secondFence.BindingUID, ProofID: "proof-2"}
	secondRecovery := operator.Resource{Spec: operator.Spec{SourceClusterID: "target-1", RecoveryID: secondFence.OperationID,
		Fence: operator.Fence{RecoveryID: secondFence.OperationID, ClusterID: "target-1", StatefulSetUID: source.StatefulSetUID, Confirmed: true, Evidence: secondProof.ProofID}},
		Status: operator.Status{Phase: "Complete", Stage: "Complete", StatefulSetUID: source.StatefulSetUID, Source: "target-1", Target: "target-2",
			TargetIdentities: []operator.FenceTarget{{PodUID: "target-pod-2", WALIdentity: "target-wal-2"}}}}
	return gcsAckLossPrivateEvidence{Version: 1, BaselineAck: gcsAcknowledgement{RequestID: "baseline", Slot: 1, Rows: `[[1,"gcs-ack-loss-baseline"]]`},
		FaultReceipt: receipt, CanonicalObservation: observation, Object: object,
		FirstRecovery:  gcsRecoveryEvidence(&gcsRecoveryOutcome{Source: source, Fence: firstFence, Proof: firstProof, Recovery: firstRecovery}),
		SecondRecovery: gcsRecoveryEvidence(&gcsRecoveryOutcome{Source: source, Fence: secondFence, Proof: secondProof, Recovery: secondRecovery}),
		SecondAck:      gcsAcknowledgement{RequestID: "successor", Slot: 2, Rows: `[[2,"gcs-ack-loss-successor"]]`}}
}

func decodeAckLossRecovery(data obj, target any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}

func TestAsyncNonemptyPublicProjectionHashesPrivateEvidenceWithoutScopeIdentifiers(t *testing.T) {
	t.Setenv("FENCING_E2E_QUALIFICATION", "gcs-async-nonempty")
	const privateCanary = "private-project-bucket-prefix-endpoint-poduid-token-canary"
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"actual-exit.txt":            []byte("0\n"),
		"source-head.txt":            []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":              []byte("registry.example/app@sha256:" + strings.Repeat("b", 64) + "\nregistry.example/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
		gcsAsyncNonemptyEventsName:   []byte(`{"event":"private","value":"` + privateCanary + `"}` + "\n"),
		gcsAsyncNonemptyEvidenceName: asyncNonemptyPrivateEvidenceFixture(privateCanary),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("strict async-nonempty public projection failed: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", gcsAsyncNonemptySummaryName))
	if err != nil || bytes.Contains(public, []byte(privateCanary)) || bytes.Contains(public, []byte("bucket")) ||
		bytes.Contains(public, []byte("prefix")) || bytes.Contains(public, []byte("endpoint")) {
		t.Fatalf("public async-nonempty summary leaked a private identifier: %s (%v)", public, err)
	}
	var summary gcsAsyncNonemptyPublicSummary
	if err := json.Unmarshal(public, &summary); err != nil || summary.Outcome != "passed" || summary.SourceAckCount != 3 ||
		!summary.PersistedNonemptyTip || !summary.SealedNonemptyTip || summary.RecoveredArchivedTip != 5 ||
		!summary.RecoveredRowsVerified || !summary.TargetQuorumVerified || !summary.TargetAckReadbackVerified ||
		len(summary.PrivateEvidenceSHA256) != 64 || len(summary.PrivateEventsSHA256) != 64 {
		t.Fatalf("public async-nonempty summary is incomplete: %+v, %v", summary, err)
	}
	for _, name := range []string{gcsAsyncNonemptyEvidenceName, gcsAsyncNonemptyEventsName} {
		if _, err := os.Stat(filepath.Join(raw, "public", name)); !os.IsNotExist(err) {
			t.Fatalf("private async-nonempty artifact %s was copied publicly", name)
		}
	}
}

func TestAsyncNonemptyArtifactAcceptsDistinctLowerGCSGeneration(t *testing.T) {
	t.Setenv("FENCING_E2E_QUALIFICATION", "gcs-async-nonempty")
	raw := t.TempDir()
	var evidence obj
	if err := json.Unmarshal(asyncNonemptyPrivateEvidenceFixture("private-scope"), &evidence); err != nil {
		t.Fatal(err)
	}
	sealed := evidence["sealedSourceHead"].(map[string]any)
	sealed["version"] = "1000" // GCS generations are unique, not monotonic.
	for name, data := range map[string][]byte{
		"actual-exit.txt":            []byte("0\n"),
		"source-head.txt":            []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":              []byte("registry.example/app@sha256:" + strings.Repeat("b", 64) + "\nregistry.example/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
		gcsAsyncNonemptyEventsName:   []byte("{}\n"),
		gcsAsyncNonemptyEvidenceName: encoded(evidence),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("rejected distinct lower GCS generation with valid sealed-head evidence: %v", err)
	}
}

func TestAsyncNonemptyArtifactProjectionRejectsInvalidEvidenceAndUnknownFiles(t *testing.T) {
	const privateCanary = "private-scope-value"
	for _, tc := range []struct {
		name   string
		mutate func(obj) obj
		file   string
	}{
		{name: "unknown evidence key", mutate: func(value obj) obj { value["unreviewed"] = privateCanary; return value }},
		{name: "unknown nested ACK key", mutate: func(value obj) obj {
			acks := value["sourceAcknowledgements"].([]any)
			acks[0].(map[string]any)["private"] = privateCanary
			return value
		}},
		{name: "wrong ACK rows", mutate: func(value obj) obj {
			value["sourceRows"] = `[[1,"other"],[2,"gcs-async-archive-2"],[3,"gcs-async-archive-3"]]`
			return value
		}},
		{name: "archive tip regressed after sealing", mutate: func(value obj) obj {
			sealed := value["sealedSourceHead"].(map[string]any)
			sealed["tip"] = float64(4)
			return value
		}},
		{name: "recovery changed proof", mutate: func(value obj) obj {
			recovery := value["recovery"].(map[string]any)
			spec := recovery["spec"].(map[string]any)
			fence := spec["fence"].(map[string]any)
			fence["evidence"] = "different-proof"
			return value
		}},
		{name: "recovery omitted loss gate", mutate: func(value obj) obj {
			recovery := value["recovery"].(map[string]any)
			recovery["spec"].(map[string]any)["allowDataLoss"] = false
			return value
		}},
		{name: "recovered tip differs", mutate: func(value obj) obj { value["recoveredTip"] = float64(4); return value }},
		{name: "target ACK rows differ", mutate: func(value obj) obj {
			value["targetAck"].(map[string]any)["rows"] = `[[4,"other"]]`
			return value
		}},
		{name: "target ACK does not advance recovered tip", mutate: func(value obj) obj {
			value["targetAck"].(map[string]any)["slot"] = float64(5)
			return value
		}},
		{name: "unknown async artifact file", file: "gcs-async-nonempty-extra.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := t.TempDir()
			if err := os.Mkdir(filepath.Join(raw, "public"), 0700); err != nil {
				t.Fatal(err)
			}
			if tc.file != "" {
				if err := os.WriteFile(filepath.Join(raw, tc.file), []byte(privateCanary), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				var evidence obj
				if err := json.Unmarshal(asyncNonemptyPrivateEvidenceFixture(privateCanary), &evidence); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(raw, gcsAsyncNonemptyEvidenceName), encoded(tc.mutate(evidence)), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(raw, gcsAsyncNonemptyEventsName), []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := projectGCSAsyncNonemptyEvidence(raw, filepath.Join(raw, "public"), false); err == nil {
				t.Fatal("accepted unreviewed async-nonempty artifact content")
			}
		})
	}
}

func TestFailedAsyncNonemptyRunPublishesOnlyHashOfPartialPrivateEvents(t *testing.T) {
	t.Setenv("FENCING_E2E_QUALIFICATION", "gcs-async-nonempty")
	const privateCanary = "private-partial-async-nonempty-scope"
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"actual-exit.txt":          []byte("124\n"),
		"source-head.txt":          []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":            []byte("registry.example/app@sha256:" + strings.Repeat("b", 64) + "\nregistry.example/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
		gcsAsyncNonemptyEventsName: []byte(`{"event":"partial","private":"` + privateCanary + `"}` + "\n"),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("failed async-nonempty run did not preserve safe failure artifacts: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", gcsAsyncNonemptySummaryName))
	if err != nil || bytes.Contains(public, []byte(privateCanary)) {
		t.Fatalf("failed-run async-nonempty projection leaked partial private events: %v", err)
	}
	var summary gcsAsyncNonemptyPublicSummary
	if err := json.Unmarshal(public, &summary); err != nil || summary.Outcome != "failed" || len(summary.PrivateEventsSHA256) != 64 {
		t.Fatalf("failed-run async-nonempty summary is incomplete: %+v, %v", summary, err)
	}
}

func TestAsyncNonemptySafeArtifactsRequiresExplicitSelectorAndEvidence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selector string
		evidence bool
	}{
		{name: "evidence without selector", evidence: true},
		{name: "successful selected run without scenario evidence", selector: "gcs-async-nonempty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FENCING_E2E_QUALIFICATION", tc.selector)
			raw := t.TempDir()
			files := map[string][]byte{
				"actual-exit.txt": []byte("0\n"),
				"source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
				"image-ids.txt": []byte("registry.example/app@sha256:" + strings.Repeat("b", 64) + "\n" +
					"registry.example/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
			}
			if tc.evidence {
				files[gcsAsyncNonemptyEventsName] = []byte("{}\n")
				files[gcsAsyncNonemptyEvidenceName] = asyncNonemptyPrivateEvidenceFixture("private-scope")
			}
			for name, data := range files {
				if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := safeArtifacts(raw); err == nil {
				t.Fatal("accepted an unselected or evidence-free async-nonempty success")
			}
		})
	}
}

func TestManualTerminalPublicProjectionRequiresExactApprovalAndRedactsPrivateEvidence(t *testing.T) {
	t.Setenv("FENCING_E2E_QUALIFICATION", gcsManualTerminalQualification)
	const privateCanary = "manual-terminal-private-scope-canary"
	raw := t.TempDir()
	evidence := manualTerminalPrivateEvidenceFixture(privateCanary)
	files := map[string][]byte{
		"actual-exit.txt": []byte("0\n"),
		"source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt": []byte("registry.example/app@sha256:" + strings.Repeat("b", 64) + "\n" +
			"registry.example/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
		gcsManualTerminalEventsName:    []byte(`{"event":"private","scope":"` + privateCanary + `"}` + "\n"),
		gcsManualTerminalChallengeName: manualTerminalChallengeRecord(evidence),
		gcsManualTerminalEvidenceName:  evidence,
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("strict manual-terminal public projection failed: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", gcsManualTerminalSummaryName))
	if err != nil || bytes.Contains(public, []byte(privateCanary)) || bytes.Contains(public, []byte("sourceClusterID")) {
		t.Fatalf("public manual-terminal summary leaked private evidence: %s (%v)", public, err)
	}
	var summary gcsManualTerminalPublicSummary
	if err := json.Unmarshal(public, &summary); err != nil || summary.Outcome != "passed" || !summary.SourceAcknowledged ||
		!summary.NonemptySourceSealed || !summary.IndependentV2Absent || !summary.ChallengeObserved ||
		!summary.IndependentApproval || !summary.NoPreapprovalTarget || !summary.RecoveryCommitted ||
		!summary.TargetReadbackVerified || len(summary.PrivateEvidenceSHA256) != 64 || len(summary.PrivateEventsSHA256) != 64 {
		t.Fatalf("manual-terminal public summary is incomplete: %+v, %v", summary, err)
	}
	for _, name := range []string{gcsManualTerminalEvidenceName, gcsManualTerminalChallengeName, gcsManualTerminalEventsName} {
		if _, err := os.Stat(filepath.Join(raw, "public", name)); !os.IsNotExist(err) {
			t.Fatalf("private manual-terminal artifact %s was copied publicly", name)
		}
	}
}

func TestManualTerminalProjectionRejectsChallengeFileForDifferentEvidence(t *testing.T) {
	t.Setenv("FENCING_E2E_QUALIFICATION", gcsManualTerminalQualification)
	raw := t.TempDir()
	if err := os.Mkdir(filepath.Join(raw, "public"), 0700); err != nil {
		t.Fatal(err)
	}
	evidenceData := manualTerminalPrivateEvidenceFixture("private-scope")
	var evidence obj
	if err := json.Unmarshal(evidenceData, &evidence); err != nil {
		t.Fatal(err)
	}
	var challenge operator.ManualRecoveryChallenge
	if err := json.Unmarshal(encoded(evidence["challenge"]), &challenge); err != nil {
		t.Fatal(err)
	}
	challenge.TargetClusterID = "different-target"
	digest, err := operator.ManualRecoveryChallengeDigest(challenge)
	if err != nil {
		t.Fatal(err)
	}
	name, err := operator.ManualRecoveryAttestationName(challenge.RecoveryUID)
	if err != nil {
		t.Fatal(err)
	}
	for file, data := range map[string][]byte{
		gcsManualTerminalEvidenceName: evidenceData,
		gcsManualTerminalChallengeName: encoded(obj{"challenge": challenge, "challengeDigest": digest,
			"attestationName": name}),
		gcsManualTerminalEventsName: []byte("{}\n"),
	} {
		if err := os.WriteFile(filepath.Join(raw, file), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := projectGCSManualTerminalEvidence(raw, filepath.Join(raw, "public"), false); err == nil {
		t.Fatal("accepted a valid but unrelated challenge file beside the evidence")
	}
}

func TestManualTerminalProjectionRejectsChangedApprovalAndProofMixing(t *testing.T) {
	t.Setenv("FENCING_E2E_QUALIFICATION", gcsManualTerminalQualification)
	for _, tc := range []struct {
		name   string
		mutate func(obj)
	}{
		{name: "preapproval target already claimed", mutate: func(e obj) { e["preApprovalTargetUnclaimed"] = false }},
		{name: "independent proof mixed into manual path", mutate: func(e obj) {
			e["fenceProof"].(map[string]any)["independentV2"] = obj{"version": 2}
		}},
		{name: "legacy independent proof mixed into manual path", mutate: func(e obj) {
			e["fenceProof"].(map[string]any)["independent"] = obj{"proofID": "legacy-independent"}
		}},
		{name: "approval binding changed", mutate: func(e obj) {
			e["independentApproval"].(map[string]any)["Spec"].(map[string]any)["sourceClusterID"] = "other-source"
		}},
		{name: "target recovery changed", mutate: func(e obj) {
			e["recovery"].(map[string]any)["status"].(map[string]any)["target"] = "other-target"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := t.TempDir()
			if err := os.Mkdir(filepath.Join(raw, "public"), 0700); err != nil {
				t.Fatal(err)
			}
			var evidence obj
			if err := json.Unmarshal(manualTerminalPrivateEvidenceFixture("private-scope"), &evidence); err != nil {
				t.Fatal(err)
			}
			tc.mutate(evidence)
			for name, data := range map[string][]byte{
				gcsManualTerminalEvidenceName: encoded(evidence),
				gcsManualTerminalEventsName:   []byte("{}\n"),
			} {
				if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := projectGCSManualTerminalEvidence(raw, filepath.Join(raw, "public"), false); err == nil {
				t.Fatal("accepted changed or mixed manual-terminal evidence")
			}
		})
	}
}

func TestManualTerminalSafeArtifactsRequiresExplicitSelectorAndEvidence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selector string
		evidence bool
	}{
		{name: "evidence without selector", evidence: true},
		{name: "selected successful run without evidence", selector: gcsManualTerminalQualification},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FENCING_E2E_QUALIFICATION", tc.selector)
			raw := t.TempDir()
			files := map[string][]byte{"actual-exit.txt": []byte("0\n"), "source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
				"image-ids.txt": []byte("registry.example/app@sha256:" + strings.Repeat("b", 64) + "\n" +
					"registry.example/operator@sha256:" + strings.Repeat("c", 64) + "\n")}
			if tc.evidence {
				files[gcsManualTerminalEventsName] = []byte("{}\n")
				files[gcsManualTerminalEvidenceName] = manualTerminalPrivateEvidenceFixture("private-scope")
			}
			for name, data := range files {
				if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := safeArtifacts(raw); err == nil {
				t.Fatal("accepted unselected or evidence-free manual-terminal success")
			}
		})
	}
}

func manualTerminalChallengeRecord(evidenceData []byte) []byte {
	var evidence obj
	if err := json.Unmarshal(evidenceData, &evidence); err != nil {
		panic("invalid manual terminal evidence fixture: " + err.Error())
	}
	var challenge operator.ManualRecoveryChallenge
	if err := json.Unmarshal(encoded(evidence["challenge"]), &challenge); err != nil {
		panic("invalid manual terminal challenge fixture: " + err.Error())
	}
	digest, _ := operator.ManualRecoveryChallengeDigest(challenge)
	name, _ := operator.ManualRecoveryAttestationName(challenge.RecoveryUID)
	return encoded(obj{"challenge": challenge, "challengeDigest": digest, "attestationName": name})
}

func manualTerminalPrivateEvidenceFixture(privateValue string) []byte {
	now := time.Now().UTC().Truncate(time.Second)
	challenge := manualTerminalChallengeFixture(privateValue)
	challenge.IssuedAt, challenge.ExpiresAt = now.Add(-time.Minute), now.Add(30*time.Minute)
	challengeDigest, _ := operator.ManualRecoveryChallengeDigest(challenge)
	attestationName, _ := operator.ManualRecoveryAttestationName(challenge.RecoveryUID)
	approval := operator.ManualRecoveryAttestationObject{Namespace: challenge.RecoveryNamespace, Name: attestationName, UID: "approval-api-uid", CreatedAt: now,
		Spec: operator.ManualRecoveryAttestationSpec{Version: 1, ChallengeDigest: challengeDigest, ManualRecoveryBinding: challenge.ManualRecoveryBinding,
			Assertions: operator.ManualRecoveryAssertions{AllHistoricalRunsTerminated: true, RecreationBlocked: true,
				OldPeerAndClientWriteAccessBlocked: true, OldArchiveWriterAccessRevoked: true, StorageQuiesced: true},
			Attester: "incident-review", EvidenceRefs: []string{"runbook://manual-terminal"}}}
	approvalData, _ := json.Marshal(approval)
	approvalHash := sha256.Sum256(approvalData)
	before := gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "1001", Tip: 1, HeadSHA256: strings.Repeat("1", 64)}
	sealed := gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "1002", Tip: 1, Sealed: true, HeadSHA256: strings.Repeat("2", 64)}
	fence := manualTerminalFenceFixture(privateValue)
	fenceHash := sha256.Sum256(encoded(fence))
	proof := operator.FenceProof{OperationID: fence.OperationID, SourceClusterID: fence.SourceClusterID, BindingUID: fence.BindingUID,
		RequestHash: hex.EncodeToString(fenceHash[:]), ProofID: "non-independent-proof"}
	recovery := operator.Resource{Metadata: map[string]any{"name": challenge.RecoveryName, "namespace": challenge.RecoveryNamespace, "uid": challenge.RecoveryUID},
		Spec: operator.Spec{StatefulSet: fence.StatefulSet, SourceClusterID: privateValue, RecoveryID: fence.OperationID,
			Durability: "async", AllowDataLoss: true,
			Fence: operator.Fence{RecoveryID: fence.OperationID, ClusterID: privateValue, StatefulSetUID: fence.StatefulSetUID, Confirmed: true}},
		Status: operator.Status{Phase: "Complete", Stage: "Complete", Source: privateValue, StatefulSetUID: fence.StatefulSetUID,
			Target: challenge.TargetClusterID, TargetBaseKind: "fork", ManualRecoveryChallengeDigest: challengeDigest,
			ManualAttestationDigest: hex.EncodeToString(approvalHash[:])}}
	return encoded(obj{"qualification": gcsManualTerminalQualification,
		"profile":               obj{"provider": "gcs", "noPVC": true, "sourceDurability": "async", "allowDataLoss": true},
		"sourceAck":             gcsAcknowledgement{RequestID: "gcs-ha-ack-1", Slot: 1, Rows: `[[1,"gcs-manual-terminal-source"]]`},
		"sourceHeadBeforeFence": before, "sealedSourceHead": sealed, "fence": fence, "fenceProof": proof,
		"challenge": challenge, "challengeDigest": challengeDigest, "independentApproval": approval,
		"approvalDigest": hex.EncodeToString(approvalHash[:]), "preApprovalTargetUnclaimed": true, "recovery": recovery,
		"targetQuorumVerified": true, "recoveredRows": `[[1,"gcs-manual-terminal-source"]]`,
		"targetAck":           gcsAcknowledgement{RequestID: "gcs-ha-ack-2", Slot: 2, Rows: `[[2,"gcs-manual-terminal-target"]]`},
		"independentV2Absent": true})
}

func manualTerminalChallengeFixture(privateValue string) operator.ManualRecoveryChallenge {
	stamp := strings.Repeat("a", 64)
	fence := manualTerminalFenceFixture(privateValue)
	fenceHash := sha256.Sum256(encoded(fence))
	return operator.ManualRecoveryChallenge{Version: 1,
		ManualRecoveryBinding: operator.ManualRecoveryBinding{EvidenceKind: operator.ManualTerminalEvidenceKind,
			RecoveryNamespace: "qualification", RecoveryName: "manual-terminal-recovery", RecoveryUID: "manual-recovery-uid",
			RecoveryID: fence.OperationID, OperationID: fence.OperationID, SourceClusterID: privateValue,
			StatefulSetName: fence.StatefulSet, StatefulSetUID: fence.StatefulSetUID, FenceRequestHash: hex.EncodeToString(fenceHash[:]), PolicyDigest: stamp, RootDigest: stamp,
			LedgerDigest: stamp, CohortDigest: stamp, StorageScopeDigest: stamp, SourceArchiveDigest: stamp,
			TargetClusterID: "manual-target", TargetBaseKind: "fork", CurrentPodUIDs: []string{"pod-1", "pod-2", "pod-3"}},
		IssuedAt: time.Now().UTC().Truncate(time.Second), ExpiresAt: time.Now().UTC().Truncate(time.Second).Add(30 * time.Minute)}
}

func manualTerminalFenceFixture(source string) operator.FenceRequest {
	return operator.FenceRequest{LogicalID: "manual-terminal", BindingUID: "binding-uid", Namespace: "qualification",
		StatefulSet: "rhiza", StatefulSetUID: "statefulset-uid", SourceClusterID: source,
		OperationID: "manual-terminal-operation", Scope: operator.ScopeGeneration,
		Targets: []operator.FenceTarget{{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-0", WALIdentity: "wal-0"}}}
}

func asyncNonemptyPrivateEvidenceFixture(privateValue string) []byte {
	target := operator.FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: privateValue, WALIdentity: "source-wal"}
	fence := operator.FenceRequest{LogicalID: privateValue, BindingUID: privateValue, Namespace: "qualification",
		StatefulSet: "rhiza", StatefulSetUID: privateValue, SourceClusterID: privateValue, OperationID: "async-nonempty-fence",
		Scope: operator.ScopeGeneration, Targets: []operator.FenceTarget{target}}
	scope := operator.AttestationStoreScope{Provider: "gcs", Bucket: privateValue, Prefix: privateValue, Endpoint: privateValue}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(scope)))
	evidence := &operator.IndependentEvidenceV2{Version: 2, RequestHash: fmt.Sprintf("%x", sha256.Sum256(encoded(fence))),
		LedgerHash: strings.Repeat("a", 64), StorageScope: scope,
		Terminals: []fencing.TerminalReference{{EvidenceVersion: 2,
			Request: fencing.StopRequest{OperationID: "source-stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{
				SourceClusterID: privateValue, NodeID: target.NodeID, PodUID: target.PodUID, RunID: "source-run", WALIdentity: target.WALIdentity}},
			RuntimeHash: strings.Repeat("c", 64), ProducerUID: "producer", ProofName: "proof", ProofUID: privateValue,
			ProofDigest: strings.Repeat("d", 64), RuntimeReceiptDigest: strings.Repeat("e", 64), MutationReceiptDigest: strings.Repeat("f", 64), StorageScopeDigest: scopeDigest}},
	}
	proof := operator.FenceProof{OperationID: fence.OperationID, SourceClusterID: fence.SourceClusterID, BindingUID: fence.BindingUID,
		RequestHash: evidence.RequestHash, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true, IndependentV2: evidence}
	proof.ProofID = "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(evidence)))
	recovery := operator.Resource{Metadata: map[string]any{"uid": privateValue},
		Spec: operator.Spec{SourceClusterID: fence.SourceClusterID, RecoveryID: fence.OperationID, Durability: "async", AllowDataLoss: true,
			Fence: operator.Fence{RecoveryID: fence.OperationID, ClusterID: fence.SourceClusterID, StatefulSetUID: fence.StatefulSetUID, Confirmed: true, Evidence: proof.ProofID}},
		Status: operator.Status{Phase: "Complete", Stage: "Complete", StatefulSetUID: fence.StatefulSetUID, Source: fence.SourceClusterID,
			Target: "private-target", TargetBaseKind: "fork", SourceDurability: "async", RecoveredTip: 5, ManifestHash: strings.Repeat("9", 64)}}
	before := gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "1001", Tip: 5, HeadSHA256: strings.Repeat("1", 64)}
	sealed := gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "1002", Tip: 5, Sealed: true, HeadSHA256: strings.Repeat("2", 64)}
	sourceAcks := []gcsAcknowledgement{
		{RequestID: "gcs-ha-ack-1", Slot: 1, Rows: `[[1,"gcs-async-archive-1"]]`},
		{RequestID: "gcs-ha-ack-2", Slot: 2, Rows: `[[2,"gcs-async-archive-2"]]`},
		{RequestID: "gcs-ha-ack-3", Slot: 4, Rows: `[[3,"gcs-async-archive-3"]]`},
	}
	rows := `[[1,"gcs-async-archive-1"],[2,"gcs-async-archive-2"],[3,"gcs-async-archive-3"]]`
	return encoded(obj{
		"qualification":          "gcs-async-nonempty-tip-recovery",
		"profile":                obj{"provider": "gcs", "bucket": privateValue, "noPVC": true, "sourceDurability": "async", "allowDataLoss": true},
		"sourceAcknowledgements": sourceAcks, "sourceRows": rows, "sourceHeadBeforeFence": before, "sealedSourceHead": sealed,
		"fence": fence, "proof": proof, "recovery": recovery, "recoveredRows": rows, "recoveredTip": uint64(5),
		"targetQuorumVerified": true, "targetAck": gcsAcknowledgement{RequestID: "gcs-ha-ack-4", Slot: 6, Rows: `[[4,"gcs-async-target-ack"]]`},
		"proofSource": "RhizaFence status plus direct versioned GCS HEAD reads; recovered rows were read linearly from the promoted target",
	})
}

func asyncEmptyPrivateEvidenceFixture(privateValue string) []byte {
	before := gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "1001", Tip: 0, Sealed: false, HeadSHA256: strings.Repeat("a", 64)}
	sealed := gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "1002", Tip: 0, Sealed: true, HeadSHA256: strings.Repeat("b", 64)}
	fence := operator.FenceRequest{OperationID: "source-fence-op", SourceClusterID: privateValue, StatefulSetUID: "source-sts", BindingUID: "binding", Namespace: privateValue, Targets: []operator.FenceTarget{{Pod: privateValue, PodUID: privateValue}}}
	proof := operator.FenceProof{OperationID: fence.OperationID, SourceClusterID: fence.SourceClusterID, BindingUID: fence.BindingUID, ProofID: "exact-proof", IndependentV2: &operator.IndependentEvidenceV2{StorageScope: operator.AttestationStoreScope{Provider: "gcs", Bucket: privateValue, Prefix: privateValue, Endpoint: privateValue}}}
	recovery := operator.Resource{
		Metadata: map[string]any{"bucket": privateValue, "podUID": privateValue},
		Spec:     operator.Spec{SourceClusterID: fence.SourceClusterID, RecoveryID: fence.OperationID, Durability: "async", AllowDataLoss: true, Fence: operator.Fence{RecoveryID: fence.OperationID, ClusterID: fence.SourceClusterID, StatefulSetUID: fence.StatefulSetUID, Confirmed: true, Evidence: proof.ProofID}},
		Status:   operator.Status{Phase: "Complete", Stage: "Complete", Source: fence.SourceClusterID, TargetBaseKind: "empty", SourceDurability: "async"},
	}
	return encoded(obj{
		"qualification":         "gcs-async-empty-tip-recovery",
		"profile":               obj{"provider": "gcs", "bucket": privateValue, "noPVC": true, "sourceDurability": "async", "allowDataLoss": true},
		"sourceAck":             gcsAcknowledgement{RequestID: "source-write", Slot: 1, Rows: `[[1,"gcs-async-ack-lost"]]`},
		"sourceHeadBeforeFence": before, "sourceHeadBeforeProof": before, "sealedSourceHead": sealed,
		"fence": fence, "proof": proof, "recovery": recovery,
		"sourceAckLost": true, "targetQuorumVerified": true, "emptyReadbackVerified": true, "targetAckReadbackVerified": true,
		"targetEmptyRows": "[]", "targetAck": gcsAcknowledgement{RequestID: "target-write", Slot: 1, Rows: `[[2,"gcs-async-target-ack"]]`},
		"ackToSeal": "5s", "proofSource": "RhizaFence status plus direct read-only GCS HEAD read; the generation fence proof is validated against captured workload identities",
	})
}

func TestMissingMembershipArtifactProjectionIsCaseBoundAndRedacted(t *testing.T) {
	raw := t.TempDir()
	t.Setenv("FENCING_E2E_QUALIFICATION", gcsMissingMembershipNegativeQualification)
	for name, data := range map[string][]byte{
		"actual-exit.txt": []byte("0\n"), "source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":                []byte("registry.invalid/app@sha256:" + strings.Repeat("b", 64) + "\nregistry.invalid/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
		gcsMissingMembershipEventsName: []byte("{}\n"),
		gcsMissingMembershipNegativeEvidenceName: encoded(obj{
			"qualification": "gcs-missing-membership-no-opt-in-rejection", "case": "negative", "sourceClusterID": "private-source",
			"statefulSetUID": "private-sts", "recoveryUID": "private-recovery", "phase": "Blocked",
			"sourceMembershipAbsent": true, "sourceSuccessorAbsent": true, "targetScopeUnclaimed": true, "missingMembershipOptInAbsent": true,
		}),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("strict negative projection failed: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", gcsMissingMembershipSummaryName))
	if err != nil || bytes.Contains(public, []byte("private-source")) || bytes.Contains(public, []byte("private-recovery")) {
		t.Fatalf("public summary leaked private identities or is missing: %s (%v)", public, err)
	}
	wrongCase := t.TempDir()
	t.Setenv("FENCING_E2E_QUALIFICATION", gcsMissingMembershipPositiveQualification)
	for name, data := range map[string][]byte{
		"actual-exit.txt": []byte("0\n"), "source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":                []byte("registry.invalid/app@sha256:" + strings.Repeat("b", 64) + "\nregistry.invalid/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
		gcsMissingMembershipEventsName: []byte("{}\n"),
		gcsMissingMembershipNegativeEvidenceName: encoded(obj{
			"qualification": "gcs-missing-membership-no-opt-in-rejection", "case": "negative", "sourceClusterID": "private-source",
			"statefulSetUID": "private-sts", "recoveryUID": "private-recovery", "phase": "Blocked",
			"sourceMembershipAbsent": true, "sourceSuccessorAbsent": true, "targetScopeUnclaimed": true, "missingMembershipOptInAbsent": true,
		}),
	} {
		if err := os.WriteFile(filepath.Join(wrongCase, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(wrongCase); err == nil {
		t.Fatal("accepted negative evidence under the positive selector")
	}
}

func TestMissingMembershipProjectorIsInactiveOutsideItsSelector(t *testing.T) {
	for _, selector := range []string{"", "gcs-async-nonempty", gcsManualTerminalQualification} {
		t.Run(selector, func(t *testing.T) {
			raw, public := t.TempDir(), t.TempDir()
			if err := os.Mkdir(filepath.Join(public, "public"), 0700); err != nil {
				t.Fatal(err)
			}
			seen, err := projectGCSMissingMembershipEvidence(raw, filepath.Join(public, "public"), selector, false)
			if err != nil || seen {
				t.Fatalf("unselected missing-membership projector ran without matching evidence: seen=%t err=%v", seen, err)
			}
			if err := os.WriteFile(filepath.Join(raw, gcsMissingMembershipNegativeEvidenceName), []byte(`{}`), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := projectGCSMissingMembershipEvidence(raw, filepath.Join(public, "public"), selector, false); err == nil {
				t.Fatal("accepted missing-membership evidence without its explicit selector")
			}
		})
	}
}

func TestMissingMembershipPositiveProjectorBindsFreshTargetSecretAndProof(t *testing.T) {
	data := missingMembershipPositiveEvidenceFixture()
	if err := validateGCSMissingMembershipPositiveEvidence(data); err != nil {
		t.Fatalf("rejected canonical positive evidence: %v", err)
	}
	raw := t.TempDir()
	t.Setenv("FENCING_E2E_QUALIFICATION", gcsMissingMembershipPositiveQualification)
	for name, content := range map[string][]byte{
		"actual-exit.txt": []byte("0\n"), "source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":                []byte("registry.example/db@sha256:" + strings.Repeat("b", 64) + "\nregistry.example/operator@sha256:" + strings.Repeat("c", 64) + "\n"),
		gcsMissingMembershipEventsName: []byte("{}\n"), gcsMissingMembershipPositiveEvidenceName: data,
	} {
		if err := os.WriteFile(filepath.Join(raw, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("valid positive missing-membership evidence did not project: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", gcsMissingMembershipSummaryName))
	if err != nil || bytes.Contains(public, []byte("fixture-ns")) || bytes.Contains(public, []byte("fixture-secret-uid")) || bytes.Contains(public, []byte("rhiza-0")) {
		t.Fatalf("public missing-membership summary leaks private identity or is absent: %s (%v)", public, err)
	}
	for _, mutate := range []struct {
		name string
		fn   func(obj)
	}{
		{name: "target membership uses source keys", fn: func(evidence obj) {
			target := evidence["targetMembership"].(map[string]any)
			members := evidence["sourceMembers"]
			target["membership"] = recovery.NewMembershipRecord("target-id", mustFixtureMembers(t, members), "async").Membership
		}},
		{name: "target secret belongs to another recovery", fn: func(evidence obj) {
			evidence["targetCredentials"].(map[string]any)["ownerOperationID"] = "other-recovery"
		}},
		{name: "target secret keys are not fresh", fn: func(evidence obj) {
			credentials := evidence["targetCredentials"].(map[string]any)
			targetMembers := credentials["members"].([]any)
			sourceMembers := evidence["sourceMembers"].([]any)
			targetMembers[0].(map[string]any)["public_key"] = sourceMembers[0].(map[string]any)["public_key"]
			credentials["membersSHA256"] = fmt.Sprintf("%x", sha256.Sum256(encoded(targetMembers)))
		}},
		{name: "proof names another operation", fn: func(evidence obj) {
			evidence["fence"].(map[string]any)["operationID"] = "other-operation"
		}},
		{name: "proof terminal misses captured WAL", fn: func(evidence obj) {
			proof := evidence["proof"].(map[string]any)
			independent := proof["independentV2"].(map[string]any)
			terminals := independent["terminals"].([]any)
			request := terminals[0].(map[string]any)["request"].(map[string]any)
			request["identity"].(map[string]any)["walIdentity"] = "different-wal"
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			var evidence obj
			if err := json.Unmarshal(data, &evidence); err != nil {
				t.Fatal(err)
			}
			mutate.fn(evidence)
			if err := validateGCSMissingMembershipPositiveEvidence(encoded(evidence)); err == nil {
				t.Fatal("accepted inconsistent target authorization or proof")
			}
		})
	}
}

func missingMembershipPositiveEvidenceFixture() []byte {
	const (
		sourceID  = "fixture-source"
		stsUID    = "fixture-sts-uid"
		binding   = "fixture-binding-uid"
		requestID = "fixture-missing-membership"
	)
	sourceRows := `[[1,"gcs-missing-membership-source-1"],[2,"gcs-missing-membership-source-2"],[3,"gcs-missing-membership-source-3"]]`
	var sourceMembers, targetMembers []quepaxa.Member
	var targets []operator.FenceTarget
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("rhiza-%d", i)
		var oldKey, newKey quepaxa.PublicKey
		oldKey[0], newKey[0] = byte(i+1), byte(i+11)
		sourceMembers = append(sourceMembers, quepaxa.Member{ID: quepaxa.NodeID(id), URL: "http://" + id + ":7000", PeerURL: "http://" + id + ":7001", LogURL: "http://" + id + ":7002", PublicKey: oldKey, WALIdentity: "wal-" + id})
		targetMembers = append(targetMembers, quepaxa.Member{ID: quepaxa.NodeID(id), URL: "http://" + id + ":7000", PeerURL: "http://" + id + ":7001", LogURL: "http://" + id + ":7002", PublicKey: newKey, WALIdentity: "wal-" + id})
		targets = append(targets, operator.FenceTarget{NodeID: id, Pod: id, PodUID: "pod-" + id, WALIdentity: "wal-" + id})
	}
	source := obj{"namespace": "fixture-ns", "statefulSet": "rhiza", "statefulSetUID": stsUID, "container": "database", "bindingUID": binding,
		"logicalID": "fixture-logical", "clusterID": sourceID, "bucket": "fixture-bucket", "prefix": "runs/fixture", "targets": targets}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "fixture-bucket", Prefix: "runs/fixture", ClusterID: sourceID}
	scopeHash, _ := scope.Hash()
	targetID, _ := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: sourceID, StatefulSetUID: stsUID})
	fence := operator.FenceRequest{LogicalID: "fixture-logical", BindingUID: binding, Namespace: "fixture-ns", StatefulSet: "rhiza", StatefulSetUID: stsUID,
		SourceClusterID: sourceID, OperationID: requestID, Scope: operator.ScopeGeneration, Targets: targets}
	requestHash := fmt.Sprintf("%x", sha256.Sum256(encoded(fence)))
	storageScope := operator.AttestationStoreScope{Provider: "gcs", Bucket: "fixture-bucket", Prefix: "runs/fixture"}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(storageScope)))
	terminals := make([]fencing.TerminalReference, 0, len(targets))
	for _, target := range targets {
		terminals = append(terminals, fencing.TerminalReference{EvidenceVersion: 2,
			Request: fencing.StopRequest{OperationID: "stop-" + target.NodeID, RequestHash: strings.Repeat("a", 64), Identity: fencing.Identity{
				SourceClusterID: sourceID, NodeID: target.NodeID, PodUID: target.PodUID, RunID: "run-" + target.NodeID, WALIdentity: target.WALIdentity}},
			RuntimeHash: strings.Repeat("b", 64), ProducerUID: "producer-" + target.NodeID, ProofName: "proof-" + target.NodeID,
			ProofUID: "proof-uid-" + target.NodeID, ProofDigest: strings.Repeat("c", 64), RuntimeReceiptDigest: strings.Repeat("d", 64),
			MutationReceiptDigest: strings.Repeat("e", 64), StorageScopeDigest: scopeDigest})
	}
	independent := &operator.IndependentEvidenceV2{Version: 2, RequestHash: requestHash, LedgerHash: strings.Repeat("f", 64), StorageScope: storageScope, Terminals: terminals}
	proof := operator.FenceProof{OperationID: requestID, SourceClusterID: sourceID, BindingUID: binding, RequestHash: requestHash,
		ProofID: "independent-v2-" + fmt.Sprintf("%x", sha256.Sum256(encoded(independent))), ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true, IndependentV2: independent}
	recoveryUID := "fixture-recovery-uid"
	ownerOperationID := recoveryUID + ":" + requestID
	credentialBytes := encoded(targetMembers)
	credentialHash := sha256.Sum256(credentialBytes)
	secretName := "rhiza-recovery-" + gcsShortID(targetID)
	bindingDigest := strings.Repeat("1", 64)
	resource := operator.Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: map[string]any{"uid": recoveryUID, "namespace": "fixture-ns", "labels": map[string]any{"chaos.rhiza.io/run": qualificationRunID}},
		Spec: operator.Spec{StatefulSet: "rhiza", Container: "database", SourceClusterID: sourceID, RecoveryID: requestID, Durability: "async", AllowDataLoss: true,
			AllowMissingSourceMembership: true, Fence: operator.Fence{RecoveryID: requestID, ClusterID: sourceID, StatefulSetUID: stsUID, Confirmed: true, Evidence: proof.ProofID}},
		Status: operator.Status{Phase: "Complete", Stage: "Complete", Source: sourceID, Target: targetID, TargetBaseKind: "fork", SourceDurability: "async", StatefulSetUID: stsUID,
			RecoveredTip: 5, ManifestHash: strings.Repeat("2", 64), ManualMembershipBindingDigest: bindingDigest, SecretName: secretName}}
	return encoded(obj{
		"qualification": "gcs-manual-missing-membership-nonempty-fork", "case": "positive",
		"profile": obj{"provider": "gcs", "noPVC": true, "sourceDurability": "async", "allowDataLoss": true, "allowMissingSourceMembership": true},
		"sourceAcknowledgements": []gcsAcknowledgement{{RequestID: "gcs-ha-ack-1", Slot: 1, Rows: `[[1,"gcs-missing-membership-source-1"]]`},
			{RequestID: "gcs-ha-ack-2", Slot: 2, Rows: `[[2,"gcs-missing-membership-source-2"]]`}, {RequestID: "gcs-ha-ack-3", Slot: 3, Rows: `[[3,"gcs-missing-membership-source-3"]]`}},
		"sourceRows":            sourceRows,
		"sourceHeadBeforeFence": gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "generation-before", Tip: 5, HeadSHA256: strings.Repeat("3", 64)},
		"sealedSourceHead":      gcsArchiveHeadEvidence{VersionType: int(objstore.Generation), Version: "generation-sealed", Tip: 5, Sealed: true, HeadSHA256: strings.Repeat("4", 64)},
		"fence":                 fence, "proof": proof, "sourceIdentity": source, "recovery": resource, "sourceMembershipAbsent": true,
		"sourceMembers": sourceMembers, "targetCredentials": obj{"secretName": secretName, "secretUID": "fixture-secret-uid", "resourceVersion": "17",
			"ownerOperationID": ownerOperationID, "immutable": true, "membersSHA256": fmt.Sprintf("%x", credentialHash), "members": targetMembers},
		"targetMembership": recovery.NewMembershipRecord(targetID, targetMembers, "async"), "manualMembershipBindingDigest": bindingDigest,
		"targetMembershipVerified": true, "recoveredRows": sourceRows, "targetQuorumVerified": true,
		"targetAck": gcsAcknowledgement{RequestID: "gcs-ha-ack-4", Slot: 6, Rows: `[[4,"gcs-missing-membership-target"]]`},
	})
}

func mustFixtureMembers(t *testing.T, raw any) []quepaxa.Member {
	t.Helper()
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var members []quepaxa.Member
	if err := json.Unmarshal(data, &members); err != nil {
		t.Fatal(err)
	}
	return members
}
