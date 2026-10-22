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

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
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

func TestRuntimeAgentDiagnosticsAllowRuntimeProfileAndBindingFailures(t *testing.T) {
	view := fencerDiagnosticsView{}
	view.RuntimeAgent.PodName = "rhiza-runtime-agent-abc"
	view.RuntimeAgent.PodUID = "runtime-agent-pod-uid"
	view.RuntimeAgent.Logs.Captured = true
	view.RuntimeAgent.Logs.SHA256 = strings.Repeat("a", 64)
	for _, code := range []string{"runtime_api_unavailable", "runtime_binding_invalid"} {
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
