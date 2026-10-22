package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	write("fencer-diagnostics.json", encoded(obj{"context": "kind-rhiza-generic-fencing", "namespace": "rhiza-fencing-e2e", "podsCaptured": true, "eventsCaptured": true, "logs": obj{"captured": true, "sha256": strings.Repeat("e", 64), "bytes": 123, "lines": 4}, "token": canary}))
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
	var resources []obj
	data, err := os.ReadFile(filepath.Join(raw, "public", "resources-summary.json"))
	if err != nil || json.Unmarshal(data, &resources) != nil || len(resources) != 2 || resources[0]["uid"] != "uid" {
		t.Fatal("resource summary lost audit identity")
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
