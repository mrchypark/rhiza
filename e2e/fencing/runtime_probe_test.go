package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeProbeRecordsEnrollmentAndSafeStopStageBeforePending(t *testing.T) {
	progress := &runtimeProbeProgress{EnrollmentCaptured: true, StopStage: "pre_container_scope", StopCode: "process_scope_unverified", Pending: true}
	data, err := json.Marshal(progress)
	if err != nil || strings.Contains(string(data), "backend") || strings.Contains(string(data), "PID") {
		t.Fatalf("progress contains unexpected detail: %s %v", data, err)
	}
	evidence := &runtimeComponentEvidence{}
	if err := applyRuntimeProbeProgress(evidence, progress); err == nil || err.Error() != "runtime-agent stop pending at pre_container_scope/process_scope_unverified" {
		t.Fatalf("pending progress error=%v", err)
	}
	if !evidence.EnrollmentCaptured || evidence.StopDiagnosticStage != progress.StopStage || evidence.StopDiagnosticCode != progress.StopCode {
		t.Fatalf("progress was not preserved in probe evidence: %+v", evidence)
	}
}

func TestRuntimeProbeRejectsUnallowlistedStopDiagnostic(t *testing.T) {
	evidence := &runtimeComponentEvidence{}
	err := applyRuntimeProbeProgress(evidence, &runtimeProbeProgress{EnrollmentCaptured: true, StopStage: "internal", StopCode: "secret/path", Pending: true})
	if err == nil || strings.Contains(err.Error(), "secret/path") || evidence.StopDiagnosticCode != "" {
		t.Fatalf("untrusted stop category was exposed: evidence=%+v err=%v", evidence, err)
	}
}

func TestRuntimeProbeStopDiagnosticSurvivesPublicArtifactProjection(t *testing.T) {
	const private = "private-backend-error /proc/4321"
	raw := t.TempDir()
	for name, data := range map[string][]byte{
		"actual-exit.txt": []byte("1\n"),
		"source-head.txt": []byte(strings.Repeat("a", 40) + "\n"),
		"image-ids.txt":   []byte("sha256:" + strings.Repeat("b", 64) + "\n"),
		"runtime-component-probe.json": encoded(obj{
			"qualification": "runtime-agent-component-only", "outcome": "failed", "failureStage": "authenticated-stop",
			"enrollmentCaptured": true, "stopDiagnosticStage": "pre_container_scope", "stopDiagnosticCode": "process_scope_unverified",
			"backendError": private, "procPath": private,
		}),
	} {
		if err := os.WriteFile(filepath.Join(raw, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatal(err)
	}
	projected, err := os.ReadFile(filepath.Join(raw, "public", "runtime-component-probe.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(projected, []byte(private)) {
		t.Fatalf("public runtime probe leaked private backend detail: %s", projected)
	}
	var evidence runtimeComponentEvidence
	if err := json.Unmarshal(projected, &evidence); err != nil {
		t.Fatal(err)
	}
	if !evidence.EnrollmentCaptured || evidence.StopDiagnosticStage != "pre_container_scope" || evidence.StopDiagnosticCode != "process_scope_unverified" {
		t.Fatalf("public projection lost stop progress: %+v", evidence)
	}
}

func TestRuntimeAgentOwnerPin(t *testing.T) {
	pod := obj{
		"metadata": obj{
			"uid":             "pod-uid",
			"labels":          obj{"app.kubernetes.io/name": runtimeAgentDSName},
			"ownerReferences": []any{obj{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": runtimeAgentDSName, "uid": "daemonset-uid", "controller": true}},
		},
		"spec": obj{"nodeName": "kind-node"},
	}
	if !runtimeAgentOwnedBy(pod, "kind-node", "daemonset-uid") {
		t.Fatal("restarted agent Pod with pinned owner was rejected")
	}
	tests := []struct {
		name   string
		mutate func(obj)
	}{
		{"wrong DaemonSet UID", func(p obj) {
			p["metadata"].(obj)["ownerReferences"] = []any{obj{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": runtimeAgentDSName, "uid": "other", "controller": true}}
		}},
		{"not controller-owned", func(p obj) { p["metadata"].(obj)["ownerReferences"].([]any)[0].(obj)["controller"] = false }},
		{"wrong node", func(p obj) { p["spec"].(obj)["nodeName"] = "other-node" }},
		{"terminating", func(p obj) { p["metadata"].(obj)["deletionTimestamp"] = "now" }},
		{"missing owner", func(p obj) { p["metadata"].(obj)["ownerReferences"] = []any{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := encodedClone(pod)
			tt.mutate(candidate)
			if runtimeAgentOwnedBy(candidate, "kind-node", "daemonset-uid") {
				t.Fatal("accepted agent Pod without exact live DaemonSet ownership")
			}
		})
	}
}

func TestProbeUIDCleanupDoesNotAcceptSameNameReplacement(t *testing.T) {
	if probeUIDGone("original", obj{"metadata": obj{"uid": "original"}}) {
		t.Fatal("cleanup considered the original UID gone while it remains")
	}
	if !probeUIDGone("original", obj{"metadata": obj{"uid": "replacement"}}) {
		t.Fatal("cleanup failed to distinguish a same-name replacement UID")
	}
	if probeUIDGone("", obj{"metadata": obj{"uid": "replacement"}}) || probeUIDGone("original", nil) {
		t.Fatal("cleanup accepted missing identity")
	}
}

func TestProbeDeletePathUsesResourceAPIGroup(t *testing.T) {
	for _, test := range []struct {
		resource string
		want     string
	}{
		{"pods", "/api/v1/namespaces/rhiza-fencing-e2e/pods/probe"},
		{"statefulsets", "/apis/apps/v1/namespaces/rhiza-fencing-e2e/statefulsets/probe"},
	} {
		got, err := probeDeletePath("rhiza-fencing-e2e", test.resource, "probe")
		if err != nil || got != test.want {
			t.Errorf("probeDeletePath(%q) = %q, %v; want %q", test.resource, got, err, test.want)
		}
	}
	if _, err := probeDeletePath("rhiza-fencing-e2e", "deployments", "probe"); err == nil {
		t.Fatal("unsupported resource accepted for UID-preconditioned deletion")
	}
}

func TestChangedRequestHashDiffers(t *testing.T) {
	for _, hash := range []string{"0", "abcdef"} {
		if changed := changedRequestHash(hash); changed == hash || len(changed) != len(hash) {
			t.Fatalf("changed request hash is not a same-length distinct value: %q -> %q", hash, changed)
		}
	}
}

func encodedClone(value obj) obj {
	data := encoded(value)
	var clone obj
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}
