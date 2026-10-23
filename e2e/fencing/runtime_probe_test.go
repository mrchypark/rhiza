package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKubernetesDeleteOptionsBindUIDAndZeroGrace(t *testing.T) {
	zero := int64(0)
	options, err := buildKubernetesDeleteOptions("captured-pod-uid", "", &zero)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		APIVersion         string `json:"apiVersion"`
		Kind               string `json:"kind"`
		GracePeriodSeconds *int64 `json:"gracePeriodSeconds"`
		Preconditions      struct {
			UID string `json:"uid"`
		} `json:"preconditions"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil || decoded.APIVersion != "v1" || decoded.Kind != "DeleteOptions" ||
		decoded.GracePeriodSeconds == nil || *decoded.GracePeriodSeconds != 0 || decoded.Preconditions.UID != "captured-pod-uid" {
		t.Fatalf("delete options lost captured UID or zero grace: %s (%v)", body, err)
	}
	if _, err := buildKubernetesDeleteOptions("", "", &zero); err == nil {
		t.Fatal("delete options accepted a missing UID precondition")
	}
}

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

func TestRuntimeProbePreservesRejectedEnrollmentAsFailureProgress(t *testing.T) {
	for _, tc := range []struct{ stage, code string }{
		{"authorization", "token_review_failed"},
		{"transport", "unavailable"},
		{"response", "http_rejected"},
		{"response", "decode_invalid"},
		{"binding", "mismatch"},
	} {
		evidence := &runtimeComponentEvidence{}
		err := applyRuntimeProbeProgress(evidence, &runtimeProbeProgress{EnrollmentStage: tc.stage, EnrollmentCode: tc.code})
		if err == nil || err.Error() != "runtime-agent enrollment rejected at "+tc.stage+"/"+tc.code {
			t.Fatalf("enrollment failure progress error=%v for %s/%s", err, tc.stage, tc.code)
		}
		if evidence.EnrollmentCaptured || evidence.EnrollmentDiagnosticStage != tc.stage || evidence.EnrollmentDiagnosticCode != tc.code {
			t.Fatalf("rejected enrollment was lost or treated as captured: %+v", evidence)
		}
	}
	bad := &runtimeComponentEvidence{}
	if err := applyRuntimeProbeProgress(bad, &runtimeProbeProgress{EnrollmentCaptured: true, EnrollmentStage: "authorization", EnrollmentCode: "token_review_failed"}); err == nil || bad.EnrollmentCaptured || bad.EnrollmentDiagnosticCode != "" {
		t.Fatalf("invalid progress claimed both enrollment and rejection: evidence=%+v err=%v", bad, err)
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
			"qualification": "runtime-agent-component-only", "outcome": "failed", "failureStage": "enrollment",
			"enrollmentCaptured": false, "enrollmentDiagnosticStage": "authorization", "enrollmentDiagnosticCode": "token_review_failed",
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
	if evidence.EnrollmentCaptured || evidence.EnrollmentDiagnosticStage != "authorization" || evidence.EnrollmentDiagnosticCode != "token_review_failed" {
		t.Fatalf("public projection lost enrollment failure progress: %+v", evidence)
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
