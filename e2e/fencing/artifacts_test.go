package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
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
	privateAuthorityLog := "fencer startup detail " + canary + "\n"
	authorityLogDigest := sha256.Sum256([]byte(privateAuthorityLog))
	privateAgentLog := "2026/10/22 00:00:00 runtime-agent enrollment rejected stage=cri_enroll code=containerd_verbose_schema_mismatch\n" +
		"2026/10/22 00:00:01 runtime-agent enrollment rejected stage=authorization code=token_review_failed\n" +
		"2026/10/22 00:00:02 runtime-agent enrollment rejected stage=cri_enroll code=containerd_verbose_schema_mismatch private=" + canary + "\n"
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
		"logs":         obj{"captured": true, "sha256": hex.EncodeToString(authorityLogDigest[:]), "bytes": len([]byte(privateAuthorityLog)), "lines": 1},
		"runtimeAgent": obj{"podName": "rhiza-runtime-agent-abc", "podUID": "runtime-agent-pod-uid", "logs": obj{"captured": true, "sha256": hex.EncodeToString(agentLogDigest[:]), "bytes": len([]byte(privateAgentLog)), "lines": 3}, "rawLog": canary}, "token": canary}))
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
	if err := os.WriteFile(filepath.Join(raw, "private-fencer-diagnostics", "authority-current.log"), []byte(privateAuthorityLog), 0600); err != nil {
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
		len(diagnostics.RuntimeAgent.Diagnostics) != 2 || diagnostics.RuntimeAgent.Diagnostics[0] != (runtimeAgentEnrollmentDiagnostic{Stage: "cri_enroll", Code: "containerd_verbose_schema_mismatch"}) ||
		diagnostics.RuntimeAgent.Diagnostics[1] != (runtimeAgentEnrollmentDiagnostic{Stage: "authorization", Code: "token_review_failed"}) {
		t.Fatalf("runtime-agent log hash projection failed: %v", err)
	}
	var resources []obj
	data, err := os.ReadFile(filepath.Join(raw, "public", "resources-summary.json"))
	if err != nil || json.Unmarshal(data, &resources) != nil || len(resources) != 2 || resources[0]["uid"] != "uid" {
		t.Fatal("resource summary lost audit identity")
	}
}

func TestFencerRejectionProjectionUsesFixedCodesOnly(t *testing.T) {
	const canary = "private-error-token-canary"
	privateLog := strings.Join([]string{
		"2026/10/24 09:59:59 fencing supervisor authentication rejected: action=register podUID=private-pod-uid error=fencing: Pod source differs",
		"2026/10/24 10:00:00 runtime enrollment pending: podUID=private-pod-uid runID=private-run-id error=fencing: runtime enrollment rejected at client/node_lookup_failed",
		"2026/10/24 10:00:00 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: startup recovery child differs from assignment",
		"2026/10/24 10:00:01 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=unrecognized private failure " + canary,
		"2026/10/24 10:00:02 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register root-init rejected: backend failure " + canary,
		"2026/10/24 10:00:03 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register startup-grant rejected: backend failure " + canary,
		"2026/10/24 10:00:04 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register ledger-register rejected: backend failure " + canary,
		"2026/10/24 10:00:05 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register producer-reserve rejected: backend failure " + canary,
		"2026/10/24 10:00:06 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register archive-scope rejected: backend failure " + canary,
		"2026/10/24 10:00:07 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register startup-grant rejected: fencing: standalone learner incarnation is not current in canonical history",
		"2026/10/24 10:00:08 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: static startup peer token is missing",
		"2026/10/24 10:00:09 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register root-init rejected: fencing: root-init phase=create cause=permission: backend failure " + canary,
		"2026/10/24 10:00:10 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register root-init rejected: fencing: root-init phase=untrusted cause=private: " + canary,
		// These handler lines are copied from the actual capture emitted by
		// TestTransientRootInitRegistrationLogsDiagnosisAndReturns503.
		"2026/10/24 18:46:31 fencing supervisor transition rejected: action=register podUID=pod-0 error=fencing: register root-init rejected: fencing: root-init phase=load cause=timeout: context deadline exceeded",
		"2026/10/24 18:46:31 fencing supervisor transition rejected: action=register podUID=pod-0 error=fencing: register root-init rejected: fencing: root-init phase=legacy_check cause=legacy_check_unavailable: cannot verify legacy cooperative ledger before canonical root bootstrap\nkubernetes API returned HTTP 503",
		"2026/10/24 10:00:13 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register root-init rejected: fencing: root-init phase=legacy_check cause=legacy_exists: legacy cooperative ledger exists; refusing canonical root bootstrap " + canary,
		"2026/10/24 10:00:02 fencing supervisor transition rejected: action=bind podUID=private-pod-uid error=fencing: startup recovery child differs from assignment",
		"2026/10/24 10:00:02 fencing supervisor authentication rejected: action=bind podUID=private-pod-uid error=fencing: Pod source differs",
		"2026/10/24 10:00:03 unrelated log fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: startup root binding is unavailable",
	}, "\n") + "\n"
	digest := sha256.Sum256([]byte(privateLog))
	raw := t.TempDir()
	diagnosticsDir := filepath.Join(raw, "private-fencer-diagnostics")
	if err := os.Mkdir(diagnosticsDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diagnosticsDir, "authority-current.log"), []byte(privateLog), 0600); err != nil {
		t.Fatal(err)
	}
	input := encoded(fencerDiagnosticsView{})
	var view fencerDiagnosticsView
	if err := json.Unmarshal(input, &view); err != nil {
		t.Fatal(err)
	}
	view.Logs.Captured = true
	view.Logs.SHA256 = hex.EncodeToString(digest[:])
	view.Logs.Bytes = int64(len([]byte(privateLog)))
	view.Logs.Lines = int64(strings.Count(privateLog, "\n"))
	projected, err := projectRuntimeAgentDiagnostics(raw, encoded(view))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(projected, []byte(canary)) || bytes.Contains(projected, []byte("private-pod-uid")) || bytes.Contains(projected, []byte("pod-0")) || bytes.Contains(projected, []byte("context deadline exceeded")) || bytes.Contains(projected, []byte("kubernetes API returned HTTP 503")) || bytes.Contains(projected, []byte("startup recovery child differs")) {
		t.Fatal("public supervisor diagnostics leaked private log contents")
	}
	var got fencerDiagnosticsView
	if err := json.Unmarshal(projected, &got); err != nil {
		t.Fatal(err)
	}
	want := []supervisorRejectionDiagnostic{
		{Stage: "authentication", Action: "register", Code: "authentication_source_mismatch"},
		{Stage: "runtime_enrollment", Action: "register", Code: "runtimeagent_client_node_lookup_failed"},
		{Stage: "transition", Action: "register", Code: "replacement_recovery_child_mismatch"},
		{Stage: "transition", Action: "register", Code: "register_guard_unknown"},
		{Stage: "transition", Action: "register", Code: "register_root_init_rejected"},
		{Stage: "transition", Action: "register", Code: "register_startup_grant_rejected"},
		{Stage: "transition", Action: "register", Code: "register_ledger_register_rejected"},
		{Stage: "transition", Action: "register", Code: "register_producer_reserve_rejected"},
		{Stage: "transition", Action: "register", Code: "register_archive_scope_rejected"},
		{Stage: "transition", Action: "register", Code: "standalone_learner_history_not_current"},
		{Stage: "transition", Action: "register", Code: "static_startup_credential_rejected"},
		{Stage: "transition", Action: "register", Code: "register_root_init_create_permission"},
		{Stage: "transition", Action: "register", Code: "register_root_init_rejected"},
		{Stage: "transition", Action: "register", Code: "register_root_init_load_timeout"},
		{Stage: "transition", Action: "register", Code: "register_root_init_legacy_check_legacy_check_unavailable"},
		{Stage: "transition", Action: "register", Code: "register_root_init_legacy_check_legacy_exists"},
	}
	if len(got.Logs.Rejections) != len(want) {
		t.Fatalf("unexpected projected supervisor rejections: %+v", got.Logs.Rejections)
	}
	for i := range want {
		if got.Logs.Rejections[i] != want[i] {
			t.Fatalf("rejection %d = %+v, want %+v", i, got.Logs.Rejections[i], want[i])
		}
	}
	if _, err := safeJSON("fencer-diagnostics.json", projected); err != nil {
		t.Fatalf("projected rejection codes failed allowlist validation: %v", err)
	}
}

func TestFencerStartupGrantRejectionProjectionUsesFixedCodes(t *testing.T) {
	const canary = "private-backend-detail-canary"
	tests := []struct {
		name      string
		sourceErr string
		wantCode  string
	}{
		{"startup authority unavailable", "fencing: startup authority unavailable", "startup_authority_unavailable"},
		{"learner identity label differs", "fencing: learner identity label differs", "learner_identity_label_mismatch"},
		{"learner logical identity unassigned", "fencing: learner logical identity is not yet assigned by canonical authority", "learner_logical_identity_unassigned"},
		{"automatic control store unavailable", "fencing: canonical automatic control store is unavailable", "automatic_control_store_unavailable"},
		{"learner StatefulSet owner differs", "fencing: learner StatefulSet owner differs", "learner_statefulset_owner_mismatch"},
		{"learner root identity ambiguous", "fencing: learner root identity is ambiguous", "learner_root_identity_ambiguous"},
		{"automatic learner root unavailable", "fencing: learner automatic root is unavailable", "learner_automatic_root_unavailable"},
		{"learner control record unavailable", "fencing: canonical learner control record unavailable: " + canary, "learner_control_record_unavailable"},
		{"automatic learner binding hash differs", "fencing: automatic learner binding hash differs", "automatic_learner_binding_hash_mismatch"},
		{"pending-removal assignment missing", "fencing: learner is not in a durable pending-removal assignment", "learner_pending_removal_assignment_missing"},
		{"learner removal proof cohort mismatch", "fencing: learner removal proof is not bound to the canonical cohort", "learner_removal_proof_cohort_mismatch"},
		{"unjournaled learner recovery child ambiguous", "fencing: unjournaled learner recovery child is ambiguous", "learner_recovery_child_ambiguous"},
		{"learner recovery child intent mismatch", "fencing: learner recovery child differs from canonical intent", "learner_recovery_child_intent_mismatch"},
		{"completed learner membership not canonical", "fencing: completed learner membership is not canonical: " + canary, "learner_membership_not_canonical"},
		{"learner operation ownership differs", "fencing: learner operation ownership differs", "learner_operation_ownership_mismatch"},
		{"learner binding missing", "fencing: learner binding missing", "learner_binding_missing"},
		{"learner root binding differs", "fencing: learner root binding differs", "learner_root_binding_mismatch"},
		{"learner credentials changed", "fencing: learner credentials changed", "learner_credentials_changed"},
		{"learner member differs", "fencing: learner member differs", "learner_member_mismatch"},
		{"static startup workload not enrolled", "fencing: static startup workload is not enrolled", "static_startup_workload_not_enrolled"},
		{"canonical terminal authority unavailable", "fencing: canonical terminal authority is unavailable", "canonical_terminal_authority_unavailable"},
		{"canonical terminal authority unavailable with cause", "fencing: canonical terminal authority is unavailable: " + canary, "canonical_terminal_authority_unavailable"},
		{"canonical old-voter closure differs", "fencing: canonical old-voter closure differs", "canonical_old_voter_closure_mismatch"},
		{"canonical old-voter terminal proof differs", "fencing: canonical old-voter terminal proof differs: " + canary, "canonical_old_voter_terminal_proof_mismatch"},
		{"canonical old-voter terminal coverage differs", "fencing: canonical old-voter terminal coverage differs: " + canary, "canonical_old_voter_terminal_coverage_mismatch"},
		{"canonical old-voter run not terminally closed", "fencing: canonical old-voter run is not terminally closed", "canonical_old_voter_run_not_terminally_closed"},
		{"canonical old-voter terminal evidence unavailable", "fencing: canonical old-voter terminal evidence is unavailable", "canonical_old_voter_terminal_evidence_unavailable"},
		{"canonical old-voter terminal run missing", "fencing: canonical old-voter terminal run is missing", "canonical_old_voter_terminal_run_missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line := "2026/10/24 10:00:00 fencing supervisor transition rejected: action=register podUID=private-pod-uid error=fencing: register startup-grant rejected: " + tt.sourceErr
			got, ok := classifySupervisorRejection(line)
			want := supervisorRejectionDiagnostic{Stage: "transition", Action: "register", Code: tt.wantCode}
			if !ok || got != want {
				t.Fatalf("classified rejection = %+v, %t; want %+v", got, ok, want)
			}
			view := fencerDiagnosticsView{}
			view.Logs.Captured = true
			view.Logs.Rejections = []supervisorRejectionDiagnostic{got}
			public, err := safeJSON("fencer-diagnostics.json", encoded(view))
			if err != nil || bytes.Contains(public, []byte(canary)) || bytes.Contains(public, []byte("private-pod-uid")) {
				t.Fatalf("safe rejection projection failed or leaked private values: %v", err)
			}
		})
	}
}

func TestFencerRejectionProjectionIsBounded(t *testing.T) {
	var lines []string
	for i := 0; i < maxSupervisorRejectionDiagnostics+2; i++ {
		lines = append(lines, fmt.Sprintf("2026/10/24 10:00:00 fencing supervisor transition rejected: action=register podUID=private-%d error=unknown", i))
	}
	privateLog := strings.Join(lines, "\n") + "\n"
	digest := sha256.Sum256([]byte(privateLog))
	raw := t.TempDir()
	diagnosticsDir := filepath.Join(raw, "private-fencer-diagnostics")
	if err := os.Mkdir(diagnosticsDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diagnosticsDir, "authority-current.log"), []byte(privateLog), 0600); err != nil {
		t.Fatal(err)
	}
	view := fencerDiagnosticsView{}
	view.Logs.Captured = true
	view.Logs.SHA256 = hex.EncodeToString(digest[:])
	view.Logs.Bytes = int64(len([]byte(privateLog)))
	view.Logs.Lines = int64(strings.Count(privateLog, "\n"))
	projected, err := projectRuntimeAgentDiagnostics(raw, encoded(view))
	if err != nil {
		t.Fatal(err)
	}
	var got fencerDiagnosticsView
	if err := json.Unmarshal(projected, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Logs.Rejections) != maxSupervisorRejectionDiagnostics || !got.Logs.RejectionsTruncated {
		t.Fatalf("rejection limit was not reported: count=%d truncated=%t", len(got.Logs.Rejections), got.Logs.RejectionsTruncated)
	}
}

func TestVoterStartupDiagnosticsAreIdentityBoundAndRedacted(t *testing.T) {
	const canary = "private-condition-message-token-canary"
	raw := t.TempDir()
	diagnostics := filepath.Join(raw, "private-voter-diagnostics")
	if err := os.Mkdir(diagnostics, 0700); err != nil {
		t.Fatal(err)
	}
	voters := []obj{}
	for ordinal := 0; ordinal < 3; ordinal++ {
		name := fmt.Sprintf("rhiza-%d", ordinal)
		dir := filepath.Join(diagnostics, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		identity, captured := "verified", true
		log := "rhiza: open WAL: private path and token=" + canary + "\n"
		phase, ready := "Running", false
		if ordinal == 1 {
			identity, captured = "identity_drift", false
			log = canary
		}
		if ordinal == 2 {
			log = "rhiza: unexpected startup failure " + canary + "\n"
		}
		for _, suffix := range []string{"current", "previous"} {
			if err := os.WriteFile(filepath.Join(dir, suffix+".log"), []byte(log), 0600); err != nil {
				t.Fatal(err)
			}
			if captured {
				if err := os.WriteFile(filepath.Join(dir, suffix+".captured"), []byte("true\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
		if ordinal == 1 {
			ready = true
		}
		conditionStatus := "False"
		if ready {
			conditionStatus = "True"
		}
		voters = append(voters, obj{
			"ordinal": fmt.Sprint(ordinal), "podUID": "private-pod-uid-" + fmt.Sprint(ordinal), "identity": identity, "logsCaptured": captured,
			"status": obj{"phase": phase, "conditions": []obj{{"type": "Ready", "status": conditionStatus, "reason": "ContainersNotReady", "message": canary}},
				"containers": []obj{{"ready": ready, "restartCount": 3, "state": obj{"waiting": obj{"reason": "CrashLoopBackOff", "message": canary}},
					"lastState": obj{"terminated": obj{"reason": "Error", "exitCode": 2, "signal": 9}}}}},
		})
	}
	projected, err := projectVoterStartupDiagnostics(raw, encoded(obj{"statefulSetIdentity": "pinned", "statefulSetUID": "private-sts-uid", "voters": voters}))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(projected, []byte(canary)) || bytes.Contains(projected, []byte("private-pod-uid")) || bytes.Contains(projected, []byte("private-sts-uid")) {
		t.Fatal("public voter diagnostics leaked private message or identity")
	}
	var view voterStartupPublicView
	if err := json.Unmarshal(projected, &view); err != nil {
		t.Fatal(err)
	}
	if !view.StatefulSetPinned || len(view.Voters) != 3 {
		t.Fatalf("pinned voter diagnostics are incomplete: %+v", view)
	}
	if !view.Voters[0].CurrentLog.Captured || !contains(view.Voters[0].CurrentLog.Startup, "wal_open_failed") {
		t.Fatalf("known startup prefix was not classified: %+v", view.Voters[0].CurrentLog)
	}
	if !view.Voters[0].PreviousLog.Captured || view.Voters[0].Containers[0].RestartCount != 3 || view.Voters[0].Containers[0].LastExitCode == nil || *view.Voters[0].Containers[0].LastExitCode != 2 {
		t.Fatalf("bounded previous log/container state was not projected: %+v", view.Voters[0])
	}
	if view.Voters[1].CurrentLog.Captured || view.Voters[1].Identity != "identity_drift" {
		t.Fatalf("identity-drift logs were not discarded: %+v", view.Voters[1])
	}
	if !contains(view.Voters[2].CurrentLog.Startup, "unknown_startup_error") {
		t.Fatalf("unknown startup failure was not safely categorized: %+v", view.Voters[2].CurrentLog)
	}
}

func TestVoterStartupStageProjectionUsesExactWrappersAndFixedCodes(t *testing.T) {
	const canary = "private-startup-error-canary"
	logs := strings.Join([]string{
		"2026/10/24 10:00:00 open rhiza: connect fencing startup gate: fencing: invalid startup grant: " + canary,
		"2026/10/24 10:00:01 open rhiza: open WAL: /private/path/" + canary,
		"2026/10/24 10:00:02 open rhiza: prepare source retry directory: /private/path/" + canary,
		"2026/10/24 10:00:03 open rhiza: read canonical archive genesis: " + canary,
		"2026/10/24 10:00:04 open rhiza: create QuePaxa core: " + canary,
		"2026/10/24 10:00:05 open rhiza: ensure voter identity: fencing: runtime enrollment identity differs: " + canary,
		"2026/10/24 10:00:06 open rhiza: ensure voter identity: read voter registration: " + canary,
		"2026/10/24 10:00:07 open rhiza: ensure voter identity: " + canary,
		"not-a-timestamp open rhiza: open WAL: " + canary,
	}, "\n") + "\n"
	logPath := filepath.Join(t.TempDir(), "current.log")
	if err := os.WriteFile(logPath, []byte(logs), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(logPath), "current.captured"), []byte("true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	view := readVoterLog(filepath.Dir(logPath), "current", true)
	if !view.Captured || len(view.StartupStages) != 8 || view.StartupStagesTruncated {
		t.Fatalf("startup stage projection is incomplete: %+v", view)
	}
	want := []string{
		"startup_grant/response_invalid",
		"wal/open_failed",
		"source_mutation/retry_directory_failed",
		"archive_genesis/read_failed",
		"consensus/core_create_failed",
		"wal_binding/runtime_identity_mismatch",
		"voter_identity/remote_read_failed",
		"voter_identity/guard_unknown",
	}
	for i := range want {
		if view.StartupStages[i] != want[i] {
			t.Fatalf("startup stage %d = %q, want %q", i, view.StartupStages[i], want[i])
		}
	}
	public, err := json.Marshal(view)
	if err != nil || bytes.Contains(public, []byte(canary)) || bytes.Contains(public, []byte("/private/path")) {
		t.Fatalf("startup stage projection leaked private error text: %v", err)
	}
	if got := classifyVoterStartupLog([]byte(logs)); !contains(got, "supervisor_admission_failed") || !contains(got, "node_open_failed") {
		t.Fatalf("legacy categories no longer preserve overlapping wrapper classifications: %v", got)
	}
}

func TestVoterStartupStageProjectionDistinguishesUnparsedOuterWrapper(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "current.log")
	if err := os.WriteFile(logPath, []byte("not-a-timestamp open rhiza: open WAL: private\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(logPath), "current.captured"), []byte("true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	view := readVoterLog(filepath.Dir(logPath), "current", true)
	if len(view.StartupStages) != 1 || view.StartupStages[0] != "node_open/outer_wrapper_unparsed" {
		t.Fatalf("unparsed outer log wrapper was not distinguished: %+v", view.StartupStages)
	}
}

func TestVoterStartupStageProjectionClassifiesIdentityAndArchiveBoundaries(t *testing.T) {
	const private = "private-node-identity-canary"
	logs := strings.Join([]string{
		"2026/10/24 10:00:00 open rhiza: load voter identity: voter state continuity cannot be established: invalid local identity: " + private,
		"2026/10/24 10:00:01 open rhiza: ensure voter identity: voter state continuity cannot be established: voter " + private + " is registered but its local identity is absent",
		"2026/10/24 10:00:02 open rhiza: load shared decision archive: load shared decision archive: " + private,
	}, "\n") + "\n"
	stages, truncated := classifyVoterStartupStages([]byte(logs))
	want := []string{
		"voter_identity/local_identity_invalid",
		"voter_identity/registration_without_local_identity",
		"archive/load_failed",
	}
	if truncated || len(stages) != len(want) {
		t.Fatalf("identity/archive stage set = %v (truncated=%v), want %v", stages, truncated, want)
	}
	for i := range want {
		if stages[i] != want[i] {
			t.Fatalf("stage %d = %q, want %q", i, stages[i], want[i])
		}
	}
	public, err := json.Marshal(stages)
	if err != nil || bytes.Contains(public, []byte(private)) {
		t.Fatalf("fixed stage codes leaked nested error text: %v", err)
	}
}

func TestVoterStartupStageProjectionPreservesPhaseAndNestedStage(t *testing.T) {
	const canary = "private-phase-error-canary"
	logs := strings.Join([]string{
		"2026/10/24 10:00:00 open rhiza: node open phase identity_storage: ensure voter identity: fencing: runtime enrollment identity differs: " + canary,
		"2026/10/24 10:00:01 open rhiza: node open phase network: " + canary,
	}, "\n") + "\n"
	stages, truncated := classifyVoterStartupStages([]byte(logs))
	want := []string{"node_open/phase_identity_storage", "wal_binding/runtime_identity_mismatch", "node_open/phase_network", "node_open/inner_error_unknown"}
	if truncated || len(stages) != len(want) {
		t.Fatalf("phase/nested stage set = %v (truncated=%v), want %v", stages, truncated, want)
	}
	for i := range want {
		if stages[i] != want[i] {
			t.Fatalf("stage %d = %q, want %q", i, stages[i], want[i])
		}
	}
	public, err := json.Marshal(stages)
	if err != nil || bytes.Contains(public, []byte(canary)) {
		t.Fatalf("fixed phase projection leaked nested error text: %v", err)
	}
}

func TestVoterStartupStageProjectionIsBounded(t *testing.T) {
	prefixes := []string{
		"connect fencing startup gate: fencing: invalid startup grant:",
		"fencing: pending-removal startup requires an explicit nonvoting learner configuration",
		"fencing: bind receipt identity differs",
		"fencing: bind role differs from startup grant",
		"fencing: WAL identity cannot change",
		"fencing: authenticated WAL binding differs",
		"fencing: source producer binding changed",
		"fencing: supervisor returned HTTP 403",
		"open WAL:",
		"open object store:",
	}
	var lines []string
	for i, prefix := range prefixes {
		lines = append(lines, fmt.Sprintf("2026/10/24 10:00:%02d open rhiza: %s private", i, prefix))
	}
	stages, truncated := classifyVoterStartupStages([]byte(strings.Join(lines, "\n")))
	if len(stages) != maxVoterStartupStages || !truncated {
		t.Fatalf("startup stage projection was not bounded: stages=%v truncated=%v", stages, truncated)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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

func TestRunScriptPinsVoterStartupEvidenceAcrossScaleAndLogFetch(t *testing.T) {
	script, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	pin := strings.Index(source, "fencing_source_sts_uid=$(k --request-timeout=10s get statefulset rhiza")
	scale := strings.Index(source, "k scale statefulset rhiza --replicas=3")
	if pin < 0 || scale < 0 || pin > scale {
		t.Fatal("source StatefulSet UID must be pinned before scaling")
	}
	collectorStart := strings.Index(source, "capture_voter_startup_diagnostics() (\n")
	if collectorStart < 0 {
		t.Fatal("voter diagnostic collector boundary is missing")
	}
	collectorEnd := strings.Index(source[collectorStart:], "\ncapture_fencer_diagnostics() {")
	if collectorEnd < 0 {
		t.Fatal("voter diagnostic collector boundary is missing")
	}
	collector := source[collectorStart : collectorStart+collectorEnd]
	umask := strings.Index(collector, "\n  umask 077\n")
	privateOutput := strings.Index(collector, `>"$FENCING_E2E_OUT/voter-startup-private.json"`)
	if umask < 0 || privateOutput < 0 || umask > privateOutput {
		t.Fatal("private voter diagnostic umask must be set before any private output is created")
	}
	for _, required := range []string{
		"for ordinal in 0 1 2",
		"--arg owner \"$pinned_uid\"",
		".controller == true and .kind == \"StatefulSet\" and .name == \"rhiza\" and .uid == $owner",
		"--tail=80 --limit-bytes=16384",
		"after_container=",
		"after_restarts=",
		"identity_drift",
		"private-voter-diagnostics",
		"chmod 600 \"$FENCING_E2E_OUT/voter-startup-private.json\"",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("voter startup collector is missing identity/bounds guard %q", required)
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
	var fence operator.FenceRequest
	if err := json.Unmarshal(encoded(evidence["fence"]), &fence); err != nil {
		t.Fatal(err)
	}
	for file, data := range map[string][]byte{
		gcsManualTerminalEvidenceName: evidenceData,
		gcsManualTerminalChallengeName: encoded(gcsManualTerminalChallengeRecord{Challenge: challenge, ChallengeDigest: digest,
			AttestationName: name, VerifiedContext: manualTerminalVerifiedContextFixture(challenge, fence)}),
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

func TestManualTerminalChallengeWaitsForVerifiedReviewContext(t *testing.T) {
	source, err := os.ReadFile("gcs_ha_manual_terminal.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	rootCheck := strings.Index(text, "sourceRoot, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)")
	requestCheck := strings.Index(text, "if hex.EncodeToString(fenceDigest[:]) != challenge.FenceRequestHash")
	unclaimedCheck := strings.Index(text, "verifyManualTerminalUnclaimed(ctx, archive, controlBucket, source, challenge, successorClaimKey, targetPrefix, controlPrefix)")
	writePacket := strings.Index(text, "writeNewPrivateAckLossFile(challengePath, challengeBytes)")
	notify := strings.Index(text, `event("independent_manual_approval_required"`)
	approvalWait := strings.Index(text, "waitForManualTerminalApproval(ctx, challenge")
	if rootCheck < 0 || requestCheck < rootCheck || unclaimedCheck < requestCheck || writePacket < unclaimedCheck || notify < writePacket || approvalWait < notify {
		t.Fatal("manual approval must be notified only after the closed root, exact request, and unclaimed scopes are verified")
	}
	challenge := manualTerminalChallengeFixture("private-scope")
	var evidence obj
	if err := json.Unmarshal(manualTerminalPrivateEvidenceFixture("private-scope"), &evidence); err != nil {
		t.Fatal(err)
	}
	var fence operator.FenceRequest
	if err := json.Unmarshal(encoded(evidence["fence"]), &fence); err != nil {
		t.Fatal(err)
	}
	record := gcsManualTerminalChallengeRecord{Challenge: challenge, VerifiedContext: manualTerminalVerifiedContextFixture(challenge, fence)}
	record.ChallengeDigest, _ = operator.ManualRecoveryChallengeDigest(challenge)
	record.AttestationName, _ = operator.ManualRecoveryAttestationName(challenge.RecoveryUID)
	data := encoded(record)
	if !bytes.Contains(data, []byte(`"runtime":null`)) || bytes.Contains(data, []byte("sessionRef")) || bytes.Contains(data, []byte("mutationPayload")) {
		t.Fatal("manual approver context must include absent runtime explicitly and exclude private mutation/session data")
	}
	if err := validateGCSManualTerminalVerifiedContext(record); err != nil {
		t.Fatalf("valid private review context was rejected: %v", err)
	}
	for _, mutate := range []func(*gcsManualTerminalVerifiedContext){
		func(context *gcsManualTerminalVerifiedContext) { context.RootDigest = strings.Repeat("b", 64) },
		func(context *gcsManualTerminalVerifiedContext) {
			context.FenceRequest.Targets[0].PodUID = "changed-pod"
		},
		func(context *gcsManualTerminalVerifiedContext) { context.HistoricalRunCount++ },
		func(context *gcsManualTerminalVerifiedContext) { context.SourceDataScope.Bucket = "other-bucket" },
	} {
		changed := record
		changed.VerifiedContext = record.VerifiedContext
		changed.VerifiedContext.FenceRequest = record.VerifiedContext.FenceRequest
		changed.VerifiedContext.FenceRequest.Targets = append([]operator.FenceTarget(nil), record.VerifiedContext.FenceRequest.Targets...)
		mutate(&changed.VerifiedContext)
		if err := validateGCSManualTerminalVerifiedContext(changed); err == nil {
			t.Fatal("accepted changed root, request, inventory count, or storage scope context")
		}
	}
}

func TestManualTerminalHistoricalRunRequiresExplicitRuntimeAndMatchingIdentity(t *testing.T) {
	valid := manualTerminalVerifiedContextFixture(manualTerminalChallengeFixture("private-scope"), manualTerminalFenceFixture("private-scope"))
	if err := validateGCSManualTerminalVerifiedContext(gcsManualTerminalChallengeRecord{
		Challenge: manualTerminalChallengeFixture("private-scope"), VerifiedContext: valid,
	}); err != nil {
		t.Fatalf("explicit null runtime should represent an unknown historical runtime: %v", err)
	}
	incompleteIdentity := valid
	incompleteIdentity.HistoricalRunInventory = append([]gcsManualTerminalHistoricalRun(nil), valid.HistoricalRunInventory...)
	incompleteIdentity.HistoricalRunInventory[0].Identity.PodUID = ""
	if err := validateGCSManualTerminalVerifiedContext(gcsManualTerminalChallengeRecord{
		Challenge: manualTerminalChallengeFixture("private-scope"), VerifiedContext: incompleteIdentity,
	}); err == nil {
		t.Fatal("accepted historical run with incomplete identity")
	}

	var omitted gcsManualTerminalHistoricalRun
	if err := json.Unmarshal([]byte(`{"identity":{"sourceClusterID":"source","nodeID":"node","podUID":"pod","runID":"run","walIdentity":"wal"}}`), &omitted); err == nil {
		t.Fatal("accepted historical run with omitted runtime field")
	}

	valid.HistoricalRunInventory[0].Runtime = &fencing.RuntimeBinding{
		AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts",
		SourceClusterID: "source", NodeID: "other-node", PodUID: "pod-0", RunID: "run-1", NodeName: "worker",
		NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox",
		ContainerID: "container", ImageID: "image",
	}
	if err := validateGCSManualTerminalVerifiedContext(gcsManualTerminalChallengeRecord{
		Challenge: manualTerminalChallengeFixture("private-scope"), VerifiedContext: valid,
	}); err == nil {
		t.Fatal("accepted historical runtime bound to a different identity")
	}
	valid.HistoricalRunInventory[0].Runtime.NodeID = "rhiza-0"
	valid.HistoricalRunInventory[0].Runtime.SourceClusterID = "private-scope"
	if err := validateGCSManualTerminalVerifiedContext(gcsManualTerminalChallengeRecord{
		Challenge: manualTerminalChallengeFixture("private-scope"), VerifiedContext: valid,
	}); err != nil {
		t.Fatalf("accepted complete matching historical runtime: %v", err)
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
	var fence operator.FenceRequest
	if err := json.Unmarshal(encoded(evidence["fence"]), &fence); err != nil {
		panic("invalid manual terminal fence fixture: " + err.Error())
	}
	return encoded(gcsManualTerminalChallengeRecord{Challenge: challenge, ChallengeDigest: digest,
		AttestationName: name, VerifiedContext: manualTerminalVerifiedContextFixture(challenge, fence)})
}

func manualTerminalVerifiedContextFixture(challenge operator.ManualRecoveryChallenge, fence operator.FenceRequest) gcsManualTerminalVerifiedContext {
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "private-bucket", Prefix: path.Join("runs", challenge.SourceClusterID), ClusterID: challenge.SourceClusterID}
	controlPrefix := path.Join(path.Dir(scope.Prefix), "control")
	targetPrefix := path.Join(path.Dir(scope.Prefix), challenge.TargetClusterID)
	source := gcsRecoverySource{Bucket: scope.Bucket, Prefix: scope.Prefix, ClusterID: scope.ClusterID, StatefulSetUID: challenge.StatefulSetUID}
	claimKey, _ := manualTerminalSuccessorClaimKey(source, controlPrefix)
	targetKeys, _ := manualTerminalTargetControlKeys(source, controlPrefix, targetPrefix, challenge.TargetClusterID)
	fenceDigest := sha256.Sum256(encoded(fence))
	return gcsManualTerminalVerifiedContext{
		RootDigest: challenge.RootDigest, FenceRequest: fence, FenceRequestHash: hex.EncodeToString(fenceDigest[:]),
		SourceDataScope: scope, SourceControlPrefix: controlPrefix, SuccessorClaimKey: claimKey,
		TargetDataScope:   fencing.ArchiveScope{Provider: "gcs", Bucket: scope.Bucket, Prefix: targetPrefix, ClusterID: challenge.TargetClusterID},
		TargetControlKeys: targetKeys, UnclaimedObservedAt: challenge.IssuedAt.Add(time.Minute),
		HistoricalRunInventory: []gcsManualTerminalHistoricalRun{{Identity: fencing.Identity{
			SourceClusterID: challenge.SourceClusterID, NodeID: "rhiza-0", PodUID: "pod-0", RunID: "run-1", WALIdentity: "wal-0",
		}, runtimePresent: true}}, HistoricalRunCount: 1,
	}
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
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "private-bucket", Prefix: path.Join("runs", privateValue), ClusterID: privateValue}
	scopeDigest, _ := scope.Hash()
	return operator.ManualRecoveryChallenge{Version: 1,
		ManualRecoveryBinding: operator.ManualRecoveryBinding{EvidenceKind: operator.ManualTerminalEvidenceKind,
			RecoveryNamespace: "qualification", RecoveryName: "manual-terminal-recovery", RecoveryUID: "manual-recovery-uid",
			RecoveryID: fence.OperationID, OperationID: fence.OperationID, SourceClusterID: privateValue,
			StatefulSetName: fence.StatefulSet, StatefulSetUID: fence.StatefulSetUID, FenceRequestHash: hex.EncodeToString(fenceHash[:]), PolicyDigest: stamp, RootDigest: stamp,
			LedgerDigest: stamp, CohortDigest: stamp, StorageScopeDigest: scopeDigest, SourceArchiveDigest: stamp,
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
