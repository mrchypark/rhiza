package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
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
	write("pre-cleanup-exit.txt", []byte("0\n"))
	write("image-ids.txt", []byte("sha256:"+strings.Repeat("b", 64)+"\n"))
	write("cleanup.log", []byte("Deleted nodes: [\"rhiza-generic-fencing-control-plane\"]\n"+canary))
	write("terminal.json", encoded(obj{"items": []obj{{"kind": "Pod", "metadata": obj{"name": "pod", "uid": "uid", "annotations": obj{"kubectl.kubernetes.io/last-applied-configuration": canary}}, "spec": obj{"containers": []obj{{"env": []obj{{"name": "TOKEN", "value": canary}}}}}, "status": obj{"phase": "Running"}}, {"kind": "Secret", "metadata": obj{"name": "credential", "uid": "credential-uid"}, "data": obj{"token": canary}, "stringData": obj{"token": canary}}}}))
	write("events.jsonl", append(encoded(obj{"event": "retired_peer_probe_rejected", "sourceACKRows": 3, "oldRuntimesGone": true, "peerToken": canary, "oldIdentity": obj{"podName": "rhiza-0", "podUID": "old-uid", "nodeID": "old-node", "walIdentity": "old-wal", "publicKey": "public-key", "credentialSecretName": "bootstrap", "credentialSecretUID": "bootstrap-uid", "private": canary}, "peerProbe": obj{"mode": "stale", "senderNodeID": "old-node", "destinationNodeID": "survivor", "configID": 9, "requestID": "peer-stale-id", "valueSHA256": "value-hash", "markerSHA256": "marker-hash", "errorPhase": "response-decode", "errorCauseType": "*errors.errorString", "errorElapsed": "1ms", "cleanupVerified": true, "token": canary}, "error": canary, "env": canary}), '\n'))
	write("runtime-diagnostics.json", encoded(runtimeDiagnosticsView{
		CapturedAt: "2026-10-28T00:00:00Z", PodCount: 1,
		Pods: []runtimePodView{{Name: "rhiza-0", UID: "123e4567-e89b-12d3-a456-426614174000", Phase: "Running", Conditions: []runtimeConditionView{{Type: "Ready", Status: "False", Reason: canary}}, Containers: []runtimeContainerView{{Name: "rhiza", Ready: false, RestartCount: 2, State: "waiting", Reason: "CrashLoopBackOff"}}, Logs: []runtimeLogSummaryView{{Container: "rhiza", Available: true, LineCount: 4, SHA256: strings.Repeat("c", 64), Signals: []string{"error", "bootstrap"}}}}},
	}))
	write("pods.json", []byte(canary))
	write("resources.yaml", []byte(canary))
	if err := os.Mkdir(filepath.Join(raw, "kind"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(raw, "kind", "raw.log"), []byte(canary), 0600); err != nil {
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
	publicEvents, err := os.ReadFile(filepath.Join(raw, "public", "events.jsonl"))
	if err != nil || bytes.Contains(publicEvents, []byte(canary)) || !bytes.Contains(publicEvents, []byte("old-uid")) || !bytes.Contains(publicEvents, []byte("peer-stale-id")) || !bytes.Contains(publicEvents, []byte("response-decode")) {
		t.Fatalf("peer-probe evidence was leaked or dropped by public filter: %v", err)
	}
	publicRuntime, err := os.ReadFile(filepath.Join(raw, "public", "runtime-diagnostics.json"))
	if err != nil || bytes.Contains(publicRuntime, []byte(canary)) || !bytes.Contains(publicRuntime, []byte("123e4567-e89b-12d3-a456-426614174000")) || !bytes.Contains(publicRuntime, []byte("CrashLoopBackOff")) || !bytes.Contains(publicRuntime, []byte(strings.Repeat("c", 64))) {
		t.Fatalf("sanitized pre-cleanup runtime diagnostics missing or leaked: %v", err)
	}
	if _, err := os.Stat(filepath.Join(raw, "public", "pods.json")); !os.IsNotExist(err) {
		t.Fatal("raw Pod dump entered public packet")
	}
	var resources []obj
	data, err := os.ReadFile(filepath.Join(raw, "public", "resources-summary.json"))
	if err != nil || json.Unmarshal(data, &resources) != nil || len(resources) != 2 || resources[0]["uid"] != "uid" {
		t.Fatal("resource summary lost audit identity")
	}
}

func TestRuntimeDiagnosticsRejectUnknownFieldsAndMalformedSignals(t *testing.T) {
	valid := `{"capturedAt":"2026-10-28T00:00:00Z","podCount":0,"pods":[]}`
	for _, input := range []string{
		`{"capturedAt":"2026-10-28T00:00:00Z","podCount":0,"pods":[],"token":"secret"}`,
		`{"capturedAt":"2026-10-28T00:00:00Z","podCount":0,"pods":[],} trailing`,
	} {
		if _, err := sanitizeRuntimeDiagnostics([]byte(input)); err == nil {
			t.Fatalf("accepted malformed or unknown-field runtime diagnostics: %s", input)
		}
	}
	if _, err := sanitizeRuntimeDiagnostics([]byte(valid)); err != nil {
		t.Fatalf("valid empty diagnostics rejected: %v", err)
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

func TestDRQualificationPublicProjectionIsBoundedAndRejectsUnknownFields(t *testing.T) {
	const requestHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	valid := obj{
		"version": 1, "targetClusterID": "target-1", "anchorID": recoveryAnchorID,
		"recoveryName": "rhiza-recovery-1", "recoveryUID": "123e4567-e89b-12d3-a456-426614174000",
		"operationID": "recovery-op", "requestHash": requestHash,
		"receipt":      recoveryanchor.Receipt{AnchorID: recoveryAnchorID, OperationID: "recovery-op", RequestHash: requestHash, Target: recoveryanchor.Binding{ClusterID: "target-1", StorageID: "storage-hash"}, Generation: 2},
		"assignments":  []obj{{"pod": "rhiza-0", "podUID": "123e4567-e89b-12d3-a456-426614174001", "nodeID": "rhiza-0", "credentialUID": "credential-uid", "initExitCode": 0}, {"pod": "rhiza-1", "podUID": "123e4567-e89b-12d3-a456-426614174002", "nodeID": "rhiza-1", "credentialUID": "credential-uid", "initExitCode": 0}, {"pod": "rhiza-2", "podUID": "123e4567-e89b-12d3-a456-426614174003", "nodeID": "rhiza-2", "credentialUID": "credential-uid", "initExitCode": 0}},
		"targetVoters": 3, "initSucceeded": true, "preRuntimeAssignmentObserved": true, "runtimeWALBound": true,
	}
	projected, err := safeJSON("dr-qualification.json", encoded(valid))
	if err != nil || bytes.Contains(projected, []byte("rawProbe")) || !bytes.Contains(projected, []byte(requestHash)) {
		t.Fatalf("valid DR qualification projection failed or leaked: %v", err)
	}
	withCanary := obj{}
	for key, value := range valid {
		withCanary[key] = value
	}
	withCanary["rawProbe"] = "fixture-private-output-canary"
	if _, err := safeJSON("dr-qualification.json", encoded(withCanary)); err == nil {
		t.Fatal("DR qualification projection accepted unreviewed raw probe content")
	}
}

func TestDRQualificationAndACKArtifactsPublishThroughFullSafePath(t *testing.T) {
	const requestHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	raw := t.TempDir()
	proof := obj{
		"version": 1, "targetClusterID": "target-1", "anchorID": recoveryAnchorID,
		"recoveryName": "rhiza-recovery-1", "recoveryUID": "123e4567-e89b-12d3-a456-426614174000",
		"operationID": "recovery-op", "requestHash": requestHash,
		"receipt":      recoveryanchor.Receipt{AnchorID: recoveryAnchorID, OperationID: "recovery-op", RequestHash: requestHash, Target: recoveryanchor.Binding{ClusterID: "target-1", StorageID: "storage-hash"}, Generation: 2},
		"assignments":  []obj{{"pod": "rhiza-0", "podUID": "123e4567-e89b-12d3-a456-426614174001", "nodeID": "rhiza-0", "credentialUID": "credential-uid", "initExitCode": 0}, {"pod": "rhiza-1", "podUID": "123e4567-e89b-12d3-a456-426614174002", "nodeID": "rhiza-1", "credentialUID": "credential-uid", "initExitCode": 0}, {"pod": "rhiza-2", "podUID": "123e4567-e89b-12d3-a456-426614174003", "nodeID": "rhiza-2", "credentialUID": "credential-uid", "initExitCode": 0}},
		"targetVoters": 3, "initSucceeded": true, "preRuntimeAssignmentObserved": true, "runtimeWALBound": true,
	}
	if err := os.WriteFile(filepath.Join(raw, "dr-qualification.json"), encoded(proof), 0600); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"actual-exit.txt": "0\n", "source-head.txt": strings.Repeat("a", 40) + "\n", "image-ids.txt": "sha256:" + strings.Repeat("b", 64) + "\n"} {
		if err := os.WriteFile(filepath.Join(raw, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(raw, "events.jsonl"), append(encoded(obj{"event": "dr_successor_runtime_ack", "drTargetClusterID": "target-1", "drWriteEpoch": 2, "drReadbackSHA256": strings.Repeat("b", 64), "drAssignmentsObserved": true}), '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := safeArtifacts(raw); err != nil {
		t.Fatalf("publish valid DR proof and runtime ACK through safeArtifacts: %v", err)
	}
	public, err := os.ReadFile(filepath.Join(raw, "public", "events.jsonl"))
	if err != nil || !bytes.Contains(public, []byte("dr_successor_runtime_ack")) || !bytes.Contains(public, []byte("drReadbackSHA256")) || bytes.Contains(public, []byte("dr-runtime-bound")) {
		t.Fatalf("public DR event lost bounded evidence or leaked raw row: %v", err)
	}
	if _, err := os.Stat(filepath.Join(raw, "public", "dr-qualification.json")); err != nil {
		t.Fatalf("public DR qualification artifact missing: %v", err)
	}
}
