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
	var resources []obj
	data, err := os.ReadFile(filepath.Join(raw, "public", "resources-summary.json"))
	if err != nil || json.Unmarshal(data, &resources) != nil || len(resources) != 2 || resources[0]["uid"] != "uid" {
		t.Fatal("resource summary lost audit identity")
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
