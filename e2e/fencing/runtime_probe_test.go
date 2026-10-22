package main

import (
	"encoding/json"
	"testing"
)

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
