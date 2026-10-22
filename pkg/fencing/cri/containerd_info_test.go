package cri

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

const containerdInfoFixture = `{"sandboxID":"sandbox-0","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{}}`

func TestContainerdVerbosePIDUsesPinnedExactIdentity(t *testing.T) {
	got, err := containerdVerbosePID(map[string]string{"info": containerdInfoFixture}, "sandbox-0", "io.containerd.runc.v2")
	if err != nil || got != 123 {
		t.Fatalf("containerd PID parse=%d, %v", got, err)
	}
	for name, info := range map[string]map[string]string{
		"missing verbose data": {},
		"extra info key":       {"info": containerdInfoFixture, "other": `{}`},
		"unknown schema field": {"info": `{"sandboxID":"sandbox-0","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{},"newField":true}`},
		"duplicate pid":        {"info": `{"sandboxID":"sandbox-0","pid":123,"pid":124,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{}}`},
		"missing pid":          {"info": `{"sandboxID":"sandbox-0","removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{}}`},
		"zero pid":             {"info": `{"sandboxID":"sandbox-0","pid":0,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{}}`},
		"wrong sandbox":        {"info": containerdInfoFixture},
		"removing container":   {"info": `{"sandboxID":"sandbox-0","pid":123,"removing":true,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{}}`},
		"wrong runtime type":   {"info": containerdInfoFixture},
	} {
		t.Run(name, func(t *testing.T) {
			sandbox, runtimeType := "sandbox-0", "io.containerd.runc.v2"
			if name == "wrong sandbox" {
				sandbox = "other-sandbox"
			}
			if name == "wrong runtime type" {
				runtimeType = "io.containerd.runhcs.v1"
			}
			if _, err := containerdVerbosePID(info, sandbox, runtimeType); err == nil {
				t.Fatal("mismatched or incomplete containerd PID evidence accepted")
			}
		})
	}
}

func TestCgroupScopeDigestBindsFullRuntimeRun(t *testing.T) {
	binding := testRuntimeBinding()
	identity := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}
	if err := binding.Validate(identity); err != nil {
		t.Fatal(err)
	}
	scope := CgroupScope{Path: "kubepods/pod-a", Dev: 1, Ino: 2, PID: 123, StartTime: 789, RuntimeHash: fencing.RuntimeBindingDigest(binding), ProfileID: "qualified-test-profile"}
	other := binding
	other.RunID = "another-run"
	if scope.RuntimeHash == fencing.RuntimeBindingDigest(other) {
		t.Fatal("run-specific runtime bindings unexpectedly share a cgroup key")
	}
}
