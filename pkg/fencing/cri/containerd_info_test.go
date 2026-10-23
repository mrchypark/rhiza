package cri

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

const containerdInfoFixture = `{"sandboxID":"sandbox-0","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{"linux":{"cgroupsPath":"/kubepods/pod-0/container-0"}}}`

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

func TestContainerdVerboseRuntimeIdentityCapturesCgroupsPath(t *testing.T) {
	identity, err := containerdVerboseRuntimeIdentity(map[string]string{"info": containerdInfoFixture}, "sandbox-0", "io.containerd.runc.v2")
	if err != nil || identity.SandboxID != "sandbox-0" || identity.PID != 123 || identity.CgroupsPath != "/kubepods/pod-0/container-0" {
		t.Fatalf("containerd runtime identity=%+v, %v", identity, err)
	}
	for name, fixture := range map[string]string{
		"missing Linux section":   `{"sandboxID":"sandbox-0","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{}}`,
		"missing cgroups path":    `{"sandboxID":"sandbox-0","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{"linux":{}}}`,
		"duplicate cgroups path":  `{"sandboxID":"sandbox-0","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":{"linux":{"cgroupsPath":"/a/container-0","cgroupsPath":"/b/container-0"}}}`,
		"runtime spec non-object": `{"sandboxID":"sandbox-0","pid":123,"removing":false,"snapshotKey":"snapshot","snapshotter":"overlayfs","runtimeType":"io.containerd.runc.v2","runtimeOptions":null,"config":{},"runtimeSpec":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := containerdVerboseRuntimeIdentity(map[string]string{"info": fixture}, "sandbox-0", "io.containerd.runc.v2"); err == nil {
				t.Fatal("untrusted runtime cgroups path was accepted")
			}
		})
	}
}

func TestContainerdCgroupsPathMapsOnlySupportedRootedForms(t *testing.T) {
	for _, tc := range []struct {
		name, input, want string
	}{
		{"OCI absolute cgroupfs path", "/kubepods/pod-a/container-0", "kubepods/pod-a/container-0"},
		{"expanded systemd path", "/kubepods.slice/kubepods-burstable.slice/cri-containerd-container-0.scope", "kubepods.slice/kubepods-burstable.slice/cri-containerd-container-0.scope"},
		{"systemd pod slice", "kubepods-burstable-pod-a.slice:cri-containerd:container-0", "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod.slice/kubepods-burstable-pod-a.slice/cri-containerd-container-0.scope"},
		{"systemd root slice", "-.slice:cri-containerd:container-0", "cri-containerd-container-0.scope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := containerdCgroupsPath(tc.input, "container-0")
			if err != nil || got != tc.want {
				t.Fatalf("containerd cgroups path=%q, %v; want %q", got, err, tc.want)
			}
		})
	}
	for _, input := range []string{
		"kubepods/pod-a/container-0",
		"/kubepods/../container-0",
		"/sys/fs/cgroup/kubepods/container-0",
		"/kubepods/pod-a/other-container",
		"/kubepods/not-a-slice/cri-containerd-container-0.scope",
		"/kubepods.slice/cri-containerd-other-container.scope",
		"kubepods.slice:wrong-prefix:container-0",
		"kubepods.slice:cri-containerd:other-container",
		"kubepods--bad.slice:cri-containerd:container-0",
		"kubepods/../container-0",
	} {
		if _, err := containerdCgroupsPath(input, "container-0"); err == nil {
			t.Errorf("unsupported containerd cgroups path %q was accepted", input)
		}
	}
}

func TestCgroupScopeDigestBindsFullRuntimeRun(t *testing.T) {
	binding := testRuntimeBinding()
	identity := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}
	if err := binding.Validate(identity); err != nil {
		t.Fatal(err)
	}
	scope := CgroupScope{Path: "kubepods/pod-a", Dev: 1, Ino: 2, PID: 123, StartTime: 789, RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: "qualified-test-profile"}
	other := binding
	other.RunID = "another-run"
	if scope.RuntimeHash == fencing.RuntimeScopeBindingDigest(other) {
		t.Fatal("run-specific runtime bindings unexpectedly share a cgroup key")
	}
}
