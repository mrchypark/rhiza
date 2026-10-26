package cri

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func writeProcIdentity(t *testing.T, procRoot string, pid uint32, startTime uint64, cgroup string) {
	t.Helper()
	dir := filepath.Join(procRoot, fmt.Sprint(pid))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	fields := []string{"S"}
	for range 18 {
		fields = append(fields, "0")
	}
	fields = append(fields, fmt.Sprint(startTime))
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(fmt.Sprintf("%d (rhiza database) %s\n", pid, strings.Join(fields, " "))), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup"), []byte("0::/"+cgroup+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
}

func testRuntimeBinding() fencing.RuntimeBinding {
	return fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts", SourceClusterID: "source",
		NodeID: "node", PodUID: "pod", RunID: "run", NodeName: "node-name", NodeUID: "node-uid", MachineID: "machine", BootID: "boot",
		RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "containerd://container", ImageID: "image"}
}

func TestCgroupScopePinsAndVerifiesDescendantEmptiness(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("host cgroup identity is Linux-specific")
	}
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	cgroups := filepath.Join(root, "cgroup")
	dir := filepath.Join(cgroups, "kubepods", "pod-a", "container")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	binding := testRuntimeBinding()
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-a/container")
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte("123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(dir, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	scope, err := captureCgroupScope(proc, cgroups, 123, "/kubepods/pod-a/container", binding, "qualified-test-profile")
	if err != nil {
		t.Fatal(err)
	}
	if scope.Path != filepath.Join("kubepods", "pod-a", "container") {
		t.Fatalf("captured unexpected cgroup path %q", scope.Path)
	}
	if err := scope.VerifyEmpty(cgroups); err == nil {
		t.Fatal("populated cgroup was accepted as empty")
	}
	if err := scope.VerifyProcess(proc, cgroups); err != nil {
		t.Fatalf("same process identity rejected before exit: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte("124\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := scope.VerifyProcess(proc, cgroups); err == nil {
		t.Fatal("process no longer listed as a direct cgroup member was accepted")
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte("123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(events, []byte("populated 0\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := scope.VerifyEmpty(cgroups); err != nil {
		t.Fatalf("empty enrolled cgroup rejected: %v", err)
	}
	writeProcIdentity(t, proc, 123, 790, "kubepods/pod-a/container")
	if err := scope.VerifyProcess(proc, cgroups); err == nil {
		t.Fatal("reused PID with another start time was accepted")
	}
	replacement := filepath.Join(cgroups, "replacement")
	if err := os.Mkdir(replacement, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(replacement, "cgroup.events"), []byte("populated 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(cgroups, "old")
	if err := os.Rename(dir, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, dir); err != nil {
		t.Fatal(err)
	}
	if err := scope.VerifyEmpty(cgroups); err == nil {
		t.Fatal("replacement cgroup inode was accepted")
	}
}

func TestCaptureCgroupScopeRejectsMissingOrEscapingIdentity(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("host cgroup identity is Linux-specific")
	}
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	cgroups := filepath.Join(root, "cgroup")
	for _, tc := range []struct {
		name, contents, want string
	}{
		{name: "root", contents: "0::/\n", want: "(root)"},
		{name: "absolute", contents: "0:://outside\n", want: "(absolute)"},
		{name: "dot", contents: "0::/kubepods/./pod-a\n", want: "(dot)"},
		{name: "noncanonical", contents: "0::/kubepods//pod-a\n", want: "(noncanonical)"},
		{name: "traversal", contents: "0::/kubepods/../outside\n", want: "(traversal)"},
		{name: "missing unified entry", contents: "2:cpu:/kubepods/pod-a\n", want: "unified cgroup path missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writeProcIdentity(t, proc, 123, 789, "kubepods/pod-a")
			if err := os.WriteFile(filepath.Join(proc, "123", "cgroup"), []byte(tc.contents), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := processCgroupPath(proc, 123)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("processCgroupPath() error = %v, want category %q", err, tc.want)
			}
		})
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-a")
	if err := os.MkdirAll(filepath.Join(cgroups, "kubepods", "pod-a", "container"), 0700); err != nil {
		t.Fatal(err)
	}
	validScopeDir := filepath.Join(cgroups, "kubepods", "pod-a", "container")
	if err := os.WriteFile(filepath.Join(validScopeDir, "cgroup.events"), []byte("populated 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, contents := range []string{"", "124\n", "123\n123\n", "0\n"} {
		if err := os.WriteFile(filepath.Join(validScopeDir, "cgroup.procs"), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := captureCgroupScope(proc, cgroups, 123, "/kubepods/pod-a/container", testRuntimeBinding(), "qualified-test-profile"); err == nil {
			t.Fatalf("cgroup membership %q was accepted", contents)
		}
	}

	// RuntimeSpec traversal remains rejected by the trusted path mapper.
	if _, err := containerdCgroupsPath("/kubepods/../outside/container", "container"); err == nil {
		t.Fatal("escaping runtime cgroups path was accepted")
	}
}

func TestCaptureCgroupScopeRejectsSymlinkEscape(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("host cgroup identity is Linux-specific")
	}
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	if err := os.MkdirAll(cgroupRoot, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "outside")
	if err := os.MkdirAll(filepath.Join(outside, "pod-a", "container"), 0700); err != nil {
		t.Fatal(err)
	}
	outsideScope := filepath.Join(outside, "pod-a", "container")
	if err := os.WriteFile(filepath.Join(outsideScope, "cgroup.events"), []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outsideScope, "cgroup.procs"), []byte("123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(cgroupRoot, "kubepods")); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-a/container")
	if _, err := captureCgroupScope(proc, cgroupRoot, 123, "/kubepods/pod-a/container", testRuntimeBinding(), "qualified-test-profile"); err == nil {
		t.Fatal("cgroup path escaping through a symlink was accepted")
	}
}

func TestCaptureCgroupScopeUsesTrustedRuntimePathForTraversalProcEntry(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("host cgroup identity is Linux-specific")
	}
	root := t.TempDir()
	proc, cgroupRoot := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	dir := filepath.Join(cgroupRoot, "kubepods", "pod-a", "container")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	writeProcIdentity(t, proc, 123, 789, "../peer")
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte("123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.events"), []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := processCgroupPath(proc, 123); err == nil {
		t.Fatal("legacy proc cgroup traversal was not rejected by its strict parser")
	}
	scope, err := captureCgroupScope(proc, cgroupRoot, 123, "/kubepods/pod-a/container", testRuntimeBinding(), "qualified-test-profile")
	if err != nil {
		t.Fatalf("trusted runtime path with exact direct PID membership was rejected: %v", err)
	}
	if scope.Path != filepath.Join("kubepods", "pod-a", "container") {
		t.Fatalf("captured unexpected trusted cgroup path %q", scope.Path)
	}
}

func TestClassifyCgroupNamespaceRelation(t *testing.T) {
	root := t.TempDir()
	paths := map[string]string{}
	for _, name := range []string{"agent", "host", "target", "other"} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		paths[name] = path
	}
	info := func(name string) os.FileInfo {
		t.Helper()
		value, err := os.Stat(paths[name])
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, tc := range []struct {
		name, agent, host, target, want string
	}{
		{"all same", "agent", "agent", "agent", "cgroup_namespace_all_same"},
		{"agent and host same", "agent", "agent", "target", "cgroup_namespace_agent_host_same_target_other"},
		{"agent and target same", "agent", "host", "agent", "cgroup_namespace_agent_target_same_host_other"},
		{"host and target same", "agent", "host", "host", "cgroup_namespace_host_target_same_agent_other"},
		{"all different", "agent", "host", "target", "cgroup_namespace_all_different"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyCgroupNamespaceRelation(info(tc.agent), info(tc.host), info(tc.target)); got != tc.want {
				t.Fatalf("namespace relation = %q, want %q", got, tc.want)
			}
		})
	}
	if got := classifyCgroupNamespaceRelation(nil, info("host"), info("target")); got != "cgroup_namespace_agent_unavailable" {
		t.Fatalf("missing agent namespace relation = %q", got)
	}
	if got := classifyCgroupNamespaceRelation(nil, nil, info("target")); got != "cgroup_namespace_multiple_unavailable" {
		t.Fatalf("multiple missing namespace relation = %q", got)
	}
}
