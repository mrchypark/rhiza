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
	dir := filepath.Join(cgroups, "kubepods", "pod-a")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	binding := testRuntimeBinding()
	writeProcIdentity(t, proc, 123, 789, "kubepods/pod-a")
	events := filepath.Join(dir, "cgroup.events")
	if err := os.WriteFile(events, []byte("populated 1\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	scope, err := captureCgroupScope(proc, cgroups, 123, binding, "qualified-test-profile")
	if err != nil {
		t.Fatal(err)
	}
	if scope.Path != filepath.Join("kubepods", "pod-a") {
		t.Fatalf("captured unexpected cgroup path %q", scope.Path)
	}
	if err := scope.VerifyEmpty(cgroups); err == nil {
		t.Fatal("populated cgroup was accepted as empty")
	}
	if err := os.WriteFile(events, []byte("populated 0\nfrozen 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := scope.VerifyEmpty(cgroups); err != nil {
		t.Fatalf("empty enrolled cgroup rejected: %v", err)
	}
	if err := scope.VerifyProcess(proc, cgroups); err != nil {
		t.Fatalf("same process identity rejected: %v", err)
	}
	writeProcIdentity(t, proc, 123, 790, "kubepods/pod-a")
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
	binding := testRuntimeBinding()
	for _, value := range []string{"2:cpu:/kubepods/pod-a\n", "0::/../outside\n", "0::/missing\n"} {
		writeProcIdentity(t, proc, 123, 789, strings.TrimPrefix(value, "0::/"))
		if strings.HasPrefix(value, "2:") {
			if err := os.WriteFile(filepath.Join(proc, "123", "cgroup"), []byte(value), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := captureCgroupScope(proc, cgroups, 123, binding, "qualified-test-profile"); err == nil {
			t.Fatalf("unsafe or missing cgroup identity accepted: %q", value)
		}
	}
}
