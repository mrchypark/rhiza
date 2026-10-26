//go:build linux

package cri

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
)

type fakeStopCgroupKernel struct {
	dev, ino, fsType   uint64
	identityErr        error
	identityOperation  string
	pidErr             error
	populatedFlag      bool
	populatedErr       error
	populatedOperation string
	pathAbsent         bool
	pathErr            error
	closed             []int
}

func (k *fakeStopCgroupKernel) identity(int) (uint64, uint64, uint64, error) {
	if k.identityErr != nil {
		operation := k.identityOperation
		if operation == "" {
			operation = "fstat"
		}
		return 0, 0, 0, cgroupObservationFailure(operation, k.identityErr)
	}
	return k.dev, k.ino, k.fsType, nil
}
func (k *fakeStopCgroupKernel) pidExited(int) error { return k.pidErr }
func (k *fakeStopCgroupKernel) populated(int) (bool, error) {
	if k.populatedErr != nil {
		operation := k.populatedOperation
		if operation == "" {
			operation = "events"
		}
		return false, cgroupObservationFailure(operation, k.populatedErr)
	}
	return k.populatedFlag, nil
}
func (k *fakeStopCgroupKernel) scopeAbsent(int, string) (bool, error) {
	if k.pathErr != nil {
		return false, cgroupObservationFailure("scope_path", k.pathErr)
	}
	return k.pathAbsent, nil
}
func (k *fakeStopCgroupKernel) close(fd int) error {
	k.closed = append(k.closed, fd)
	return nil
}

func TestStopCgroupPinRejectsLiveTargetPID(t *testing.T) {
	pid := uint32(os.Getpid())
	start, err := processStartTime("/proc", pid)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := pinPID("/proc", pid, start)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := verifyPIDFDExited(fd); !errors.Is(err, ErrUnverified) || stopCgroupFailureCode(err) != "target_process_live" {
		t.Fatalf("live process was accepted as exited: %v", err)
	}
}

func TestVerifyPIDFDRejectsInvalidDescriptorAsUnavailable(t *testing.T) {
	if err := verifyPIDFDExited(1 << 20); !errors.Is(err, ErrUnverified) || stopCgroupFailureCode(err) != "target_process_state_unavailable" {
		t.Fatalf("invalid pidfd was not classified as unavailable: %v", err)
	}
}

func TestPinnedStopCgroupVerifyEmptyUsesExactKernelState(t *testing.T) {
	tests := []struct {
		name              string
		pidErr            error
		populated         bool
		popErr            error
		popErrOperation   string
		pathAbsent        bool
		pathErr           error
		identityErr       error
		identityOperation string
		dev               uint64
		fsType            uint64
		wantCode          string
		wantErr           bool
	}{
		{name: "live pid despite removed path", pidErr: stopCgroupFailure("target_process_live", ErrUnverified), pathAbsent: true, wantCode: "target_process_live", wantErr: true},
		{name: "unreadable pid state", pidErr: ErrUnverified, pathAbsent: true, wantCode: "target_process_state_unavailable", wantErr: true},
		{name: "exited pid and removed scope after events disappeared", popErr: unix.ENOENT, pathAbsent: true},
		{name: "missing events while captured path remains", popErr: unix.ENOENT, wantCode: "cgroup_scope_not_removed", wantErr: true},
		{name: "missing events with replacement path", popErr: unix.ENOENT, wantCode: "cgroup_scope_not_removed", wantErr: true},
		{name: "removed path lookup denied", popErr: unix.ENOENT, pathErr: unix.EACCES, wantCode: "cgroup_scope_path_denied", wantErr: true},
		{name: "invalid retained root descriptor", popErr: unix.ENOENT, pathErr: unix.EBADF, pathAbsent: true, wantCode: "cgroup_scope_path_bad_fd", wantErr: true},
		{name: "ENOENT from another operation is not removal proof", popErr: unix.ENOENT, popErrOperation: "fstat", pathAbsent: true, wantCode: "cgroup_fstat_not_found", wantErr: true},
		{name: "changed cgroup identity", dev: 6, pathAbsent: true, wantCode: "cgroup_identity_changed", wantErr: true},
		{name: "non-cgroup2 inode", fsType: uint64(unix.TMPFS_MAGIC), pathAbsent: true, wantCode: "cgroup_identity_changed", wantErr: true},
		{name: "unreadable cgroup stat identity", identityErr: errors.New("private kernel error"), wantCode: "cgroup_fstat_other", wantErr: true},
		{name: "unavailable cgroup statfs identity", identityErr: unix.ENODEV, identityOperation: "fstatfs", wantCode: "cgroup_fstatfs_no_device", wantErr: true},
		{name: "exited pid but populated cgroup", populated: true, pathAbsent: true, wantCode: "cgroup_populated", wantErr: true},
		{name: "unreadable cgroup population", popErr: errors.New("private kernel error"), pathAbsent: true, wantCode: "cgroup_events_other", wantErr: true},
		{name: "exited pid and linked empty cgroup", populated: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dev := test.dev
			if dev == 0 {
				dev = 4
			}
			fsType := test.fsType
			if fsType == 0 {
				fsType = uint64(unix.CGROUP2_SUPER_MAGIC)
			}
			kernel := &fakeStopCgroupKernel{dev: dev, ino: 5, fsType: fsType, identityErr: test.identityErr, identityOperation: test.identityOperation,
				pidErr: test.pidErr, populatedFlag: test.populated, populatedErr: test.popErr, populatedOperation: test.popErrOperation, pathAbsent: test.pathAbsent, pathErr: test.pathErr}
			pin := &pinnedStopCgroup{rootFD: 12, cgroupFD: 10, pidFD: 11, dev: 4, ino: 5, path: "pod/db", kernel: kernel}
			err := pin.verifyEmpty()
			if test.wantErr != errors.Is(err, ErrUnverified) {
				t.Fatalf("verifyEmpty error=%v, wantErr=%v", err, test.wantErr)
			}
			if test.wantCode != "" {
				diagnostic := preSandboxScopeDiagnostic(err)
				got, ok := StopDiagnosticFrom(diagnostic)
				if !ok || got.Stage != "pre_sandbox_scope" || got.Code != test.wantCode || !errors.Is(diagnostic, ErrUnverified) {
					t.Fatalf("bounded diagnostic=%+v, want pre_sandbox_scope/%s, err=%v", got, test.wantCode, err)
				}
			}
			pin.close()
			if len(kernel.closed) != 3 || kernel.closed[0] != 10 || kernel.closed[1] != 11 || kernel.closed[2] != 12 {
				t.Fatalf("production pin did not close exact descriptors: %v", kernel.closed)
			}
		})
	}
}

func TestLinuxStopCgroupIdentityClassifiesActualFstatFailure(t *testing.T) {
	_, _, _, err := (linuxStopCgroupKernel{}).identity(-1)
	code, ok := cgroupObservationCode(err)
	if !ok || code != "cgroup_fstat_bad_fd" {
		t.Fatalf("Fstat failure diagnostic=%q, ok=%t, err=%v", code, ok, err)
	}
	diagnostic := preSandboxScopeDiagnostic(stopCgroupFailure(code, err))
	got, ok := StopDiagnosticFrom(diagnostic)
	if !ok || got.Stage != "pre_sandbox_scope" || got.Code != "cgroup_fstat_bad_fd" {
		t.Fatalf("Fstat diagnostic was not safely projected: %+v, ok=%t", got, ok)
	}
}

func TestLinuxStopCgroupIdentityClassifiesFstatfsFailure(t *testing.T) {
	kernel := linuxStopCgroupKernel{
		fstat: func(int, *unix.Stat_t) error { return nil },
		fstatfs: func(int, *unix.Statfs_t) error {
			return unix.ENODEV
		},
	}
	_, _, _, err := kernel.identity(10)
	code, ok := cgroupObservationCode(err)
	if !ok || code != "cgroup_fstatfs_no_device" {
		t.Fatalf("Fstatfs failure diagnostic=%q, ok=%t, err=%v", code, ok, err)
	}
}

func TestLinuxStopCgroupScopeAbsenceRejectsNonCgroupRoot(t *testing.T) {
	root := t.TempDir()
	relative := filepath.Join("pod", "db")
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(rootFD)
	kernel := linuxStopCgroupKernel{}
	if absent, err := kernel.scopeAbsent(rootFD, relative); err == nil || absent {
		t.Fatalf("non-cgroup root was accepted for scope-removal proof: absent=%t err=%v", absent, err)
	}
}

func TestLinuxStopCgroupScopeAbsenceClassifiesExactRootENOENT(t *testing.T) {
	called := false
	kernel := linuxStopCgroupKernel{
		fstatfs: func(fd int, stat *unix.Statfs_t) error {
			if fd != 42 {
				t.Fatalf("scope lookup used root fd %d, want 42", fd)
			}
			stat.Type = int64(unix.CGROUP2_SUPER_MAGIC)
			return nil
		},
		openat2: func(fd int, path string, how *unix.OpenHow) (int, error) {
			called = true
			if fd != 42 || path != "pod/db" || how.Resolve != unix.RESOLVE_BENEATH|unix.RESOLVE_NO_SYMLINKS|unix.RESOLVE_NO_XDEV {
				t.Fatalf("scope lookup lost pinned-root constraints: fd=%d path=%q how=%+v", fd, path, how)
			}
			return -1, unix.ENOENT
		},
	}
	absent, err := kernel.scopeAbsent(42, "pod/db")
	if !called || !absent || err != nil {
		t.Fatalf("exact retained-root ENOENT was not classified as removed scope: called=%t absent=%t err=%v", called, absent, err)
	}
}

func TestCRIResumeRetainsProductionPinAfterPathDisappears(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux cgroup pin semantics are required")
	}
	p, f, binding, request, proc, cgroupRoot, scope := newStopCgroupFixture(t)
	kernel := &fakeStopCgroupKernel{dev: scope.Dev, ino: scope.Ino, fsType: uint64(unix.CGROUP2_SUPER_MAGIC), populatedErr: unix.ENOENT, pathAbsent: true}
	p.pinStop = func(string, string, CgroupScope) (stopCgroupPin, error) {
		return &pinnedStopCgroup{rootFD: 12, cgroupFD: 10, pidFD: 11, dev: scope.Dev, ino: scope.Ino, path: scope.Path, kernel: kernel}, nil
	}
	cgroup := filepath.Join(cgroupRoot, scope.Path)
	var removeErr error
	f.afterContainerStop = func() { removeErr = os.RemoveAll(cgroup) }
	f.mu.Lock()
	f.sandboxError, f.sandboxErrorOnce = codes.Unavailable, true
	f.mu.Unlock()
	partial, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if removeErr != nil || !errors.Is(err, ErrUnverified) || !partial.ContainerStopAcknowledged || partial.SandboxStopAcknowledged {
		t.Fatalf("interrupted exact stop did not preserve its partial result: %+v %v remove=%v", partial, err, removeErr)
	}
	if _, err := os.Stat(cgroup); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("captured cgroup pathname unexpectedly remains: %v", err)
	}
	recovered, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if err != nil || !recovered.RecoveredTerminalState || !recovered.ContainerExited || !recovered.SandboxNotReady || !recovered.CgroupEmpty {
		t.Fatalf("retry did not use retained production cgroup pin: %+v %v", recovered, err)
	}
	if len(kernel.closed) != 3 || kernel.closed[0] != 10 || kernel.closed[1] != 11 || kernel.closed[2] != 12 {
		t.Fatalf("retained production descriptors were not released after completion: %v", kernel.closed)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 1 || f.stopSandboxes != 2 {
		t.Fatalf("retry changed exact CRI sequence: containers=%d sandboxes=%d", f.stopContainers, f.stopSandboxes)
	}
}
