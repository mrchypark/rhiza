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
	dev, ino, fsType, links uint64
	identityErr             error
	pidErr                  error
	populatedFlag           bool
	populatedErr            error
	closed                  []int
}

func (k *fakeStopCgroupKernel) identity(int) (uint64, uint64, uint64, uint64, error) {
	return k.dev, k.ino, k.fsType, k.links, k.identityErr
}
func (k *fakeStopCgroupKernel) pidExited(int) error { return k.pidErr }
func (k *fakeStopCgroupKernel) populated(int) (bool, error) {
	return k.populatedFlag, k.populatedErr
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
		name        string
		links       uint64
		pidErr      error
		populated   bool
		popErr      error
		identityErr error
		dev         uint64
		wantCode    string
		wantErr     bool
	}{
		{name: "live pid despite detached inode", links: 0, pidErr: stopCgroupFailure("target_process_live", ErrUnverified), wantCode: "target_process_live", wantErr: true},
		{name: "unreadable pid state", links: 0, pidErr: ErrUnverified, wantCode: "target_process_state_unavailable", wantErr: true},
		{name: "exited pid and detached exact inode", links: 0},
		{name: "changed cgroup identity", links: 1, dev: 6, wantCode: "cgroup_identity_changed", wantErr: true},
		{name: "unreadable cgroup identity", links: 1, identityErr: errors.New("private kernel error"), wantCode: "cgroup_observation_unavailable", wantErr: true},
		{name: "exited pid but linked populated cgroup", links: 1, populated: true, wantCode: "cgroup_populated", wantErr: true},
		{name: "unreadable cgroup population", links: 1, popErr: errors.New("private kernel error"), wantCode: "cgroup_observation_unavailable", wantErr: true},
		{name: "exited pid and linked empty cgroup", links: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dev := test.dev
			if dev == 0 {
				dev = 4
			}
			kernel := &fakeStopCgroupKernel{dev: dev, ino: 5, fsType: uint64(unix.CGROUP2_SUPER_MAGIC), links: test.links, identityErr: test.identityErr,
				pidErr: test.pidErr, populatedFlag: test.populated, populatedErr: test.popErr}
			pin := &pinnedStopCgroup{cgroupFD: 10, pidFD: 11, dev: 4, ino: 5, kernel: kernel}
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
			if len(kernel.closed) != 2 || kernel.closed[0] != 10 || kernel.closed[1] != 11 {
				t.Fatalf("production pin did not close exact descriptors: %v", kernel.closed)
			}
		})
	}
}

func TestCRIResumeRetainsProductionPinAfterPathDisappears(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux cgroup pin semantics are required")
	}
	p, f, binding, request, proc, cgroupRoot, scope := newStopCgroupFixture(t)
	kernel := &fakeStopCgroupKernel{dev: scope.Dev, ino: scope.Ino, fsType: uint64(unix.CGROUP2_SUPER_MAGIC), links: 0}
	p.pinStop = func(string, string, CgroupScope) (stopCgroupPin, error) {
		return &pinnedStopCgroup{cgroupFD: 10, pidFD: 11, dev: scope.Dev, ino: scope.Ino, kernel: kernel}, nil
	}
	cgroup := filepath.Join(cgroupRoot, scope.Path)
	original := cgroup + "-renamed"
	var renameErr error
	f.afterContainerStop = func() { renameErr = os.Rename(cgroup, original) }
	f.mu.Lock()
	f.sandboxError, f.sandboxErrorOnce = codes.Unavailable, true
	f.mu.Unlock()
	partial, err := p.StopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if renameErr != nil || !errors.Is(err, ErrUnverified) || !partial.ContainerStopAcknowledged || partial.SandboxStopAcknowledged {
		t.Fatalf("interrupted exact stop did not preserve its partial result: %+v %v rename=%v", partial, err, renameErr)
	}
	if _, err := os.Stat(cgroup); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("captured cgroup pathname unexpectedly remains: %v", err)
	}
	recovered, err := p.ResumeStopInCgroup(t.Context(), binding, request, proc, cgroupRoot, scope)
	if err != nil || !recovered.RecoveredTerminalState || !recovered.ContainerExited || !recovered.SandboxNotReady || !recovered.CgroupEmpty {
		t.Fatalf("retry did not use retained production cgroup pin: %+v %v", recovered, err)
	}
	if len(kernel.closed) != 2 || kernel.closed[0] != 10 || kernel.closed[1] != 11 {
		t.Fatalf("retained production descriptors were not released after completion: %v", kernel.closed)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopContainers != 1 || f.stopSandboxes != 2 {
		t.Fatalf("retry changed exact CRI sequence: containers=%d sandboxes=%d", f.stopContainers, f.stopSandboxes)
	}
}
