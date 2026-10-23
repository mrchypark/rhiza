//go:build linux

package cri

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"

	"golang.org/x/sys/unix"
)

// stopCgroupPin keeps the enrolled cgroup inode and process incarnation
// addressable across CRI StopContainer. containerd may unlink the cgroup as
// part of that RPC, before the caller can inspect its former pathname.
type pinnedStopCgroup struct {
	mu       sync.Mutex
	rootFD   int
	cgroupFD int
	pidFD    int
	dev      uint64
	ino      uint64
	path     string
	kernel   stopCgroupKernel
}

// stopCgroupKernel is deliberately limited to the three kernel observations
// used by verifyEmpty, plus FD close. Tests can exercise the production
// decision logic without mounting or modifying a real cgroup hierarchy.
type stopCgroupKernel interface {
	identity(int) (dev, ino, fsType uint64, err error)
	pidExited(int) error
	populated(int) (bool, error)
	scopeAbsent(rootFD int, relative string) (bool, error)
	close(int) error
}

type linuxStopCgroupKernel struct {
	fstat   func(int, *unix.Stat_t) error
	fstatfs func(int, *unix.Statfs_t) error
	openat2 func(int, string, *unix.OpenHow) (int, error)
}

// cgroupObservationError records only a fixed operation and errno class. Its
// cause is retained for trusted in-process inspection but never serialized.
type cgroupObservationError struct {
	operation string
	class     string
	cause     error
}

func (e *cgroupObservationError) Error() string { return "fencing: cgroup observation failed" }
func (e *cgroupObservationError) Unwrap() error { return e.cause }

func cgroupObservationFailure(operation string, err error) error {
	class := "other"
	switch {
	case errors.Is(err, unix.EBADF):
		class = "bad_fd"
	case errors.Is(err, unix.EACCES), errors.Is(err, unix.EPERM):
		class = "denied"
	case errors.Is(err, unix.ENOENT):
		class = "not_found"
	case errors.Is(err, unix.ENODEV):
		class = "no_device"
	case errors.Is(err, unix.EIO):
		class = "io"
	}
	return &cgroupObservationError{operation: operation, class: class, cause: err}
}

func cgroupObservationCode(err error) (string, bool) {
	var observation *cgroupObservationError
	if !errors.As(err, &observation) || observation == nil {
		return "", false
	}
	return "cgroup_" + observation.operation + "_" + observation.class, true
}

func missingCgroupEvents(err error) bool {
	var observation *cgroupObservationError
	return errors.As(err, &observation) && observation != nil && observation.operation == "events" && errors.Is(observation.cause, unix.ENOENT)
}

func (k linuxStopCgroupKernel) identity(fd int) (uint64, uint64, uint64, error) {
	fstat, fstatfs := k.fstat, k.fstatfs
	if fstat == nil {
		fstat = unix.Fstat
	}
	if fstatfs == nil {
		fstatfs = unix.Fstatfs
	}
	var stat unix.Stat_t
	var fs unix.Statfs_t
	if err := fstat(fd, &stat); err != nil {
		return 0, 0, 0, cgroupObservationFailure("fstat", err)
	}
	if err := fstatfs(fd, &fs); err != nil {
		return 0, 0, 0, cgroupObservationFailure("fstatfs", err)
	}
	// kernfs synthesizes directory nlink as subdirs+2; it is not a reliable
	// cgroup-removal signal and must not authorize a terminal observation.
	return uint64(stat.Dev), stat.Ino, uint64(fs.Type), nil
}

func (linuxStopCgroupKernel) pidExited(fd int) error { return verifyPIDFDExited(fd) }

func (linuxStopCgroupKernel) populated(fd int) (bool, error) {
	populated, err := cgroupPopulated("/proc/self/fd/" + strconv.Itoa(fd))
	if err != nil {
		return false, cgroupObservationFailure("events", err)
	}
	return populated, nil
}

// scopeAbsent checks the captured relative path beneath the exact root FD
// retained when the target cgroup was pinned. cgroup v2 rejects rename (its
// kernfs syscall table has no rename callback), so a missing entry here cannot
// be a moved scope. A new entry, symlink, mount crossing, or lookup error is
// not treated as removal.
func (k linuxStopCgroupKernel) scopeAbsent(rootFD int, relative string) (bool, error) {
	var rootFS unix.Statfs_t
	fstatfs := k.fstatfs
	if fstatfs == nil {
		fstatfs = unix.Fstatfs
	}
	if err := fstatfs(rootFD, &rootFS); err != nil {
		return false, cgroupObservationFailure("scope_path", err)
	}
	if uint64(rootFS.Type) != uint64(unix.CGROUP2_SUPER_MAGIC) {
		return false, cgroupObservationFailure("scope_path", unix.ENODEV)
	}
	openat2 := k.openat2
	if openat2 == nil {
		openat2 = unix.Openat2
	}
	fd, err := openat2(rootFD, relative, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return true, nil
		}
		return false, cgroupObservationFailure("scope_path", err)
	}
	_ = unix.Close(fd)
	return false, nil
}

func (linuxStopCgroupKernel) close(fd int) error { return unix.Close(fd) }

func pinStopCgroup(procRoot, cgroupRoot string, scope CgroupScope) (stopCgroupPin, error) {
	root, err := filepath.EvalSymlinks(cgroupRoot)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve cgroup root", ErrUnverified)
	}
	rootFD, fd, dev, ino, err := openPinnedCgroupWithRoot(root, scope.Path, scope.Dev, scope.Ino)
	if err != nil {
		return nil, err
	}
	pidFD, err := pinPID(procRoot, scope.PID, scope.StartTime)
	if err != nil {
		_ = unix.Close(fd)
		_ = unix.Close(rootFD)
		return nil, err
	}
	return &pinnedStopCgroup{rootFD: rootFD, cgroupFD: fd, pidFD: pidFD, dev: dev, ino: ino, path: scope.Path, kernel: linuxStopCgroupKernel{}}, nil
}

func (p *pinnedStopCgroup) verifyEmpty() error {
	if p == nil {
		return stopCgroupFailure("cgroup_observation_unavailable", fmt.Errorf("%w: enrolled cgroup pin is unavailable", ErrUnverified))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rootFD < 0 || p.cgroupFD < 0 || p.pidFD < 0 || p.path == "" {
		return stopCgroupFailure("cgroup_observation_unavailable", fmt.Errorf("%w: enrolled cgroup pin is unavailable", ErrUnverified))
	}
	kernel := p.kernel
	if kernel == nil {
		kernel = linuxStopCgroupKernel{}
	}
	dev, ino, fsType, err := kernel.identity(p.cgroupFD)
	if err != nil {
		code, ok := cgroupObservationCode(err)
		if !ok {
			code = "cgroup_observation_unavailable"
		}
		return stopCgroupFailure(code, fmt.Errorf("%w: pinned cgroup identity cannot be read", ErrUnverified))
	}
	if fsType != uint64(unix.CGROUP2_SUPER_MAGIC) || dev != p.dev || ino != p.ino {
		return stopCgroupFailure("cgroup_identity_changed", fmt.Errorf("%w: pinned cgroup identity changed", ErrUnverified))
	}
	if err := kernel.pidExited(p.pidFD); err != nil {
		if stopCgroupFailureCode(err) == "cgroup_scope_unverified" {
			return stopCgroupFailure("target_process_state_unavailable", fmt.Errorf("%w: enrolled process exit state cannot be verified", ErrUnverified))
		}
		return err
	}
	populated, err := kernel.populated(p.cgroupFD)
	if err != nil {
		if missingCgroupEvents(err) {
			absent, absentErr := kernel.scopeAbsent(p.rootFD, p.path)
			if absentErr != nil {
				code, ok := cgroupObservationCode(absentErr)
				if !ok {
					code = "cgroup_observation_unavailable"
				}
				return stopCgroupFailure(code, fmt.Errorf("%w: pinned cgroup path cannot be checked", ErrUnverified))
			}
			if absent {
				// cgroup_destroy_locked removes cgroup.events only after checking
				// populated==0 and no online children, then marks the cgroup dying
				// so it cannot accept another task or child.
				return nil
			}
			return stopCgroupFailure("cgroup_scope_not_removed", fmt.Errorf("%w: cgroup.events is missing while the captured path remains", ErrUnverified))
		}
		code, ok := cgroupObservationCode(err)
		if !ok {
			code = "cgroup_observation_unavailable"
		}
		return stopCgroupFailure(code, fmt.Errorf("%w: pinned cgroup population cannot be read", ErrUnverified))
	}
	if populated {
		return stopCgroupFailure("cgroup_populated", fmt.Errorf("%w: pinned cgroup or descendant remains populated", ErrUnverified))
	}
	return nil
}

func verifyPIDFDExited(pidFD int) error {
	process := []unix.PollFd{{Fd: int32(pidFD), Events: unix.POLLIN}}
	ready, err := unix.Poll(process, 0)
	if err != nil {
		return stopCgroupFailure("target_process_state_unavailable", fmt.Errorf("%w: enrolled process exit state cannot be read", ErrUnverified))
	}
	if process[0].Revents&unix.POLLNVAL != 0 {
		return stopCgroupFailure("target_process_state_unavailable", fmt.Errorf("%w: enrolled process exit state cannot be read", ErrUnverified))
	}
	if ready == 0 || process[0].Revents&(unix.POLLIN|unix.POLLHUP|unix.POLLERR) == 0 {
		return stopCgroupFailure("target_process_live", fmt.Errorf("%w: enrolled process has not exited", ErrUnverified))
	}
	return nil
}

func (p *pinnedStopCgroup) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cgroupFD >= 0 {
		kernel := p.kernel
		if kernel == nil {
			kernel = linuxStopCgroupKernel{}
		}
		_ = kernel.close(p.cgroupFD)
		p.cgroupFD = -1
	}
	if p.pidFD >= 0 {
		kernel := p.kernel
		if kernel == nil {
			kernel = linuxStopCgroupKernel{}
		}
		_ = kernel.close(p.pidFD)
		p.pidFD = -1
	}
	if p.rootFD >= 0 {
		kernel := p.kernel
		if kernel == nil {
			kernel = linuxStopCgroupKernel{}
		}
		_ = kernel.close(p.rootFD)
		p.rootFD = -1
	}
}
