//go:build linux

package cri

import (
	"errors"
	"fmt"
	"strconv"

	"golang.org/x/sys/unix"
)

// stopCgroupPin keeps the enrolled cgroup inode and process incarnation
// addressable across CRI StopContainer. containerd may unlink the cgroup as
// part of that RPC, before the caller can inspect its former pathname.
type pinnedStopCgroup struct {
	cgroupFD int
	pidFD    int
	dev      uint64
	ino      uint64
	kernel   stopCgroupKernel
}

// stopCgroupKernel is deliberately limited to the three kernel observations
// used by verifyEmpty, plus FD close. Tests can exercise the production
// decision logic without mounting or modifying a real cgroup hierarchy.
type stopCgroupKernel interface {
	identity(int) (dev, ino, fsType, links uint64, err error)
	pidExited(int) error
	populated(int) (bool, error)
	close(int) error
}

type linuxStopCgroupKernel struct {
	fstat   func(int, *unix.Stat_t) error
	fstatfs func(int, *unix.Statfs_t) error
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

func (k linuxStopCgroupKernel) identity(fd int) (uint64, uint64, uint64, uint64, error) {
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
		return 0, 0, 0, 0, cgroupObservationFailure("fstat", err)
	}
	if err := fstatfs(fd, &fs); err != nil {
		return 0, 0, 0, 0, cgroupObservationFailure("fstatfs", err)
	}
	return uint64(stat.Dev), stat.Ino, uint64(fs.Type), uint64(stat.Nlink), nil
}

func (linuxStopCgroupKernel) pidExited(fd int) error { return verifyPIDFDExited(fd) }

func (linuxStopCgroupKernel) populated(fd int) (bool, error) {
	populated, err := cgroupPopulated("/proc/self/fd/" + strconv.Itoa(fd))
	if err != nil {
		return false, cgroupObservationFailure("events", err)
	}
	return populated, nil
}

func (linuxStopCgroupKernel) close(fd int) error { return unix.Close(fd) }

func pinStopCgroup(procRoot, cgroupRoot string, scope CgroupScope) (stopCgroupPin, error) {
	fd, dev, ino, err := openPinnedCgroup(cgroupRoot, scope.Path, scope.Dev, scope.Ino)
	if err != nil {
		return nil, err
	}
	pidFD, err := pinPID(procRoot, scope.PID, scope.StartTime)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &pinnedStopCgroup{cgroupFD: fd, pidFD: pidFD, dev: dev, ino: ino, kernel: linuxStopCgroupKernel{}}, nil
}

func (p *pinnedStopCgroup) verifyEmpty() error {
	if p == nil || p.cgroupFD < 0 || p.pidFD < 0 {
		return stopCgroupFailure("cgroup_observation_unavailable", fmt.Errorf("%w: enrolled cgroup pin is unavailable", ErrUnverified))
	}
	kernel := p.kernel
	if kernel == nil {
		kernel = linuxStopCgroupKernel{}
	}
	dev, ino, fsType, links, err := kernel.identity(p.cgroupFD)
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
	if links == 0 {
		// cgroup v2 permits removing a cgroup directory only after it and all
		// descendants are empty. The pinned inode proves this is the enrolled
		// scope, even though its former name no longer resolves.
		return nil
	}
	populated, err := kernel.populated(p.cgroupFD)
	if err != nil {
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
}
