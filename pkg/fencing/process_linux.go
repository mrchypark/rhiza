//go:build linux

package fencing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// VerifyPrivatePIDNamespace verifies the process is PID 1 with the supported
// private procfs. It performs no process termination and is safe for init
// entrypoints which need the same namespace boundary before doing work.
func VerifyPrivatePIDNamespace() error {
	if os.Getpid() != 1 {
		return fmt.Errorf("fencing: operation requires PID 1 in a private process namespace")
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs("/proc", &filesystem); err != nil {
		return err
	}
	self, err := os.Readlink("/proc/self")
	if err != nil || filesystem.Type != 0x9fa0 || self != "1" {
		return fmt.Errorf("fencing: private PID namespace procfs is not verifiable")
	}
	return nil
}

// CheckInitNamespaceQuiescence proves no child is live without signaling any
// process. Only already-exited children of this PID 1 are reaped.
func CheckInitNamespaceQuiescence(ctx context.Context) error {
	if err := VerifyPrivatePIDNamespace(); err != nil {
		return fmt.Errorf("fencing: refusing to inspect outside the init PID namespace: %w", err)
	}
	return checkInitNamespaceQuiescence(ctx, "/proc", func() (int, error) {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
		return pid, err
	}, func(name string) error {
		_, err := os.Stat(name)
		return err
	})
}

func checkInitNamespaceQuiescence(ctx context.Context, procRoot string, wait func() (int, error), stat func(string) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pid, err := wait()
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.ECHILD || pid == 0 {
			break
		}
		if err != nil || pid < 0 {
			return fmt.Errorf("fencing: reap completed init child: %w", err)
		}
	}
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return fmt.Errorf("fencing: inspect init process namespace: %w", err)
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}
		if err := stat(filepath.Join(procRoot, entry.Name())); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("fencing: inspect init child process: %w", err)
		}
		return fmt.Errorf("fencing: init process namespace still contains child processes")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// PID 1 owns all processes in the supported private container PID namespace.
// Process groups alone are insufficient: a child can fork and call setsid.
func terminatePrivateNamespace(ctx context.Context) error {
	if err := VerifyPrivatePIDNamespace(); err != nil {
		return fmt.Errorf("fencing: refusing to inspect/terminate outside the supervisor PID namespace: %w", err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return err
		}
		remaining := false
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid <= 1 {
				continue
			}
			remaining = true
			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
				return err
			}
		}
		for {
			var status syscall.WaitStatus
			pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
			if pid <= 0 || err != nil {
				break
			}
		}
		if !remaining {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("fencing: private namespace did not terminate")
		case <-tick.C:
		}
	}
}
