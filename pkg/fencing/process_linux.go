//go:build linux

package fencing

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
)

// PID 1 owns all processes in the supported private container PID namespace.
// Process groups alone are insufficient: a child can fork and call setsid.
func terminatePrivateNamespace(ctx context.Context) error {
	if os.Getpid() != 1 {
		return fmt.Errorf("fencing: refusing to inspect/terminate outside the supervisor PID namespace")
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs("/proc", &filesystem); err != nil {
		return err
	}
	self, err := os.Readlink("/proc/self")
	if err != nil || filesystem.Type != 0x9fa0 || self != "1" {
		return fmt.Errorf("fencing: private PID namespace procfs is not verifiable")
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
