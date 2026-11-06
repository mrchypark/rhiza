//go:build linux

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// A pidfd pins the process identity; no later numeric-PID signal is used.
type faultTarget struct {
	mu sync.Mutex
	fd int
}

func openFaultTarget(pid int, expectedStart string, ownerPID int) (*faultTarget, error) {
	if pid <= 1 || pid == os.Getpid() {
		return nil, fmt.Errorf("invalid fixed fault target")
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	target := &faultTarget{fd: fd}
	parent, start, err := faultProcessIdentity(pid)
	if err != nil || parent != ownerPID || start != expectedStart || expectedStart == "" {
		target.close()
		return nil, fmt.Errorf("fault target birth/parent identity mismatch")
	}
	if err := target.alive(); err != nil {
		target.close()
		return nil, err
	}
	return target, nil
}

func faultProcessIdentity(pid int) (int, string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, "", err
	}
	// comm is parenthesized and may itself contain spaces or parentheses.
	end := strings.LastIndex(string(data), ") ")
	if end < 0 {
		return 0, "", fmt.Errorf("invalid process identity")
	}
	fields := strings.Fields(string(data)[end+2:])
	if len(fields) < 20 {
		return 0, "", fmt.Errorf("short process identity")
	}
	parent, err := strconv.Atoi(fields[1])
	return parent, fields[19], err
}

func (t *faultTarget) alive() error {
	if t.fd < 0 {
		return fmt.Errorf("fault identity handle closed")
	}
	poll := []unix.PollFd{{Fd: int32(t.fd), Events: unix.POLLIN}}
	n, err := unix.Poll(poll, 0)
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("fault target already exited or unavailable")
	}
	return nil
}

func (t *faultTarget) signal() (time.Time, time.Time, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.alive(); err != nil {
		return time.Time{}, time.Time{}, err
	}
	before := time.Now()
	err := unix.PidfdSendSignal(t.fd, unix.SIGKILL, nil, 0)
	after := time.Now()
	return before, after, err
}

func (t *faultTarget) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fd >= 0 {
		_ = unix.Close(t.fd)
		t.fd = -1
	}
}
