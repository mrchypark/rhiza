//go:build linux

package fencing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestCheckInitNamespaceQuiescenceOnlyObservesOrReapsExitedChildren(t *testing.T) {
	t.Run("live process blocks without signal path", func(t *testing.T) {
		proc := t.TempDir()
		if err := os.Mkdir(filepath.Join(proc, "234"), 0700); err != nil {
			t.Fatal(err)
		}
		waitCalls := 0
		wait := func() (int, error) { waitCalls++; return -1, syscall.ECHILD }
		stat := func(name string) error { _, err := os.Stat(name); return err }
		if err := checkInitNamespaceQuiescence(context.Background(), proc, wait, stat); err == nil {
			t.Fatal("live process was treated as quiescent")
		}
		if waitCalls != 1 {
			t.Fatalf("wait calls=%d, want one; helper must not signal or retry live processes", waitCalls)
		}
	})

	t.Run("reaps only an already-exited owned child", func(t *testing.T) {
		proc := t.TempDir()
		calls := 0
		wait := func() (int, error) {
			calls++
			if calls == 1 {
				return 345, nil
			}
			return -1, syscall.ECHILD
		}
		if err := checkInitNamespaceQuiescence(context.Background(), proc, wait, func(string) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if calls != 2 {
			t.Fatalf("wait calls=%d, want reaped child then ECHILD", calls)
		}
	})

	t.Run("process disappearance after enumeration is not a live child", func(t *testing.T) {
		proc := t.TempDir()
		if err := os.Mkdir(filepath.Join(proc, "456"), 0700); err != nil {
			t.Fatal(err)
		}
		wait := func() (int, error) { return -1, syscall.ECHILD }
		if err := checkInitNamespaceQuiescence(context.Background(), proc, wait, func(string) error { return os.ErrNotExist }); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("wait interruption retries, other wait errors fail", func(t *testing.T) {
		proc := t.TempDir()
		calls := 0
		wait := func() (int, error) {
			calls++
			if calls == 1 {
				return -1, syscall.EINTR
			}
			return -1, syscall.ECHILD
		}
		if err := checkInitNamespaceQuiescence(context.Background(), proc, wait, func(string) error { return nil }); err != nil || calls != 2 {
			t.Fatalf("EINTR handling err=%v calls=%d", err, calls)
		}
		if err := checkInitNamespaceQuiescence(context.Background(), proc, func() (int, error) { return -1, syscall.EPERM }, func(string) error { return nil }); !errors.Is(err, syscall.EPERM) {
			t.Fatalf("wait error=%v, want EPERM", err)
		}
	})

	t.Run("proc lookup errors fail and context cancellation is honored", func(t *testing.T) {
		proc := t.TempDir()
		if err := os.Mkdir(filepath.Join(proc, "567"), 0700); err != nil {
			t.Fatal(err)
		}
		wait := func() (int, error) { return -1, syscall.ECHILD }
		if err := checkInitNamespaceQuiescence(context.Background(), proc, wait, func(string) error { return syscall.EACCES }); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("proc lookup error=%v, want EACCES", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := checkInitNamespaceQuiescence(ctx, proc, wait, func(string) error { return nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled check=%v, want context.Canceled", err)
		}
	})

	t.Run("proc enumeration failure fails closed", func(t *testing.T) {
		if err := checkInitNamespaceQuiescence(context.Background(), filepath.Join(t.TempDir(), "missing"), func() (int, error) { return -1, syscall.ECHILD }, func(string) error { return nil }); err == nil {
			t.Fatal("missing proc root was treated as empty")
		}
	})
}

func TestCheckInitNamespaceQuiescenceRejectsNonPIDOne(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("running as PID 1; requires the container integration profile")
	}
	if err := CheckInitNamespaceQuiescence(context.Background()); err == nil {
		t.Fatal("quiescence check accepted a non-PID-1 process")
	}
}
