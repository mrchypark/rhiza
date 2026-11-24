package qlog

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsLockHelper(t *testing.T) {
	dir := os.Getenv("RHIZA_TEST_WAL_LOCK_DIR")
	if dir == "" {
		return
	}
	_, lock, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	fmt.Println("LOCKED")
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
}

func TestWindowsLockExcludesProcessAndRecoversAfterDeath(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWindowsLockHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_TEST_WAL_LOCK_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := false
	defer func() {
		if !done {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "LOCKED\n" {
		t.Fatalf("helper ready: %q %v", line, err)
	}
	contender, err := os.OpenFile(filepath.Join(dir, "lock.qlog"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	lockErr := lockWAL(contender)
	if lockErr == nil {
		_ = unlockWAL(contender)
	}
	if err := contender.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(lockErr, windows.ERROR_LOCK_VIOLATION) {
		t.Fatalf("expected lock violation, got %v", lockErr)
	}
	if _, lock, err := Acquire(dir); err == nil {
		_ = lock.Release()
		t.Fatal("second process acquired locked WAL")
	} else if err.Error() != "another process is using this WAL" {
		t.Fatalf("unexpected contention error: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	done = true
	clean, lock, err := Acquire(dir)
	if err != nil {
		t.Fatalf("acquire after process death: %v", err)
	}
	if clean {
		t.Error("process death reported clean start")
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
}
