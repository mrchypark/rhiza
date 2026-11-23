//go:build rhiza_stall_diagnostics && linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStallDiagnosticSignalWritesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stacks")
	t.Setenv("RHIZA_STALL_TRACE_FILE", path)
	stop := startStallDiagnostics(context.Background())
	defer stop()
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(data), "goroutine profile") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no diagnostic snapshot")
}
