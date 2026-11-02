//go:build rhiza_ack_diagnostic && rhiza_local_testhooks

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestACKDiagnosticWindow(t *testing.T) {
	c := newACKCollector()
	c.observe(localtesthooks.ACKPhaseEvent{Phase: "propose", Nanos: 7})
	if _, err := c.finish(); err == nil {
		t.Fatal("finish before start accepted")
	}
	if err := c.start(); err != nil {
		t.Fatal(err)
	}
	if err := c.start(); err == nil {
		t.Fatal("second start reset counters")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				c.observe(localtesthooks.ACKPhaseEvent{Phase: "propose", Nanos: 200000, Commands: 2, Bytes: 8})
			}
		})
	}
	wg.Wait()
	c.observe(localtesthooks.ACKPhaseEvent{Phase: "propose", Nanos: 9000000000, Failed: true})
	c.observe(localtesthooks.ACKPhaseEvent{Phase: "not-a-phase", Nanos: 1})
	c.observe(localtesthooks.ACKPhaseEvent{Phase: "apply", Nanos: -1})
	data, err := c.finish()
	if err != nil {
		t.Fatal(err)
	}
	c.observe(localtesthooks.ACKPhaseEvent{Phase: "propose", Nanos: 7})
	var result ackWindow
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	p := result.Phases["propose"]
	if p.Samples != 81 || p.Commands != 160 || p.Bytes != 640 || p.Failures != 1 || p.Buckets[1] != 80 || p.Buckets[13] != 1 || result.Invalid != 2 {
		t.Fatalf("unexpected bounded aggregate: %+v invalid=%d", p, result.Invalid)
	}
	if _, err := c.finish(); err == nil {
		t.Fatal("second finish accepted")
	}
}

func TestACKDiagnosticSignalOwnership(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	stop, err := startACKSignalCollector(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	waitFile := func(suffix string) {
		t.Helper()
		name := filepath.Join(dir, fmt.Sprintf("%d.%s", os.Getpid(), suffix))
		deadline := time.Now().Add(time.Second)
		for {
			if _, err := os.Stat(name); err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("missing %s", suffix)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitFile("ready")
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	waitFile("started")
	trace := localtesthooks.NewACKPhaseTrace()
	trace.Finish("wal_durable", false)
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR2); err != nil {
		t.Fatal(err)
	}
	waitFile("json")
	stop()
	if _, err := startACKSignalCollector(dir); err == nil {
		t.Fatal("existing owner output overwritten")
	}
	if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%d.invalid", os.Getpid()))); !os.IsNotExist(err) {
		t.Fatal("unexpected invalid marker", err)
	}
}

func TestACKDiagnosticCancelledOwnerRestoresHook(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	var calls int
	restore := localtesthooks.SetACKPhase(func(localtesthooks.ACKPhaseEvent) { calls++ })
	defer restore()
	stop, err := startACKSignalCollector(dir)
	if err != nil {
		t.Fatal(err)
	}
	stop() // Cancellation must join the owner even without a finish signal.
	stop()
	localtesthooks.NewACKPhaseTrace().Finish("apply", false)
	if calls != 1 {
		t.Fatalf("previous hook was not restored: calls=%d", calls)
	}
	if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%d.json", os.Getpid()))); !os.IsNotExist(err) {
		t.Fatal("cancelled window claimed a completed result", err)
	}
}
