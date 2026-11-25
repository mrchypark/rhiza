package qlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestReservedCompactionCommittedCleanupFailureRequiresReopen(t *testing.T) {
	for _, failure := range []string{"remove", "sync"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			w, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Append(Entry{Slot: 1, Type: EntryReceipt, Payload: []byte("before")}); err != nil {
				t.Fatal(err)
			}
			c, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil, 900)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Abort()
			if err := c.Build(); err != nil {
				t.Fatal(err)
			}
			oldRemove, oldSync := w.remove, w.syncDir
			removedSegment := false
			w.remove = func(p string) error {
				if strings.HasPrefix(filepath.Base(p), "seg_") {
					if failure == "remove" {
						return syscall.EIO
					}
					removedSegment = true
				}
				return oldRemove(p)
			}
			w.syncDir = func(p string) error {
				if failure == "sync" && removedSegment {
					return syscall.EIO
				}
				return oldSync(p)
			}
			if err := c.Commit(); err == nil {
				t.Fatal("committed cleanup failure was ignored")
			}
			if err := w.Append(Entry{Slot: 2, Type: EntryReceipt}); err == nil {
				t.Fatal("append allowed before cleanup recovery")
			}
			w.remove, w.syncDir = oldRemove, oldSync
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			entries, err := w.Read()
			if err != nil || len(entries) != 1 || entries[0].Type != EntryCheckpoint {
				t.Fatalf("reopen: entries=%v err=%v", entries, err)
			}
			if err := w.Append(Entry{Slot: 2, Type: EntryReceipt}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReservedCompactionProtectsPublishedWALLimit(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetMaxBytes(1024); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Slot: 1, Type: EntryReceipt, Payload: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	plan, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil, 900)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	if got := w.Capacity(); got.Used != 900 || got.RewriteReserved != 900 {
		t.Fatalf("capacity=%+v", got)
	}
	if err := w.Append(Entry{Slot: 2, Type: EntryReceipt, Payload: bytes.Repeat([]byte("x"), 100)}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("append beyond post-compaction capacity: %v", err)
	}
	if err := w.Append(Entry{Slot: 2, Type: EntryReceipt, Payload: []byte("tail")}); err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := w.Capacity(); got.RewriteReserved != 0 {
		t.Fatalf("reservation after commit: %+v", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	entries, err := w.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[1].Slot != 2 || !bytes.Equal(entries[1].Payload, []byte("tail")) {
		t.Fatalf("recovered entries=%+v", entries)
	}
}

func TestReservedCompactionAbortAndUndersizedReservation(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetMaxBytes(1024); err != nil {
		t.Fatal(err)
	}
	plan, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); !errors.Is(err, ErrCapacity) {
		t.Fatalf("undersized reservation: %v", err)
	}
	plan.Abort()
	if got := w.Capacity(); got.RewriteReserved != 0 {
		t.Fatalf("reservation after abort: %+v", got)
	}
	if err := w.Append(Entry{Slot: 2, Type: EntryReceipt, Payload: bytes.Repeat([]byte("x"), 200)}); err != nil {
		t.Fatal(err)
	}
}

func TestReservedCompactionSyncFailureReleasesOnAbort(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetMaxBytes(1024); err != nil {
		t.Fatal(err)
	}
	if _, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint}, nil, ^uint64(0)); err == nil {
		t.Fatal("accepted overflowing reservation")
	}
	plan, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil, 900)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetMaxBytes(899); !errors.Is(err, ErrCapacity) {
		t.Fatalf("shrinking below reservation: %v", err)
	}
	if err := w.RestoreSegment(99, (Entry{Slot: 2, Type: EntryReceipt}).Encode()); err == nil {
		t.Fatal("valid segment accepted during reservation")
	}
	oldSync := w.syncFile
	w.syncFile = func(*os.File) error { return syscall.EIO }
	err = plan.Build()
	w.syncFile = oldSync
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("sync failure=%v", err)
	}
	plan.Abort()
	if got := w.Capacity(); got.RewriteReserved != 0 {
		t.Fatalf("reservation after abort: %+v", got)
	}
}

func TestReservedCompactionCleanupFailureFailsClosedUntilReopen(t *testing.T) {
	for _, phase := range []string{"temporary", "target"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			w, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.SetMaxBytes(1024); err != nil {
				t.Fatal(err)
			}
			if err := w.Append(Entry{Slot: 1, Type: EntryReceipt, Payload: []byte("before")}); err != nil {
				t.Fatal(err)
			}
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
			plan, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil, 900)
			if err != nil {
				t.Fatal(err)
			}
			oldRemove, oldSync := w.remove, w.syncFile
			w.remove = func(path string) error {
				if phase == "temporary" && strings.HasPrefix(filepath.Base(path), ".rhiza-compact-") || phase == "target" && path == plan.targetPath {
					return syscall.EACCES
				}
				return oldRemove(path)
			}
			if phase == "temporary" {
				w.syncFile = func(*os.File) error { return syscall.EIO }
			}
			buildErr := plan.Build()
			if phase == "temporary" && (!errors.Is(buildErr, syscall.EIO) || !errors.Is(buildErr, syscall.EACCES)) {
				t.Fatalf("build error=%v", buildErr)
			}
			if phase == "target" && buildErr != nil {
				t.Fatal(buildErr)
			}
			plan.Abort()
			if got := w.Capacity(); got.RewriteReserved != 900 || !errors.Is(got.Fatal, syscall.EACCES) {
				t.Fatalf("cleanup failure capacity=%+v", got)
			}
			if err := w.Append(Entry{Slot: 2, Type: EntryReceipt}); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("append after failed cleanup=%v", err)
			}
			w.remove, w.syncFile = oldRemove, oldSync
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := reopened.Capacity(); got.RewriteReserved != 0 || got.Fatal != nil {
				t.Fatalf("reopened capacity=%+v", got)
			}
			entries, err := reopened.Read()
			if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].Payload, []byte("before")) {
				t.Fatalf("reopened entries=%+v error=%v", entries, err)
			}
			temps, err := filepath.Glob(filepath.Join(dir, ".rhiza-compact-*"))
			if err != nil || len(temps) != 0 {
				t.Fatalf("temporary files=%v error=%v", temps, err)
			}
			if _, err := os.Stat(plan.targetPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("uncommitted target remains: %v", err)
			}
		})
	}
}

func TestReservedCompactionCleanupDirSyncFailureFailsClosed(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetMaxBytes(1024); err != nil {
		t.Fatal(err)
	}
	plan, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil, 900)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	oldSyncDir := w.syncDir
	w.syncDir = func(string) error { return syscall.EIO }
	plan.Abort()
	if got := w.Capacity(); got.RewriteReserved != 900 || !errors.Is(got.Fatal, syscall.EIO) {
		t.Fatalf("unsynced cleanup capacity=%+v", got)
	}
	if err := w.Append(Entry{Slot: 2, Type: EntryReceipt}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("append after unsynced cleanup=%v", err)
	}
	w.syncDir = oldSyncDir
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Capacity(); got.RewriteReserved != 0 || got.Fatal != nil {
		t.Fatalf("reopened capacity=%+v", got)
	}
}

func TestReservedCompactionOrphanCleanupAfterDeferredRecovery(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetMaxBytes(1024); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Slot: 1, Type: EntryReceipt, Payload: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	plan, err := w.BeginCompactionReserved(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil, 900)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	oldRemove := w.remove
	w.remove = func(path string) error {
		if path == plan.targetPath {
			return syscall.EACCES
		}
		return oldRemove(path)
	}
	plan.Abort()
	w.remove = oldRemove
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenLocalDeferredCleanup(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Stat(plan.targetPath); err != nil {
		t.Fatalf("orphan must remain until recovery completes: %v", err)
	}
	entries, err := reopened.Read()
	if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].Payload, []byte("before")) {
		t.Fatalf("selected authority entries=%+v error=%v", entries, err)
	}
	if err := reopened.FinalizeDeferredCleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plan.targetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan after deferred cleanup: %v", err)
	}
	if got := reopened.Capacity(); got.RewriteReserved != 0 || got.Fatal != nil {
		t.Fatalf("capacity after cleanup=%+v", got)
	}
}
