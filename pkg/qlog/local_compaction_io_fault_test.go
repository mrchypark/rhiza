package qlog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func TestLocalCompactionIOFailuresPreserveSelectedAuthority(t *testing.T) {
	for _, fault := range []string{"build-enospc", "build-short-write", "build-sync", "commit-active-sync", "commit-manifest-sync"} {
		t.Run(fault, func(t *testing.T) {
			w, before := localCompactionIOFixture(t)
			defer w.Close()
			originalSegments, globErr := filepath.Glob(filepath.Join(w.dir, "seg_*.log"))
			if globErr != nil || len(originalSegments) == 0 {
				t.Fatalf("list original segments: %v (%v)", originalSegments, globErr)
			}
			c, err := w.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil)
			if err != nil {
				t.Fatal(err)
			}
			oldWrite, oldSync, oldDir := w.writeAt, w.syncFile, w.syncDir
			defer func() { w.writeAt, w.syncFile, w.syncDir = oldWrite, oldSync, oldDir }()
			switch fault {
			case "build-enospc":
				w.writeAt = func(f *os.File, p []byte, off int64) (int, error) {
					n, _ := f.WriteAt(p[:min(8, len(p))], off)
					return n, syscall.ENOSPC
				}
			case "build-short-write":
				w.writeAt = func(f *os.File, p []byte, off int64) (int, error) { return f.WriteAt(p[:len(p)-1], off) }
			case "build-sync":
				w.syncFile = func(*os.File) error { return syscall.EIO }
			case "commit-active-sync":
				if err := c.Build(); err != nil {
					t.Fatal(err)
				}
				w.syncFile = func(f *os.File) error {
					if f.Name() == w.current.file.Name() {
						return syscall.EIO
					}
					return oldSync(f)
				}
			case "commit-manifest-sync":
				if err := c.Build(); err != nil {
					t.Fatal(err)
				}
				failed := false
				w.syncDir = func(dir string) error {
					if dir == w.dir && !failed {
						failed = true
						return syscall.EIO
					}
					return oldDir(dir)
				}
			}
			if fault == "build-enospc" || fault == "build-short-write" || fault == "build-sync" {
				err = c.Build()
			} else {
				err = c.Commit()
			}
			if err == nil {
				t.Fatalf("%s unexpectedly succeeded", fault)
			}
			got, readErr := w.Read()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if fault != "commit-manifest-sync" && !reflect.DeepEqual(got, before) {
				t.Fatalf("prepublication failure changed selected WAL entries: got=%v want=%v", got, before)
			}
			if fault == "commit-manifest-sync" && w.Capacity().Fatal == nil {
				t.Fatal("post-rename manifest sync failure did not mark WAL fatal/uncertain")
			}
			if fault != "commit-manifest-sync" {
				selected, openErr := OpenReadOnly(w.dir)
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer selected.Close()
				entries, _ := selected.Read()
				if len(entries) == 0 || entries[0].Type == EntryCheckpoint {
					t.Fatalf("failed compaction advanced selected floor: %+v", entries)
				}
			}
			for _, path := range originalSegments {
				if _, statErr := os.Stat(path); statErr != nil {
					t.Fatalf("failed compaction removed old WAL authority file %s: %v", path, statErr)
				}
			}
			c.Abort()
		})
	}
}

func TestLocalCompactionCleanupFailureIsDurableDebtNotCommitFailure(t *testing.T) {
	w, _ := localCompactionIOFixture(t)
	defer w.Close()
	segments, err := filepath.Glob(filepath.Join(w.dir, "seg_*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("list source segments: %v (%v)", segments, err)
	}
	c, err := w.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Build(); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(); err != nil {
		t.Fatal(err)
	}
	oldRemove := w.remove
	obsolete := map[string]bool{}
	for _, path := range segments {
		obsolete[path] = true
	}
	w.remove = func(path string) error {
		if obsolete[path] {
			return syscall.EIO
		}
		return oldRemove(path)
	}
	if err := w.FinalizeDeferredCleanup(); err == nil {
		t.Fatal("cleanup fault was not reported")
	}
	if got := w.Capacity().Fatal; got != nil {
		t.Fatalf("cleanup debt poisoned committed WAL: %v", got)
	}
	for path := range obsolete {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("obsolete segment removed despite cleanup failure: %v", err)
		}
	}
	w.remove = oldRemove
	if err := w.FinalizeDeferredCleanup(); err != nil {
		t.Fatalf("retry cleanup: %v", err)
	}
	for path := range obsolete {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("obsolete segment remains after successful cleanup: %v", err)
		}
	}
}

func TestLocalCompactionCleanupDirSyncFailureRemainsRetryableDebt(t *testing.T) {
	w, _ := localCompactionIOFixture(t)
	defer w.Close()
	segments, err := filepath.Glob(filepath.Join(w.dir, "seg_*.log"))
	if err != nil || len(segments) == 0 {
		t.Fatalf("list source segments: %v (%v)", segments, err)
	}
	c, err := w.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Build(); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(); err != nil {
		t.Fatal(err)
	}
	oldSyncDir := w.syncDir
	w.syncDir = func(dir string) error {
		if dir == w.dir {
			return syscall.EIO
		}
		return oldSyncDir(dir)
	}
	if err := w.FinalizeDeferredCleanup(); err == nil {
		t.Fatal("directory sync failure was not reported")
	}
	if got := w.Capacity().Fatal; got != nil {
		t.Fatalf("cleanup directory-sync debt poisoned committed WAL: %v", got)
	}
	for _, path := range segments {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cleanup did not remove obsolete segment %s: %v", path, err)
		}
	}
	w.syncDir = oldSyncDir
	if err := w.FinalizeDeferredCleanup(); err != nil {
		t.Fatalf("retry cleanup directory sync: %v", err)
	}
}

func TestLocalDeferredCleanupEndsAfterStartupFinalization(t *testing.T) {
	w, _ := localCompactionIOFixture(t)
	defer w.Close()
	if err := w.FinalizeDeferredCleanup(); err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 2; cycle++ {
		old := append([]*Segment(nil), w.segments...)
		c, err := w.BeginCompaction(Entry{Slot: uint64(cycle + 1), Type: EntryCheckpoint, Payload: []byte("base")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Build(); err != nil {
			t.Fatal(err)
		}
		if err := c.Commit(); err != nil {
			t.Fatal(err)
		}
		for _, seg := range old {
			if _, err := os.Stat(w.segmentPath(seg.index)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("runtime compaction %d retained obsolete segment %d: %v", cycle, seg.index, err)
			}
		}
	}
}
func localCompactionIOFixture(t *testing.T) (*WAL, []Entry) {
	t.Helper()
	w, err := OpenLocalDeferredCleanup(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{{Slot: 1, Type: EntryProposal, Payload: []byte("old")}, {Slot: 2, Type: EntryReceipt, Payload: []byte("receipt")}}
	for _, entry := range entries {
		if err := w.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	return w, entries
}
