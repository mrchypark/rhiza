package materializer

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestLocalRestoreIOFailuresResumeExactBase(t *testing.T) {
	for _, fault := range []string{"intent-enospc", "intent-short-write", "intent-sync", "intent-dir-sync", "intent-rename", "stage-enospc", "stage-short-write", "stage-sync", "stage-graph-dir-sync", "install-dir-sync", "install-rename"} {
		t.Run(fault, func(t *testing.T) {
			dbPath, authority, files, cleanup := makeLocalRestoreFixture(t)
			defer cleanup()
			if err := PrepareLocalRestore(dbPath, authority, files); err != nil {
				t.Fatal(err)
			}
			oldWrite, oldSync, oldDirSync, oldRename := localRestoreWriteFile, localRestoreSyncFile, localRestoreSyncDir, localRestoreRename
			t.Cleanup(func() {
				localRestoreWriteFile, localRestoreSyncFile, localRestoreSyncDir, localRestoreRename = oldWrite, oldSync, oldDirSync, oldRename
			})
			failed := false
			localRestoreWriteFile = func(file *os.File, data []byte) (int, error) {
				journal := strings.Contains(file.Name(), ".rhiza-local-restore-journal-")
				stage := strings.Contains(file.Name(), ".rhiza-local-restore-") && !journal
				if !failed && ((strings.Contains(fault, "enospc") || strings.Contains(fault, "short-write")) && ((strings.HasPrefix(fault, "intent-") && journal) || (strings.HasPrefix(fault, "stage-") && stage))) {
					failed = true
					if strings.Contains(fault, "enospc") {
						n := min(8, len(data))
						written, _ := file.Write(data[:n])
						return written, syscall.ENOSPC
					}
					if strings.Contains(fault, "short-write") {
						return file.Write(data[:len(data)-1])
					}
				}
				return oldWrite(file, data)
			}
			localRestoreSyncFile = func(file *os.File) error {
				journal := strings.Contains(file.Name(), ".rhiza-local-restore-journal-")
				stage := strings.Contains(file.Name(), ".rhiza-local-restore-") && !journal
				if !failed && ((fault == "intent-sync" && journal) || (fault == "stage-sync" && stage)) {
					failed = true
					return syscall.EIO
				}
				return oldSync(file)
			}
			rootSyncs := 0
			localRestoreSyncDir = func(dir string) error {
				if !failed && fault == "stage-graph-dir-sync" && strings.HasSuffix(dir, string(filepath.Separator)+"stage"+string(filepath.Separator)+"latticedb") {
					failed = true
					return syscall.EIO
				}
				if dir == filepath.Dir(dbPath) {
					rootSyncs++
					if !failed && fault == "intent-dir-sync" {
						failed = true
						return syscall.EIO
					}
					// Staged journal=1, backup SQL=2, backup graph=3,
					// then graph publication=4. Fail after one role has moved.
					if !failed && fault == "install-dir-sync" && rootSyncs == 4 {
						failed = true
						return syscall.EIO
					}
				}
				return oldDirSync(dir)
			}
			localRestoreRename = func(oldPath, newPath string) error {
				if !failed && fault == "intent-rename" && strings.Contains(oldPath, ".rhiza-local-restore-journal-") {
					failed = true
					return syscall.EIO
				}
				if !failed && fault == "install-rename" && strings.Contains(oldPath, string(filepath.Separator)+"stage"+string(filepath.Separator)) {
					failed = true
					return syscall.EIO
				}
				return oldRename(oldPath, newPath)
			}
			journal, err := readLocalRestoreJournal(localRestoreJournalPath(dbPath))
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(fault, "intent-") {
				err = writeLocalRestoreJournal(dbPath, journal)
			} else {
				err = resumeLocalRestore(dbPath, journal, files)
			}
			if err == nil || !failed {
				t.Fatalf("fault %s: err=%v injected=%v", fault, err, failed)
			}
			localRestoreWriteFile, localRestoreSyncFile, localRestoreSyncDir, localRestoreRename = oldWrite, oldSync, oldDirSync, oldRename
			if _, statErr := os.Stat(localRestoreJournalPath(dbPath)); statErr == nil {
				m, openErr := OpenLocalFromBase(dbPath, 1, authority, files, nil)
				if openErr != nil {
					t.Fatalf("resume exact base after %s: %v", fault, openErr)
				}
				if m.Tip() != authority.Index {
					t.Fatalf("resumed tip=%d want %d", m.Tip(), authority.Index)
				}
				if closeErr := m.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				if finalizeErr := FinalizeLocalRestore(dbPath, authority); finalizeErr != nil {
					t.Fatal(finalizeErr)
				}
			} else if !errors.Is(statErr, os.ErrNotExist) {
				t.Fatal(statErr)
			} else if strings.HasPrefix(fault, "stage-") || strings.HasPrefix(fault, "install-") {
				t.Fatalf("restore failure %s lost its durable intent", fault)
			}
		})
	}
}
