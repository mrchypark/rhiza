package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
)

func TestLocalCheckpointPublishIOFailuresNeverExposePartialRoot(t *testing.T) {
	for _, fault := range []string{"enospc", "short-write", "role-sync", "descriptor-sync", "rename", "root-dir-sync"} {
		t.Run(fault, func(t *testing.T) {
			identity, files, prefix := localCheckpointIOFixture(t)
			restore := installLocalCheckpointIOFault(t, fault, identity.rootDir)
			_, err := identity.Publish(context.Background(), files, 5, prefix, 1)
			restore()
			if err == nil {
				t.Fatalf("Publish succeeded with injected %s failure", fault)
			}
			entries, readErr := os.ReadDir(identity.rootDir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			for _, entry := range entries {
				if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".capture-") {
					t.Fatalf("failed publication left temporary/incomplete root entry %q", entry.Name())
				}
				rootPath := filepath.Join(identity.rootDir, entry.Name())
				if _, _, err := identity.OpenRoot(mustLocalHash(entry.Name())); err != nil {
					t.Fatalf("failed publication exposed partial root %q: %v", entry.Name(), err)
				}
				if _, err := os.Stat(filepath.Join(rootPath, "descriptor.json")); err != nil {
					t.Fatalf("published root %q is incomplete: %v", entry.Name(), err)
				}
			}
			if fault != "root-dir-sync" && len(entries) != 0 {
				t.Fatalf("prepublication %s failure left %d root entries", fault, len(entries))
			}
		})
	}
}

func localCheckpointIOFixture(t *testing.T) (localCheckpointIdentity, []materializer.CheckpointFile, [32]byte) {
	t.Helper()
	identity, err := openLocalCheckpointIdentity(&types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	source := t.TempDir()
	files := []materializer.CheckpointFile{
		{Role: materializer.CheckpointSQLite, Path: filepath.Join(source, "sqlite.capture")},
		{Role: materializer.CheckpointGraphData, Path: filepath.Join(source, "graph.capture")},
	}
	for _, file := range files {
		if err := os.WriteFile(file.Path, []byte(strings.Repeat(string(file.Role), 256)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var prefix [32]byte
	prefix[0] = 9
	return identity, files, prefix
}

func installLocalCheckpointIOFault(t *testing.T, fault, rootDir string) func() {
	t.Helper()
	oldWrite, oldSync, oldDirSync, oldRename := localCheckpointWriteFile, localCheckpointSyncFile, localCheckpointSyncDir, localCheckpointRename
	t.Cleanup(func() {
		localCheckpointWriteFile, localCheckpointSyncFile, localCheckpointSyncDir, localCheckpointRename = oldWrite, oldSync, oldDirSync, oldRename
	})
	failed := false
	switch fault {
	case "enospc":
		localCheckpointWriteFile = func(file *os.File, data []byte) (int, error) {
			if strings.Contains(file.Name(), ".capture-") && !strings.HasSuffix(file.Name(), "descriptor.json") && !failed {
				failed = true
				n := min(8, len(data))
				written, _ := file.Write(data[:n])
				return written, syscall.ENOSPC
			}
			return oldWrite(file, data)
		}
	case "short-write":
		localCheckpointWriteFile = func(file *os.File, data []byte) (int, error) {
			if strings.Contains(file.Name(), ".capture-") && !failed {
				failed = true
				return file.Write(data[:len(data)-1])
			}
			return oldWrite(file, data)
		}
	case "role-sync":
		localCheckpointSyncFile = func(file *os.File) error {
			if strings.Contains(file.Name(), ".capture-") && !strings.HasSuffix(file.Name(), "descriptor.json") && !failed {
				failed = true
				return syscall.EIO
			}
			return oldSync(file)
		}
	case "descriptor-sync":
		localCheckpointSyncFile = func(file *os.File) error {
			if strings.HasSuffix(file.Name(), "descriptor.json") && !failed {
				failed = true
				return syscall.EIO
			}
			return oldSync(file)
		}
	case "rename":
		localCheckpointRename = func(old, new string) error {
			if filepath.Dir(new) == rootDir && !failed {
				failed = true
				return syscall.EIO
			}
			return oldRename(old, new)
		}
	case "root-dir-sync":
		localCheckpointSyncDir = func(dir string) error {
			if dir == rootDir && !failed {
				failed = true
				return syscall.EIO
			}
			return oldDirSync(dir)
		}
	default:
		t.Fatalf("unknown checkpoint I/O fault %q", fault)
	}
	return func() {
		localCheckpointWriteFile, localCheckpointSyncFile, localCheckpointSyncDir, localCheckpointRename = oldWrite, oldSync, oldDirSync, oldRename
	}
}
