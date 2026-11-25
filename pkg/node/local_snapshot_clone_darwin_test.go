//go:build darwin

package node

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
)

func TestLocalSnapshotCloneIsolatesHardlinkAndMetadata(t *testing.T) {
	clones, copies := requireRealLocalSnapshotClones(t)
	identity, files, prefix := localCheckpointIOFixture(t)
	alias := files[0].Path + ".alias"
	if err := os.Link(files[0].Path, alias); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(files[0].Path, "user.rhiza-test", []byte("private source metadata"), 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(files[0].Path, 0400); err != nil {
		t.Fatal(err)
	}
	descriptor, published, err := publishSnapshotForCloneTest(identity, files, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if *clones != len(files) || *copies != 0 {
		t.Fatalf("wanted %d clones and no fallback copy, got clones=%d copies=%d", len(files), *clones, *copies)
	}
	sourceInfo, err := os.Stat(files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range published {
		info, err := os.Stat(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("clone mode %o", info.Mode().Perm())
		}
		if _, err := unix.Getxattr(file.Path, "user.rhiza-test", nil); !errors.Is(err, unix.ENOATTR) {
			t.Fatalf("source attribute remained on clone: %v", err)
		}
		probe, err := os.CreateTemp(filepath.Dir(file.Path), ".fresh-metadata-")
		if err != nil {
			t.Fatal(err)
		}
		probePath := probe.Name()
		if err := probe.Close(); err != nil {
			t.Fatal(err)
		}
		cloneAttrs := snapshotTestXattrs(t, file.Path)
		freshAttrs := snapshotTestXattrs(t, probePath)
		if err := os.Remove(probePath); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(cloneAttrs, freshAttrs) {
			t.Fatal("clone metadata differs from a fresh 0600 file")
		}
		if file.Role == files[0].Role && os.SameFile(sourceInfo, info) {
			t.Fatal("clone shares source inode")
		}
	}
	if err := os.Chmod(alias, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(alias, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := unix.Getxattr(alias, "user.rhiza-test", nil); err != nil || got == 0 {
		t.Fatalf("source metadata unexpectedly changed: size=%d err=%v", got, err)
	}
	if _, _, err := identity.OpenRoot(mustLocalHash(descriptor.RootHash)); err != nil {
		t.Fatalf("source alias corrupted clone: %v", err)
	}
}

func snapshotTestXattrs(t *testing.T, path string) map[string]string {
	t.Helper()
	size, err := unix.Listxattr(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]byte, size)
	if size != 0 {
		if _, err := unix.Listxattr(path, names); err != nil {
			t.Fatal(err)
		}
	}
	result := make(map[string]string)
	for _, name := range strings.Split(string(names), "\x00") {
		if name == "" {
			continue
		}
		size, err := unix.Getxattr(path, name, nil)
		if err != nil {
			t.Fatal(err)
		}
		data := make([]byte, size)
		if size != 0 {
			if _, err := unix.Getxattr(path, name, data); err != nil {
				t.Fatal(err)
			}
		}
		result[name] = string(data)
	}
	return result
}

func TestLocalSnapshotCloneSourceMutationDuringClone(t *testing.T) {
	identity, files, prefix := localCheckpointIOFixture(t)
	before, err := os.ReadFile(files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	old := cloneLocalSnapshot
	defer func() { cloneLocalSnapshot = old }()
	changed := false
	cloneLocalSnapshot = func(src, dst int, name string, flags int) error {
		if err := old(src, dst, name, flags); err != nil {
			return err
		}
		if !changed {
			changed = true
			return os.WriteFile(files[0].Path, []byte("changed after clone"), 0600)
		}
		return nil
	}
	_, published, err := publishSnapshotForCloneTest(identity, files, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("source mutation was not injected")
	}
	for _, file := range published {
		if file.Role == files[0].Role {
			got, err := os.ReadFile(file.Path)
			if err != nil || string(got) != string(before) {
				t.Fatalf("clone changed with source: %q %v", got, err)
			}
		}
	}
}

func TestLocalSnapshotCloneMetadataFallbackAndIOErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failure  error
		fallback bool
	}{
		{"cannot-normalize", unix.EPERM, true}, {"io", unix.EIO, false}, {"no-space", unix.ENOSPC, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity, files, prefix := localCheckpointIOFixture(t)
			old := sanitizeLocalSnapshotMetadata
			oldWrite := localCheckpointWriteFile
			writes := 0
			localCheckpointWriteFile = func(file *os.File, data []byte) (int, error) {
				if filepath.Base(file.Name()) != "descriptor.json" {
					writes++
				}
				return oldWrite(file, data)
			}
			sanitizeLocalSnapshotMetadata = func(int, string) error { return tc.failure }
			defer func() { sanitizeLocalSnapshotMetadata = old; localCheckpointWriteFile = oldWrite }()
			_, err := identity.publishSnapshot(context.Background(), files, 5, prefix, 1)
			if tc.fallback && err != nil {
				t.Fatalf("metadata fallback failed: %v", err)
			}
			if !tc.fallback && !errors.Is(err, tc.failure) {
				t.Fatalf("got %v want %v", err, tc.failure)
			}
			if tc.fallback && writes == 0 {
				t.Fatal("metadata fallback did not copy snapshot files")
			}
			if !tc.fallback && writes != 0 {
				t.Fatal("unsafe I/O error fell back to copying")
			}
			if !tc.fallback {
				entries, readErr := os.ReadDir(identity.rootDir)
				if readErr != nil || len(entries) != 0 {
					t.Fatalf("failed metadata left roots: %v %v", entries, readErr)
				}
			}
		})
	}
}

func TestLocalSnapshotCloneMetadataProbeCleanupFailureIsFailClosed(t *testing.T) {
	for _, failure := range []error{unix.EIO, unix.EACCES} {
		t.Run(failure.Error(), func(t *testing.T) {
			clones, copies := requireRealLocalSnapshotClones(t)
			identity, files, prefix := localCheckpointIOFixture(t)
			old := removeLocalCloneMetadataProbe
			called := false
			removeLocalCloneMetadataProbe = func(string) error { called = true; return failure }
			t.Cleanup(func() { removeLocalCloneMetadataProbe = old })
			_, err := identity.publishSnapshot(context.Background(), files, 5, prefix, 1)
			if !called || !errors.Is(err, errLocalCloneMetadataProbe) || !errors.Is(err, failure) {
				t.Fatalf("probe cleanup failure was not propagated: called=%v err=%v", called, err)
			}
			if *clones != 1 || *copies != 0 {
				t.Fatalf("unexpected clone/fallback: clones=%d copies=%d", *clones, *copies)
			}
			entries, readErr := os.ReadDir(identity.rootDir)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("failed cleanup left roots: %v %v", entries, readErr)
			}
		})
	}
}

func TestLocalSnapshotCloneDoesNotCopySourceACL(t *testing.T) {
	clones, copies := requireRealLocalSnapshotClones(t)
	identity, files, prefix := localCheckpointIOFixture(t)
	if output, err := exec.Command("/bin/chmod", "+a", "everyone allow readattr", files[0].Path).CombinedOutput(); err != nil {
		t.Fatalf("set source ACL: %v: %s", err, output)
	}
	sourceACL, err := exec.Command("/bin/ls", "-lde", files[0].Path).CombinedOutput()
	if err != nil || !strings.Contains(string(sourceACL), "everyone allow readattr") {
		t.Fatalf("source ACL missing: %v: %s", err, sourceACL)
	}
	_, published, err := publishSnapshotForCloneTest(identity, files, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if *clones != len(files) || *copies != 0 {
		t.Fatalf("wanted %d clones and no fallback copy, got clones=%d copies=%d", len(files), *clones, *copies)
	}
	for _, file := range published {
		acl, err := exec.Command("/bin/ls", "-lde", file.Path).CombinedOutput()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(acl), "everyone allow readattr") {
			t.Fatalf("source ACL copied to clone: %s", acl)
		}
	}
}

func requireRealLocalSnapshotClones(t *testing.T) (*int, *int) {
	t.Helper()
	oldClone, oldWrite := cloneLocalSnapshot, localCheckpointWriteFile
	clones, copies := 0, 0
	cloneLocalSnapshot = func(src, dst int, name string, flags int) error {
		err := oldClone(src, dst, name, flags)
		if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.ENOSYS) {
			t.Skipf("filesystem does not support the clone path: %v", err)
		}
		if err == nil {
			clones++
		}
		return err
	}
	localCheckpointWriteFile = func(file *os.File, data []byte) (int, error) {
		if filepath.Base(file.Name()) != "descriptor.json" {
			copies++
		}
		return oldWrite(file, data)
	}
	t.Cleanup(func() { cloneLocalSnapshot, localCheckpointWriteFile = oldClone, oldWrite })
	return &clones, &copies
}

func publishSnapshotForCloneTest(identity localCheckpointIdentity, files []materializer.CheckpointFile, prefix [32]byte) (localCheckpointDescriptor, []materializer.CheckpointFile, error) {
	descriptor, err := identity.publishSnapshot(context.Background(), files, 5, prefix, 1)
	if err != nil {
		return descriptor, nil, err
	}
	_, published, err := identity.OpenRoot(mustLocalHash(descriptor.RootHash))
	return descriptor, published, err
}

func TestLocalSnapshotCloneFallbackAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		fallback bool
	}{
		{"cross-device", unix.EXDEV, true}, {"unsupported", unix.ENOTSUP, true}, {"no-syscall", unix.ENOSYS, true},
		{"io", unix.EIO, false}, {"no-space", unix.ENOSPC, false}, {"permission", unix.EACCES, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity, files, prefix := localCheckpointIOFixture(t)
			old := cloneLocalSnapshot
			cloneLocalSnapshot = func(int, int, string, int) error { return tc.err }
			defer func() { cloneLocalSnapshot = old }()
			_, err := identity.publishSnapshot(context.Background(), files, 5, prefix, 1)
			if tc.fallback && err != nil {
				t.Fatalf("fallback failed: %v", err)
			}
			if !tc.fallback && !errors.Is(err, tc.err) {
				t.Fatalf("got %v want %v", err, tc.err)
			}
			if !tc.fallback {
				entries, readErr := os.ReadDir(identity.rootDir)
				if readErr != nil || len(entries) != 0 {
					t.Fatalf("failed clone left roots: %v, %v", entries, readErr)
				}
			}
		})
	}
}

func TestLocalSnapshotCloneRefusesExistingTargetAndSymlink(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(source, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := cloneAndHashLocalSnapshot(context.Background(), source, target, 1024); !errors.Is(err, unix.EEXIST) {
		t.Fatalf("existing target: %v", err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "existing" {
		t.Fatalf("target changed: %q %v", got, err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := cloneAndHashLocalSnapshot(context.Background(), link, filepath.Join(dir, "clone"), 1024); err == nil {
		t.Fatal("symlink source accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := cloneAndHashLocalSnapshot(ctx, source, filepath.Join(dir, "cancelled"), 1024); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context: %v", err)
	}
}

func TestLocalSnapshotCloneSyncFailureDoesNotPublish(t *testing.T) {
	identity, files, prefix := localCheckpointIOFixture(t)
	old := localCheckpointSyncFile
	called := false
	localCheckpointSyncFile = func(file *os.File) error {
		if filepath.Base(file.Name()) == "sqlite.db" {
			called = true
			return unix.EIO
		}
		return old(file)
	}
	defer func() { localCheckpointSyncFile = old }()
	_, err := identity.publishSnapshot(context.Background(), files, 5, prefix, 1)
	if !called || !errors.Is(err, unix.EIO) {
		t.Fatalf("injected sync was not propagated: called=%v error=%v", called, err)
	}
	entries, err := os.ReadDir(identity.rootDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("incomplete root: %v %v", entries, err)
	}
}

func TestLocalSnapshotCloneCrashBeforeFileSync(t *testing.T) {
	if os.Getenv("RHIZA_CLONE_CRASH_CHILD") == "1" {
		dataDir := os.Getenv("RHIZA_CLONE_CRASH_DIR")
		identity, err := openLocalCheckpointIdentity(&types.ExecutionConfig{Local: true, ClusterID: "cluster", NodeID: "node", DataDir: dataDir})
		if err != nil {
			t.Fatal(err)
		}
		files := []materializer.CheckpointFile{{Role: materializer.CheckpointSQLite, Path: os.Getenv("RHIZA_CLONE_SQLITE")}, {Role: materializer.CheckpointGraphData, Path: os.Getenv("RHIZA_CLONE_GRAPH")}}
		var prefix [32]byte
		prefix[0] = 9
		old := cloneLocalSnapshot
		cloneLocalSnapshot = func(src, dst int, name string, flags int) error {
			if err := old(src, dst, name, flags); err != nil {
				return err
			}
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			return errors.New("kill unexpectedly returned")
		}
		_, _ = identity.publishSnapshot(context.Background(), files, 5, prefix, 1)
		t.Fatal("child survived injected kill")
	}
	identity, files, _ := localCheckpointIOFixture(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestLocalSnapshotCloneCrashBeforeFileSync$", "-test.count=1")
	cmd.Env = append(os.Environ(), "RHIZA_CLONE_CRASH_CHILD=1", "RHIZA_CLONE_CRASH_DIR="+filepath.Dir(identity.rootDir), "RHIZA_CLONE_SQLITE="+files[0].Path, "RHIZA_CLONE_GRAPH="+files[1].Path)
	if err := cmd.Run(); err == nil {
		t.Fatal("child survived")
	}
	if ctx.Err() != nil {
		t.Fatalf("child timed out: %v", ctx.Err())
	}
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child status %v", cmd.ProcessState)
	}
	entries, err := os.ReadDir(identity.rootDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".capture-") {
			t.Fatalf("incomplete checkpoint exposed as root: %s", entry.Name())
		}
	}
	for _, file := range files {
		if _, err := os.Stat(file.Path); err != nil {
			t.Fatalf("source lost after crash: %v", err)
		}
	}
}
