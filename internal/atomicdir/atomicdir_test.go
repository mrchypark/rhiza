package atomicdir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestRenameNoReplaceMovesDirectory(t *testing.T) {
	root := canonicalTempDir(t)
	oldPath, newPath := filepath.Join(root, "old"), filepath.Join(root, "new")
	if err := os.Mkdir(oldPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldPath, "evidence"), []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameNoReplace(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source remains: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(newPath, "evidence")); err != nil || string(got) != "kept" {
		t.Fatalf("moved evidence=%q err=%v", got, err)
	}
}

func TestRenameNoReplaceDestinationRaceNeverOverwrites(t *testing.T) {
	root := canonicalTempDir(t)
	oldPath, newPath := filepath.Join(root, "old"), filepath.Join(root, "new")
	if err := os.Mkdir(oldPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldPath, "evidence"), []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := renameNoReplaceWithHook(oldPath, newPath, func() {
		if err := os.Mkdir(newPath, 0o700); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(newPath, "evidence"), []byte("destination"), 0o600); err != nil {
			t.Error(err)
		}
	})
	if !errors.Is(err, unix.EEXIST) && !errors.Is(err, unix.ENOTEMPTY) {
		t.Fatalf("race error=%v", err)
	}
	if got, err := os.ReadFile(filepath.Join(oldPath, "evidence")); err != nil || string(got) != "source" {
		t.Fatalf("source evidence=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(newPath, "evidence")); err != nil || string(got) != "destination" {
		t.Fatalf("destination evidence=%q err=%v", got, err)
	}
}

func TestRenameNoReplaceHoldsOpenedParent(t *testing.T) {
	root := canonicalTempDir(t)
	parent := filepath.Join(root, "parent")
	movedParent := filepath.Join(root, "moved-parent")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	oldPath, newPath := filepath.Join(parent, "old"), filepath.Join(parent, "new")
	if err := os.Mkdir(oldPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldPath, "evidence"), []byte("original-parent"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := renameNoReplaceWithHook(oldPath, newPath, func() {
		if err := os.Rename(parent, movedParent); err != nil {
			t.Error(err)
		}
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(parent, "new"), []byte("replacement-parent"), 0o600); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(movedParent, "new", "evidence")); err != nil || string(got) != "original-parent" {
		t.Fatalf("held-parent evidence=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(parent, "new")); err != nil || string(got) != "replacement-parent" {
		t.Fatalf("replacement parent was modified: %q err=%v", got, err)
	}
}

func TestRenameNoReplaceRejectsUnsafePaths(t *testing.T) {
	root := canonicalTempDir(t)
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(realParent, "old"), 0o700); err != nil {
		t.Fatal(err)
	}
	linkParent := filepath.Join(root, "link")
	if err := os.Symlink(realParent, linkParent); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		old  string
		new  string
	}{
		{name: "symlink component", old: filepath.Join(linkParent, "old"), new: filepath.Join(linkParent, "new")},
		{name: "parent traversal", old: root + "/real/../real/old", new: filepath.Join(root, "real", "new")},
		{name: "different parent", old: filepath.Join(realParent, "old"), new: filepath.Join(root, "new")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := RenameNoReplace(tc.old, tc.new); err == nil {
				t.Fatal("unsafe path accepted")
			}
		})
	}
	if _, err := os.Lstat(filepath.Join(realParent, "old")); err != nil {
		t.Fatalf("unsafe move removed source: %v", err)
	}
}
