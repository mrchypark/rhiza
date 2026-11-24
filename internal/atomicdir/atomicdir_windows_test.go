//go:build windows

package atomicdir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRenameNoReplaceFailsClosed(t *testing.T) {
	root := t.TempDir()
	source, destination := filepath.Join(root, "source"), filepath.Join(root, "destination")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := RenameNoReplace(source, destination); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("expected unsupported, got %v", err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("source changed: %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created: %v", err)
	}
}
