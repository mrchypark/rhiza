package node

import (
	"path/filepath"
	"testing"
)

func TestLocalFreeBytes(t *testing.T) {
	dir := t.TempDir()
	free, err := localFreeBytes(dir)
	if err != nil {
		t.Fatalf("free space for existing directory: %v", err)
	}
	if free == 0 {
		t.Fatal("test filesystem has no available space")
	}
	if _, err := localFreeBytes(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("expected missing path to fail")
	}
}
