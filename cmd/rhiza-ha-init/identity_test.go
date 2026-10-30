package main

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPrepareIdentityInterruptedPublication(t *testing.T) {
	for _, stage := range []string{"before-link", "after-link"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			name := filepath.Join(dir, "identity.json")
			interrupted := errors.New("interrupted")
			_, err := prepareIdentityFileWithHook(name, "cluster", "node", func(s string) error {
				if s == stage {
					return interrupted
				}
				return nil
			})
			if !errors.Is(err, interrupted) {
				t.Fatalf("interruption not reached: %v", err)
			}
			before, statErr := os.ReadFile(name)
			if stage == "before-link" && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("final file visible before publish: %v", statErr)
			}
			if stage == "after-link" && statErr != nil {
				t.Fatalf("published file missing: %v", statErr)
			}
			if _, err := prepareIdentityFile(name, "cluster", "node"); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			after, err := os.ReadFile(name)
			if err != nil || stage == "after-link" && string(before) != string(after) {
				t.Fatal("retry replaced published identity")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatal("temporary identity not cleaned up")
			}
		})
	}
}

func TestPrepareIdentityConcurrentCreators(t *testing.T) {
	name := filepath.Join(t.TempDir(), "identity.json")
	const count = 64
	tokens := make([]string, count)
	errs := make([]error, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range tokens {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			tokens[i], errs[i] = prepareIdentityFile(name, "cluster", "node")
		}(i)
	}
	close(start)
	wg.Wait()
	for i := range tokens {
		if errs[i] != nil || tokens[i] != tokens[0] {
			t.Fatalf("creator %d failed or differed: %v", i, errs[i])
		}
	}
}

func TestPrepareIdentityPersistence(t *testing.T) {
	name := filepath.Join(t.TempDir(), "identity.json")
	first, err := prepareIdentityFile(name, "cluster", "node")
	if err != nil {
		t.Fatal(err)
	}
	second, err := prepareIdentityFile(name, "cluster", "node")
	if err != nil || first != second || len(first) != 64 {
		t.Fatalf("retry did not preserve identity: %v", err)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("permissions: %v %v", info, err)
	}
	if _, err := prepareIdentityFile(name, "cluster", "other"); err == nil {
		t.Fatal("mismatched identity accepted")
	}
}

func TestPrepareIdentityRejectsExistingInvalidFile(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		mode       os.FileMode
	}{
		{"truncated", `{"version":1`, 0600},
		{"empty", "", 0600},
		{"wrong-mode", `{}`, 0644},
		{"trailing", `{} {}`, 0600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "identity.json")
			if err := os.WriteFile(name, []byte(tc.body), tc.mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(name, tc.mode); err != nil {
				t.Fatal(err)
			}
			if _, err := prepareIdentityFile(name, "cluster", "node"); err == nil {
				t.Fatal("invalid identity accepted")
			}
			got, err := os.ReadFile(name)
			if err != nil || string(got) != tc.body {
				t.Fatal("existing identity overwritten")
			}
		})
	}
}

func TestPrepareIdentityRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "target"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "identity.json")
	if err := os.Symlink("target", name); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareIdentityFile(name, "cluster", "node"); err == nil {
		t.Fatal("symlink accepted")
	}
}
