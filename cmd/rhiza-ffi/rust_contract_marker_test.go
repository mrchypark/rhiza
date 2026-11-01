//go:build cgo

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// This marker is CI run control, not a membership/startup authority or receipt.
// A partial/unknown publication is retained and cannot authorize initial SQL.
func rustContractRunMode(path string) (bool, error) {
	const content = "managed-rust-contract-started-v1\n"
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	initial := err == nil
	if errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return false, errors.New("invalid existing Rust run-start marker")
		}
		f, err = os.Open(path)
		if err != nil {
			return false, err
		}
		opened, statErr := f.Stat()
		if statErr != nil || !os.SameFile(info, opened) {
			_ = f.Close()
			return false, errors.New("Rust run-start marker changed")
		}
		got, readErr := io.ReadAll(io.LimitReader(f, int64(len(content)+1)))
		if readErr != nil || !bytes.Equal(got, []byte(content)) {
			_ = f.Close()
			return false, errors.New("corrupt Rust run-start marker")
		}
	} else if err == nil {
		_, err = f.WriteString(content)
	}
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		return false, err
	}
	err = errors.Join(f.Sync(), f.Close())
	if err != nil {
		return false, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return false, err
	}
	return initial, nil
}

func TestRustContractRunStartedMarker(t *testing.T) {
	if os.Getenv("RHIZA_RUST_CONTRACT_MARKER") != "1" {
		t.Skip("CI-only durable Rust initial-run selection")
	}
	initial, err := rustContractRunMode("/var/lib/rhiza/rust-contract-started")
	if err != nil {
		t.Fatal("Rust run-start marker publication/validation failed")
	}
	if initial {
		fmt.Println("RHIZA_RUST_CONTRACT_MODE=initial")
	} else {
		fmt.Println("RHIZA_RUST_CONTRACT_MODE=restart")
	}
}

func TestRustContractRunMarkerIsExclusiveAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "started")
	initial, err := rustContractRunMode(path)
	if err != nil || !initial {
		t.Fatalf("first durable publication: %v", err)
	}
	// A crash after start, even without any ACK marker, is a restart—not a new
	// initial SQL permission. Rust must validate its retained prior ACK itself.
	initial, err = rustContractRunMode(path)
	if err != nil || initial {
		t.Fatalf("existing start was treated as initial: %v", err)
	}
	concurrent := filepath.Join(dir, "concurrent")
	var starts atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			first, err := rustContractRunMode(concurrent)
			if err != nil && first {
				t.Error("unknown publication authorized initial SQL")
			}
			if first {
				starts.Add(1)
			}
		})
	}
	wg.Wait()
	if starts.Load() != 1 {
		t.Fatal("concurrent publication authorized multiple initial runs")
	}
	if first, err := rustContractRunMode(concurrent); err != nil || first {
		t.Fatalf("durable concurrent winner missing: %v", err)
	}
	for _, name := range []string{"corrupt", "symlink", "public"} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(dir, name)
			if name == "symlink" {
				err = os.Symlink(path, p)
			} else {
				mode := os.FileMode(0600)
				if name == "public" {
					mode = 0644
				}
				err = os.WriteFile(p, []byte("unknown"), mode)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := os.Lstat(p)
			if _, err := rustContractRunMode(p); err == nil {
				t.Fatal("unsafe existing marker accepted")
			}
			after, err := os.Lstat(p)
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("unknown marker replaced")
			}
		})
	}
}
