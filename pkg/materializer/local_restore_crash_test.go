package materializer

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalRestoreResumesAfterHardProcessKill(t *testing.T) {
	for _, phase := range []string{
		"after-intent",
		"after-staged",
		"after-backup-sql",
		"after-backup-graph",
		"after-publish-graph",
		"after-publish-sql",
		"after-validation",
		"after-installed",
		"after-cleanup",
	} {
		t.Run(phase, func(t *testing.T) {
			dbPath, authority, files, cleanup := makeLocalRestoreFixture(t)
			defer cleanup()
			before := localRestoreLiveDigest(t, dbPath)
			readyFile := filepath.Join(t.TempDir(), "at-boundary")
			payload, err := json.Marshal(struct {
				DBPath    string
				Authority LocalRestoreAuthority
				Files     []CheckpointFile
				Phase     string
				ReadyFile string
			}{dbPath, authority, files, phase, readyFile})
			if err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command(os.Args[0], "-test.run=^TestLocalRestoreHardKillHelper$")
			cmd.Env = append(os.Environ(), "RHIZA_LOCAL_RESTORE_HARD_KILL="+string(payload))
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(readyFile); err == nil {
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("check child boundary marker: %v", err)
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("child did not reach durable restore boundary %q", phase)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := cmd.Process.Kill(); err != nil {
				_ = cmd.Wait()
				t.Fatalf("send hard process kill at %q: %v", phase, err)
			}
			err = cmd.Wait()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != -1 {
				t.Fatalf("child termination=%v, want signal termination from Process.Kill", err)
			}

			if phase == "after-cleanup" {
				m, err := OpenLocalFromBase(dbPath, 1, authority, files, nil)
				if err != nil {
					t.Fatalf("resume exact installed restore after kill at cleanup: %v", err)
				}
				if m.Tip() != authority.Index || m.graphTip() != authority.Index {
					t.Fatalf("resumed post-cleanup tips SQL=%d Graph=%d, want %d", m.Tip(), m.graphTip(), authority.Index)
				}
				if err := m.Close(); err != nil {
					t.Fatal(err)
				}
				if err := FinalizeLocalRestore(dbPath, authority); err != nil {
					t.Fatalf("finalize restore after kill at cleanup: %v", err)
				}
				m, err = Open(dbPath, 1)
				if err != nil {
					t.Fatalf("generic open after cleanup recovery: %v", err)
				}
				if m.Tip() != authority.Index || m.graphTip() != authority.Index {
					t.Fatalf("final post-cleanup tips SQL=%d Graph=%d, want %d", m.Tip(), m.graphTip(), authority.Index)
				}
				if err := m.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}

			if err := PrepareLocalRestore(dbPath, authority, files); err != nil {
				t.Fatalf("resume exact authority after kill at %q: %v", phase, err)
			}
			m, err := OpenLocalFromBase(dbPath, 1, authority, files, nil)
			if err != nil {
				t.Fatalf("resume restore after kill at %q: %v", phase, err)
			}
			if m.Tip() != authority.Index || m.graphTip() != authority.Index {
				t.Fatalf("resumed tips SQL=%d Graph=%d, want %d", m.Tip(), m.graphTip(), authority.Index)
			}
			if _, err := os.Stat(localRestoreJournalPath(dbPath)); err != nil {
				t.Fatalf("restore journal disappeared before finalize after %q: %v", phase, err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if err := FinalizeLocalRestore(dbPath, authority); err != nil {
				t.Fatalf("finalize resumed restore after %q: %v", phase, err)
			}
			m, err = Open(dbPath, 1)
			if err != nil {
				t.Fatalf("generic open after finalized restore from %q: %v", phase, err)
			}
			if m.Tip() != authority.Index || m.graphTip() != authority.Index {
				t.Fatalf("final tips SQL=%d Graph=%d, want %d", m.Tip(), m.graphTip(), authority.Index)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if after := localRestoreLiveDigest(t, dbPath); after == before {
				t.Fatalf("restore boundary %q resumed without installing the authorized checkpoint", phase)
			}
		})
	}
}

func TestLocalRestoreHardKillHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_RESTORE_HARD_KILL")
	if data == "" {
		return
	}
	var request struct {
		DBPath    string
		Authority LocalRestoreAuthority
		Files     []CheckpointFile
		Phase     string
		ReadyFile string
	}
	if err := json.Unmarshal([]byte(data), &request); err != nil {
		t.Fatal(err)
	}
	localRestoreFailpoint = func(name string) error {
		if name != request.Phase {
			return nil
		}
		if err := os.WriteFile(request.ReadyFile, []byte(name), 0o600); err != nil {
			return err
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	if err := PrepareLocalRestore(request.DBPath, request.Authority, request.Files); err != nil {
		t.Fatal(err)
	}
	m, err := OpenLocalFromBase(request.DBPath, 1, request.Authority, request.Files, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if request.Phase == "after-cleanup" {
		if err := FinalizeLocalRestore(request.DBPath, request.Authority); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("requested hard-kill boundary %q was not reached", request.Phase)
}
