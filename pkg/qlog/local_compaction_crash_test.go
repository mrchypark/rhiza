package qlog

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type localWALCrashRequest struct {
	Dir       string
	Phase     string
	ReadyFile string
}

func TestLocalCompactionRecoversAcrossHardProcessKills(t *testing.T) {
	for _, phase := range []string{
		"after-compaction-build-synced-before-manifest",
		"after-manifest-switch-before-old-cleanup",
	} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			wal, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range []Entry{
				{Slot: 1, Type: EntryProposal, Payload: []byte("proposal-at-base")},
				{Slot: 1, Type: EntryReceipt, Payload: []byte("receipt-at-base")},
				{Slot: 2, Type: EntryProposal, Payload: []byte("retained-proposal")},
				{Slot: 2, Type: EntryReceipt, Payload: []byte("receipt-above-base")},
			} {
				if err := wal.Append(entry); err != nil {
					_ = wal.Close()
					t.Fatal(err)
				}
			}
			if err := wal.Sync(); err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}

			readyFile := filepath.Join(t.TempDir(), "at-boundary")
			payload, err := json.Marshal(localWALCrashRequest{Dir: dir, Phase: phase, ReadyFile: readyFile})
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLocalCompactionHardKillHelper$")
			cmd.Env = append(os.Environ(), "RHIZA_LOCAL_WAL_CRASH="+string(payload))
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waitForWALCrashBoundary(t, cmd, readyFile, phase)

			reopened, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen after hard kill at %q: %v", phase, err)
			}
			entries, err := reopened.Read()
			if err != nil {
				_ = reopened.Close()
				t.Fatal(err)
			}
			if phase == "after-compaction-build-synced-before-manifest" {
				want := []Entry{
					{Slot: 1, Type: EntryProposal, Payload: []byte("proposal-at-base")},
					{Slot: 1, Type: EntryReceipt, Payload: []byte("receipt-at-base")},
					{Slot: 2, Type: EntryProposal, Payload: []byte("retained-proposal")},
					{Slot: 2, Type: EntryReceipt, Payload: []byte("receipt-above-base")},
					{Slot: 3, Type: EntryReceipt, Payload: []byte("tail-during-compaction")},
				}
				if len(entries) != len(want) {
					t.Fatalf("pre-manifest recovery entries=%#v, want original history plus durable tail", entries)
				}
				for i := range want {
					if entries[i].Slot != want[i].Slot || entries[i].Type != want[i].Type || string(entries[i].Payload) != string(want[i].Payload) {
						t.Fatalf("pre-manifest entry %d=%+v, want slot=%d type=%d payload=%q", i, entries[i], want[i].Slot, want[i].Type, want[i].Payload)
					}
				}
			} else {
				if len(entries) != 4 || entries[0].Type != EntryCheckpoint || entries[0].Slot != 1 || string(entries[0].Payload) != "checkpoint-base" {
					t.Fatalf("post-manifest recovery base=%#v, want selected checkpoint base", entries)
				}
				proposalHash := sha256.Sum256([]byte("retained-proposal"))
				if entries[1].Type != EntryProposal || entries[1].Hash != proposalHash || string(entries[1].Payload) != "retained-proposal" {
					t.Fatalf("post-manifest retained proposal=%#v", entries[1])
				}
				if entries[2].Type != EntryReceipt || string(entries[2].Payload) != "receipt-above-base" || entries[3].Type != EntryReceipt || string(entries[3].Payload) != "tail-during-compaction" {
					t.Fatalf("post-manifest suffix entries=%#v", entries[2:])
				}
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLocalCompactionHardKillHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_WAL_CRASH")
	if data == "" {
		return
	}
	var request localWALCrashRequest
	if err := json.Unmarshal([]byte(data), &request); err != nil {
		t.Fatal(err)
	}
	wal, err := Open(request.Dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(16 << 20); err != nil {
		t.Fatal(err)
	}
	localWALCrashBoundary = func(name string) {
		if name == request.Phase {
			if err := os.WriteFile(request.ReadyFile, []byte(name), 0o600); err != nil {
				t.Fatalf("write crash boundary marker: %v", err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	proposal := []byte("retained-proposal")
	plan, err := wal.BeginCompaction(
		Entry{Slot: 1, Type: EntryCheckpoint, Hash: [32]byte{1}, Payload: []byte("checkpoint-base")},
		map[[32]byte][]byte{sha256.Sum256(proposal): proposal},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(Entry{Slot: 3, Type: EntryReceipt, Payload: []byte("tail-during-compaction")}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("requested hard-kill boundary %q was not reached", request.Phase)
}

func waitForWALCrashBoundary(t *testing.T, cmd *exec.Cmd, readyFile, phase string) {
	t.Helper()
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
			t.Fatalf("child did not reach WAL boundary %q", phase)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		_ = cmd.Wait()
		t.Fatalf("send hard process kill at %q: %v", phase, err)
	}
	err := cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != -1 {
		t.Fatalf("child termination=%v, want signal termination from Process.Kill", err)
	}
}
