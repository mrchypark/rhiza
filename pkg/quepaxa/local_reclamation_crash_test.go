package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

type localCheckpointKillRequest struct {
	Dir       string
	Phase     string
	ReadyFile string
	ApplyFile string
}

func TestLocalCheckpointReclamationResumesAfterHardProcessKill(t *testing.T) {
	for _, phase := range []string{
		"after-prepared-marker-sync-before-seal",
		"after-seal-decision-durable-before-compact",
	} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			readyFile := filepath.Join(t.TempDir(), "at-boundary")
			applyFile := filepath.Join(t.TempDir(), "applied-through")
			payload, err := json.Marshal(localCheckpointKillRequest{Dir: dir, Phase: phase, ReadyFile: readyFile, ApplyFile: applyFile})
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLocalCheckpointReclamationHardKillHelper$")
			cmd.Env = append(os.Environ(), "RHIZA_LOCAL_CHECKPOINT_CRASH="+string(payload))
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waitForLocalCheckpointCrashBoundary(t, cmd, readyFile, phase)

			wal, err := qlog.Open(dir)
			if err != nil {
				t.Fatalf("reopen WAL after hard kill at %q: %v", phase, err)
			}
			const fixedLimit = int64(16 << 20)
			if err := wal.SetMaxBytes(fixedLimit); err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			core, err := New(Config{NodeID: "local", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "local"}}}, WAL: wal, LocalMode: true})
			if err != nil {
				_ = wal.Close()
				t.Fatalf("recover Core after hard kill at %q: %v", phase, err)
			}
			core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
			seal := expectedLocalCrashSeal(t, core)
			if floor := core.CompactionFloor(); floor != 0 {
				t.Fatalf("floor before resume after %q=%d, want 0", phase, floor)
			}
			if index, root, ok := core.LatestPreparedCheckpoint(); !ok || index != 1 || root != seal.RootHash {
				t.Fatalf("prepared root after %q=(%d,%x,%v), want (1,%x,true)", phase, index, root, ok, seal.RootHash)
			}
			sealed, hasSeal, err := core.LatestCheckpointSeal()
			if err != nil {
				t.Fatalf("inspect seal after %q: %v", phase, err)
			}
			wantSealed := phase == "after-seal-decision-durable-before-compact"
			if hasSeal != wantSealed || hasSeal && (sealed.Index != 1 || sealed.RootHash != seal.RootHash) {
				t.Fatalf("seal after %q=(%+v,%v), want present=%v for exact root", phase, sealed, hasSeal, wantSealed)
			}
			wantTip := Slot(1)
			if wantSealed {
				wantTip = 2
			}
			if core.Tip() != wantTip {
				t.Fatalf("tip after %q=%d, want %d", phase, core.Tip(), wantTip)
			}

			if _, err := core.ReclaimLocalCheckpoint(context.Background(), seal, func(_ context.Context, through Slot) error {
				if through != 2 {
					return fmt.Errorf("apply-through=%d, want seal slot 2", through)
				}
				return recordAppliedThroughOnce(applyFile, through)
			}); err != nil {
				_ = wal.Close()
				t.Fatalf("resume exact checkpoint after %q: %v", phase, err)
			}
			if floor, root, ok := core.RecoveryRoot(); !ok || floor != 1 || root != seal.RootHash {
				_ = wal.Close()
				t.Fatalf("recovered base after %q=(%d,%x,%v), want exact root at 1", phase, floor, root, ok)
			}
			if capacity := wal.Capacity(); capacity.Limit != fixedLimit || capacity.Used > fixedLimit {
				_ = wal.Close()
				t.Fatalf("WAL capacity after %q=%+v, want fixed limit %d and used<=limit", phase, capacity, fixedLimit)
			}
			if applied, err := os.ReadFile(applyFile); err != nil || string(applied) != "2" {
				_ = wal.Close()
				t.Fatalf("idempotent apply marker after %q=%q err=%v, want one apply through slot 2", phase, applied, err)
			}
			entries, err := wal.Read()
			if err != nil || len(entries) == 0 || entries[0].Type != qlog.EntryCheckpoint || entries[0].Slot != 1 || entries[0].Hash != seal.RootHash {
				_ = wal.Close()
				t.Fatalf("compacted WAL after %q starts with %#v err=%v, want exact checkpoint base", phase, entries, err)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLocalCheckpointReclamationHardKillHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LOCAL_CHECKPOINT_CRASH")
	if data == "" {
		return
	}
	var request localCheckpointKillRequest
	if err := json.Unmarshal([]byte(data), &request); err != nil {
		t.Fatal(err)
	}
	core, _ := localReserveCoreWithLimit(t, request.Dir, "local", 16<<20)
	if slot, _, err := core.Propose(context.Background(), []byte("state at checkpoint")); err != nil || slot != 1 {
		t.Fatalf("seed proposal slot=%d err=%v", slot, err)
	}
	seal := expectedLocalCrashSeal(t, core)
	localCheckpointCrashBoundary = func(name string) {
		if name == request.Phase {
			if err := os.WriteFile(request.ReadyFile, []byte(name), 0o600); err != nil {
				t.Fatalf("write crash boundary marker: %v", err)
			}
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	if _, err := core.ReclaimLocalCheckpoint(context.Background(), seal, func(_ context.Context, through Slot) error {
		return recordAppliedThroughOnce(request.ApplyFile, through)
	}); err != nil {
		t.Fatalf("reclaim returned before crash boundary %q: %v", request.Phase, err)
	}
	t.Fatalf("requested hard-kill boundary %q was not reached", request.Phase)
}

func expectedLocalCrashSeal(t *testing.T, core *Core) CheckpointSeal {
	t.Helper()
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	core.mu.RLock()
	prefix := core.prefixes[1]
	core.mu.RUnlock()
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	return CheckpointSeal{
		ConfigID: core.ConfigID(), Index: 1,
		RootHash: sha256.Sum256([]byte("local root")), StateHash: sha256.Sum256([]byte("local state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
}

func recordAppliedThroughOnce(path string, through Slot) error {
	want := []byte(strconv.FormatUint(uint64(through), 10))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		got, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if string(got) != string(want) {
			return fmt.Errorf("applied-through marker %q differs from %q", got, want)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := f.Write(want); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func waitForLocalCheckpointCrashBoundary(t *testing.T, cmd *exec.Cmd, readyFile, phase string) {
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
			t.Fatalf("child did not reach checkpoint boundary %q", phase)
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
