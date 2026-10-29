package rhiza_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// Observe the existing Go local-checkpoint format, not the new archive tuple.
// Only atomically published hash-named directories count; staging does not.
func embeddedCheckpointSlot(t *testing.T, dataDir string) uint64 {
	t.Helper()
	roots, err := os.ReadDir(filepath.Join(dataDir, "local-checkpoints"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	var latest uint64
	for _, root := range roots {
		hash, err := hex.DecodeString(root.Name())
		if !root.IsDir() || err != nil || len(hash) != 32 {
			continue
		}
		path := filepath.Join(dataDir, "local-checkpoints", root.Name(), "descriptor.json")
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		} // A previous published root can be collected.
		if err != nil {
			t.Fatal(err)
		}
		var descriptor struct {
			Slot  uint64 `json:"slot"`
			Files []struct {
				Name   string `json:"name"`
				Length uint64 `json:"length"`
			} `json:"files"`
		}
		if err := json.Unmarshal(data, &descriptor); err != nil {
			t.Fatalf("published checkpoint descriptor: %v", err)
		}
		if descriptor.Slot == 0 || len(descriptor.Files) != 2 {
			t.Fatalf("incomplete published checkpoint %s", path)
		}
		complete := true
		for _, file := range descriptor.Files {
			info, err := os.Stat(filepath.Join(filepath.Dir(path), file.Name))
			if os.IsNotExist(err) {
				complete = false
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if !info.Mode().IsRegular() || info.Size() <= 0 || uint64(info.Size()) != file.Length {
				t.Fatalf("checkpoint file size/type mismatch: %s", file.Name)
			}
		}
		if complete {
			latest = max(latest, descriptor.Slot)
		}
	}
	return latest
}

// Reclamation is triggered by a real bounded WAL through ordinary SQL writes.
// A timer or a configured checkpoint threshold alone is not checkpoint evidence.
func embeddedAdvanceCheckpoint(h *embeddedHistory, after uint64, prefix string, names *[]string) uint64 {
	h.t.Helper()
	for i := range 48 {
		name := fmt.Sprintf("%s-%02d-%s", prefix, i, strings.Repeat("x", 64<<10))
		h.append(name)
		*names = append(*names, name)
		if slot := embeddedCheckpointSlot(h.t, h.cfg.DataDir); slot > after {
			h.t.Logf("published checkpoint advanced %d -> %d after %d writes", after, slot, i+1)
			return slot
		}
	}
	h.t.Fatalf("no published checkpoint advanced beyond slot %d after bounded WAL workload", after)
	return 0
}

func embeddedCheckpointConfig(t *testing.T) rhiza.Config {
	t.Helper()
	cfg := embeddedStoreConfig(t)
	cfg.MaxWALBytes = 2 << 20
	// Keep periodic publication from immediately absorbing the explicit tail.
	// The finite WAL limit still forces Local checkpoint reclamation.
	cfg.CheckpointInterval = 24 * time.Hour
	cfg.CheckpointTailBytes = 1 << 30
	return cfg
}

func TestEmbeddedCheckpointFixtureBaseline(t *testing.T) {
	cfg := rhiza.Config{Local: true, NodeID: "checkpoint-fixture", DataDir: t.TempDir(), MaxWALBytes: 2 << 20}
	h := newEmbeddedHistoryConfig(t, false, cfg)
	names := []string{"original"}
	if slot := embeddedAdvanceCheckpoint(h, h.original.Slot, "fixture-baseline", &names); slot <= h.original.Slot {
		t.Fatal("bounded WAL workload did not create a later checkpoint")
	}
	h.check(names...)
	h.close()
}

func TestEmbeddedObjectStorageCheckpointAndTailTotalLoss(t *testing.T) {
	cfg := embeddedCheckpointConfig(t)
	cfg.ClusterID = "embedded-checkpoint-tail"
	// Observe BeforeAck publication without racing unrelated periodic sync.
	cfg.ObjStoreSyncInterval = 24 * time.Hour
	h := newEmbeddedHistoryConfig(t, false, cfg)
	names := []string{"original"}
	checkpoint := embeddedAdvanceCheckpoint(h, h.original.Slot, "checkpoint-prefix", &names)
	tailRequest := rhiza.ExecuteRequest{RequestID: "checkpoint-tail", SQL: "INSERT INTO backup_items(name) VALUES (?) RETURNING id, name", Args: []any{"checkpoint-tail"}}
	tail, err := h.db.ExecuteReturning(h.ctx, tailRequest)
	if err != nil {
		t.Fatal(err)
	}
	names = append(names, "checkpoint-tail")
	if latest := embeddedCheckpointSlot(t, h.cfg.DataDir); tail.Slot <= checkpoint || latest >= tail.Slot {
		t.Fatalf("tail is not beyond published checkpoint: base=%d latest=%d tail=%d", checkpoint, latest, tail.Slot)
	}
	// Prove before-ack remote durability before Close can flush any missing tail.
	archive, err := recovery.OpenLocalArchive(h.ctx, embeddedBucket(t, h.cfg), path.Join(h.cfg.ObjStorePrefix, string(h.cfg.ClusterID), "local/v1"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := archive.Snapshot()
	if snapshot == nil || uint64(snapshot.Seal.Index) < checkpoint || uint64(archive.Tip()) < tail.Slot {
		t.Fatalf("remote checkpoint/tail not published before Close: snapshot=%+v tip=%d want checkpoint>=%d tail>=%d", snapshot, archive.Tip(), checkpoint, tail.Slot)
	}
	decisions := archive.DecisionsFrom(snapshot.Seal.Index+1, int(tail.Slot-uint64(snapshot.Seal.Index)))
	if len(decisions) == 0 || uint64(decisions[len(decisions)-1].Slot) != tail.Slot {
		t.Fatal("remote decisions do not cover acknowledged tail before Close")
	}
	h.close()
	// Capture only after all writer/collector SDK work and gate release drain.
	remote := embeddedRemoteImage(t, h.cfg)
	embeddedReplaceRemote(t, h.cfg, remote)
	if err := os.RemoveAll(h.cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	h.open(h.cfg)
	h.check(names...)
	if restored := embeddedCheckpointSlot(t, h.cfg.DataDir); restored < checkpoint {
		t.Fatalf("restored checkpoint=%d want at least %d; replaying a genesis-only archive does not establish snapshot restore", restored, checkpoint)
	}
	retry, err := h.db.ExecuteReturning(h.ctx, tailRequest)
	if err != nil || retry.Slot != tail.Slot {
		t.Fatalf("tail retry slot=%d want=%d err=%v", retry.Slot, tail.Slot, err)
	}
	if len(retry.Statements) != 1 || len(retry.Statements[0].Rows) != 1 || fmt.Sprint(retry.Statements[0].Rows) != fmt.Sprint(tail.Statements[0].Rows) {
		t.Fatalf("tail RETURNING retry=%+v want=%+v", retry, tail)
	}
	h.append("after-checkpoint-restore")
	names = append(names, "after-checkpoint-restore")
	h.close()
	h.open(h.cfg)
	h.check(names...)
	h.close()
	remote = embeddedRemoteImage(t, h.cfg)
	embeddedReplaceRemote(t, h.cfg, remote)
	if err := os.RemoveAll(h.cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	h.open(h.cfg)
	h.check(names...)
	h.close()
}

func TestEmbeddedObjectStorageLocalOlderThanCheckpointProofNonmutating(t *testing.T) {
	h := newEmbeddedHistoryConfig(t, false, embeddedCheckpointConfig(t))
	h.close()
	stale := embeddedLocalImage(t, h.cfg.DataDir)
	h.open(h.cfg)
	names := []string{"original"}
	first := embeddedAdvanceCheckpoint(h, h.original.Slot, "first-rebase", &names)
	second := embeddedAdvanceCheckpoint(h, first, "second-rebase", &names)
	if second <= first {
		t.Fatal("second checkpoint failed to advance")
	}
	h.close()
	current := embeddedLocalImage(t, h.cfg.DataDir)
	embeddedReplaceLocal(t, h.cfg.DataDir, stale)
	// A nonempty stale local root cannot be treated as an empty-root restore,
	// even when the archive has moved its checkpoint beyond the local tip.
	h.rejectUnchanged()
	embeddedReplaceLocal(t, h.cfg.DataDir, current)
	h.open(h.cfg)
	h.check(names...)
	h.close()
}
