//go:build rhiza_local_testhooks

package rhiza

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type learnerCrashRequest struct {
	DataDir      string `json:"data_dir"`
	BucketDir    string `json:"bucket_dir"`
	ProgressFile string `json:"progress_file"`
	TriggerFile  string `json:"trigger_file"`
	ReleaseFile  string `json:"release_file"`
	OutputFile   string `json:"output_file"`
	Target       string `json:"target"`
	LeaseMode    bool   `json:"lease_mode,omitempty"`
}

func TestLearnerHardKillAtAdoptionWALBoundaries(t *testing.T) {
	for _, target := range []string{
		"core:after-prepared-marker-sync-before-seal",
		"qlog:after-compaction-build-synced-before-manifest",
		"qlog:after-manifest-switch-before-old-cleanup",
	} {
		t.Run(target, func(t *testing.T) {
			ctx := context.Background()
			bucketDir := t.TempDir()
			source := newLearnerCheckpointSource(t, bucketDir)
			source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
			source.execute(t, "INSERT INTO items VALUES (1, 1)")
			if err := source.archive.SyncThrough(ctx, source.core, source.core.Tip()); err != nil {
				t.Fatal(err)
			}
			req := startLearnerCrashChild(t, bucketDir, target, false)
			defer killLearnerCrashChild(t, req)
			waitForLearnerCrashFile(t, req.ProgressFile+".ready", 20*time.Second)
			_, sealed := source.publishCheckpoint(t, true)
			touchLearnerCrashFile(req.TriggerFile)
			waitForLearnerCrashFile(t, req.ProgressFile+".target", 20*time.Second)
			killLearnerCrashChild(t, req)

			removeReplicaMaterializedFiles(t, req.DataDir)
			reopened, err := OpenLearner(ctx, learnerReplicaConfig(req.DataDir, bucketDir, "crash-reader"))
			if err != nil {
				t.Fatalf("reopen after hard kill at %s: %v", target, err)
			}
			defer reopened.Close()
			assertLearnerRecoveredSuccessor(t, reopened, sealed.Index, 1)
		})
	}
}

func TestLearnerExpiredPinDuringBuildAllowsSuccessorGCAndRestart(t *testing.T) {
	testLearnerExpiredPinAllowsSuccessorGCAndRestart(t, "qlog:after-compaction-build-synced-before-manifest")
}

func TestLearnerExpiredPinAtManifestAllowsSuccessorGCAndRestart(t *testing.T) {
	testLearnerExpiredPinAllowsSuccessorGCAndRestart(t, "qlog:after-manifest-switch-before-old-cleanup")
}

func testLearnerExpiredPinAllowsSuccessorGCAndRestart(t *testing.T, target string) {
	t.Helper()
	for _, advanced := range []bool{false, true} {
		name := "successor_above_local_tip"
		if advanced {
			name = "successor_at_or_below_local_tip"
		}
		t.Run(name, func(t *testing.T) {
			oldLease, oldRenew := learnerCheckpointLease, learnerCheckpointRenew
			learnerCheckpointLease, learnerCheckpointRenew = 900*time.Millisecond, 150*time.Millisecond
			t.Cleanup(func() { learnerCheckpointLease, learnerCheckpointRenew = oldLease, oldRenew })

			ctx := context.Background()
			bucketDir := t.TempDir()
			source := newLearnerCheckpointSource(t, bucketDir)
			source.execute(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
			source.execute(t, "INSERT INTO items VALUES (1, 1)")
			if err := source.archive.SyncThrough(ctx, source.core, source.core.Tip()); err != nil {
				t.Fatal(err)
			}
			req := startLearnerCrashChild(t, bucketDir, target, true)
			defer killLearnerCrashChild(t, req)
			waitForLearnerCrashFile(t, req.ProgressFile+".ready", 20*time.Second)
			firstRoot, first := source.publishCheckpoint(t, true)
			if advanced {
				source.execute(t, "UPDATE items SET value=2 WHERE id=1")
				if err := source.archive.SyncThrough(ctx, source.core, source.core.Tip()); err != nil {
					t.Fatal(err)
				}
			}
			touchLearnerCrashFile(req.TriggerFile)
			waitForLearnerCrashPhase(t, req.ProgressFile+".target", 20*time.Second)
			waitForLearnerCrashPhase(t, req.ProgressFile+".renew", 5*time.Second)
			expireLearnerCheckpointPin(t, bucketDir)
			touchLearnerCrashFile(req.ReleaseFile + ".renew")
			waitForLearnerCrashPhase(t, req.ProgressFile+".renew-failed", 5*time.Second)

			if !advanced {
				source.execute(t, "UPDATE items SET value=2 WHERE id=1")
			}
			successorRoot, successor := source.publishCheckpoint(t, true)
			if successor.Index <= first.Index {
				t.Fatalf("successor index %d did not advance beyond %d", successor.Index, first.Index)
			}
			syncStatus, err := os.ReadFile(req.ProgressFile + ".sync")
			if err != nil {
				t.Fatal(err)
			}
			var preRestartTip quepaxa.Slot
			if _, err := fmt.Sscanf(string(syncStatus), "tip=%d", &preRestartTip); err != nil {
				t.Fatalf("parse learner state at compaction boundary %q: %v", syncStatus, err)
			}
			if advanced && preRestartTip < successor.Index {
				t.Fatalf("learner tip=%d did not reach successor index %d", preRestartTip, successor.Index)
			}
			if !advanced && preRestartTip >= successor.Index {
				t.Fatalf("learner tip=%d unexpectedly reached successor index %d", preRestartTip, successor.Index)
			}
			predecessorOnlyBlocks := learnerPredecessorOnlyBlocks(firstRoot, successorRoot)
			if len(predecessorOnlyBlocks) == 0 {
				t.Fatal("successor checkpoint shares every predecessor block; cannot prove block deletion")
			}
			for _, key := range predecessorOnlyBlocks {
				if exists, err := source.bucket.Exists(ctx, key); err != nil || !exists {
					t.Fatalf("predecessor-only block %q missing before GC: exists=%t err=%v", key, exists, err)
				}
			}
			if err := source.checkpoints.GarbageCollectFrom(ctx, nil, 1, uint64(successor.Index), 0); err != nil {
				t.Fatal(err)
			}
			if err := source.checkpoints.GarbageCollectFrom(ctx, nil, 1, uint64(successor.Index), 0); err != nil {
				t.Fatal(err)
			}
			if _, err := source.checkpoints.OpenRoot(ctx, firstRoot.Index, firstRoot.RootHash); err == nil {
				t.Fatal("expired recovery pin retained predecessor root after successor floor advanced")
			}
			for _, key := range predecessorOnlyBlocks {
				if exists, err := source.bucket.Exists(ctx, key); err != nil || exists {
					t.Fatalf("predecessor-only block %q survived successor GC: exists=%t err=%v", key, exists, err)
				}
			}
			touchLearnerCrashFile(req.ReleaseFile)
			waitForLearnerCrashPhase(t, req.ProgressFile+".done", 10*time.Second)
			done, err := os.ReadFile(req.ProgressFile + ".done")
			if err != nil || !strings.Contains(string(done), "ready=false") || !strings.Contains(string(done), "checkpoint floor committed after pin lease loss") {
				t.Fatalf("expired-pin compaction did not latch sticky readiness failure: done=%q err=%v", done, err)
			}
			var doneReady bool
			var doneFloor, doneTip uint64
			if _, err := fmt.Sscanf(string(done), "ready=%t floor=%d tip=%d", &doneReady, &doneFloor, &doneTip); err != nil {
				t.Fatalf("parse learner state after compaction %q: %v", done, err)
			}
			if quepaxa.Slot(doneTip) != preRestartTip {
				t.Fatalf("learner tip changed across stalled compaction: boundary=%d after=%d", preRestartTip, doneTip)
			}
			killLearnerCrashChild(t, req)
			removeReplicaMaterializedFiles(t, req.DataDir)

			reopened, err := OpenLearner(ctx, learnerReplicaConfig(req.DataDir, bucketDir, "crash-reader"))
			if err != nil {
				t.Fatalf("restart after expired-pin successor GC: %v", err)
			}
			defer reopened.Close()
			if reopened.core.Tip() < successor.Index {
				t.Fatalf("reopened learner tip=%d below successor index %d", reopened.core.Tip(), successor.Index)
			}
			assertLearnerRecoveredSuccessor(t, reopened, successor.Index, 2)
		})
	}
}

func learnerPredecessorOnlyBlocks(predecessor, successor *checkpoint.Checkpoint) []string {
	retained := make(map[string]struct{})
	for _, file := range successor.Files {
		for _, block := range file.Blocks {
			retained[learnerCheckpointBlockKey(block)] = struct{}{}
		}
	}
	var obsolete []string
	for _, file := range predecessor.Files {
		for _, block := range file.Blocks {
			key := learnerCheckpointBlockKey(block)
			if _, ok := retained[key]; !ok {
				obsolete = append(obsolete, key)
			}
		}
	}
	return obsolete
}

func learnerCheckpointBlockKey(block checkpoint.Block) string {
	if block.Generation == 0 {
		return "learner-adoption/checkpoint/blocks/" + block.Hash + ".block"
	}
	return fmt.Sprintf("learner-adoption/checkpoint/blocks/%s_%020d.block", block.Hash, block.Generation)
}

func assertLearnerRecoveredSuccessor(t *testing.T, r *ReadReplica, floor quepaxa.Slot, want int64) {
	t.Helper()
	if r.core.CompactionFloor() < floor || uint64(r.material.Tip()) < uint64(floor) {
		t.Fatalf("restart floor=%d material tip=%d, want floor at least %d", r.core.CompactionFloor(), r.material.Tip(), floor)
	}
	rows, err := r.Query(context.Background(), QueryRequest{SQL: "SELECT value FROM items WHERE id=1"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != want {
		t.Fatalf("restored application state=%#v err=%v want=%d", rows.Rows, err, want)
	}
}

func TestLearnerCheckpointBoundaryHelper(t *testing.T) {
	data := os.Getenv("RHIZA_LEARNER_CRASH_REQUEST")
	if data == "" {
		return
	}
	var req learnerCrashRequest
	if err := json.Unmarshal([]byte(data), &req); err != nil {
		t.Fatal(err)
	}
	oldLease, oldRenew := learnerCheckpointLease, learnerCheckpointRenew
	if req.LeaseMode {
		learnerCheckpointLease, learnerCheckpointRenew = 900*time.Millisecond, 150*time.Millisecond
	}
	defer func() { learnerCheckpointLease, learnerCheckpointRenew = oldLease, oldRenew }()
	r, err := OpenLearner(context.Background(), learnerReplicaConfig(req.DataDir, req.BucketDir, "crash-reader"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.syncMu.Lock()
	r.nextCheckpointProbe = time.Now()
	r.syncMu.Unlock()
	r.fetch = func(ctx context.Context, _ quepaxa.NodeID, from quepaxa.Slot, limit int) (network.DecisionsResponse, error) {
		if err := r.archive.Load(ctx); err != nil {
			return network.DecisionsResponse{}, err
		}
		values, tip, err := r.archive.DecisionsFrom(ctx, from, limit)
		return network.DecisionsResponse{Tip: tip, Decisions: values}, err
	}
	restore := localtesthooks.Set(func(name string) {
		appendLearnerCrashEvent(req.ProgressFile+".events", fmt.Sprintf("event=%q target=%q match=%t", name, req.Target, name == req.Target))
		if name == req.Target {
			writeLearnerCrashPhase(req.ProgressFile+".target", name)
			appendLearnerCrashEvent(req.ProgressFile+".events", "target-marker-written")
			waitLearnerCrashRelease(req.ReleaseFile)
		}
		if req.LeaseMode && name == "learner:checkpoint-pin-renewal-started" {
			writeLearnerCrashPhase(req.ProgressFile+".renew", name)
			waitLearnerCrashRelease(req.ReleaseFile + ".renew")
		}
		if req.LeaseMode && name == "learner:checkpoint-pin-renewal-failed" {
			writeLearnerCrashPhase(req.ProgressFile+".renew-failed", name)
		}
	})
	defer restore()
	writeLearnerCrashPhase(req.ProgressFile+".ready", fmt.Sprintf("tip=%d target=%q", r.core.Tip(), req.Target))
	waitLearnerCrashRelease(req.TriggerFile)
	deadline := time.Now().Add(20 * time.Second)
	lastSync := ""
	for time.Now().Before(deadline) {
		if err := r.Sync(context.Background()); err != nil && !req.LeaseMode {
			writeLearnerCrashPhase(req.ProgressFile+".sync-error", err.Error())
			t.Fatalf("learner sync before target boundary: %v", err)
		}
		syncStatus := fmt.Sprintf("tip=%d floor=%d mode=%s status=%+v", r.core.Tip(), r.core.CompactionFloor(), r.mode, r.Status())
		if syncStatus != lastSync {
			writeLearnerCrashPhase(req.ProgressFile+".sync", syncStatus)
			lastSync = syncStatus
		}
		if req.LeaseMode && !r.Ready() && r.core.CompactionFloor() > 0 {
			status := r.Status()
			writeLearnerCrashPhase(req.ProgressFile+".done", fmt.Sprintf("ready=%t floor=%d tip=%d status=%+v", r.Ready(), r.core.CompactionFloor(), r.core.Tip(), status))
			waitLearnerCrashRelease(req.ReleaseFile)
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("learner did not finish requested boundary %s (tip=%d floor=%d status=%+v)", req.Target, r.core.Tip(), r.core.CompactionFloor(), r.Status())
}

func startLearnerCrashChild(t *testing.T, bucketDir, target string, leaseMode bool) learnerCrashRequest {
	t.Helper()
	req := learnerCrashRequest{DataDir: t.TempDir(), BucketDir: bucketDir,
		ProgressFile: filepath.Join(t.TempDir(), "progress"), TriggerFile: filepath.Join(t.TempDir(), "trigger"),
		ReleaseFile: filepath.Join(t.TempDir(), "release"), Target: target, LeaseMode: leaseMode}
	req.OutputFile = req.ProgressFile + ".child.log"
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLearnerCheckpointBoundaryHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_LEARNER_CRASH_REQUEST="+string(payload))
	output, err := os.Create(req.OutputFile)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		_ = output.Close()
		t.Fatal(err)
	}
	_ = output.Close()
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	learnerCrashChildren.mu.Lock()
	learnerCrashChildren.cmds[req.DataDir] = cmd
	learnerCrashChildren.mu.Unlock()
	return req
}

var learnerCrashChildren = struct {
	mu   sync.Mutex
	cmds map[string]*exec.Cmd
}{cmds: make(map[string]*exec.Cmd)}

func killLearnerCrashChild(t *testing.T, req learnerCrashRequest) {
	t.Helper()
	learnerCrashChildren.mu.Lock()
	cmd := learnerCrashChildren.cmds[req.DataDir]
	delete(learnerCrashChildren.cmds, req.DataDir)
	learnerCrashChildren.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func expireLearnerCheckpointPin(t *testing.T, bucketDir string) {
	t.Helper()
	dir := filepath.Join(bucketDir, "learner-adoption", "checkpoint", "recovery-pins")
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("read persisted recovery pins: entries=%v err=%v", entries, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		var record map[string]any
		data, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(data, &record) != nil {
			continue
		}
		record["lease_until_unix_ms"] = time.Now().Add(-time.Second).UnixMilli()
		data, err = json.Marshal(record)
		if err != nil || os.WriteFile(path, data, 0o600) != nil {
			t.Fatalf("expire recovery pin %s: %v", path, err)
		}
		return
	}
	t.Fatal("no persisted learner recovery pin found to expire")
}

func writeLearnerCrashPhase(path, phase string) {
	if err := os.WriteFile(path, []byte(phase), 0o600); err != nil {
		panic(err)
	}
}

func appendLearnerCrashEvent(path, event string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	if _, err := fmt.Fprintln(file, event); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
}

func waitLearnerCrashRelease(path string) {
	for !fileExists(path) {
		time.Sleep(time.Millisecond)
	}
}

func waitForLearnerCrashFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fileExists(path) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	base := strings.TrimSuffix(path, ".target")
	ready, _ := os.ReadFile(base + ".ready")
	syncStatus, _ := os.ReadFile(base + ".sync")
	syncErr, _ := os.ReadFile(base + ".sync-error")
	events, _ := os.ReadFile(base + ".events")
	childLog, _ := os.ReadFile(base + ".child.log")
	t.Fatalf("learner subprocess did not reach %s; ready=%q sync=%q sync_error=%q events=%q child_log=%q", path, ready, syncStatus, syncErr, events, childLog)
}

func waitForLearnerCrashPhase(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	data, err := os.ReadFile(path)
	t.Fatalf("learner subprocess did not write phase %s; content=%q err=%v", path, data, err)
}

func touchLearnerCrashFile(path string) {
	if err := os.WriteFile(path, []byte("go"), 0o600); err != nil {
		panic(err)
	}
}

func fileExists(path string) bool { _, err := os.Stat(path); return err == nil }
