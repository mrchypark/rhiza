package rhiza_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// List names only: a collector may delete an unreachable object between List
// and Get. A List/Get inventory during collection would manufacture failures.
func embeddedGCNames(t *testing.T, cfg rhiza.Config) map[string]bool {
	t.Helper()
	b := embeddedBucket(t, cfg)
	return embeddedGCList(t, b)
}

func embeddedGCList(t *testing.T, b objstore.Bucket) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	names := make(map[string]bool)
	if err := b.Iter(ctx, "", func(name string) error { names[name] = true; return nil }, objstore.WithRecursiveIter()); err != nil {
		t.Fatal(err)
	}
	return names
}

func embeddedGCWait(t *testing.T, cfg rhiza.Config, accept func(map[string]bool) bool) map[string]bool {
	t.Helper()
	b := embeddedBucket(t, cfg)
	deadline := time.Now().Add(20 * time.Second)
	for {
		names := embeddedGCList(t, b)
		if accept(names) {
			return names
		}
		if time.Now().After(deadline) {
			t.Fatalf("automatic GC did not satisfy object inventory within 20s (%d objects remain)", len(names))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func embeddedGCRoots(names map[string]bool) []string {
	var result []string
	for name := range names {
		if strings.Contains("/"+name, "/local/v1/checkpoint/roots/") && strings.HasSuffix(name, ".json") {
			result = append(result, name)
		}
	}
	sort.Strings(result)
	return result
}

func TestEmbeddedObjectStorageGCRetentionAndTotalLoss(t *testing.T) {
	const retain = 3
	cfg := embeddedCheckpointConfig(t)
	cfg.ClusterID = "embedded-gc"
	cfg.ObjStoreSyncInterval = 24 * time.Hour
	cfg.CheckpointRetainCount = retain
	// Generate an observable backlog before starting the automatic collector.
	cfg.ObjStoreGCInterval = 24 * time.Hour
	cfg.ObjStoreGCGracePeriod = 24 * time.Hour
	h := newEmbeddedHistoryConfig(t, false, cfg)
	names := []string{"original"}
	var committed []string
	previous := h.original.Slot
	for i := 0; i < retain+2; i++ {
		before := embeddedGCNames(t, cfg)
		previous = embeddedAdvanceCheckpoint(h, previous, fmt.Sprintf("gc-snapshot-%d", i), &names)
		after := embeddedGCNames(t, cfg)
		var added []string
		for _, key := range embeddedGCRoots(after) {
			if !before[key] {
				added = append(added, key)
			}
		}
		if len(added) != 1 {
			t.Fatalf("checkpoint %d published %d new snapshot objects, want one: %v", i, len(added), added)
		}
		committed = append(committed, added[0])
		t.Logf("checkpoint %d remote portable roots=%d", i, len(embeddedGCRoots(after)))
	}
	h.close()
	before := embeddedGCNames(t, cfg)
	if count := len(embeddedGCRoots(before)); count < retain+2 {
		t.Fatalf("snapshot backlog=%d want >=%d", count, retain+2)
	}
	cfg.ObjStoreGCInterval = 20 * time.Millisecond
	cfg.ObjStoreGCGracePeriod = time.Nanosecond
	h.cfg = cfg
	h.open(cfg)
	after := embeddedGCWait(t, cfg, func(current map[string]bool) bool {
		if len(current) >= len(before) {
			return false
		}
		for _, key := range committed[len(committed)-retain:] {
			if !current[key] {
				return false
			}
		}
		for _, key := range committed[:len(committed)-retain] {
			if current[key] {
				return false
			}
		}
		return true
	})
	t.Logf("automatic GC reduced remote object count %d -> %d", len(before), len(after))
	h.check(names...)
	h.append("gc-live-writer")
	names = append(names, "gc-live-writer")
	// Stop and drain all storage work before copying the whole namespace.
	h.close()
	cfg.ObjStoreGCInterval = 24 * time.Hour
	h.cfg = cfg
	published := embeddedRemoteImage(t, cfg)
	embeddedReplaceRemote(t, cfg, published)
	if err := os.RemoveAll(cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	h.open(cfg)
	h.check(names...)
	if slot := embeddedCheckpointSlot(t, cfg.DataDir); slot < previous {
		t.Fatalf("post-GC recovery checkpoint=%d want >=%d", slot, previous)
	}
	h.append("gc-after-whole-root-loss")
	names = append(names, "gc-after-whole-root-loss")
	h.close()
	h.open(cfg)
	h.check(names...)
	h.close()
}

func TestEmbeddedObjectStorageGCUnknownHEADNonmutating(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing-%v", missing), func(t *testing.T) {
			cfg := embeddedStoreConfig(t)
			cfg.ObjStoreGCInterval = 20 * time.Millisecond
			cfg.ObjStoreGCGracePeriod = time.Millisecond
			h := newEmbeddedHistoryConfig(t, false, cfg)
			h.close()
			original := embeddedRemoteImage(t, cfg)
			damaged := make(map[string][]byte, len(original))
			found := 0
			for name, data := range original {
				if path.Base(name) == "HEAD" && strings.Contains("/"+name, "/local/v1/") {
					found++
					if !missing {
						damaged[name] = []byte("{unknown-head\n")
					}
				} else {
					damaged[name] = bytes.Clone(data)
				}
			}
			if found != 1 {
				t.Fatalf("expected one Local HEAD, got %d", found)
			}
			embeddedReplaceRemote(t, cfg, damaged)
			h.rejectUnchanged()
			// A refused Open must leave no maintenance worker deleting in the background.
			time.Sleep(3 * cfg.ObjStoreGCInterval)
			if !reflect.DeepEqual(embeddedRemoteImage(t, cfg), damaged) {
				t.Fatal("objects changed after refused startup")
			}
			embeddedReplaceRemote(t, cfg, original)
			h.open(cfg)
			h.check("original")
			h.close()
		})
	}
}

func embeddedGCArchive(t *testing.T, ctx context.Context, b objstore.Bucket, cfg rhiza.Config) *recovery.LocalArchive {
	t.Helper()
	a, err := recovery.OpenLocalArchive(ctx, b, path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1"))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestEmbeddedObjectStorageGCPinRelease(t *testing.T) {
	cfg := embeddedCheckpointConfig(t)
	cfg.ClusterID = "embedded-gc"
	cfg.ObjStoreSyncInterval = 24 * time.Hour
	cfg.CheckpointRetainCount = 3
	cfg.ObjStoreGCInterval = 24 * time.Hour
	cfg.ObjStoreGCGracePeriod = time.Nanosecond
	h := newEmbeddedHistoryConfigTimeout(t, false, cfg, 5*time.Minute)
	// Keep immutable SQLite pages across several checkpoints so protection is
	// tested for shared content, rather than only disjoint snapshot files.
	h.execute("gc-shared-schema", "CREATE TABLE gc_shared_payload (payload BLOB)")
	// Blocks are 64 MiB: leave an unchanged middle block between SQLite's
	// changing header and append region. zeroblob keeps the WAL request small.
	for i := 0; i < 26; i++ {
		result, err := h.db.Execute(h.ctx, rhiza.ExecuteRequest{RequestID: fmt.Sprintf("gc-shared-data-%d", i), SQL: "INSERT INTO gc_shared_payload VALUES (zeroblob(10485760))"})
		if err != nil || result.Status != "committed" {
			t.Fatalf("shared fixture mutation=%+v err=%v", result, err)
		}
	}
	payloadRows, payloadQueryErr := h.db.Query(h.ctx, rhiza.QueryRequest{SQL: "SELECT sum(length(payload)) FROM gc_shared_payload"})
	if payloadQueryErr != nil || len(payloadRows.Rows) != 1 || fmt.Sprint(payloadRows.Rows[0][0]) != "272629760" {
		t.Fatalf("shared fixture payload absent: rows=%v query_err=%v", payloadRows.Rows, payloadQueryErr)
	}
	names := []string{"original"}
	slot := embeddedAdvanceCheckpoint(h, h.original.Slot, "gc-pinned-old", &names)
	slot = embeddedAdvanceCheckpoint(h, slot, "gc-shared-warm", &names)
	h.close()
	b := embeddedBucket(t, cfg)
	a := embeddedGCArchive(t, h.ctx, b, cfg)
	old := a.Snapshot()
	if old == nil {
		t.Fatal("committed snapshot missing")
	}
	pin, err := a.PinRestore(h.ctx, *old, "embedded-gc-old-root", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if pin != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := pin.Close(cleanupCtx); err != nil {
				t.Errorf("close restore pin: %v", err)
			}
		}
	})
	h.open(cfg)
	for i := 0; i < cfg.CheckpointRetainCount+1; i++ {
		slot = embeddedAdvanceCheckpoint(h, slot, fmt.Sprintf("gc-after-pin-%d", i), &names)
	}
	h.close()
	if err := pin.Renew(h.ctx, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	a = embeddedGCArchive(t, h.ctx, b, cfg)
	// Repeated passes exercise positive 1ns grace without waiting through the
	// production grace period.
	for i := 0; i < 3; i++ {
		if err := a.GarbageCollect(h.ctx, cfg.CheckpointRetainCount, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
	}
	protected := embeddedGCNames(t, cfg)
	oldKey := embeddedGCRootKey(cfg, *old)
	if !protected[oldKey] {
		t.Fatal("GC deleted old snapshot protected beyond retention by restore pin")
	}
	manager := checkpoint.NewManager(b, path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1"), "", old.Seal.ConfigID)
	root, err := manager.OpenRoot(h.ctx, uint64(old.Seal.Index), old.PortableRoot)
	if err != nil {
		t.Fatal(err)
	}
	current := a.Snapshot()
	if current == nil {
		t.Fatal("current snapshot missing")
	}
	latestRoot, err := manager.OpenRoot(h.ctx, uint64(current.Seal.Index), current.PortableRoot)
	if err != nil {
		t.Fatal(err)
	}
	oldBlocks := make(map[string]bool)
	largeNativeFile := false
	for _, file := range root.Files {
		if len(file.Blocks) >= 4 && file.Blocks[0].Size > 0 && file.Size > 3*file.Blocks[0].Size {
			largeNativeFile = true
		}
		for _, block := range file.Blocks {
			oldBlocks[embeddedGCBlockKey(cfg, block)] = true
		}
	}
	if !largeNativeFile {
		t.Fatal("fixture snapshot does not exceed three actual native object blocks")
	}
	shared := 0
	for _, file := range latestRoot.Files {
		for _, block := range file.Blocks {
			if oldBlocks[embeddedGCBlockKey(cfg, block)] {
				shared++
			}
		}
	}
	if shared == 0 {
		t.Fatal("fixture produced no shared blocks between pinned and current roots")
	}
	// A block is uniquely old only if no other protected portable root names
	// its actual generation-qualified object key.
	otherBlocks := make(map[string]bool)
	for _, key := range embeddedGCRoots(protected) {
		if key == oldKey {
			continue
		}
		r, err := b.Get(h.ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read root: %v close: %v", readErr, closeErr)
		}
		var other checkpoint.Checkpoint
		if err := json.Unmarshal(data, &other); err != nil {
			t.Fatal(err)
		}
		for _, file := range other.Files {
			for _, block := range file.Blocks {
				otherBlocks[embeddedGCBlockKey(cfg, block)] = true
			}
		}
	}
	var uniqueOld []string
	for key := range oldBlocks {
		if !protected[key] {
			t.Fatalf("pinned block absent before release: %s", key)
		}
		if !otherBlocks[key] {
			uniqueOld = append(uniqueOld, key)
		}
	}
	if len(uniqueOld) == 0 {
		t.Fatal("fixture produced no uniquely old native block candidate")
	}
	files, err := manager.DownloadRootFiles(h.ctx, root.Index, root.RootHash, t.TempDir())
	if err != nil || len(files) != 2 {
		t.Fatalf("pinned old root download files=%d err=%v", len(files), err)
	}
	if err := pin.Close(h.ctx); err != nil {
		t.Fatal(err)
	}
	pin = nil
	for i := 0; i < 3; i++ {
		if err := a.GarbageCollect(h.ctx, cfg.CheckpointRetainCount, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
	}
	released := embeddedGCNames(t, cfg)
	for _, key := range uniqueOld {
		if released[key] {
			t.Fatalf("uniquely old block survived pin release and GC: %s", key)
		}
	}
	for key := range oldBlocks {
		if otherBlocks[key] && !released[key] {
			t.Fatalf("shared native block deleted: %s", key)
		}
	}
	if released[oldKey] {
		t.Fatal("released root outside retention was not collected")
	}
	if len(released) >= len(protected) {
		t.Fatalf("pin release did not reclaim objects: %d -> %d", len(protected), len(released))
	}
	if err := os.RemoveAll(cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	h.open(cfg)
	h.check(names...)
	rows, err := h.db.Query(h.ctx, rhiza.QueryRequest{SQL: "SELECT sum(length(payload)) FROM gc_shared_payload"})
	if err != nil || len(rows.Rows) != 1 || fmt.Sprint(rows.Rows[0][0]) != "272629760" {
		t.Fatalf("shared payload after collection and total loss: rows=%v err=%v", rows.Rows, err)
	}
	h.append("gc-after-pin-release-restore")
	names = append(names, "gc-after-pin-release-restore")
	h.close()
	h.open(cfg)
	h.check(names...)
	h.close()
}

// This decorates the actual provider: deletion happens, but its acknowledgement
// is lost. A timeout must not let a later owner assume the old deleter stopped.
type embeddedGCUncertainDelete struct {
	objstore.Bucket
	target    string
	uncertain bool
}

func (b *embeddedGCUncertainDelete) Delete(ctx context.Context, name string) error {
	err := b.Bucket.Delete(ctx, name)
	if err == nil && name == b.target {
		b.uncertain = true
		return errors.New("test: delete completed but acknowledgement lost")
	}
	return err
}

func TestEmbeddedObjectStorageGCUncertainDeleteGateHeld(t *testing.T) {
	cfg := embeddedCheckpointConfig(t)
	cfg.ClusterID = "embedded-gc"
	cfg.ObjStoreSyncInterval = 24 * time.Hour
	cfg.CheckpointRetainCount = 1
	cfg.ObjStoreGCInterval = 24 * time.Hour
	cfg.ObjStoreGCGracePeriod = time.Nanosecond
	h := newEmbeddedHistoryConfig(t, false, cfg)
	names := []string{"original"}
	slot := embeddedAdvanceCheckpoint(h, h.original.Slot, "gc-uncertain-old", &names)
	b := embeddedBucket(t, cfg)
	embeddedAdvanceCheckpoint(h, slot, "gc-uncertain-new", &names)
	h.close()
	abandoned := []byte("owned abandoned extent, never referenced by HEAD")
	target := path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1/extents", fmt.Sprintf("%x", sha256.Sum256(abandoned)))
	if err := b.Upload(h.ctx, target, bytes.NewReader(abandoned), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	fault := &embeddedGCUncertainDelete{Bucket: b, target: target}
	a := embeddedGCArchive(t, h.ctx, fault, cfg)
	for i := 0; i < 4 && !fault.uncertain; i++ {
		err := a.GarbageCollect(h.ctx, 1, time.Nanosecond)
		if err != nil && !fault.uncertain {
			t.Fatalf("GC failed before injected deletion: %v", err)
		}
		if fault.uncertain && err == nil {
			t.Fatal("ambiguous deletion reported success")
		}
	}
	if !fault.uncertain {
		t.Fatal("collector never attempted the abandoned extent deletion")
	}
	// Reopening read-only does not resolve the old owner's outstanding deletion.
	// Test a new handle with a healthy provider, not merely the poisoned handle.
	a = embeddedGCArchive(t, h.ctx, b, cfg)
	current := a.Snapshot()
	if current == nil {
		t.Fatal("current snapshot missing")
	}
	before := embeddedRemoteImageState(t, cfg, true)
	var gate struct {
		Version uint   `json:"version"`
		Token   string `json:"token"`
	}
	gateKey := path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1/local-gc-control")
	if err := json.Unmarshal(before[gateKey], &gate); err != nil || gate.Version != 1 || gate.Token == "" {
		t.Fatalf("uncertain deleter did not leave a held gate: %q err=%v", before[gateKey], err)
	}
	for i := 0; i < 2; i++ {
		if err := a.GarbageCollect(h.ctx, 1, time.Nanosecond); !errors.Is(err, checkpoint.ErrLocalMaintenanceBusy) {
			t.Fatalf("new collector must refuse unresolved deletion gate: %v", err)
		}
		pin, err := a.PinRestore(h.ctx, *current, "embedded-gc-after-uncertain", time.Minute)
		if err == nil {
			if pin != nil {
				_ = pin.Close(h.ctx)
			}
			t.Fatal("restore pin admitted while uncertain deleter still owns gate")
		}
		if !errors.Is(err, checkpoint.ErrLocalMaintenanceBusy) {
			t.Fatalf("pin refused for reason other than held gate: %v", err)
		}
		if err := a.PublishSnapshot(h.ctx, *current); !errors.Is(err, checkpoint.ErrLocalMaintenanceBusy) {
			t.Fatalf("publication must refuse unresolved deletion gate: %v", err)
		}
	}
	if !reflect.DeepEqual(embeddedRemoteImageState(t, cfg, true), before) {
		t.Fatal("rejected gate contenders changed remote evidence")
	}
}

func embeddedGCRootKey(cfg rhiza.Config, snapshot recovery.LocalSnapshot) string {
	return path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1/checkpoint/roots", fmt.Sprintf("%020d_%x.json", uint64(snapshot.Seal.Index), snapshot.PortableRoot))
}

func TestEmbeddedObjectStorageGCPreparedRootPreserved(t *testing.T) {
	cfg := embeddedCheckpointConfig(t)
	cfg.ClusterID = "embedded-gc"
	cfg.ObjStoreSyncInterval = 24 * time.Hour
	cfg.ObjStoreGCInterval = 24 * time.Hour
	cfg.CheckpointRetainCount = 1
	h := newEmbeddedHistoryConfig(t, false, cfg)
	names := []string{"original"}
	embeddedAdvanceCheckpoint(h, h.original.Slot, "gc-before-prepared", &names)
	h.close()
	b := embeddedBucket(t, cfg)
	a := embeddedGCArchive(t, h.ctx, b, cfg)
	current := a.Snapshot()
	if current == nil {
		t.Fatal("current snapshot missing")
	}
	manager := checkpoint.NewManager(b, path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1"), "", current.Seal.ConfigID)
	files, err := manager.DownloadRootFiles(h.ctx, uint64(current.Seal.Index), current.PortableRoot, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: file.Role, Path: file.Path})
	}
	// A real portable root uploaded before a seal/HEAD commit is prepared, not
	// retired. Even a numerically newer index cannot displace committed state.
	prepared, err := manager.CreateLocalFiles(h.ctx, sources, uint64(current.Seal.Index)+1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := a.GarbageCollect(h.ctx, 1, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
	}
	for _, root := range []struct {
		index uint64
		hash  [32]byte
	}{
		{uint64(current.Seal.Index), current.PortableRoot}, {prepared.Index, prepared.RootHash},
	} {
		files, err := manager.DownloadRootFiles(h.ctx, root.index, root.hash, t.TempDir())
		if err != nil || len(files) != 2 {
			t.Fatalf("committed/prepared root %d lost: files=%d err=%v", root.index, len(files), err)
		}
	}
	h.open(cfg)
	h.check(names...)
	h.close()
}

func embeddedGCBlockKey(cfg rhiza.Config, block checkpoint.Block) string {
	name := block.Hash + ".block"
	if block.Generation != 0 {
		name = fmt.Sprintf("%s_%020d.block", block.Hash, block.Generation)
	}
	return path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1/checkpoint/blocks", name)
}

func TestEmbeddedObjectStorageGCDeletionScope(t *testing.T) {
	cfg := embeddedCheckpointConfig(t)
	cfg.ClusterID = "embedded-gc"
	cfg.ObjStoreSyncInterval = 24 * time.Hour
	cfg.ObjStoreGCInterval = 24 * time.Hour
	cfg.ObjStoreGCGracePeriod = 24 * time.Hour
	h := newEmbeddedHistoryConfig(t, false, cfg)
	names := []string{"original"}
	embeddedAdvanceCheckpoint(h, h.original.Slot, "gc-scope", &names)
	h.close()
	b := embeddedBucket(t, cfg)
	a := embeddedGCArchive(t, h.ctx, b, cfg)
	prefix := path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID), "local/v1")
	payload := []byte("owned namespace sentinel")
	hash := fmt.Sprintf("%x", sha256.Sum256(payload))
	foreign := []string{
		path.Join(prefix+"-sibling", "extents", hash),
		path.Join(prefix+"-sibling", "checkpoint/blocks", hash+".block"),
		path.Join(prefix, "extents-foreign", hash),
		path.Join(prefix, "checkpoint/blocks-foreign", hash+".block"),
		path.Join(prefix, "extents/unknown-subdir", hash),
		path.Join(prefix, "checkpoint/blocks/unknown-subdir", hash+".block"),
		path.Join(prefix, "unrelated", hash+".block"),
		"unrelated/owned-gc-sentinel",
	}
	for _, key := range foreign {
		if err := b.Upload(h.ctx, key, bytes.NewReader(payload), objstore.WithIfNotExists()); err != nil {
			t.Fatal(err)
		}
	}
	candidate := path.Join(prefix, "extents", hash)
	if err := b.Upload(h.ctx, candidate, bytes.NewReader(payload), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	// Unknown extent paths fail closed before any deletion. Block lookalikes
	// are ignored by their exact canonical-key guard.
	if err := a.GarbageCollect(h.ctx, 1, time.Nanosecond); err == nil || !strings.Contains(err.Error(), "unknown Local extent metadata") {
		t.Fatalf("nested extent path must fail closed: %v", err)
	}
	refused := embeddedRemoteImage(t, cfg)
	for _, key := range append(append([]string(nil), foreign...), candidate) {
		if !bytes.Equal(refused[key], payload) {
			t.Fatalf("refused GC changed object: %s", key)
		}
	}
	// Remove only this fixture's unknown extent; never alter the gate. The
	// remaining nested block sentinel must survive successful collection.
	nestedExtent := path.Join(prefix, "extents/unknown-subdir", hash)
	if err := b.Delete(h.ctx, nestedExtent); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := a.GarbageCollect(h.ctx, 1, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
	}
	image := embeddedRemoteImage(t, cfg)
	if _, remains := image[candidate]; remains {
		t.Fatal("owned unreferenced extent was not deleted")
	}
	for _, key := range foreign {
		if key == nestedExtent {
			continue
		}
		if !bytes.Equal(image[key], payload) {
			t.Fatalf("GC changed foreign object: %s", key)
		}
	}
	if err := os.RemoveAll(cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	h.open(cfg)
	h.check(names...)
	h.close()
}

func TestEmbeddedGCInventoryAtomicTemps(t *testing.T) {
	cfg := rhiza.Config{ObjStoreProvider: rhiza.ObjectStoreProviderFilesystem, ObjStoreDir: t.TempDir()}
	b := embeddedBucket(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "fixture/local/v1/"
	temporary := []string{"HEAD.swap", "local-gc-control.swap", "local-gc-protocol.swap"}
	retained := []string{"HEAD", "checkpoint/roots/HEAD.swap", "extents/local-gc-control.swap", "unrelated.swap"}
	for _, name := range append(append([]string(nil), retained...), temporary...) {
		if err := b.Upload(ctx, prefix+name, strings.NewReader(name)); err != nil {
			t.Fatal(err)
		}
	}
	image := embeddedRemoteImage(t, cfg)
	for _, name := range append(append([]string(nil), retained...), temporary...) {
		if string(image[prefix+name]) != name {
			t.Fatalf("real object omitted from image: %s", name)
		}
	}
}
