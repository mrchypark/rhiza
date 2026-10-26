package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestNodeOpenAdoptsHAStageWithoutCheckpointBlockReads(t *testing.T) {
	fixture := newHAAdoptionFixture(t)
	if err := removeHAAdoptionBlocks(t, fixture.bucket, fixture.root); err != nil {
		t.Fatal(err)
	}

	config := fixture.configFor(t.TempDir())
	config.HAInitRestoreDir = fixture.stageDir
	restored := New(config)
	restoredClosed := false
	t.Cleanup(func() {
		if !restoredClosed {
			if err := restored.Shutdown(); err != nil {
				t.Errorf("cleanup restored node: %v", err)
			}
		}
	})
	if err := restored.Open(fixture.ctx); err != nil {
		t.Fatalf("open from staged checkpoint with checkpoint blocks unavailable: %v", err)
	}
	if !restored.recoveryPhases.haStageAdopted || !restored.recoveryPhases.materializerRestoreComplete ||
		restored.recoveryPhases.checkpointDownloadVerifyStarted {
		t.Fatalf("Node.Open did not adopt the stage without remote checkpoint payload reads: %+v", restored.recoveryPhases)
	}
	if got := restored.material.Tip(); got < fixture.root.Index {
		t.Fatalf("materialized tip=%d is behind staged checkpoint=%d", got, fixture.root.Index)
	}
	readHAAdoptionValue(t, fixture.ctx, restored, "before-stage", "seed")

	if _, err := restored.server.KVPut(fixture.ctx, network.KVMutationRequest{
		RequestID: "ha-restore-adoption-tail", Key: "after-stage", Value: []byte("tail"),
	}); err != nil {
		t.Fatalf("commit post-restore tail: %v", err)
	}
	readHAAdoptionValue(t, fixture.ctx, restored, "after-stage", "tail")
	newerTip := restored.material.Tip()
	if newerTip <= fixture.root.Index {
		t.Fatalf("post-restore tail tip=%d did not advance beyond checkpoint=%d", newerTip, fixture.root.Index)
	}
	if err := restored.Shutdown(); err != nil {
		t.Fatalf("shutdown restored node: %v", err)
	}
	restoredClosed = true

	// Recreate only the remote checkpoint payloads so ordinary WAL recovery can
	// validate the historical seal. The stage remains present but is ineligible
	// because this is the same Pod's non-empty WAL/DataDir.
	if err := restoreHAAdoptionBlocks(t, fixture.bucket, fixture.blocks); err != nil {
		t.Fatal(err)
	}
	restarted := New(config)
	if err := restarted.Open(fixture.ctx); err != nil {
		t.Fatalf("restart the same Pod with its newer WAL: %v", err)
	}
	defer func() {
		if err := restarted.Shutdown(); err != nil {
			t.Errorf("shutdown same-Pod restart: %v", err)
		}
	}()
	if restarted.recoveryPhases.haStageAdopted {
		t.Fatal("same-Pod WAL restart adopted the fresh-Pod init stage")
	}
	if got := restarted.material.Tip(); got < newerTip {
		t.Fatalf("same-Pod restart rolled materialized tip back: got %d, want at least %d", got, newerTip)
	}
	readHAAdoptionValue(t, fixture.ctx, restarted, "after-stage", "tail")
}

func TestNodeOpenRejectsEqualTipTamperedHAStage(t *testing.T) {
	fixture := newHAAdoptionFixture(t)
	index, present, err := readHAStageIndex(fixture.stageDir)
	if err != nil || !present || !index.HasCheckpoint {
		t.Fatalf("read staged checkpoint index: present=%t err=%v", present, err)
	}
	if len(index.Files) == 0 {
		t.Fatal("fixture stage has no materializer files")
	}
	stageFile := filepath.Join(fixture.stageDir, index.Files[0].Name)
	file, err := os.OpenFile(stageFile, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("X"), 0); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := removeHAAdoptionBlocks(t, fixture.bucket, fixture.root); err != nil {
		t.Fatal(err)
	}

	config := fixture.configFor(t.TempDir())
	config.HAInitRestoreDir = fixture.stageDir
	if err := New(config).Open(fixture.ctx); err == nil {
		t.Fatal("Node.Open accepted a tampered stage at the same certified archive tip")
	}
}

type haAdoptionFixture struct {
	ctx      context.Context
	bucket   *objectstore.MeteredBucket
	config   *types.ExecutionConfig
	root     *checkpoint.Checkpoint
	blocks   []haAdoptionBlock
	stageDir string
}

type haAdoptionBlock struct {
	key  string
	data []byte
}

func newHAAdoptionFixture(t *testing.T) haAdoptionFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	const clusterID = "ha-init-adoption"
	archiveDir := t.TempDir()
	member := peerMember(clusterID, "n1", "ha-init-adoption-peer")
	config := &types.ExecutionConfig{
		DataDir: t.TempDir(), ClusterID: clusterID, NodeID: "n1", Members: []quepaxa.Member{member},
		ObjStoreProvider: string(objectstore.ProviderFilesystem), ObjStoreDir: archiveDir,
		PeerToken: "ha-init-adoption-peer", BindAddr: "127.0.0.1:0", PeerAddr: "127.0.0.1:0",
	}
	first := New(config)
	firstClosed := false
	t.Cleanup(func() {
		if !firstClosed {
			if err := first.Shutdown(); err != nil {
				t.Errorf("cleanup initial checkpoint publisher: %v", err)
			}
		}
	})
	if err := first.Open(ctx); err != nil {
		t.Fatalf("open initial singleton node: %v", err)
	}
	if _, err := first.server.KVPut(ctx, network.KVMutationRequest{
		RequestID: "ha-restore-adoption-seed", Key: "before-stage", Value: []byte("seed"),
	}); err != nil {
		_ = first.Shutdown()
		t.Fatalf("write checkpoint seed: %v", err)
	}
	first.checkpointer.Stop()
	if err := first.checkpointer.CheckpointOnShutdown(ctx, first.material.StateTip()); err != nil {
		_ = first.Shutdown()
		t.Fatalf("publish certified checkpoint: %v", err)
	}
	root, sealed, err := first.core.LatestCheckpointSeal()
	if err != nil || !sealed {
		_ = first.Shutdown()
		t.Fatalf("read certified checkpoint seal: sealed=%t err=%v", sealed, err)
	}
	checkpointRoot, err := first.checkpoints.OpenRoot(ctx, uint64(root.Index), root.RootHash)
	if err != nil {
		_ = first.Shutdown()
		t.Fatalf("open published checkpoint root: %v", err)
	}
	if err := first.Shutdown(); err != nil {
		t.Fatalf("shutdown checkpoint publisher: %v", err)
	}
	firstClosed = true

	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: archiveDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	prefix := string(config.ClusterID)
	membership := recovery.NewMembershipRecord(clusterID, config.Members, string(types.ObjectStoreDurabilityAsync))
	selection, err := recovery.VerifyHAArchiveForRestore(ctx, bucket, prefix,
		quepaxa.Cluster{ConfigID: 1, Members: config.Members}, membership)
	if err != nil {
		t.Fatalf("verify archive for init restore: %v", err)
	}
	if !selection.HasCheckpoint || selection.CheckpointIndex != uint64(root.Index) || selection.CheckpointRootHash != root.RootHash {
		t.Fatalf("selected HA root=%+v, want certified checkpoint index=%d", selection, root.Index)
	}
	stageDir := filepath.Join(t.TempDir(), "ha-init-stage")
	if err := stageHACheckpointFiles(ctx, bucket, selection, stageDir); err != nil {
		t.Fatalf("stage verified checkpoint: %v", err)
	}
	blocks := captureHAAdoptionBlocks(t, bucket, prefix, checkpointRoot)
	return haAdoptionFixture{ctx: ctx, bucket: bucket, config: config, root: checkpointRoot, blocks: blocks, stageDir: stageDir}
}

func (f haAdoptionFixture) configFor(dataDir string) *types.ExecutionConfig {
	config := *f.config
	config.DataDir = dataDir
	config.HAInitRestoreDir = ""
	return &config
}

func captureHAAdoptionBlocks(t *testing.T, bucket *objectstore.MeteredBucket, prefix string, root *checkpoint.Checkpoint) []haAdoptionBlock {
	t.Helper()
	var blocks []haAdoptionBlock
	seen := make(map[string]bool)
	for _, file := range root.Files {
		for _, block := range file.Blocks {
			name := block.Hash + ".block"
			if block.Generation != 0 {
				name = fmt.Sprintf("%s_%020d.block", block.Hash, block.Generation)
			}
			key := filepath.ToSlash(path.Join(prefix, "checkpoint", "blocks", name))
			if seen[key] {
				continue
			}
			seen[key] = true
			reader, err := bucket.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("read checkpoint block %q: %v", key, err)
			}
			data, readErr := io.ReadAll(reader)
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("read checkpoint block %q: %v", key, errors.Join(readErr, closeErr))
			}
			blocks = append(blocks, haAdoptionBlock{key: key, data: data})
		}
	}
	if len(blocks) == 0 {
		t.Fatal("certified checkpoint has no block objects")
	}
	return blocks
}

func removeHAAdoptionBlocks(t *testing.T, bucket *objectstore.MeteredBucket, root *checkpoint.Checkpoint) error {
	t.Helper()
	seen := make(map[string]bool)
	for _, file := range root.Files {
		for _, block := range file.Blocks {
			name := block.Hash + ".block"
			if block.Generation != 0 {
				name = fmt.Sprintf("%s_%020d.block", block.Hash, block.Generation)
			}
			key := filepath.ToSlash(path.Join("ha-init-adoption", "checkpoint", "blocks", name))
			if seen[key] {
				continue
			}
			seen[key] = true
			if err := bucket.Delete(context.Background(), key); err != nil {
				return fmt.Errorf("delete checkpoint block %q to enforce zero-payload-read test: %w", key, err)
			}
		}
	}
	return nil
}

func restoreHAAdoptionBlocks(t *testing.T, bucket *objectstore.MeteredBucket, blocks []haAdoptionBlock) error {
	t.Helper()
	for _, block := range blocks {
		if err := bucket.Upload(context.Background(), block.key, bytes.NewReader(block.data)); err != nil {
			return fmt.Errorf("restore checkpoint block %q for ordinary WAL verification: %w", block.key, err)
		}
	}
	return nil
}

func readHAAdoptionValue(t *testing.T, ctx context.Context, node *Node, key, want string) {
	t.Helper()
	response, err := node.server.KVGet(ctx, network.KVGetRequest{Key: key, Consistency: "linearizable"})
	if err != nil || string(response.Value) != want {
		t.Fatalf("linearizable read key=%q value=%q, want %q: %v", key, response.Value, want, err)
	}
}
