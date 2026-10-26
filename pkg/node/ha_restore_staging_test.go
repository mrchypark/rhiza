package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestStageHACheckpointFilesPublishesVerifiedNativeFilesAtomically(t *testing.T) {
	ctx := context.Background()
	bucket, selection := createHAStageFixture(t, ctx)
	root := filepath.Join(t.TempDir(), "ha-restore")
	dataDir := filepath.Join(t.TempDir(), "main-data-must-not-be-created")

	if err := stageHACheckpointFiles(ctx, bucket, selection, root); err != nil {
		t.Fatalf("stage verified checkpoint: %v", err)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("staging touched unrelated main DataDir: stat error=%v", err)
	}
	var index haRestoreStageIndex
	data, err := os.ReadFile(filepath.Join(root, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	if index.Version != 1 || index.SourcePrefix != selection.SourcePrefix || index.ArchiveConfigID != selection.ArchiveConfigID ||
		index.BootstrapConfigID != selection.BootstrapConfigID || index.Tip != uint64(selection.Tip) || index.PrefixHash != fmtHash(selection.PrefixHash) ||
		index.Index != selection.CheckpointIndex || index.RootHash != fmtHash(selection.CheckpointRootHash) || index.StateHash != fmtHash(selection.CheckpointStateHash) {
		t.Fatalf("staged index does not identify exact selected root: %+v", index)
	}
	if len(index.Files) != 1 || index.Files[0].Role != checkpoint.RoleSQLite || index.Files[0].Name != filepath.Join("files", "sqlite.db") {
		t.Fatalf("unexpected staged files: %+v", index.Files)
	}
	if _, err := os.Stat(filepath.Join(root, index.Files[0].Name)); err != nil {
		t.Fatalf("staged native checkpoint file missing: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("staging directory contains unexpected partial/internal files: %v", entries)
	}
	if err := stageHACheckpointFiles(ctx, bucket, selection, root); err != nil {
		t.Fatalf("idempotent retry of complete stage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "files", "sqlite.db"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := stageHACheckpointFiles(ctx, bucket, selection, root); err == nil {
		t.Fatal("staging accepted an existing tampered directory")
	}
}

func TestStageHACheckpointFilesPublishesVerifiedCheckpointlessHistory(t *testing.T) {
	ctx := context.Background()
	bucket, selection := createHAStageFixture(t, ctx)
	selection.HasCheckpoint = false
	selection.CheckpointIndex = 0
	selection.CheckpointRootHash = [32]byte{}
	selection.CheckpointStateHash = [32]byte{}
	stageDir := filepath.Join(t.TempDir(), "ha-history-only")
	if err := stageHACheckpointFiles(ctx, bucket, selection, stageDir); err != nil {
		t.Fatalf("stage checkpointless but verified archive history: %v", err)
	}
	index, present, err := readHAStageIndex(stageDir)
	if err != nil || !present || index.HasCheckpoint || index.Tip != uint64(selection.Tip) || len(index.Files) != 0 {
		t.Fatalf("checkpointless stage index=%+v present=%t err=%v", index, present, err)
	}
	if err := stageHACheckpointFiles(ctx, bucket, selection, stageDir); err != nil {
		t.Fatalf("idempotent checkpointless stage retry: %v", err)
	}
}

func TestStageHACheckpointFilesRejectsOversizedExistingIndexWithoutMutation(t *testing.T) {
	ctx := context.Background()
	bucket, selection := createHAStageFixture(t, ctx)
	stageDir := filepath.Join(t.TempDir(), "oversized-stage")
	if err := os.Mkdir(stageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(stageDir, "index.json")
	want := bytes.Repeat([]byte("x"), int(haRestoreStageIndexMaxBytes+1))
	if err := os.WriteFile(indexPath, want, 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(t.TempDir(), "must-not-be-created")
	if err := stageHACheckpointFiles(ctx, bucket, selection, stageDir); err == nil {
		t.Fatal("staging accepted an oversized pre-existing index")
	}
	got, err := os.ReadFile(indexPath)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("oversized existing index was mutated: read err=%v bytes=%d want=%d", err, len(got), len(want))
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("oversized stage check touched unrelated DataDir: stat error=%v", err)
	}
}

func TestStageHACheckpointFilesRejectsWrongRootWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	bucket, selection := createHAStageFixture(t, ctx)
	stageDir := filepath.Join(t.TempDir(), "should-not-exist")
	selection.CheckpointRootHash[0] ^= 0xff
	if err := stageHACheckpointFiles(ctx, bucket, selection, stageDir); err == nil {
		t.Fatal("wrong checkpoint root was accepted")
	}
	assertHAStageNotPublished(t, stageDir)
}

func TestStageHACheckpointFilesRejectsStateMismatchWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	bucket, selection := createHAStageFixture(t, ctx)
	stageDir := filepath.Join(t.TempDir(), "should-not-exist")
	selection.CheckpointStateHash[0] ^= 0xff
	if err := stageHACheckpointFiles(ctx, bucket, selection, stageDir); err == nil {
		t.Fatal("checkpoint state hash mismatch was accepted")
	}
	assertHAStageNotPublished(t, stageDir)
}

func TestStageHACheckpointFilesRejectsCorruptBytesWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	bucket, selection := createHAStageFixture(t, ctx)
	manager := checkpoint.NewManager(bucket, selection.SourcePrefix, "", selection.ArchiveConfigID)
	root, err := manager.OpenRoot(ctx, selection.CheckpointIndex, selection.CheckpointRootHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Files) == 0 || len(root.Files[0].Blocks) == 0 {
		t.Fatal("fixture checkpoint has no content block")
	}
	block := root.Files[0].Blocks[0]
	blockName := block.Hash + ".block"
	if block.Generation != 0 {
		blockName = fmt.Sprintf("%s_%020d.block", block.Hash, block.Generation)
	}
	corruptKey := filepath.ToSlash(filepath.Join(selection.SourcePrefix, "checkpoint", "blocks", blockName))
	if err := bucket.Upload(ctx, corruptKey, strings.NewReader("corrupt checkpoint bytes")); err != nil {
		t.Fatal(err)
	}
	stageDir := filepath.Join(t.TempDir(), "should-not-exist")
	if err := stageHACheckpointFiles(ctx, bucket, selection, stageDir); err == nil {
		t.Fatal("corrupt checkpoint bytes were accepted")
	}
	assertHAStageNotPublished(t, stageDir)
}

func createHAStageFixture(t *testing.T, ctx context.Context) (objstore.Bucket, recovery.HAArchiveRestoreSelection) {
	t.Helper()
	bucket := objstore.NewInMemBucket()
	prefix := "ha-stage-test"
	manager := checkpoint.NewManager(bucket, prefix, t.TempDir(), recovery.ArchiveConfigID)
	source := filepath.Join(t.TempDir(), "source.sqlite")
	policySnapshot(t, source, "staged-value")
	claim, err := manager.AcquirePublisherClaim(ctx, "stage-fixture", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: source}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	return bucket, recovery.HAArchiveRestoreSelection{
		SourcePrefix: prefix, ArchiveConfigID: recovery.ArchiveConfigID, BootstrapConfigID: 1,
		BootstrapMembership: recovery.NewMembershipRecord("ha-stage-test", []quepaxa.Member{{ID: "n1"}}, "async"),
		Tip:                 8, PrefixHash: [32]byte{1}, HasCheckpoint: true,
		CheckpointIndex: root.Index, CheckpointRootHash: root.RootHash, CheckpointStateHash: root.Hash,
	}
}

func assertHAStageNotPublished(t *testing.T, stageDir string) {
	t.Helper()
	if _, err := os.Lstat(stageDir); !os.IsNotExist(err) {
		t.Fatalf("failed staging published a destination: stat error=%v", err)
	}
}

func fmtHash(hash [32]byte) string {
	return fmt.Sprintf("%x", hash)
}
