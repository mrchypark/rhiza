package recovery

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestForkMaterializesAnchoredTargetWithoutSourceExtents(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	sourceMembers := []quepaxa.Member{{ID: "old", Token: "old"}}
	w, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "old", Cluster: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, WAL: w})
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "dedupe", Events: []types.GraphStreamEvent{{Stream: "anchor", Kind: "created", Payload: "state"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(core.Tip())
	if !ok {
		t.Fatal("missing decision")
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	cp := checkpoint.NewManager(bucket, "source", "", 1)
	claim, err := cp.AcquirePublisherClaim(ctx, "source", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	sources := make([]checkpoint.Source, 0, len(files))
	for _, f := range files {
		sources = append(sources, checkpoint.Source{Role: string(f.Role), Path: f.Path})
	}
	root, err := cp.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cp.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute); err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(core.Tip())
	next, following, _ := core.CheckpointLeaderOrders(core.Tip())
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, _ := quepaxa.EncodeCheckpointSeal(seal)
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	archive := NewManager(bucket, "source", 1)
	defer archive.Close()
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatal(err)
	}
	base, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("missing seal")
	}
	if err := archive.TrimThrough(ctx, sealed, base); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "source", "op"); err != nil {
		t.Fatal(err)
	}
	if err := cp.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	targetMembers := []quepaxa.Member{{ID: "new", Token: "new"}}
	targetMembership := NewMembershipRecord("anchor-source", targetMembers, "async")
	result, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "anchor-source", SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, TargetMembers: targetMembers, TargetMembership: targetMembership, OperationID: "op"})
	if err != nil {
		t.Fatal(err)
	}
	anchor, hash, err := ReadGenerationAnchor(ctx, bucket, "anchor-source")
	if err != nil || anchor.SourceTip != result.Tip {
		t.Fatalf("anchor=%+v err=%v", anchor, err)
	}
	target := NewManager(bucket, "anchor-source", 1)
	defer target.Close()
	if err := target.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if got, ok := target.GenerationAnchor(); !ok || got != hash {
		t.Fatal("target archive did not retain generation anchor")
	}
	if _, _, err := target.DecisionsFrom(ctx, 1, 1); err == nil {
		t.Fatal("target retained source extents")
	}
	var prefixHash, rootHash [32]byte
	targetCP := checkpoint.NewManager(bucket, "anchor-source", "", 1)
	targetRoot, err := targetCP.OpenRoot(ctx, anchor.Checkpoint.Index, rootHashForTest(t, anchor.Checkpoint.RootHash))
	if err != nil {
		t.Fatal(err)
	}
	filesDir := t.TempDir()
	checkpointFiles, err := targetCP.DownloadAndVerifyRootFiles(ctx, targetRoot, filesDir)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := materializer.Open(filepath.Join(filesDir, "restored.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restoreFiles := make([]materializer.CheckpointFile, 0, len(checkpointFiles))
	for _, file := range checkpointFiles {
		restoreFiles = append(restoreFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if err := restored.RestoreCheckpoint(ctx, restoreFiles); err != nil {
		t.Fatal(err)
	}
	if _, found, err := restored.GraphMutationReceipt(ctx, "dedupe"); err != nil || !found {
		t.Fatalf("materialized request dedup was not retained: found=%v err=%v", found, err)
	}
	_, _ = hex.Decode(prefixHash[:], []byte(anchor.SourcePrefixHash))
	_, _ = hex.Decode(rootHash[:], []byte(anchor.Checkpoint.RootHash))
	targetWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer targetWAL.Close()
	targetCore, err := quepaxa.New(quepaxa.Config{NodeID: "new", Cluster: quepaxa.Cluster{ConfigID: 1, Members: targetMembers}, WAL: targetWAL, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := targetCore.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{ConfigID: 1, Index: quepaxa.Slot(anchor.SourceTip), PrefixHash: prefixHash, RootHash: rootHash, AnchorHash: hash}); err != nil {
		t.Fatal(err)
	}
	if slot, _, err := targetCore.Propose(ctx, quepaxa.EncodeReadBarrier([quepaxa.ReadBarrierNonceSize]byte{2})); err != nil || uint64(slot) != anchor.SourceTip+1 {
		t.Fatalf("target proposal slot=%d err=%v", slot, err)
	}
	if err := target.SyncThrough(ctx, targetCore, targetCore.Tip()); err != nil {
		t.Fatal(err)
	}

	// Publish a normal checkpoint in B, then recover B into C. B's bootstrap
	// differs from A, and its ordinary checkpoint must retain B's anchor.
	bValue, ok := targetCore.CertifiedValue(targetCore.Tip())
	if !ok {
		t.Fatal("missing B write")
	}
	if err := restored.ApplyBatch(ctx, []quepaxa.DecidedValue{bValue}); err != nil {
		t.Fatal(err)
	}
	bClaim, err := targetCP.AcquirePublisherClaim(ctx, "b-checkpoint", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bFiles, bIndex, bCleanup, err := restored.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer bCleanup()
	var bSources []checkpoint.Source
	for _, file := range bFiles {
		bSources = append(bSources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	bRoot, err := targetCP.CreateFiles(ctx, bClaim, bSources, bIndex)
	if err != nil {
		t.Fatal(err)
	}
	bClaim, err = targetCP.BindPublisherClaim(ctx, bClaim, bIndex, bRoot.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bPrefix, _ := targetCore.PrefixHash(targetCore.Tip())
	bNext, bFollowing, err := targetCore.CheckpointLeaderOrders(targetCore.Tip())
	if err != nil {
		t.Fatal(err)
	}
	bHistory, err := targetCore.CheckpointMembership(targetCore.Tip())
	if err != nil {
		t.Fatal(err)
	}
	bSeal := quepaxa.CheckpointSeal{ConfigID: 1, Index: targetCore.Tip(), RootHash: bRoot.RootHash, StateHash: bRoot.Hash, PrefixHash: bPrefix, NextLeaderOrder: bNext, FollowingLeaderOrder: bFollowing, Membership: &bHistory, GenerationAnchorHash: hash}
	targetCore.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return targetCP.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := targetCore.PrepareCheckpoint(ctx, bSeal); err != nil {
		t.Fatal(err)
	}
	bEncoded, err := quepaxa.EncodeCheckpointSeal(bSeal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := targetCore.Propose(ctx, bEncoded); err != nil {
		t.Fatal(err)
	}
	if err := target.SyncThrough(ctx, targetCore, targetCore.Tip()); err != nil {
		t.Fatal(err)
	}
	bSealed, ok, err := targetCore.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("B seal: %v", err)
	}
	bDecision, ok := targetCore.CertifiedValue(bSealed.DecisionSlot)
	if !ok {
		t.Fatal("missing B seal decision")
	}
	if err := target.TrimThrough(ctx, bSealed, bDecision); err != nil {
		t.Fatal(err)
	}
	if err := targetCP.ReleasePublisherClaim(ctx, bClaim); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "anchor-source", "b-to-c"); err != nil {
		t.Fatal(err)
	}
	cMembers := []quepaxa.Member{{ID: "third", Token: "third"}}
	cOptions := ForkOptions{SourcePrefix: "anchor-source", TargetPrefix: "third-generation", SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: targetMembers}, TargetMembers: cMembers, TargetMembership: NewMembershipRecord("third-generation", cMembers, "async"), OperationID: "b-to-c"}
	cResult, err := Fork(ctx, bucket, cOptions)
	if err != nil || cResult.Tip != uint64(targetCore.Tip()) {
		t.Fatalf("B to C: %+v %v", cResult, err)
	}
	if _, err := Fork(ctx, bucket, cOptions); err != nil {
		t.Fatalf("B to C retry: %v", err)
	}
	cAnchor, _, err := ReadGenerationAnchor(ctx, bucket, "third-generation")
	if err != nil {
		t.Fatal(err)
	}
	cCP := checkpoint.NewManager(bucket, "third-generation", "", 1)
	cRoot, err := cCP.OpenRoot(ctx, cAnchor.Checkpoint.Index, rootHashForTest(t, cAnchor.Checkpoint.RootHash))
	if err != nil {
		t.Fatal(err)
	}
	cDir := t.TempDir()
	cFiles, err := cCP.DownloadAndVerifyRootFiles(ctx, cRoot, cDir)
	if err != nil {
		t.Fatal(err)
	}
	cState, err := materializer.Open(filepath.Join(cDir, "restored.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cState.Close()
	var cRestore []materializer.CheckpointFile
	for _, file := range cFiles {
		cRestore = append(cRestore, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if err := cState.RestoreCheckpoint(ctx, cRestore); err != nil {
		t.Fatal(err)
	}
	if _, found, err := cState.GraphMutationReceipt(ctx, "dedupe"); err != nil || !found {
		t.Fatalf("C lost A receipt: found=%v err=%v", found, err)
	}
	wrongBootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "wrong", Token: "wrong"}}}
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "wrong-bootstrap", SourceBootstrap: wrongBootstrap, TargetMembers: targetMembers, TargetMembership: targetMembership, OperationID: "wrong-bootstrap"}); err == nil {
		t.Fatal("fork accepted an incorrect source bootstrap")
	}
	wrongTarget := NewMembershipRecord("anchor-source", []quepaxa.Member{{ID: "new", Token: "other"}}, "async")
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "wrong-target", SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, TargetMembers: targetMembers, TargetMembership: wrongTarget, OperationID: "wrong-target"}); err == nil {
		t.Fatal("fork accepted mismatched target membership")
	}
}

func TestForkMaterializesBaseZeroMembershipTransition(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	initial := []quepaxa.Member{{ID: "a", Token: "a"}, {ID: "b", Token: "b"}}
	transport := newArchiveTestCluster(t, initial)
	source := transport.cores["a"]
	if _, err := source.BeginReconfiguration(ctx, quepaxa.Cluster{ConfigID: 2, Members: initial[:1]}); err != nil {
		t.Fatal(err)
	}
	if err := source.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	archive := NewManager(bucket, "base-zero", 1)
	defer archive.Close()
	if err := archive.SyncThrough(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "base-zero", "base-zero-op"); err != nil {
		t.Fatal(err)
	}
	targetMembers := []quepaxa.Member{{ID: "c", Token: "c"}}
	result, err := Fork(ctx, bucket, ForkOptions{
		SourcePrefix: "base-zero", TargetPrefix: "base-zero-target", SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: initial},
		TargetMembers: targetMembers, TargetMembership: NewMembershipRecord("base-zero-target", targetMembers, "async"), OperationID: "base-zero-op",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Tip != uint64(source.Tip()) {
		t.Fatalf("tip=%d want %d", result.Tip, source.Tip())
	}
}

func rootHashForTest(t *testing.T, value string) [32]byte {
	t.Helper()
	var hash [32]byte
	if _, err := hex.Decode(hash[:], []byte(value)); err != nil {
		t.Fatal(err)
	}
	return hash
}
