package recovery

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
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

var errAnchoredHeadResponseLost = errors.New("anchored archive head response lost")

type failAfterAnchoredHeadBucket struct {
	objstore.Bucket
	key        string
	fail       bool
	headWrites int
}

func (b *failAfterAnchoredHeadBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if name != b.key {
		return b.Bucket.Upload(ctx, name, reader, options...)
	}
	b.headWrites++
	if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
		return err
	}
	if b.fail {
		b.fail = false
		return errAnchoredHeadResponseLost
	}
	return nil
}

func TestForkMaterializesAnchoredTargetWithoutSourceExtents(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	sourceMembers := []quepaxa.Member{{ID: "old", PublicKey: testPublicKey("old")}}
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
	targetMembers := []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("new")}}
	targetMembership := NewMembershipRecord("anchor-source", targetMembers, "async")
	options := ForkOptions{SourcePrefix: "source", TargetPrefix: "anchor-source", SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, TargetMembers: targetMembers, TargetMembership: targetMembership, OperationID: "op"}
	faultBucket := &failAfterAnchoredHeadBucket{Bucket: bucket, key: forkKey("anchor-source", "archive/head.bin"), fail: true}
	if _, err := Fork(ctx, faultBucket, options); !errors.Is(err, errAnchoredHeadResponseLost) {
		t.Fatalf("fork did not stop at the committed-head response-loss boundary: %v", err)
	}
	if _, ok, err := readForkResult(ctx, bucket, "anchor-source"); err != nil || ok {
		t.Fatalf("fork result unexpectedly committed before retry: present=%v err=%v", ok, err)
	}
	partialTarget := NewManager(bucket, "anchor-source", 1)
	if err := partialTarget.Load(ctx); err != nil {
		t.Fatalf("load committed target head after lost response: %v", err)
	}
	partialHash, present := partialTarget.GenerationAnchor()
	partialTip := partialTarget.Tip()
	partialTarget.Close()
	if !present || partialHash == ([32]byte{}) || partialTip == 0 {
		t.Fatal("target head did not retain the committed generation anchor")
	}
	result, err := Fork(ctx, faultBucket, options)
	if err != nil {
		t.Fatalf("resume committed materialization: %v", err)
	}
	if faultBucket.headWrites != 1 {
		t.Fatalf("retry reinitialized target archive head: writes=%d", faultBucket.headWrites)
	}
	anchor, hash, err := ReadGenerationAnchor(ctx, bucket, "anchor-source")
	storedResult, resultPresent, resultErr := readForkResult(ctx, bucket, "anchor-source")
	if err != nil || resultErr != nil || !resultPresent || storedResult != result || anchor.SourceTip != result.Tip || anchor.SourcePrefixHash != result.PrefixHash || partialTip != quepaxa.Slot(result.Tip) {
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
	cMembers := []quepaxa.Member{{ID: "third", PublicKey: testPublicKey("third")}}
	cOptions := ForkOptions{SourcePrefix: "anchor-source", TargetPrefix: "third-generation", SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: targetMembers}, TargetMembers: cMembers, TargetMembership: NewMembershipRecord("third-generation", cMembers, "async"), OperationID: "b-to-c"}
	cResult, err := Fork(ctx, bucket, cOptions)
	if err != nil || cResult.Tip != uint64(targetCore.Tip()) {
		t.Fatalf("B to C: %+v %v", cResult, err)
	}
	if _, err := Fork(ctx, bucket, cOptions); err != nil {
		t.Fatalf("B to C retry: %v", err)
	}
	// Model interruption after anchor/head publication but before result commit.
	if err := bucket.Delete(ctx, forkResultKey("third-generation")); err != nil {
		t.Fatal(err)
	}
	if resumed, err := Fork(ctx, bucket, cOptions); err != nil || resumed != cResult {
		t.Fatalf("resume incomplete B to C result: %+v %v", resumed, err)
	}
	// A missing HEAD with an anchor but no result is ambiguous (it may have
	// been deleted after publication), so do not repair it from source replay.
	if err := bucket.Delete(ctx, forkResultKey("third-generation")); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Delete(ctx, forkKey("third-generation", "archive/head.bin")); err != nil {
		t.Fatal(err)
	}
	if _, err := Fork(ctx, bucket, cOptions); err == nil {
		t.Fatal("fork recreated a missing target head after anchor publication")
	}
	if _, ok, err := readForkResult(ctx, bucket, "third-generation"); err != nil || ok {
		t.Fatalf("fork committed a result despite missing target head: present=%v err=%v", ok, err)
	}
	missingHead := NewManager(bucket, "third-generation", 1)
	if err := missingHead.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := missingHead.HeadVersion(); ok {
		t.Fatal("fork recreated the missing target archive head")
	}
	missingHead.Close()
	// Model interruption after CURRENT publication but before anchor/head/result.
	// The retry must compare the rebuilt checkpoint bytes to CURRENT rather than
	// acquiring a normal publisher slot above C's fixed generation tip.
	if err := bucket.Delete(ctx, GenerationAnchorKey("third-generation")); err != nil {
		t.Fatal(err)
	}
	if resumed, err := Fork(ctx, bucket, cOptions); err != nil || resumed != cResult {
		t.Fatalf("resume pre-anchor B to C result: %+v %v", resumed, err)
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
	wrongBootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "wrong", PublicKey: testPublicKey("wrong")}}}
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "wrong-bootstrap", SourceBootstrap: wrongBootstrap, TargetMembers: targetMembers, TargetMembership: targetMembership, OperationID: "wrong-bootstrap"}); err == nil {
		t.Fatal("fork accepted an incorrect source bootstrap")
	}
	wrongTarget := NewMembershipRecord("anchor-source", []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("other")}}, "async")
	if _, err := Fork(ctx, bucket, ForkOptions{SourcePrefix: "source", TargetPrefix: "wrong-target", SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, TargetMembers: targetMembers, TargetMembership: wrongTarget, OperationID: "wrong-target"}); err == nil {
		t.Fatal("fork accepted mismatched target membership")
	}
}

func TestForkMaterializesBaseZeroMembershipTransition(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	initial := []quepaxa.Member{{ID: "a", PublicKey: testPublicKey("a")}, {ID: "b", PublicKey: testPublicKey("b")}}
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
	targetMembers := []quepaxa.Member{{ID: "c", PublicKey: testPublicKey("c")}}
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
