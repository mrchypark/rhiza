package recovery

import (
	"context"
	"errors"
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

func TestVerifyCertifiedBarrier(t *testing.T) {
	ctx := context.Background()
	member := quepaxa.Member{ID: "source"}
	initial := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	source, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: initial, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}

	var nonce [quepaxa.ReadBarrierNonceSize]byte
	nonce[0] = 0x5a
	barrierSlot, _, err := source.Propose(ctx, quepaxa.EncodeReadBarrier(nonce))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(ctx, []byte("after-barrier")); err != nil {
		t.Fatal(err)
	}

	bucket := objstore.NewInMemBucket()
	writer := NewManager(bucket, "cluster", 1)
	defer writer.Close()
	if err := writer.SyncThrough(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}
	verifier := NewManager(bucket, "cluster", 1)
	defer verifier.Close()

	decision, membership, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot, nonce)
	if err != nil {
		t.Fatalf("verify valid barrier: %v", err)
	}
	if decision.Slot != barrierSlot || membership.ConfigID != initial.ConfigID || len(membership.Members) != 1 || membership.Members[0].ID != member.ID {
		t.Fatalf("verified wrong barrier/membership: slot=%d membership=%+v", decision.Slot, membership)
	}

	wrongNonce := nonce
	wrongNonce[1] = 0x01
	if _, _, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot, wrongNonce); err == nil {
		t.Fatal("verification accepted a different nonce")
	}
	if _, _, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot+1, nonce); err == nil {
		t.Fatal("verification accepted a non-barrier value at another slot")
	}

	// A verifier must not trust a long-lived caller Manager's validated extent
	// cache after the backing object disappears.
	if err := verifier.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifier.DecisionsFrom(ctx, 1, 256); err != nil {
		t.Fatal(err)
	}
	verifier.mu.Lock()
	ref := verifier.extents[0]
	verifier.mu.Unlock()
	if err := bucket.Delete(ctx, verifier.key(extentObjectKey(ref.hash, ref.object))); err != nil {
		t.Fatal(err)
	}
	// The old receiver's cache is specifically unsafe here: with an unchanged
	// HEAD it still reports the formerly validated extent as readable.
	if err := verifier.Load(ctx); err != nil {
		t.Fatalf("cached receiver reload unexpectedly noticed immutable extent loss: %v", err)
	}
	if values, _, err := verifier.DecisionsFrom(ctx, 1, 256); err != nil || len(values) == 0 {
		t.Fatalf("cached receiver did not reproduce stale extent evidence: values=%d err=%v", len(values), err)
	}
	if _, _, err := verifier.VerifyCertifiedBarrier(ctx, initial, true, barrierSlot, nonce); err == nil {
		t.Fatal("verification trusted cached extent after its backing object disappeared")
	}
}

func TestVerifyCertifiedBarrierWithEvidenceUsesAnchorFloorAndRejectsBadExpectation(t *testing.T) {
	ctx := context.Background()
	fixture := newCertifiedBaseFixture(t)
	initial, slot, nonce := publishAnchoredBarrier(t, fixture)
	verifier := NewManager(fixture.bucket, fixture.prefix, 1)
	defer verifier.Close()
	opts := evidenceOptionsForAnchorFixture(fixture)
	state, err := verifier.VerifyCertifiedBarrierWithEvidenceState(ctx, initial, true, slot, nonce, opts)
	if err != nil {
		t.Fatalf("verify anchored barrier after administrative floor: %v", err)
	}
	if state.Decision.Slot != slot || state.Membership.ConfigID != 1 || len(state.Membership.Members) != len(initial.Members) ||
		state.PrefixHash == ([32]byte{}) || state.Pending || state.HasAbort {
		t.Fatalf("wrong anchored barrier state: %+v", state)
	}

	wrongNonce := nonce
	wrongNonce[0] ^= 0x80
	if _, _, err := verifier.VerifyCertifiedBarrierWithEvidence(ctx, initial, true, slot, wrongNonce, opts); err == nil {
		t.Fatal("accepted a barrier with the wrong nonce")
	}
	missing := opts
	missing.ExpectedAnchorHash[0] ^= 1
	if _, _, err := verifier.VerifyCertifiedBarrierWithEvidence(ctx, initial, true, slot, nonce, missing); err == nil {
		t.Fatal("accepted a missing or mismatched expected anchor")
	}
}

func TestVerifyCertifiedBarrierWithEvidenceRejectsCompactedBarrier(t *testing.T) {
	ctx := context.Background()
	fixture := newCertifiedBaseFixture(t)
	initial, slot, _ := publishAnchoredBarrier(t, fixture)
	publishAnchoredBarrierCheckpoint(t, fixture, initial)
	verifier := NewManager(fixture.bucket, fixture.prefix, 1)
	defer verifier.Close()
	if _, _, err := verifier.VerifyCertifiedBarrierWithEvidence(ctx, initial, true, slot, [quepaxa.ReadBarrierNonceSize]byte{0x5a}, evidenceOptionsForAnchorFixture(fixture)); err == nil {
		t.Fatal("accepted a barrier decision compacted behind the selected checkpoint base")
	}
}

func evidenceOptionsForAnchorFixture(fixture certifiedBaseFixture) EvidenceOptions {
	return EvidenceOptions{
		ExpectedAnchorHash: fixture.anchorHash, ExpectedMembership: fixture.membership,
		ExpectedForkResult:   ForkResult{Tip: fixture.anchor.SourceTip, ManifestHash: fixture.anchor.SourceManifest, PrefixHash: fixture.anchor.SourcePrefixHash},
		ExpectedSourcePrefix: fixture.anchor.SourcePrefix, ExpectedOperationID: fixture.anchor.OperationID,
	}
}

func publishAnchoredBarrier(t *testing.T, fixture certifiedBaseFixture) (quepaxa.Cluster, quepaxa.Slot, [quepaxa.ReadBarrierNonceSize]byte) {
	t.Helper()
	ctx := context.Background()
	initial := quepaxa.Cluster{ConfigID: 1, Members: fixture.members}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: fixture.members[0].ID, Cluster: initial, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return fixture.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	prefixHash := mustGenerationHash(fixture.anchor.SourcePrefixHash)
	rootHash := mustGenerationHash(fixture.anchor.Checkpoint.RootHash)
	if err := core.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{
		ConfigID: 1, Index: quepaxa.Slot(fixture.anchor.SourceTip), PrefixHash: prefixHash,
		RootHash: rootHash, AnchorHash: fixture.anchorHash,
	}); err != nil {
		t.Fatal(err)
	}
	var nonce [quepaxa.ReadBarrierNonceSize]byte
	nonce[0] = 0x5a
	slot, _, err := core.Propose(ctx, quepaxa.EncodeReadBarrier(nonce))
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "post-barrier-write", Events: []types.GraphStreamEvent{{Stream: "barrier", Kind: "created", Payload: "certified"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := fixture.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	return initial, slot, nonce
}

func publishAnchoredBarrierCheckpoint(t *testing.T, fixture certifiedBaseFixture, initial quepaxa.Cluster) {
	t.Helper()
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: initial.Members[0].ID, Cluster: initial, WAL: wal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return fixture.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := core.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{
		ConfigID: 1, Index: quepaxa.Slot(fixture.anchor.SourceTip), PrefixHash: mustGenerationHash(fixture.anchor.SourcePrefixHash),
		RootHash: mustGenerationHash(fixture.anchor.Checkpoint.RootHash), AnchorHash: fixture.anchorHash,
	}); err != nil {
		t.Fatal(err)
	}
	var nonce [quepaxa.ReadBarrierNonceSize]byte
	nonce[0] = 0x5a
	if _, _, err := core.Propose(ctx, quepaxa.EncodeReadBarrier(nonce)); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "post-barrier-write", Events: []types.GraphStreamEvent{{Stream: "barrier", Kind: "created", Payload: "certified"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	anchorRoot, err := fixture.checkpoints.OpenRoot(ctx, fixture.anchor.Checkpoint.Index, mustGenerationHash(fixture.anchor.Checkpoint.RootHash))
	if err != nil {
		t.Fatal(err)
	}
	files, err := fixture.checkpoints.DownloadAndVerifyRootFiles(ctx, anchorRoot, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "restored.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	restoreFiles := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		restoreFiles = append(restoreFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if err := state.RestoreCheckpoint(ctx, restoreFiles); err != nil {
		t.Fatal(err)
	}
	decisions := make([]quepaxa.DecidedValue, 0, int(core.Tip())-int(fixture.anchor.SourceTip))
	for index := quepaxa.Slot(fixture.anchor.SourceTip + 1); index <= core.Tip(); index++ {
		decision, ok := core.CertifiedValue(index)
		if !ok {
			t.Fatalf("missing certified target decision at %d", index)
		}
		decisions = append(decisions, decision)
	}
	if err := state.ApplyBatch(ctx, decisions); err != nil {
		t.Fatal(err)
	}
	checkpointFiles, index, cleanup, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	sources := make([]checkpoint.Source, 0, len(checkpointFiles))
	for _, file := range checkpointFiles {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	claim, err := fixture.checkpoints.AcquirePublisherClaim(ctx, "barrier-compaction", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := fixture.checkpoints.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = fixture.checkpoints.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := core.PrefixHash(core.Tip())
	if !ok {
		t.Fatal("target checkpoint prefix unavailable")
	}
	next, following, err := core.CheckpointLeaderOrders(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership, GenerationAnchorHash: fixture.anchorHash}
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	if err := fixture.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("target checkpoint seal unavailable: %v", err)
	}
	decision, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("target checkpoint decision unavailable")
	}
	if err := fixture.checkpoints.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := fixture.archive.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatal(err)
	}
	if err := fixture.checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveHeadRejectsGenesisPrefix(t *testing.T) {
	_, err := encodeHead(archiveHead{
		ConfigID: 1, Generation: 1, BasePrefix: [32]byte{1}, Tip: 1,
		TailHash: [32]byte{2}, TailObject: 1,
	})
	if err == nil {
		t.Fatal("archive codec accepted a nonzero genesis prefix")
	}
}

func TestReplayCertifiedThroughRejectsMissingAndCanceledPages(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "observer"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID: "observer", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "source"}}}, WAL: wal,
	})
	if err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("page unavailable")
	err = ReplayCertifiedThrough(ctx, core, 1, func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		return nil, 1, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("missing page error=%v, want %v", err, wantErr)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	err = ReplayCertifiedThrough(canceled, core, 1, func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		t.Fatal("readPage called for canceled context")
		return nil, 0, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replay error=%v, want context.Canceled", err)
	}
}
