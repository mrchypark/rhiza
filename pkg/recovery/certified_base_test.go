package recovery

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestInitializeCertifiedRecoveryBaseAnchorOnlyAndTrustedLineage(t *testing.T) {
	ctx := context.Background()
	fixture := newCertifiedBaseFixture(t)
	view, err := fixture.archive.CaptureCertifiedView()
	if err != nil {
		t.Fatal(err)
	}
	verifier := fixture.newObserver(t, "anchor-only")
	root, err := InitializeCertifiedRecoveryBase(ctx, verifier.core, view, fixture.membership, fixture.anchorHash, fixture.checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	if root == nil || root.Index != fixture.anchor.Checkpoint.Index || verifier.core.Tip() != quepaxa.Slot(fixture.anchor.SourceTip) {
		t.Fatalf("anchor-only base root=%+v observerTip=%d", root, verifier.core.Tip())
	}
	if verifier.core.GenerationAnchorHash() != fixture.anchorHash {
		t.Fatal("observer did not retain the trusted generation anchor")
	}

	wrongHash := fixture.anchorHash
	wrongHash[0] ^= 1
	wrong := fixture.newObserver(t, "wrong-anchor")
	if _, err := InitializeCertifiedRecoveryBase(ctx, wrong.core, view, fixture.membership, wrongHash, fixture.checkpoints); err == nil {
		t.Fatal("observer accepted an archive candidate as a different trusted anchor")
	}
	if wrong.core.GenerationAnchorHash() != ([32]byte{}) || wrong.core.Tip() != 0 {
		t.Fatal("wrong trusted anchor changed observer state")
	}
}

func TestInitializeCertifiedRecoveryBaseValidatesEqualIndexCheckpointCertificate(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	archive := NewManager(bucket, "equal-index", 1)
	defer archive.Close()
	writerWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer writerWAL.Close()
	members := []quepaxa.Member{{ID: "target"}}
	writer, err := quepaxa.New(quepaxa.Config{
		NodeID: "target", Cluster: quepaxa.Cluster{ConfigID: 1, Members: members},
		WAL: writerWAL, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	writer.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if _, _, err := writer.Propose(ctx, []byte("state before equal-index checkpoint")); err != nil {
		t.Fatal(err)
	}
	sealIndex := writer.Tip()
	prefix, ok := writer.PrefixHash(sealIndex)
	if !ok {
		t.Fatal("writer checkpoint prefix is unavailable")
	}
	next, following, err := writer.CheckpointLeaderOrders(sealIndex)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := writer.CheckpointMembership(sealIndex)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: sealIndex, RootHash: testHash(5), StateHash: testHash(6),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
		Membership: &membership,
	}
	if err := writer.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.Propose(ctx, sealValue); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := writer.LatestCheckpointSeal()
	if err != nil || !ok || sealed.Index != sealIndex {
		t.Fatalf("writer checkpoint seal=%+v ok=%v err=%v", sealed, ok, err)
	}
	baseDecision, ok := writer.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("writer checkpoint decision is unavailable")
	}
	if err := archive.SyncThrough(ctx, writer, writer.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := archive.TrimThrough(ctx, sealed, baseDecision); err != nil {
		t.Fatal(err)
	}
	view, err := archive.CaptureCertifiedView()
	if err != nil {
		t.Fatal(err)
	}
	if _, anchored := view.GenerationAnchor(); anchored {
		t.Fatal("regular checkpoint fixture unexpectedly has generation lineage")
	}
	if base, _, ok := view.RecoveryBase(); !ok || base.Index != seal.Index {
		t.Fatalf("archive recovery base=%+v ok=%v, want index %d", base, ok, seal.Index)
	}

	observerWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer observerWAL.Close()
	observer, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID: "verify-equal-index", Cluster: quepaxa.Cluster{ConfigID: 1, Members: members},
		WAL: observerWAL, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	observer.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := observer.RestoreCheckpointBase(ctx, seal, baseDecision); err != nil {
		t.Fatal(err)
	}
	if observer.Tip() != seal.Index {
		t.Fatalf("observer tip=%d, want same index as base %d", observer.Tip(), seal.Index)
	}
	if _, err := InitializeCertifiedRecoveryBase(ctx, observer, view, MembershipRecord{}, [32]byte{}, nil); err != nil {
		t.Fatalf("valid same-index checkpoint certificate was rejected: %v", err)
	}
	if observer.Tip() != seal.Index || observer.GenerationAnchorHash() != ([32]byte{}) {
		t.Fatal("valid same-index verification unexpectedly changed observer state")
	}

	invalidView := view
	invalidView.head = cloneArchiveHead(view.head)
	invalidDecision := *invalidView.head.BaseDecision
	var certificate map[string]any
	if err := json.Unmarshal(invalidDecision.Certificate, &certificate); err != nil {
		t.Fatal(err)
	}
	summaries, ok := certificate["summaries"].([]any)
	if !ok || len(summaries) == 0 {
		t.Fatalf("checkpoint certificate summaries=%v", certificate["summaries"])
	}
	first, ok := summaries[0].(map[string]any)
	if !ok {
		t.Fatalf("checkpoint certificate summary=%T", summaries[0])
	}
	first["recorder_id"] = "not-a-member"
	invalidDecision.Certificate, err = json.Marshal(certificate)
	if err != nil {
		t.Fatal(err)
	}
	invalidView.head.BaseDecision = &invalidDecision
	if _, err := InitializeCertifiedRecoveryBase(ctx, observer, invalidView, MembershipRecord{}, [32]byte{}, nil); err == nil || !strings.Contains(err.Error(), "validate checkpoint recovery decision") {
		t.Fatalf("invalid same-index certificate error=%v, want certificate validation failure", err)
	}
	if observer.Tip() != seal.Index || observer.GenerationAnchorHash() != ([32]byte{}) {
		t.Fatal("invalid same-index certificate changed observer state")
	}
}

func TestInitializeCertifiedRecoveryBaseUsesNewerCheckpointAfterAnchorRootGC(t *testing.T) {
	ctx := context.Background()
	fixture := newCertifiedBaseFixture(t)
	initialView, err := fixture.archive.CaptureCertifiedView()
	if err != nil {
		t.Fatal(err)
	}
	writerWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer writerWAL.Close()
	writer, err := quepaxa.New(quepaxa.Config{
		NodeID: "target", Cluster: quepaxa.Cluster{ConfigID: 1, Members: fixture.members},
		WAL: writerWAL, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	writer.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return fixture.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	anchorRoot, err := InitializeCertifiedRecoveryBase(ctx, writer, initialView, fixture.membership, fixture.anchorHash, fixture.checkpoints)
	if err != nil {
		t.Fatal(err)
	}
	if anchorRoot == nil || anchorRoot.Index != fixture.anchor.Checkpoint.Index {
		t.Fatalf("initial generation checkpoint=%+v", anchorRoot)
	}
	baseRoot, err := fixture.checkpoints.OpenRoot(ctx, fixture.anchor.Checkpoint.Index, mustGenerationHash(fixture.anchor.Checkpoint.RootHash))
	if err != nil {
		t.Fatal(err)
	}
	checkpointFiles, err := fixture.checkpoints.DownloadAndVerifyRootFiles(ctx, baseRoot, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "state.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	var restoreFiles []materializer.CheckpointFile
	for _, file := range checkpointFiles {
		restoreFiles = append(restoreFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if err := state.RestoreCheckpoint(ctx, restoreFiles); err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.Propose(ctx, quepaxa.EncodeReadBarrier([quepaxa.ReadBarrierNonceSize]byte{1})); err != nil {
		t.Fatal(err)
	}
	if err := fixture.archive.SyncThrough(ctx, writer, writer.Tip()); err != nil {
		t.Fatal(err)
	}
	decision, ok := writer.CertifiedValue(writer.Tip())
	if !ok {
		t.Fatal("writer decision is unavailable")
	}
	if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.checkpoints.AcquirePublisherClaim(ctx, "newer-base", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	root, err := fixture.checkpoints.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = fixture.checkpoints.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := writer.PrefixHash(writer.Tip())
	if !ok {
		t.Fatal("new checkpoint prefix is unavailable")
	}
	next, following, err := writer.CheckpointLeaderOrders(writer.Tip())
	if err != nil {
		t.Fatal(err)
	}
	membership, err := writer.CheckpointMembership(writer.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: writer.Tip(), RootHash: root.RootHash, StateHash: root.Hash,
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
		Membership: &membership, GenerationAnchorHash: fixture.anchorHash,
	}
	if err := writer.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	if err := fixture.archive.SyncThrough(ctx, writer, writer.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := writer.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("new checkpoint seal unavailable: %v", err)
	}
	decision, ok = writer.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("checkpoint decision is unavailable")
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
	oldRootHash := mustGenerationHash(fixture.anchor.Checkpoint.RootHash)
	oldRootName := hex.EncodeToString(oldRootHash[:])
	aged := false
	for name := range fixture.bucket.Objects() {
		if strings.Contains(name, "target-generation/checkpoint/roots/") && strings.Contains(name, oldRootName) {
			if err := fixture.bucket.ChangeLastModified(name, time.Now().Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			aged = true
		}
	}
	if !aged {
		t.Fatal("could not age the superseded generation checkpoint fixture")
	}
	if err := fixture.checkpoints.GarbageCollectFrom(ctx, nil, 1, uint64(root.Index), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.checkpoints.OpenRoot(ctx, fixture.anchor.Checkpoint.Index, mustGenerationHash(fixture.anchor.Checkpoint.RootHash)); err == nil {
		t.Fatal("superseded generation anchor root survived checkpoint GC")
	}

	current := NewManager(fixture.bucket, fixture.prefix, 1)
	t.Cleanup(current.Close)
	if err := current.Load(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := current.CaptureCertifiedView()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyExpectedAnchorForView(ctx, view, EvidenceOptions{
		ExpectedAnchorHash: fixture.anchorHash, ExpectedMembership: fixture.membership,
		ExpectedForkResult:   ForkResult{Tip: fixture.anchor.SourceTip, ManifestHash: fixture.anchor.SourceManifest, PrefixHash: fixture.anchor.SourcePrefixHash},
		ExpectedSourcePrefix: fixture.anchor.SourcePrefix, ExpectedOperationID: fixture.anchor.OperationID,
	}); err != nil {
		t.Fatalf("selected-view anchor verification depended on collected original root: %v", err)
	}
	verifier := fixture.newObserver(t, "newer-checkpoint")
	verifier.core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return fixture.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if _, err := InitializeCertifiedRecoveryBase(ctx, verifier.core, view, fixture.membership, fixture.anchorHash, fixture.checkpoints); err != nil {
		t.Fatal(err)
	}
	if verifier.core.Tip() != sealed.Index || verifier.core.GenerationAnchorHash() != fixture.anchorHash {
		t.Fatalf("newer recovery base tip=%d anchor=%x", verifier.core.Tip(), verifier.core.GenerationAnchorHash())
	}
}

type certifiedBaseFixture struct {
	bucket      *objstore.InMemBucket
	prefix      string
	archive     *Manager
	checkpoints *checkpoint.Manager
	anchor      GenerationAnchor
	anchorHash  [32]byte
	membership  MembershipRecord
	members     []quepaxa.Member
}

func newCertifiedBaseFixture(t *testing.T) certifiedBaseFixture {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	sourceMembers := []quepaxa.Member{{ID: "source", PublicKey: testPublicKey("source")}}
	sourceWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceWAL.Close() })
	source, err := quepaxa.New(quepaxa.Config{NodeID: "source", Cluster: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, WAL: sourceWAL})
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "source-state", Events: []types.GraphStreamEvent{{Stream: "anchor", Kind: "created", Payload: "state"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	decision, ok := source.CertifiedValue(source.Tip())
	if !ok {
		t.Fatal("source decision is unavailable")
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "source.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	checkpoints := checkpoint.NewManager(bucket, "source", "", 1)
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "source", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	root, err := checkpoints.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = checkpoints.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := source.PrefixHash(source.Tip())
	if !ok {
		t.Fatal("source prefix is unavailable")
	}
	next, following, err := source.CheckpointLeaderOrders(source.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: source.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	source.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	if err := source.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	archive := NewManager(bucket, "source", 1)
	t.Cleanup(archive.Close)
	if err := archive.SyncThrough(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := source.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("source checkpoint seal unavailable: %v", err)
	}
	decision, ok = source.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("source checkpoint decision is unavailable")
	}
	if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "source", "generation-op"); err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	members := []quepaxa.Member{{ID: "target", PublicKey: testPublicKey("target")}}
	membership := NewMembershipRecord("target-generation", members, "async")
	if _, err := Fork(ctx, bucket, ForkOptions{
		SourcePrefix: "source", TargetPrefix: "target-generation",
		SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers},
		TargetMembers:   members, TargetMembership: membership, OperationID: "generation-op",
	}); err != nil {
		t.Fatal(err)
	}
	anchor, anchorHash, err := ReadGenerationAnchor(ctx, bucket, "target-generation")
	if err != nil {
		t.Fatal(err)
	}
	targetArchive := NewManager(bucket, "target-generation", 1)
	t.Cleanup(targetArchive.Close)
	if err := targetArchive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	targetCheckpoints := checkpoint.NewManager(bucket, "target-generation", "", 1)
	return certifiedBaseFixture{
		bucket: bucket, prefix: "target-generation", archive: targetArchive,
		checkpoints: targetCheckpoints, anchor: anchor, anchorHash: anchorHash,
		membership: membership, members: members,
	}
}

type certifiedBaseObserver struct {
	wal  *qlog.WAL
	core *quepaxa.Core
}

func (f certifiedBaseFixture) newObserver(t *testing.T, name string) certifiedBaseObserver {
	t.Helper()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID: quepaxa.NodeID("verify-" + name), Cluster: quepaxa.Cluster{ConfigID: 1, Members: f.members},
		WAL: wal, EnableReconfiguration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return f.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	return certifiedBaseObserver{wal: wal, core: core}
}

func testHash(value byte) [32]byte {
	var hash [32]byte
	hash[0] = value
	return hash
}

func TestCertifiedArchiveViewDefensivelyCopiesHead(t *testing.T) {
	fixture := newCertifiedBaseFixture(t)
	view, err := fixture.archive.CaptureCertifiedView()
	if err != nil {
		t.Fatal(err)
	}
	before, ok := view.GenerationAnchor()
	if !ok || before != fixture.anchorHash {
		t.Fatal("captured archive view lost its generation anchor")
	}
	view.head.LineageAnchor.Hash[0] ^= 1
	after, ok := view.GenerationAnchor()
	if !ok || after == before {
		t.Fatal("test failed to mutate its captured view")
	}
	fresh, err := fixture.archive.CaptureCertifiedView()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := fresh.GenerationAnchor(); !ok || got != before {
		t.Fatal("mutating the captured value changed the archive head")
	}
}
