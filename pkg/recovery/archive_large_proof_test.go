package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
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

type largeProofFixture struct {
	ctx         context.Context
	bucket      objstore.Bucket
	prefix      string
	bootstrap   quepaxa.Cluster
	core        *quepaxa.Core
	archive     *Manager
	checkpoints *checkpoint.Manager
	seal        quepaxa.CheckpointSeal
	decision    quepaxa.DecidedValue
	sealBytes   []byte
	headBytes   int
}

func newLargeProofCluster(t testing.TB, members []quepaxa.Member) *archiveTestTransport {
	t.Helper()
	transport := &archiveTestTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core, len(members))}
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		localWAL := wal
		t.Cleanup(func() { _ = localWAL.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: localWAL, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	return transport
}

// newLargeProofFixture creates a real membership history, materializer root,
// prepared checkpoint, and quorum-certified checkpoint decision. The large
// bootstrap member URL is authenticated by the Core's immutable bootstrap
// when the membership history is checked.
func newLargeProofFixture(t testing.TB, urlBytes int, prefix string, withTransitions bool) *largeProofFixture {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	url := "https://node.invalid/" + strings.Repeat("a", urlBytes-len("https://node.invalid/"))
	members := []quepaxa.Member{{ID: "n1", URL: url}}
	if withTransitions {
		members = append(members, quepaxa.Member{ID: "n2"}, quepaxa.Member{ID: "n3"})
	}
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: members}
	transport := newLargeProofCluster(t, bootstrap.Members)
	core := transport.cores["n1"]
	if withTransitions {
		reconfigCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		t.Cleanup(cancel)
		for _, target := range []quepaxa.Cluster{
			{ConfigID: 2, Members: append([]quepaxa.Member(nil), bootstrap.Members[:2]...)},
			{ConfigID: 3, Members: append([]quepaxa.Member(nil), bootstrap.Members[:1]...)},
		} {
			if _, err := core.BeginReconfiguration(reconfigCtx, target); err != nil {
				t.Fatalf("begin real membership transition to config %d: %v", target.ConfigID, err)
			}
			if err := core.FinishReconfiguration(reconfigCtx); err != nil {
				t.Fatalf("finish real membership transition to config %d: %v", target.ConfigID, err)
			}
		}
	}
	command, err := types.EncodeGraphCommand(types.GraphCommand{
		RequestID: "large-proof-checkpoint",
		Events:    []types.GraphStreamEvent{{Stream: "large-proof", Kind: "created", Payload: "checkpointed"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, command); err != nil {
		t.Fatal(err)
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "materializer.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	for slot := quepaxa.Slot(1); slot <= core.Tip(); slot++ {
		value, ok := core.CertifiedValue(slot)
		if !ok {
			t.Fatalf("missing source decision at slot %d", slot)
		}
		if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{value}); err != nil {
			t.Fatalf("apply source decision at slot %d: %v", slot, err)
		}
	}
	cp := checkpoint.NewManager(bucket, prefix, "", 1)
	claim, err := cp.AcquirePublisherClaim(ctx, prefix, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claimReleased := false
	t.Cleanup(func() {
		if !claimReleased {
			_ = cp.ReleasePublisherClaim(ctx, claim)
		}
	})
	files, index, cleanup, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	root, err := cp.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = cp.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefixHash, ok := core.PrefixHash(core.Tip())
	if !ok {
		t.Fatal("missing source prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: core.ConfigID(), Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash,
		PrefixHash: prefixHash, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership,
	}
	core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatalf("verify actual checkpoint root: %v", err)
	}
	sealBytes, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatalf("encode membership checkpoint seal: %v", err)
	}
	decisionSlot, _, err := core.Propose(ctx, sealBytes)
	if err != nil {
		t.Fatalf("certify membership checkpoint: %v", err)
	}
	decision, ok := core.CertifiedValue(decisionSlot)
	if !ok {
		t.Fatal("missing certified checkpoint decision")
	}
	archive := NewManager(bucket, prefix, 1)
	t.Cleanup(archive.Close)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("publish source decisions: %v", err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok || sealed.DecisionSlot != decisionSlot {
		t.Fatalf("latest sealed checkpoint=%+v ok=%v err=%v", sealed, ok, err)
	}
	if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatalf("trim through certified checkpoint: %v", err)
	}
	reader := NewManager(bucket, prefix, 1)
	if err := reader.Load(ctx); err != nil {
		reader.Close()
		t.Fatalf("reload trimmed archive: %v", err)
	}
	reader.Close()
	if err := cp.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatalf("release checkpoint publisher after verified publish: %v", err)
	}
	claimReleased = true
	headObject, err := bucket.Get(ctx, prefix+"/archive/head.bin")
	if err != nil {
		t.Fatalf("read published archive HEAD: %v", err)
	}
	headBytes, readErr := io.ReadAll(headObject)
	closeErr := headObject.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read HEAD: read=%v close=%v", readErr, closeErr)
	}
	return &largeProofFixture{
		ctx: ctx, bucket: bucket, prefix: prefix, bootstrap: bootstrap, core: core,
		archive: archive, checkpoints: cp, seal: seal, decision: decision,
		sealBytes: sealBytes, headBytes: len(headBytes),
	}
}

func TestLargeMembershipCheckpointProofSurvivesArchiveLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name        string
		urlBytes    int
		wantSeal    func(int) bool
		transitions bool
	}{
		{name: "duplicate-head-overflow", urlBytes: 34 << 10, wantSeal: func(n int) bool { return n < maxHeadSize }, transitions: false},
		{name: "seal-over-head-limit", urlBytes: 13 << 10, wantSeal: func(n int) bool { return n > maxHeadSize && n <= quepaxa.MaxReplicatedValueBytes }, transitions: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLargeProofFixture(t, tc.urlBytes, "large-proof-"+tc.name, tc.transitions)
			if !tc.wantSeal(len(fixture.sealBytes)) {
				t.Fatalf("encoded authenticated seal=%d bytes; does not cover requested boundary", len(fixture.sealBytes))
			}
			if fixture.seal.Membership.Genesis.Members[0].URL != fixture.bootstrap.Members[0].URL || tc.transitions && len(fixture.seal.Membership.Transitions) != 2 {
				t.Fatalf("membership genesis/history does not match the intended authenticated fixture: transitions=%d", len(fixture.seal.Membership.Transitions))
			}
			decisionBytes, err := encodeBaseDecision(fixture.decision)
			if err != nil {
				t.Fatal(err)
			}
			legacyHeadBytes := headHeaderSize + len(fixture.sealBytes) + len(decisionBytes) + archiveCRCSize
			if legacyHeadBytes <= maxHeadSize {
				t.Fatalf("legacy duplicated HEAD size=%d, want >%d", legacyHeadBytes, maxHeadSize)
			}
			t.Logf("seal=%d B decision=%d B legacy duplicated HEAD=%d B new HEAD=%d B bound=%d B", len(fixture.sealBytes), len(decisionBytes), legacyHeadBytes, fixture.headBytes, maxArchiveHeadSize)
			if fixture.headBytes > maxArchiveHeadSize {
				t.Fatalf("published HEAD size=%d, exceeds bounded format limit %d", fixture.headBytes, maxArchiveHeadSize)
			}
			if fixture.headBytes >= legacyHeadBytes {
				t.Fatalf("published HEAD size=%d did not remove duplicated seal (%d-byte legacy HEAD)", fixture.headBytes, legacyHeadBytes)
			}
			if err := fixture.core.ValidateCheckpointBase(fixture.ctx, fixture.seal, fixture.decision); err != nil {
				t.Fatalf("validate certified membership proof against bootstrap: %v", err)
			}

			snapshot, err := fixture.archive.BeginRecoverySnapshot(fixture.ctx, "large-proof-lifecycle", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if !fixture.archive.cas {
				t.Fatal("in-memory archive fixture does not exercise HEAD compare-and-swap")
			}
			if seal, decision, ok := snapshot.RecoveryBase(); !ok || !bytes.Equal(decision.Value, fixture.sealBytes) || seal.RootHash != fixture.seal.RootHash {
				t.Fatal("recovery snapshot did not retain the certified large base")
			}
			if err := fixture.archive.Cleanup(fixture.ctx, 0); err != nil {
				t.Fatalf("GC with pinned snapshot: %v", err)
			}
			if err := fixture.archive.Cleanup(fixture.ctx, 0); err != nil {
				t.Fatalf("second GC pass with pinned snapshot: %v", err)
			}
			if seal, decision, ok := snapshot.RecoveryBase(); !ok || !bytes.Equal(decision.Value, fixture.sealBytes) || seal.RootHash != fixture.seal.RootHash {
				t.Fatal("GC invalidated the pinned large recovery proof")
			}
			if err := snapshot.Close(fixture.ctx); err != nil {
				t.Fatal(err)
			}

			loaded := NewManager(fixture.bucket, fixture.prefix, 1)
			defer loaded.Close()
			if err := loaded.Load(fixture.ctx); err != nil {
				t.Fatalf("load after GC: %v", err)
			}
			seal, decision, ok := loaded.RecoveryBase()
			if !ok || !bytes.Equal(decision.Value, fixture.sealBytes) || seal.RootHash != fixture.seal.RootHash {
				t.Fatal("GC removed or changed the large proof referenced by HEAD")
			}
			if err := fixture.core.ValidateCheckpointBase(fixture.ctx, seal, decision); err != nil {
				t.Fatalf("validate reloaded recovery proof: %v", err)
			}

			restoreWAL, err := qlog.Open(filepath.Join(t.TempDir(), "restore-wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer restoreWAL.Close()
			restoredCore, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "recovery-observer", Cluster: fixture.bootstrap, WAL: restoreWAL, EnableReconfiguration: true})
			if err != nil {
				t.Fatal(err)
			}
			restoredCore.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
				return fixture.checkpoints.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
			})
			if err := restoredCore.RestoreCheckpointBase(fixture.ctx, seal, decision); err != nil {
				t.Fatalf("restore membership checkpoint base: %v", err)
			}

			root, err := fixture.checkpoints.OpenRoot(fixture.ctx, uint64(seal.Index), seal.RootHash)
			if err != nil {
				t.Fatal(err)
			}
			files, err := fixture.checkpoints.DownloadAndVerifyRootFiles(fixture.ctx, root, t.TempDir())
			if err != nil {
				t.Fatalf("download actual checkpoint files: %v", err)
			}
			restoredState, err := materializer.Open(filepath.Join(t.TempDir(), "restored.db"), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer restoredState.Close()
			restoreFiles := make([]materializer.CheckpointFile, 0, len(files))
			for _, file := range files {
				restoreFiles = append(restoreFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
			}
			if err := restoredState.RestoreCheckpoint(fixture.ctx, restoreFiles); err != nil {
				t.Fatalf("restore actual checkpoint files: %v", err)
			}
			if _, found, err := restoredState.GraphMutationReceipt(fixture.ctx, "large-proof-checkpoint"); err != nil || !found {
				t.Fatalf("restored graph mutation receipt found=%v err=%v", found, err)
			}

			if err := Seal(fixture.ctx, fixture.bucket, fixture.prefix, "large-proof-fork"); err != nil {
				t.Fatalf("seal source archive: %v", err)
			}
			targetMembers := []quepaxa.Member{{ID: "target", PublicKey: testPublicKey("large-proof-target")}}
			targetMembership := NewMembershipRecord("large-proof-target", targetMembers, "async")
			if _, err := Fork(fixture.ctx, fixture.bucket, ForkOptions{
				SourcePrefix: fixture.prefix, TargetPrefix: "large-proof-target", SourceBootstrap: fixture.bootstrap,
				TargetMembers: targetMembers, TargetMembership: targetMembership, OperationID: "large-proof-fork",
			}); err != nil {
				t.Fatalf("fork from sealed large-proof source: %v", err)
			}
			targetArchive := NewManager(fixture.bucket, "large-proof-target", 1)
			defer targetArchive.Close()
			if err := targetArchive.Load(fixture.ctx); err != nil {
				t.Fatalf("load forked archive: %v", err)
			}
			anchor, anchorHash, err := ReadGenerationAnchor(fixture.ctx, fixture.bucket, "large-proof-target")
			if err != nil {
				t.Fatalf("read forked generation anchor: %v", err)
			}
			if got, ok := targetArchive.GenerationAnchor(); !ok || got != anchorHash {
				t.Fatal("forked archive lost its generation anchor")
			}
			targetCheckpoints := checkpoint.NewManager(fixture.bucket, "large-proof-target", "", 1)
			targetRootHash := rootHashForTest(t, anchor.Checkpoint.RootHash)
			targetRoot, err := targetCheckpoints.OpenRoot(fixture.ctx, anchor.Checkpoint.Index, targetRootHash)
			if err != nil {
				t.Fatalf("open forked checkpoint root: %v", err)
			}
			targetFiles, err := targetCheckpoints.DownloadAndVerifyRootFiles(fixture.ctx, targetRoot, t.TempDir())
			if err != nil {
				t.Fatalf("verify forked checkpoint files: %v", err)
			}
			targetState, err := materializer.Open(filepath.Join(t.TempDir(), "forked.db"), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer targetState.Close()
			targetRestoreFiles := make([]materializer.CheckpointFile, 0, len(targetFiles))
			for _, file := range targetFiles {
				targetRestoreFiles = append(targetRestoreFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
			}
			if err := targetState.RestoreCheckpoint(fixture.ctx, targetRestoreFiles); err != nil {
				t.Fatalf("restore forked checkpoint files: %v", err)
			}
			if _, found, err := targetState.GraphMutationReceipt(fixture.ctx, "large-proof-checkpoint"); err != nil || !found {
				t.Fatalf("forked graph receipt found=%v err=%v", found, err)
			}
		})
	}
}

func BenchmarkAuthenticatedMembershipCheckpointProof(b *testing.B) {
	for _, tc := range []struct {
		name        string
		urlBytes    int
		transitions bool
	}{
		{name: "small-head", urlBytes: 512},
		{name: "duplicate-head-boundary", urlBytes: 34 << 10},
		{name: "near-max-decision-value", urlBytes: quepaxa.MaxReplicatedValueBytes - (4 << 10)},
		{name: "real-large-history", urlBytes: 13 << 10, transitions: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			fixture := newLargeProofFixture(b, tc.urlBytes, "large-proof-bench-"+tc.name, tc.transitions)
			if len(fixture.sealBytes) > quepaxa.MaxReplicatedValueBytes {
				b.Fatalf("authenticated seal=%d exceeds decision value cap %d", len(fixture.sealBytes), quepaxa.MaxReplicatedValueBytes)
			}
			if tc.name == "near-max-decision-value" && len(fixture.sealBytes) < quepaxa.MaxReplicatedValueBytes-(16<<10) {
				b.Fatalf("seal=%d is not near max decision value size %d", len(fixture.sealBytes), quepaxa.MaxReplicatedValueBytes)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.sealBytes)))
			b.ResetTimer()
			for range b.N {
				if err := fixture.core.ValidateCheckpointBase(fixture.ctx, fixture.seal, fixture.decision); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(fixture.sealBytes)), "seal-B")
			b.ReportMetric(float64(fixture.headBytes), "head-B")
			if fixture.seal.Membership == nil || len(fixture.decision.Certificate) == 0 || sha256.Sum256(fixture.decision.Value) != fixture.decision.Hash {
				b.Fatal("benchmark fixture lacks real membership or certified decision")
			}
		})
	}
}
