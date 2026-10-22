package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
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

type frozenSourceWriteTrackingBucket struct {
	objstore.Bucket
	prefix string
	writes []string
}

func (b *frozenSourceWriteTrackingBucket) Upload(ctx context.Context, name string, body io.Reader, opts ...objstore.ObjectUploadOption) error {
	if strings.HasPrefix(name, b.prefix+"/") {
		b.writes = append(b.writes, "upload:"+name)
		return fmt.Errorf("frozen source write attempted")
	}
	return b.Bucket.Upload(ctx, name, body, opts...)
}

func (b *frozenSourceWriteTrackingBucket) Delete(ctx context.Context, name string) error {
	if strings.HasPrefix(name, b.prefix+"/") {
		b.writes = append(b.writes, "delete:"+name)
		return fmt.Errorf("frozen source delete attempted")
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *frozenSourceWriteTrackingBucket) DeleteVersion(ctx context.Context, name string, version objstore.ObjectVersion) error {
	if strings.HasPrefix(name, b.prefix+"/") {
		b.writes = append(b.writes, "delete-version:"+name)
		return fmt.Errorf("frozen source versioned delete attempted")
	}
	deleter, ok := b.Bucket.(interface {
		DeleteVersion(context.Context, string, objstore.ObjectVersion) error
	})
	if !ok {
		return fmt.Errorf("underlying bucket does not support versioned delete")
	}
	return deleter.DeleteVersion(ctx, name, version)
}

func TestForkFrozenSourceCopiesWithoutSourceWritesAndBindsEnvironment(t *testing.T) {
	ctx, bucket, _, archive := newSealableArchive(t)
	defer archive.Close()

	tracked := &frozenSourceWriteTrackingBucket{Bucket: bucket, prefix: "cluster"}
	options := frozenForkTestOptions(t)
	var environmentChecks int
	options.VerifyEnvironment = func(_ context.Context, want FrozenBucketEnvironment) error {
		environmentChecks++
		if want != options.BucketEnvironment {
			return fmt.Errorf("unexpected bucket environment binding")
		}
		return nil
	}
	before, err := bucket.Attributes(ctx, "cluster/archive/head.bin")
	if err != nil || before.Version == nil {
		t.Fatalf("read source HEAD version: version=%v err=%v", before.Version, err)
	}

	result, err := ForkFrozenSource(ctx, tracked, options)
	if err != nil {
		t.Fatalf("frozen fork: %v", err)
	}
	if len(tracked.writes) != 0 {
		t.Fatalf("frozen fork mutated source: %v", tracked.writes)
	}
	if environmentChecks < 3 {
		t.Fatalf("bucket environment was not checked before, during, and after materialization: checks=%d", environmentChecks)
	}
	if result.Source.Frontier.BucketMetageneration != options.BucketEnvironment.Metageneration || result.Source.Frontier.LifecyclePolicyDigest != options.BucketEnvironment.LifecycleDigest {
		t.Fatalf("frozen frontier omitted retention environment: %+v", result.Source.Frontier)
	}
	after, err := bucket.Attributes(ctx, "cluster/archive/head.bin")
	if err != nil || after.Version == nil || *after.Version != *before.Version {
		t.Fatalf("source HEAD version changed: before=%v after=%v err=%v", before.Version, after.Version, err)
	}

	checksBeforeRetry := environmentChecks
	retry, err := ForkFrozenSource(ctx, tracked, options)
	if err != nil || !sameForkResult(retry.ForkResult, result.ForkResult) || !sameFrozenSourceBinding(retry.Source, result.Source) {
		t.Fatalf("frozen fork retry result=%+v err=%v", retry, err)
	}
	if environmentChecks <= checksBeforeRetry {
		t.Fatal("cached frozen fork did not revalidate bucket environment")
	}
	competing := options
	competing.OperationID = "different-operation"
	if _, err := ForkFrozenSource(ctx, tracked, competing); err == nil {
		t.Fatal("different operation ID selected a second target for the same source identity")
	}
	changedEnvironment := options
	changedEnvironment.BucketEnvironment.Metageneration = "8"
	changedEnvironment.VerifyEnvironment = func(context.Context, FrozenBucketEnvironment) error { return nil }
	if _, err := ForkFrozenSource(ctx, tracked, changedEnvironment); err == nil {
		t.Fatal("changed bucket metageneration reopened the frozen target")
	}
}

func TestForkFrozenSourceRejectsCachedResultDetachedFromFrontier(t *testing.T) {
	for _, field := range []string{"tip", "manifest"} {
		t.Run(field, func(t *testing.T) {
			ctx, bucket, _, archive := newSealableArchive(t)
			defer archive.Close()
			options := frozenForkTestOptions(t)
			if _, err := ForkFrozenSource(ctx, bucket, options); err != nil {
				t.Fatal(err)
			}
			result, ok, err := readForkResult(ctx, bucket, options.TargetPrefix)
			if err != nil || !ok {
				t.Fatalf("read frozen fork result: ok=%v err=%v", ok, err)
			}
			switch field {
			case "tip":
				result.Tip++
			case "manifest":
				result.ManifestHash = strings.Repeat("e", 64)
			}
			data, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if err := bucket.Upload(ctx, forkResultKey(options.TargetPrefix), bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			if _, err := ForkFrozenSource(ctx, bucket, options); err == nil {
				t.Fatalf("cached frozen result with detached %s was accepted", field)
			}
		})
	}
}

func TestFrozenGenerationAnchorBindsSourceTipAndManifestToFrontier(t *testing.T) {
	for _, field := range []string{"tip", "manifest"} {
		t.Run(field, func(t *testing.T) {
			ctx, bucket, _, archive := newSealableArchive(t)
			defer archive.Close()
			options := frozenForkTestOptions(t)
			if _, err := ForkFrozenSource(ctx, bucket, options); err != nil {
				t.Fatal(err)
			}
			anchor, _, err := ReadGenerationAnchor(ctx, bucket, options.TargetPrefix)
			if err != nil {
				t.Fatal(err)
			}
			switch field {
			case "tip":
				anchor.SourceTip++
			case "manifest":
				anchor.SourceManifest = strings.Repeat("e", 64)
			}
			data, err := json.Marshal(anchor)
			if err != nil {
				t.Fatal(err)
			}
			if err := bucket.Upload(ctx, GenerationAnchorKey(options.TargetPrefix), bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(data)
			if _, err := VerifyGenerationAnchor(ctx, bucket, options.TargetPrefix, options.TargetMembership, hash); err == nil {
				t.Fatalf("generation anchor with detached source %s was accepted", field)
			}
		})
	}
}

func TestForkFrozenSourceFailsWhenCapturedExtentIsMissingOrChanged(t *testing.T) {
	for _, mutation := range []string{"missing", "changed"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, bucket, _, archive := newSealableArchive(t)
			defer archive.Close()
			options := frozenForkTestOptions(t)
			result, err := ForkFrozenSource(ctx, bucket, options)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Source.Frontier.Extents) == 0 {
				t.Fatal("fixture has no immutable source extent")
			}
			extent := result.Source.Frontier.Extents[0]
			name := path.Join(options.SourcePrefix, extentObjectKeyFromFrozen(extent))
			switch mutation {
			case "missing":
				if err := bucket.Delete(ctx, name); err != nil {
					t.Fatal(err)
				}
			case "changed":
				if err := bucket.Upload(ctx, name, bytes.NewReader([]byte("not the captured extent"))); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ForkFrozenSource(ctx, bucket, options); err == nil {
				t.Fatalf("cached frozen fork accepted %s captured extent", mutation)
			}
		})
	}
}

func TestForkFrozenSourceFailsWhenReferencedCheckpointIsMissing(t *testing.T) {
	ctx, bucket, options := newCheckpointedFrozenSource(t)
	manager := NewManager(bucket, options.SourcePrefix, ArchiveConfigID)
	frontier, _, _, err := captureFrozenSource(ctx, manager)
	manager.Close()
	if err != nil || frontier.Checkpoint == nil {
		t.Fatalf("capture checkpointed source: checkpoint=%+v err=%v", frontier.Checkpoint, err)
	}
	checkpointKey := path.Join(options.SourcePrefix, fmt.Sprintf("checkpoint/roots/%020d_%s.json", frontier.Checkpoint.Index, frontier.Checkpoint.RootHash))
	if err := bucket.Delete(ctx, checkpointKey); err != nil {
		t.Fatal(err)
	}
	if _, err := ForkFrozenSource(ctx, bucket, options); err == nil {
		t.Fatal("frozen fork accepted a missing checkpoint referenced by the source frontier")
	}
}

func TestForkFrozenSourceCopiesCheckpointedMaterializationWithoutSourceWrites(t *testing.T) {
	ctx, bucket, options := newCheckpointedFrozenSource(t)
	tracked := &frozenSourceWriteTrackingBucket{Bucket: bucket, prefix: options.SourcePrefix}
	result, err := ForkFrozenSource(ctx, tracked, options)
	if err != nil {
		t.Fatalf("frozen fork from checkpointed source: %v", err)
	}
	if len(tracked.writes) != 0 {
		t.Fatalf("frozen fork mutated source objects: %v", tracked.writes)
	}
	if result.Source.Frontier.Checkpoint == nil {
		t.Fatal("frozen result omitted the source checkpoint reference")
	}
	anchor, anchorHash, err := ReadGenerationAnchor(ctx, bucket, options.TargetPrefix)
	if err != nil || anchor.SourceTip != result.Tip || anchor.SourceManifest != result.ManifestHash {
		t.Fatalf("target anchor=%+v err=%v", anchor, err)
	}
	if _, err := VerifyGenerationAnchor(ctx, bucket, options.TargetPrefix, options.TargetMembership, anchorHash); err != nil {
		t.Fatalf("verify frozen target anchor: %v", err)
	}
	target := NewManager(bucket, options.TargetPrefix, ArchiveConfigID)
	if err := target.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if target.Tip() != quepaxa.Slot(result.Tip) {
		t.Fatalf("target archive tip=%d want=%d", target.Tip(), result.Tip)
	}
	target.Close()
	targetCP := checkpoint.NewManager(bucket, options.TargetPrefix, "", ArchiveConfigID)
	root, err := targetCP.OpenRoot(ctx, anchor.Checkpoint.Index, rootHashForTest(t, anchor.Checkpoint.RootHash))
	if err != nil {
		t.Fatalf("open target materialization checkpoint: %v", err)
	}
	dir := t.TempDir()
	files, err := targetCP.DownloadAndVerifyRootFiles(ctx, root, dir)
	if err != nil {
		t.Fatalf("verify target checkpoint files: %v", err)
	}
	restored, err := materializer.Open(filepath.Join(dir, "restored.sqlite"), int(ArchiveConfigID))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	checkpointFiles := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		checkpointFiles = append(checkpointFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if err := restored.RestoreCheckpoint(ctx, checkpointFiles); err != nil {
		t.Fatalf("restore target materialization checkpoint: %v", err)
	}
	if _, found, err := restored.GraphMutationReceipt(ctx, "frozen-checkpoint"); err != nil || !found {
		t.Fatalf("target did not preserve the checkpointed graph mutation receipt: found=%v err=%v", found, err)
	}
}

func newCheckpointedFrozenSource(t *testing.T) (context.Context, objstore.Bucket, FrozenForkOptions) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	members := []quepaxa.Member{{ID: "n1", PublicKey: testPublicKey("n1")}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: ArchiveConfigID, Members: members}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "frozen-checkpoint", Events: []types.GraphStreamEvent{{Stream: "frozen", Kind: "created", Payload: "checkpointed state"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(core.Tip())
	if !ok {
		t.Fatal("missing source decision")
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "source.sqlite"), int(ArchiveConfigID))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	cp := checkpoint.NewManager(bucket, "cluster", "", ArchiveConfigID)
	claim, err := cp.AcquirePublisherClaim(ctx, "frozen-checkpoint", 0, time.Minute)
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
	root, err := cp.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = cp.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(core.Tip())
	next, following, _ := core.CheckpointLeaderOrders(core.Tip())
	seal := quepaxa.CheckpointSeal{ConfigID: ArchiveConfigID, Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, encoded)
	if err != nil {
		t.Fatal(err)
	}
	sealDecision, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing certified checkpoint decision")
	}
	archive := NewManager(bucket, "cluster", ArchiveConfigID)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := archive.TrimThrough(ctx, quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: slot}, sealDecision); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "cluster", "frozen-checkpoint"); err != nil {
		t.Fatal(err)
	}
	archive.Close()
	if err := cp.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	return ctx, bucket, frozenForkTestOptions(t)
}

func extentObjectKeyFromFrozen(extent FrozenExtent) string {
	return fmt.Sprintf("archive/blocks/%s_%020d.bin", extent.Hash, extent.ObjectID)
}

func TestForkFrozenSourceArbitratesConcurrentOperationsForOneStableTarget(t *testing.T) {
	ctx, bucket, _, archive := newSealableArchive(t)
	defer archive.Close()
	base := frozenForkTestOptions(t)
	options := []FrozenForkOptions{base, base}
	options[0].OperationID = "concurrent-a"
	options[1].OperationID = "concurrent-b"
	start := make(chan struct{})
	type outcome struct {
		result FrozenForkResult
		err    error
	}
	results := make(chan outcome, len(options))
	for _, option := range options {
		option := option
		go func() {
			<-start
			result, err := ForkFrozenSource(ctx, bucket, option)
			results <- outcome{result: result, err: err}
		}()
	}
	close(start)
	succeeded := 0
	for range options {
		if result := <-results; result.err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("concurrent competing operations succeeded=%d, want exactly one", succeeded)
	}
}

func TestForkFrozenSourceStopsWhenBucketEnvironmentChangesDuringCopy(t *testing.T) {
	ctx, bucket, _, archive := newSealableArchive(t)
	defer archive.Close()

	options := frozenForkTestOptions(t)
	var checks int
	options.VerifyEnvironment = func(_ context.Context, want FrozenBucketEnvironment) error {
		checks++
		if want != options.BucketEnvironment {
			return fmt.Errorf("unexpected bucket environment binding")
		}
		if checks == 2 {
			return errors.New("bucket lifecycle metadata changed")
		}
		return nil
	}
	_, err := ForkFrozenSource(ctx, bucket, options)
	if err == nil {
		t.Fatal("frozen fork succeeded after its bucket environment changed")
	}
	if checks != 2 {
		t.Fatalf("environment recheck count=%d, want preflight and pre-anchor", checks)
	}
	if _, present, readErr := readForkResult(ctx, bucket, options.TargetPrefix); readErr != nil || present {
		t.Fatalf("failed frozen copy published a result: present=%v err=%v", present, readErr)
	}
}

func TestFrozenTargetIDUsesOnlyStableSourceIdentity(t *testing.T) {
	identity := FrozenSourceIdentity{ScopeHash: strings.Repeat("a", 64), ClusterID: "source", StatefulSetUID: "uid-1"}
	first, err := FrozenTargetID(identity)
	if err != nil {
		t.Fatal(err)
	}
	again, err := FrozenTargetID(identity)
	if err != nil || first != again {
		t.Fatalf("stable identity target IDs differ: first=%q again=%q err=%v", first, again, err)
	}
	identity.StatefulSetUID = "uid-2"
	changed, err := FrozenTargetID(identity)
	if err != nil || first == changed {
		t.Fatalf("new source incarnation reused target ID %q: changed=%q err=%v", first, changed, err)
	}
}

func TestFrozenFrontierRejectsSameBytesAtNewObjectVersion(t *testing.T) {
	ctx, bucket, _, archive := newSealableArchive(t)
	defer archive.Close()

	manager := NewManager(bucket, "cluster", ArchiveConfigID)
	frontier, _, headBytes, err := captureFrozenSource(ctx, manager)
	if err != nil {
		t.Fatal(err)
	}
	manager.Close()
	frontier.BucketMetageneration = "7"
	frontier.LifecyclePolicyDigest = strings.Repeat("d", 64)
	attributes, err := bucket.Attributes(ctx, "cluster/archive/head.bin")
	if err != nil || attributes.Version == nil {
		t.Fatalf("source HEAD attributes: version=%v err=%v", attributes.Version, err)
	}
	if err := bucket.Upload(ctx, "cluster/archive/head.bin", bytes.NewReader(headBytes), objstore.WithIfMatch(attributes.Version)); err != nil {
		t.Fatal(err)
	}
	if err := verifyFrozenFrontier(ctx, bucket, "cluster", frontier, headBytes); err == nil {
		t.Fatal("same source bytes at a new object version matched the frozen frontier")
	}
}

func TestForkFrozenSourceRequiresVerifiedBucketEnvironment(t *testing.T) {
	identity := FrozenSourceIdentity{ScopeHash: strings.Repeat("a", 64), ClusterID: "source", StatefulSetUID: "uid-1"}
	targetID, err := FrozenTargetID(identity)
	if err != nil {
		t.Fatal(err)
	}
	options := FrozenForkOptions{
		ForkOptions: ForkOptions{
			SourcePrefix: "source", TargetPrefix: path.Join(path.Dir("source"), targetID),
			SourceBootstrap:  quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}},
			TargetMembers:    []quepaxa.Member{{ID: "n2", PublicKey: testPublicKey("n2")}},
			TargetMembership: NewMembershipRecord(targetID, []quepaxa.Member{{ID: "n2", PublicKey: testPublicKey("n2")}}, "async"),
			OperationID:      "op-1",
		},
		SourceIdentity: identity, FenceProofDigest: strings.Repeat("b", 64), ClosedRootDigest: strings.Repeat("c", 64),
	}
	if _, err := ForkFrozenSource(context.Background(), objstore.NewInMemBucket(), options); err == nil {
		t.Fatal("frozen fork accepted without a verified bucket lifecycle binding")
	}
}

func frozenForkTestOptions(t *testing.T) FrozenForkOptions {
	t.Helper()
	identity := FrozenSourceIdentity{ScopeHash: strings.Repeat("a", 64), ClusterID: "cluster", StatefulSetUID: "uid-1"}
	targetID, err := FrozenTargetID(identity)
	if err != nil {
		t.Fatal(err)
	}
	targetMembers := []quepaxa.Member{{ID: "n2", PublicKey: testPublicKey("n2")}}
	return FrozenForkOptions{
		ForkOptions: ForkOptions{
			SourcePrefix: "cluster", TargetPrefix: path.Join(path.Dir("cluster"), targetID),
			SourceBootstrap:  quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}},
			TargetMembers:    targetMembers,
			TargetMembership: NewMembershipRecord(targetID, targetMembers, "async"),
			OperationID:      "recovery-op-1",
		},
		SourceIdentity: identity, FenceProofDigest: strings.Repeat("b", 64), ClosedRootDigest: strings.Repeat("c", 64),
		BucketEnvironment: FrozenBucketEnvironment{Metageneration: "7", LifecycleDigest: strings.Repeat("d", 64)},
	}
}
