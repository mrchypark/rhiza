//go:build rhiza_local_testhooks

package node

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A retained install must validate its already-certified producer as a
// read-only root, not as a new voter-eligible proposal.
func TestManagedGraphAdmissionRetainedSealValidationAllowsNonvotingLearner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture, selected := newManagedGraphRetainedSealFixture(t, ctx)
	replaceReadCoreWithLearner(t, fixture)
	if fixture.node.core.IsVoter() || fixture.node.core.CanParticipateReadIndex() || fixture.node.Ready() {
		t.Fatalf("fixture promoted the retained learner: voter=%t read_index=%t ready=%t", fixture.node.core.IsVoter(), fixture.node.core.CanParticipateReadIndex(), fixture.node.Ready())
	}
	if err := fixture.node.validateManagedGraphSealForCut(ctx, fixture.node.core, selected); err != nil {
		t.Fatalf("nonvoting learner rejected the exact certified read-only producer: %v", err)
	}
	wrongProducer := selected
	wrongProducer.DecisionSlot++
	if err := fixture.node.validateManagedGraphSealForCut(ctx, fixture.node.core, wrongProducer); err == nil {
		t.Fatalf("wrong producer slot %d was accepted", wrongProducer.DecisionSlot)
	}
}

func newManagedGraphRetainedSealFixture(t *testing.T, ctx context.Context) (*managedGraphAppendOnlyAdmissionFixture, quepaxa.SealedCheckpoint) {
	t.Helper()
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	t.Cleanup(fixture.transport.releaseAll)
	fixture.transport.releaseAll()
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{
		RequestID: "retained-learner-source",
		Cypher:    "CREATE (:RetainedLearnerRegression {source:true})",
	})
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := fixture.node.core.Propose(ctx, value); err != nil || slot != 9 {
		t.Fatalf("certify actual Graph10 source value: slot=%d err=%v", slot, err)
	}
	if err := fixture.node.material.Apply(ctx, 9, value); err != nil {
		t.Fatal(err)
	}

	files, cut, releaseFiles, err := fixture.node.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cut != 9 || len(files) != 2 {
		releaseFiles()
		t.Fatalf("fixture did not produce the actual paired Graph10 checkpoint: cut=%d files=%+v", cut, files)
	}
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := fixture.node.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	releaseFiles()
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := fixture.node.core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual Graph10 source prefix is unavailable")
	}
	next, following, err := fixture.node.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash,
		StateHash: root.Hash, PrefixHash: prefix,
		NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
	for _, peer := range fixture.transport.graphReviewTransport {
		if err := peer.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatalf("prepare actual object-backed Graph10 checkpoint: %v", err)
		}
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := fixture.node.core.Propose(ctx, sealValue); err != nil || slot != 10 {
		t.Fatalf("certify actual Graph10 checkpoint producer: slot=%d err=%v", slot, err)
	}
	selected, found, err := fixture.node.core.LatestManagedCheckpointSeal()
	if err != nil || !found || selected.Index != seal.Index || selected.DecisionSlot != 10 {
		t.Fatalf("exact producer was not selected: seal=%+v found=%t err=%v", selected, found, err)
	}
	if err := fixture.node.checkpoints.Verify(ctx, uint64(selected.Index), selected.RootHash, selected.StateHash); err != nil {
		t.Fatalf("selected pair is not backed by actual checkpoint objects: %v", err)
	}
	prefixK, ok := fixture.node.core.PrefixHash(fixture.node.core.Tip())
	if !ok {
		t.Fatal("actual K prefix is unavailable")
	}
	retainedCut := materializer.ManagedGraphInstallCut{
		CheckpointSlot: uint64(selected.Index), CheckpointRoot: selected.RootHash,
		CheckpointStateHash: selected.StateHash, CheckpointPrefixHash: selected.PrefixHash,
		CertifiedTip: uint64(fixture.node.core.Tip()), CertifiedPrefixHash: prefixK,
	}
	retainedSeal, err := fixture.node.managedGraphSealForCut(ctx, fixture.node.core, retainedCut)
	if err != nil || retainedSeal.DecisionSlot != selected.DecisionSlot {
		t.Fatalf("retained journal did not resolve the exact certified producer: seal=%+v err=%v", retainedSeal, err)
	}
	return fixture, retainedSeal
}
