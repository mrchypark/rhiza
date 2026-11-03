package node

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// Actual three-Core certificates and actual paired roots. No newer hint is
// accepted solely because its cut is larger, and no current pointer is rolled back.
func TestManagedGraphStalePromotionAuthenticatesNewerLocator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	peers := graphReviewCores(t, false)
	core := peers["n1"]
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	bucket := objstore.NewInMemBucket()
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, "stale", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "stale", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	for _, peer := range peers {
		peer.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return cp.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
	}
	makeSeal := func(id string) quepaxa.SealedCheckpoint {
		t.Helper()
		value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: id, Cypher: "CREATE (:StalePromotion {n:1})"})
		if err != nil {
			t.Fatal(err)
		}
		slot, _, err := core.Propose(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		if err := mat.Apply(ctx, uint64(slot), value); err != nil {
			t.Fatal(err)
		}
		files, cut, release, err := mat.CheckpointFilesAt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		sources := make([]checkpoint.Source, len(files))
		for i, f := range files {
			sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
		}
		root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
		if err != nil {
			t.Fatal(err)
		}
		prefix, ok := core.PrefixHash(slot)
		if !ok {
			t.Fatal("actual prefix absent")
		}
		next, following, err := core.CheckpointLeaderOrders(slot)
		if err != nil {
			t.Fatal(err)
		}
		seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: slot, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
		for _, peer := range peers {
			if err := peer.PrepareCheckpoint(ctx, seal); err != nil {
				t.Fatal(err)
			}
		}
		encoded, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			t.Fatal(err)
		}
		sealSlot, _, err := core.Propose(ctx, encoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := mat.Apply(ctx, uint64(sealSlot), encoded); err != nil {
			t.Fatal(err)
		}
		sealed, exists, err := core.LatestCheckpointSeal()
		if err != nil || !exists {
			t.Fatal(err)
		}
		return sealed
	}
	old := makeSeal("old")
	newer := makeSeal("new")
	n := New(&types.ExecutionConfig{})
	n.core, n.checkpoints, n.archive = core, cp, archive
	if err := n.promoteManagedGraphSeal(ctx, newer); err != nil {
		t.Fatal(err)
	}
	want, found, err := cp.LoadManagedGraphLocator(ctx)
	if err != nil || !found || want.Index <= uint64(old.Index) {
		t.Fatal("actual newer locator missing", err)
	}
	if err := n.promoteManagedGraphSeal(ctx, old); err != nil {
		t.Fatal("valid monotonic locator advancement blocked old captured startup", err)
	}
	got, found, err := cp.LoadManagedGraphLocator(ctx)
	if err != nil || !found || got != want {
		t.Fatal("stale promotion rolled back current", err)
	}
	// Keep the newer index, but invalidate its exact archived seal identity.
	bad := want
	bad.SealDecisionHash[0] ^= 1
	raw, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, "stale/checkpoint/CURRENT", bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	if err := n.promoteManagedGraphSeal(ctx, old); err == nil {
		t.Fatal("larger unauthenticated CURRENT was accepted")
	}
	// An ordinary same-cut conflict is not the monotonic higher-cut case.
	bad.Index = uint64(old.Index)
	raw, err = json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, "stale/checkpoint/CURRENT", bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	if err := n.promoteManagedGraphSeal(ctx, old); err == nil {
		t.Fatal("same-cut conflicting hint was accepted")
	}
}
