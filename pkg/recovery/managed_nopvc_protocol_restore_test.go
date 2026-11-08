package recovery

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestManagedNoPVCGraphProtocolOpenPublishLoadReconstructs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const protocol = sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	const window = types.DefaultIdempotencyWindowSlots
	bucket := objstore.NewInMemBucket()
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "graph-nopvc", PublicKey: testPublicKey("graph-nopvc")}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "graph-nopvc", Cluster: bootstrap, WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	archive, err := NewAppendOnlyManagerWithValueValidator(bucket, "graph-nopvc", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, "graph-nopvc", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cp.Load(ctx); err != nil {
		t.Fatal(err)
	}
	producer, err := materializer.OpenManagedSQLKVGraphWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "producer.sqlite"), 1, window)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = producer.Close() })
	baseValue, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "graph-base", Cypher: "CREATE (:NoPVCGraph {id:'base'})", Events: []types.GraphStreamEvent{{Stream: "graph-nopvc", Kind: "created", Payload: "base"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, baseValue); err != nil {
		t.Fatal("certify graph base", err)
	}
	if err := producer.Apply(ctx, 1, baseValue); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	files, cut, cleanup, err := producer.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	prefixHash, ok := core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("base prefix missing")
	}
	next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefixHash, NextLeaderOrder: next, FollowingLeaderOrder: following}
	core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal("prepare actual pair", err)
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	sealSlot, _, err := core.Propose(ctx, sealValue)
	if err != nil {
		t.Fatal("certify actual pair", err)
	}
	sealDecision, ok := core.CertifiedValue(sealSlot)
	if !ok {
		t.Fatal("certified pair decision unavailable")
	}
	if err := producer.ApplyBatch(ctx, []quepaxa.DecidedValue{sealDecision}); err != nil {
		t.Fatal("apply certified control between Graph decisions", err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("certified pair missing: %v", err)
	}
	decision, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("pair decision certificate missing")
	}
	locator := checkpoint.ManagedGraphLocator{Policy: int(sqlpolicy.ManagedGraphRequestMaterializationPolicy), Index: uint64(sealed.Index), RootHash: sealed.RootHash, SealDecisionSlot: uint64(decision.Slot), SealDecisionHash: decision.Hash}
	if err := cp.PromoteManagedGraphLocator(ctx, locator); err != nil {
		t.Fatal(err)
	}
	suffix, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "graph-suffix", Cypher: "CREATE (:NoPVCGraph {id:'suffix'})", Events: []types.GraphStreamEvent{{Stream: "graph-nopvc", Kind: "created", Payload: "suffix"}}})
	if err != nil {
		t.Fatal(err)
	}
	suffixSlot, _, err := core.Propose(ctx, suffix)
	if err != nil {
		t.Fatal("certify graph suffix", err)
	}
	suffixDecision, ok := core.CertifiedValue(suffixSlot)
	if !ok {
		t.Fatal("certified Graph suffix unavailable")
	}
	if err := producer.ApplyBatch(ctx, []quepaxa.DecidedValue{suffixDecision}); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	view, err := OpenReadOnlyViewForApplicationProtocol(ctx, bucket, "graph-nopvc", 1, bootstrap, sqlpolicy.ManagedGraphRequestMaterializationPolicy, window, protocol, false)
	if err != nil {
		t.Fatal("verify actual Graph pair and suffix", err)
	}
	if view.Base() != quepaxa.Slot(cut) || view.Tip() != core.Tip() || len(view.Decisions()) == 0 {
		t.Fatalf("Graph view lost pair/suffix: base=%d tip=%d decisions=%d", view.Base(), view.Tip(), len(view.Decisions()))
	}
	identity := mixedPreparedIdentity()
	identity.Generation = 0
	handoff, target := filepath.Join(t.TempDir(), "handoff"), filepath.Join(t.TempDir(), "reconstructed")
	if err := PublishPreparedStartupForPolicy(ctx, view, handoff, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, window); err != nil {
		t.Fatal("publish prepared Graph pair", err)
	}
	verified, err := LoadPreparedStartupForApplicationProtocol(ctx, bucket, "graph-nopvc", 1, bootstrap, handoff, target, identity, sqlpolicy.ManagedGraphRequestMaterializationPolicy, window, protocol, false)
	if err != nil {
		t.Fatal("load and independently reconstruct Graph pair", err)
	}
	reopened, err := materializer.OpenManagedSQLKVGraphWithAppendOnlyOriginals(filepath.Join(verified.Directory(), "sqlite.db"), 1, window)
	if err != nil {
		t.Fatal("open reconstructed final-profile materializer", err)
	}
	defer reopened.Close()
	if reopened.Tip() != uint64(view.Tip()) {
		t.Fatalf("reconstructed tip=%d, want %d", reopened.Tip(), view.Tip())
	}
	for _, id := range []string{"graph-base", "graph-suffix"} {
		if _, found, err := reopened.RetainedRequestOriginal(ctx, types.MutationGraph, id); err != nil || !found {
			t.Fatalf("reconstructed retained original %q missing: found=%t err=%v", id, found, err)
		}
	}

	t.Run("published-only-excludes-unpublished-graph-tail", func(t *testing.T) {
		const policy = sqlpolicy.ManagedGraphRequestMaterializationPolicy
		const tail = "graph-unpublished"
		readonly := &haReadOnlyRecordingBucket{Bucket: bucket}
		before := requestViewBucketHashes(t, ctx, bucket)
		published, err := OpenReadOnlyViewForApplicationProtocol(ctx, readonly, "graph-nopvc", 1, bootstrap, policy, window, protocol, false)
		if err != nil {
			t.Fatal(err)
		}
		if published.Tip() != suffixSlot || published.Tip() != core.Tip() || published.Tip() != archive.Tip() {
			t.Fatalf("published tip=%d, want suffix=%d, core=%d, archive=%d", published.Tip(), suffixSlot, core.Tip(), archive.Tip())
		}
		value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: tail, Cypher: "CREATE (:NoPVCGraph {id:'unpublished'})"})
		if err != nil {
			t.Fatal(err)
		}
		slot, _, err := core.Propose(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := core.CertifiedValue(slot)
		if !ok || len(d.Certificate) == 0 || slot <= published.Tip() {
			t.Fatalf("missing later certified tail at slot %d", slot)
		}
		if err := core.EnsureDurableThrough(ctx, slot); err != nil {
			t.Fatal(err)
		}
		if err := producer.ApplyBatch(ctx, []quepaxa.DecidedValue{d}); err != nil {
			t.Fatal(err)
		}
		const query = "MATCH (n:NoPVCGraph) RETURN n.id ORDER BY n.id"
		if rows, err := producer.GraphQuery(ctx, query, nil); err != nil || !reflect.DeepEqual(rows.Rows, [][]any{{"base"}, {"suffix"}, {"unpublished"}}) {
			t.Fatalf("producer nodes=%v err=%v", rows.Rows, err)
		}
		if _, found, err := producer.RetainedRequestOriginal(ctx, types.MutationGraph, tail); err != nil || !found {
			t.Fatalf("producer tail original missing: found=%t err=%v", found, err)
		}
		restore, err := PrepareReadOnlyRestoreForPolicy(ctx, published, filepath.Join(t.TempDir(), "published-only"), policy, window)
		if err != nil {
			t.Fatal(err)
		}
		if restore.Tip != published.Tip() || restore.HeadHash != published.HeadHash() || restore.PrefixHash != published.PrefixHash() {
			t.Fatal("restore changed the pinned published cut")
		}
		// Inspect only the isolated materializer, without opening a voter.
		m, err := materializer.OpenManagedSQLKVGraphWithAppendOnlyOriginals(restore.SQLitePath, 1, window)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := m.Close(); err != nil {
				t.Error(err)
			}
		}()
		if rows, err := m.GraphQuery(ctx, query, nil); err != nil || !reflect.DeepEqual(rows.Rows, [][]any{{"base"}, {"suffix"}}) {
			t.Fatalf("restored nodes=%v err=%v", rows.Rows, err)
		}
		for _, id := range []string{"graph-base", "graph-suffix"} {
			want, found, err := producer.RetainedRequestOriginal(ctx, types.MutationGraph, id)
			if err != nil || !found {
				t.Fatalf("producer original %q missing: found=%t err=%v", id, found, err)
			}
			got, found, err := m.RetainedRequestOriginal(ctx, types.MutationGraph, id)
			if err != nil || !found || !reflect.DeepEqual(got, want) {
				t.Fatalf("restored original/receipt %q differs: found=%t err=%v", id, found, err)
			}
		}
		if _, found, err := m.RetainedRequestOriginal(ctx, types.MutationGraph, tail); err != nil || found {
			t.Fatalf("unpublished tail original: found=%t err=%v", found, err)
		}
		if readonly.mutations != 0 || !reflect.DeepEqual(before, requestViewBucketHashes(t, ctx, bucket)) {
			t.Fatal("source objects changed or read-only path attempted a mutation")
		}
	})
}

func TestManagedNoPVCRecoveryProtocolBoundariesAndLegacyFunctionTypes(t *testing.T) {
	var openLegacy func(context.Context, objstore.Bucket, string, uint, quepaxa.Cluster, sqlpolicy.MaterializationPolicy, uint64) (*ReadOnlyView, error) = OpenReadOnlyViewForPolicy
	var openHeadLegacy func(context.Context, objstore.Bucket, string, uint, quepaxa.Cluster, []byte, sqlpolicy.MaterializationPolicy, uint64) (*ReadOnlyView, error) = OpenReadOnlyViewAtHeadForPolicy
	var loadLegacy func(context.Context, objstore.Bucket, string, uint, quepaxa.Cluster, string, string, PreparedStartupIdentity, sqlpolicy.MaterializationPolicy, uint64) (*VerifiedPreparedStartup, error) = LoadPreparedStartupForPolicy
	if openLegacy == nil || openHeadLegacy == nil || loadLegacy == nil {
		t.Fatal("legacy recovery function signatures changed")
	}
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "member", PublicKey: testPublicKey("member")}}}
	identity := mixedPreparedIdentity()
	identity.Generation = 0
	for _, profile := range []struct {
		policy   sqlpolicy.MaterializationPolicy
		protocol string
		retired  string
	}{
		{sqlpolicy.ManagedRequestMaterializationPolicy, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestApplicationProtocol},
		{sqlpolicy.ManagedGraphRequestMaterializationPolicy, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestApplicationProtocol},
	} {
		t.Run(profile.protocol, func(t *testing.T) {
			for _, protocol := range []string{"", profile.protocol + "-wrong", profile.retired} {
				if _, err := OpenReadOnlyViewForApplicationProtocol(context.Background(), nil, "", 1, bootstrap, profile.policy, types.DefaultIdempotencyWindowSlots, protocol, true); err == nil {
					t.Errorf("Open accepted missing/wrong/retired protocol %q", protocol)
				}
				if _, err := OpenReadOnlyViewAtHeadForApplicationProtocol(context.Background(), nil, "", 1, bootstrap, []byte("head"), profile.policy, types.DefaultIdempotencyWindowSlots, protocol, true); err == nil {
					t.Errorf("AtHead accepted missing/wrong/retired protocol %q", protocol)
				}
				if _, err := LoadPreparedStartupForApplicationProtocol(context.Background(), nil, "", 1, bootstrap, "missing", "target", identity, profile.policy, types.DefaultIdempotencyWindowSlots, protocol, true); err == nil {
					t.Errorf("Load accepted missing/wrong/retired protocol %q", protocol)
				}
			}
			if _, err := OpenReadOnlyViewForPolicy(context.Background(), nil, "", 1, bootstrap, profile.policy, types.DefaultIdempotencyWindowSlots); err == nil {
				t.Error("protocol-less legacy Open accepted final profile")
			}
			if _, err := OpenReadOnlyViewAtHeadForPolicy(context.Background(), nil, "", 1, bootstrap, []byte("head"), profile.policy, types.DefaultIdempotencyWindowSlots); err == nil {
				t.Error("protocol-less legacy AtHead accepted final profile")
			}
			if _, err := LoadPreparedStartupForPolicy(context.Background(), nil, "", 1, bootstrap, "missing", "target", identity, profile.policy, types.DefaultIdempotencyWindowSlots); err == nil {
				t.Error("protocol-less legacy Load accepted final profile")
			}
			wrongGeneration := identity
			wrongGeneration.Generation = 1
			if _, err := LoadPreparedStartupForApplicationProtocol(context.Background(), nil, "", 1, bootstrap, "missing", "target", wrongGeneration, profile.policy, types.DefaultIdempotencyWindowSlots, profile.protocol, true); err == nil {
				t.Error("Load accepted nonzero final generation")
			}
			if err := validatePreparedProfile(wrongGeneration, profile.policy, types.DefaultIdempotencyWindowSlots, profile.protocol); err == nil {
				t.Error("prepared identity validator accepted nonzero final generation")
			}
		})
	}
	if err := validateReadOnlyProfile(sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol); err == nil {
		t.Fatal("recovery accepted cross-profile protocol mismatch")
	}
	if err := validateReadOnlyProfile(sqlpolicy.ManagedGraphRequestMaterializationPolicy, 2048, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol); err == nil {
		t.Fatal("recovery accepted trusted protocol with wrong window")
	}
}
