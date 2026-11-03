package managedbootstrap

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// Actual native pair + certified >128KiB suffix. Reaching the independent
// REMOVE refusal proves only this verifier boundary, not successful repair.
func TestManagedGraphRepairControlRetainsTrustedLargeValueBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "graph-repair"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "graph-repair", Cluster: cluster, WAL: wal, EnableReconfiguration: true, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	propose := func(value []byte) quepaxa.DecidedValue {
		t.Helper()
		slot, _, err := core.Propose(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := core.CertifiedValue(slot)
		if !ok || len(d.Certificate) == 0 {
			t.Fatal("actual certificate absent")
		}
		if err := mat.ApplyBatch(ctx, []quepaxa.DecidedValue{d}); err != nil {
			t.Fatal(err)
		}
		return d
	}
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "base", Cypher: "CREATE (:GraphRepair {id:'base'})"})
	if err != nil {
		t.Fatal(err)
	}
	propose(value)
	files, cut, release, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	bucket := objstore.NewInMemBucket()
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, "graph-repair", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]checkpoint.Source, len(files))
	for i, f := range files {
		sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
	}
	root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(core.Tip())
	next, following, err := core.CheckpointLeaderOrders(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(core.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	core.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	decision := propose(encoded)
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "graph-repair", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := cp.PromoteManagedGraphLocator(ctx, checkpoint.ManagedGraphLocator{Policy: 10, Index: uint64(seal.Index), RootHash: root.RootHash, SealDecisionSlot: uint64(decision.Slot), SealDecisionHash: decision.Hash}); err != nil {
		t.Fatal(err)
	}
	args := make([]any, 21800)
	for i := range args {
		args[i] = float64(.25)
	}
	value, err = types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "large-suffix", Cypher: "CREATE (:GraphRepair {id:'suffix'})", Args: map[string]any{"values": args}})
	if err != nil {
		t.Fatal(err)
	}
	if len(value) <= quepaxa.MaxReplicatedValueBytes || len(value) > types.MaxManagedGraphExecutionBytes {
		t.Fatal("fixture not profile10-only value", len(value))
	}
	propose(value)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyViewForPolicyWithReconfiguration(ctx, bucket, "graph-repair", 1, cluster, sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1024, true)
	if err != nil {
		t.Fatal(err)
	}
	capture := operator.V1Alpha2RepairHandoff{SourcePrefix: "graph-repair", HeadHash: fmt.Sprintf("%x", view.HeadHash()), Survivors: cluster}
	err = VerifyRepairControl(ctx, bucket, cluster, view, capture)
	if err == nil || !strings.Contains(err.Error(), "fixed source does not prove captured REMOVE target") {
		t.Fatalf("Graph suffix failed before independent REMOVE authority gate: bytes=%d error=%v", len(value), err)
	}
	legacyWal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer legacyWal.Close()
	legacy, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "legacy-verifier", Cluster: cluster, WAL: legacyWal, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	legacy.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
	})
	if err := legacy.RestoreCheckpointBase(ctx, seal, decision); err != nil {
		t.Fatal(err)
	}
	if err := legacy.AcceptCertifiedValues(view.Decisions()); err == nil {
		t.Fatal("legacy observer silently accepted larger Graph suffix")
	}
	t.Logf("actual paired cut=%d certified Graph suffix bytes=%d tip=%d; trusted verifier reached unchanged REMOVE refusal; legacy rejected", cut, len(value), core.Tip())
}
