package recovery

import (
	"context"
	"path/filepath"
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

func TestManagedGraphLocatorUsesExactCertifiedSealWithoutTrimming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bucket := objstore.NewInMemBucket()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: cluster, WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
	if err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "first\xff", Cypher: "CREATE (:CheckpointCI {n:1})"})
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
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, "graph-locator", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(slot)
	next, following, err := core.CheckpointLeaderOrders(slot)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: slot, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
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
	sealSlot, _, err := core.Propose(ctx, encoded)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(sealSlot)
	if !ok || len(decision.Certificate) == 0 {
		t.Fatal("actual seal certificate missing")
	}
	tail, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "suffix", Cypher: "CREATE (:CheckpointCI {n:2})"})
	if err != nil {
		t.Fatal(err)
	}
	last, _, err := core.Propose(ctx, tail)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := NewAppendOnlyManagerWithValueValidator(bucket, "graph-locator", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if err := archive.SyncThrough(ctx, core, last); err != nil {
		t.Fatal(err)
	}
	l := checkpoint.ManagedGraphLocator{Policy: 10, Index: cut, RootHash: root.RootHash, SealDecisionSlot: uint64(sealSlot), SealDecisionHash: decision.Hash}
	if err := cp.PromoteManagedGraphLocator(ctx, l); err != nil {
		t.Fatal(err)
	}
	if err := archive.loadHead(ctx, make(map[extentObject]Extent), true); err != nil {
		t.Fatal(err)
	}
	snapshot := &RecoverySnapshot{manager: archive, head: archive.head, refs: archive.extents}
	if snapshot.head.Base != 0 || snapshot.Tip() != last || last <= sealSlot || uint64(seal.Index) >= uint64(sealSlot) {
		t.Fatal("fixture lacks untrimmed nonempty certified suffix")
	}
	got, gotDecision, err := VerifyManagedGraphLocator(ctx, snapshot, cp, core, l)
	if err != nil || got.RootHash != root.RootHash || gotDecision.Slot != sealSlot {
		t.Fatalf("valid locator: seal=%+v err=%v", got, err)
	}
	for _, change := range []func(*checkpoint.ManagedGraphLocator){
		func(v *checkpoint.ManagedGraphLocator) { v.Policy = 9 },
		func(v *checkpoint.ManagedGraphLocator) { v.SealDecisionHash[0] ^= 1 },
		func(v *checkpoint.ManagedGraphLocator) {
			v.SealDecisionSlot = uint64(last)
			d, _ := core.CertifiedValue(last)
			v.SealDecisionHash = d.Hash
		},
		func(v *checkpoint.ManagedGraphLocator) { v.RootHash[0] ^= 1 },
		func(v *checkpoint.ManagedGraphLocator) { v.SealDecisionSlot = uint64(last + 1) },
	} {
		bad := l
		change(&bad)
		if _, _, err := VerifyManagedGraphLocator(ctx, snapshot, cp, core, bad); err == nil {
			t.Fatalf("forged hint accepted: %+v", bad)
		}
	}
	if snapshot.head.Base != 0 || core.CompactionFloor() != 0 {
		t.Fatal("locator verification trimmed or installed a recovery base")
	}
}
