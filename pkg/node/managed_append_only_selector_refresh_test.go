package node

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestManagedAppendOnlyRootSelectorUsesRemoteCertificateWithStaleArchiveManager(t *testing.T) {
	ctx := context.Background()
	n, core, observerArchive, bucket := newManagedAppendOnlySelectorFixture(t)
	if observerArchive.Tip() != 1 || core.Tip() != 2 {
		t.Fatalf("fixture must leave observer archive behind the actual QCKP: archive=%d Core=%d", observerArchive.Tip(), core.Tip())
	}
	if _, exists, err := n.checkpoints.LoadManagedAppendOnlyLocator(ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy); err != nil || exists {
		t.Fatalf("fixture must have no CURRENT ranked above the local certificate: exists=%t err=%v", exists, err)
	}

	writerArchive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "readonly-selector", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(writerArchive.Close)
	writerArchive.SetGroupDelay(0)
	if err := writerArchive.Load(ctx); err != nil {
		t.Fatal("load independent writer archive manager", err)
	}
	if writerArchive.Tip() != 1 {
		t.Fatalf("writer manager did not load the same pre-QCKP prefix: got=%d want=1", writerArchive.Tip())
	}
	sealed, exists, err := core.LatestManagedCheckpointSeal()
	if err != nil || !exists || sealed.DecisionSlot != 2 {
		t.Fatalf("Core lacks the actual slot-2 QCKP before archive publication: seal=%+v exists=%t err=%v", sealed, exists, err)
	}
	if err := writerArchive.SyncThrough(ctx, core, sealed.DecisionSlot); err != nil {
		t.Fatal("publish actual certified QCKP through independent writer archive", err)
	}
	if writerArchive.Tip() != 2 || observerArchive.Tip() != 1 {
		t.Fatalf("archive managers did not retain distinct captured tips: writer=%d observer=%d", writerArchive.Tip(), observerArchive.Tip())
	}
	certified, found := core.CertifiedValue(sealed.DecisionSlot)
	if !found || certified.Hash == (quepaxa.ValueHash{}) {
		t.Fatalf("Core lost the actual QCKP decision needed for same-rank CURRENT: found=%t hash=%x", found, certified.Hash)
	}
	if err := n.checkpoints.PromoteManagedAppendOnlyLocator(ctx, checkpoint.ManagedAppendOnlyLocator{
		Policy: int(sqlpolicy.ManagedGraphRequestMaterializationPolicy), Index: uint64(sealed.Index),
		RootHash: sealed.RootHash, SealDecisionSlot: uint64(sealed.DecisionSlot), SealDecisionHash: certified.Hash,
	}); err != nil {
		t.Fatal("install same-rank CURRENT hint for selection fixture", err)
	}
	before := bucket.mutationSnapshot()
	root, selected, err := n.selectManagedAppendOnlyRoot(ctx)
	after := bucket.mutationSnapshot()
	if len(after) != len(before) {
		t.Errorf("read-only selection mutated provider after remote certificate publication: before=%+v after=%+v", before, after)
	}
	if err != nil || root == nil || selected == nil || selected.DecisionSlot != sealed.DecisionSlot || selected.RootHash != sealed.RootHash {
		t.Fatalf("selection failed to use already durable exact QCKP: root=%+v selected=%+v err=%v", root, selected, err)
	}
}
