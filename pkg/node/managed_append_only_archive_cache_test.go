package node

import (
	"context"
	"database/sql"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestManagedAppendOnlyRootSelectorSameFloorSuffixIsCacheIndependent(t *testing.T) {
	ctx := context.Background()
	n, core, archive, bucket := newManagedAppendOnlySelectorFixture(t)
	if archive.Tip() != 1 || core.Tip() != 2 {
		t.Fatalf("fixture must start with local QCKP beyond loaded archive: archive=%d Core=%d", archive.Tip(), core.Tip())
	}
	if err := archive.SyncThrough(ctx, core, 2); err != nil {
		t.Fatal("publish actual A producer", err)
	}
	sealedA, ok, err := core.LatestManagedCheckpointSeal()
	if err != nil || !ok || sealedA.DecisionSlot != 2 {
		t.Fatalf("actual A producer unavailable: seal=%+v ok=%t err=%v", sealedA, ok, err)
	}
	if err := n.promoteManagedAppendOnlySeal(ctx, sealedA); err != nil {
		t.Fatal("publish actual A as CURRENT", err)
	}
	decisionA, ok := core.CertifiedValue(sealedA.DecisionSlot)
	if !ok {
		t.Fatal("actual A producer certificate unavailable")
	}
	if err := archive.TrimThrough(ctx, sealedA, decisionA); err != nil {
		t.Fatal("establish actual physical A archive base", err)
	}
	if base, _, hasBase := archive.RecoveryBase(); !hasBase || base.Index != sealedA.Index || archive.Tip() != 2 {
		t.Fatalf("archive lacks physical A base with retained A producer: base=%+v hasBase=%t tip=%d", base, hasBase, archive.Tip())
	}
	rootFiles, err := n.checkpoints.DownloadRootFiles(ctx, uint64(sealedA.Index), sealedA.RootHash, t.TempDir())
	if err != nil {
		t.Fatal("download verified physical A for an actual alternate B root:", err)
	}
	sources := make([]checkpoint.Source, len(rootFiles))
	for i, file := range rootFiles {
		sources[i] = checkpoint.Source{Role: file.Role, Path: file.Path}
		if file.Role != checkpoint.RoleSQLite {
			continue
		}
		db, err := sql.Open("sqlite3", file.Path)
		if err != nil {
			t.Fatal("open downloaded A SQLite root:", err)
		}
		_, execErr := db.ExecContext(ctx, "PRAGMA user_version=1")
		closeErr := db.Close()
		if execErr != nil || closeErr != nil {
			t.Fatalf("prepare actual alternate B SQLite root: exec=%v close=%v", execErr, closeErr)
		}
	}
	rootB, err := n.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, uint64(sealedA.Index))
	if err != nil || rootB.RootHash == sealedA.RootHash {
		t.Fatalf("actual same-cut B root is not distinct from A: root=%+v A=%x err=%v", rootB, sealedA.RootHash, err)
	}
	sealB := sealedA.CheckpointSeal
	sealB.RootHash, sealB.StateHash = rootB.RootHash, rootB.Hash
	valueB, err := quepaxa.EncodeCheckpointSeal(sealB)
	if err != nil {
		t.Fatal("encode actual B checkpoint seal:", err)
	}
	if slot, _, err := core.Propose(ctx, valueB); err != nil || slot != 3 {
		t.Fatalf("certify actual same-floor B producer: slot=%d err=%v", slot, err)
	}
	sealedB, ok, err := core.LatestManagedCheckpointSeal()
	if err != nil || !ok || sealedB.DecisionSlot != 3 || sealedB.Index != sealedA.Index || sealedB.RootHash != rootB.RootHash {
		t.Fatalf("actual B producer does not preserve same-floor A identity: B=%+v A=%+v ok=%t err=%v", sealedB, sealedA, ok, err)
	}
	writer, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "readonly-selector", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal("create independent B publisher:", err)
	}
	t.Cleanup(func() {
		writer.Close()
	})
	writer.SetGroupDelay(0)
	if err := writer.Load(ctx); err != nil {
		t.Fatal("load independent B publisher:", err)
	}
	if err := writer.SyncThrough(ctx, core, sealedB.DecisionSlot); err != nil {
		t.Fatal("publish actual B producer after preserving A base:", err)
	}
	if archive.Tip() != 2 || writer.Tip() != 3 {
		t.Fatalf("reader and writer did not retain the intended cache boundary: reader=%d writer=%d", archive.Tip(), writer.Tip())
	}

	before := bucket.mutationSnapshot()
	firstRoot, firstSeal, firstErr := n.selectManagedAppendOnlyRoot(ctx)
	middle := bucket.mutationSnapshot()
	secondRoot, secondSeal, secondErr := n.selectManagedAppendOnlyRoot(ctx)
	after := bucket.mutationSnapshot()
	if len(middle) != len(before) || len(after) != len(middle) {
		t.Fatalf("read-only selector mutated provider: before=%+v after-first=%+v after-second=%+v", before, middle, after)
	}
	if firstErr != nil || firstRoot == nil || firstSeal == nil || firstSeal.DecisionSlot != sealedB.DecisionSlot {
		t.Errorf("first selection after loading valid A-base/B-suffix differs: root=%+v seal=%+v err=%v", firstRoot, firstSeal, firstErr)
	}
	if secondErr != nil || secondRoot == nil || secondSeal == nil || secondSeal.DecisionSlot != sealedB.DecisionSlot {
		t.Errorf("cache-hit selection of same A-base/B-suffix differs: root=%+v seal=%+v err=%v", secondRoot, secondSeal, secondErr)
	}
	if firstErr == nil && secondErr == nil && (firstRoot.Hash != secondRoot.Hash || firstSeal.RootHash != secondSeal.RootHash || firstSeal.DecisionSlot != secondSeal.DecisionSlot) {
		t.Fatalf("selector chose different exact roots across cache transition: first=%+v second=%+v", firstSeal, secondSeal)
	}
}

func TestManagedAppendOnlyRootSelectorRejectsSealedArchiveOnRefreshAndCacheHit(t *testing.T) {
	ctx := context.Background()
	n, core, archive, bucket := newManagedAppendOnlySelectorFixture(t)
	writer, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "readonly-selector", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal("create independent archive publisher:", err)
	}
	t.Cleanup(func() {
		writer.Close()
	})
	writer.SetGroupDelay(0)
	if err := writer.Load(ctx); err != nil {
		t.Fatal("load independent archive publisher:", err)
	}
	if err := writer.SyncThrough(ctx, core, 2); err != nil {
		t.Fatal("publish actual certified QCKP:", err)
	}
	if err := recovery.Seal(ctx, bucket, "readonly-selector", "selector-cache-test"); err != nil {
		t.Fatal("seal actual archive after publishing QCKP:", err)
	}
	if archive.Tip() != 1 || writer.Tip() != 2 || writer.Sealed() {
		t.Fatalf("fixture cache/seal state unexpected: cachedTip=%d writerTip=%d writerSealed=%t", archive.Tip(), writer.Tip(), writer.Sealed())
	}
	if reader, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "readonly-selector", 1, types.ValidateManagedGraphExecutionPolicy); err != nil {
		t.Fatal("construct independent verifier for sealed head:", err)
	} else {
		reader.SetGroupDelay(0)
		if err := reader.Load(ctx); err != nil || !reader.Sealed() || reader.Tip() != 2 {
			reader.Close()
			t.Fatalf("published archive head is not sealed at the actual QCKP: tip=%d sealed=%t err=%v", reader.Tip(), reader.Sealed(), err)
		}
		reader.Close()
	}

	before := bucket.mutationSnapshot()
	firstRoot, firstSeal, firstErr := n.selectManagedAppendOnlyRoot(ctx)
	middle := bucket.mutationSnapshot()
	secondRoot, secondSeal, secondErr := n.selectManagedAppendOnlyRoot(ctx)
	after := bucket.mutationSnapshot()
	if len(middle) != len(before) || len(after) != len(middle) {
		t.Fatalf("read-only selector mutated sealed provider: before=%+v after-first=%+v after-second=%+v", before, middle, after)
	}
	if firstErr == nil || firstRoot != nil || firstSeal != nil {
		t.Errorf("selector accepted sealed head after refresh: root=%+v seal=%+v err=%v", firstRoot, firstSeal, firstErr)
	}
	if secondErr == nil || secondRoot != nil || secondSeal != nil {
		t.Errorf("selector accepted sealed head on cache hit: root=%+v seal=%+v err=%v", secondRoot, secondSeal, secondErr)
	}
}
