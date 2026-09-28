package quepaxa

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestReclaimLocalCheckpointOwnsPrepareSealApplyAndCompaction(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if slot, _, err := core.Propose(context.Background(), []byte("state at checkpoint")); err != nil || slot != 1 {
		t.Fatalf("seed proposal slot=%d err=%v", slot, err)
	}
	seal := testLocalSeal(t, core, 1)
	validations := 0
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error {
		validations++
		return nil
	})
	applied := false
	slot, err := core.ReclaimLocalCheckpoint(context.Background(), seal, func(_ context.Context, appliedThrough Slot) error {
		if appliedThrough != 2 || core.Tip() != 2 {
			t.Fatalf("apply callback slot=%d Core tip=%d", appliedThrough, core.Tip())
		}
		applied = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if slot != 2 || !applied || validations < 3 {
		t.Fatalf("seal slot=%d applied=%v exact root validations=%d", slot, applied, validations)
	}
	if floor, root, ok := core.RecoveryRoot(); !ok || floor != 1 || root != seal.RootHash {
		t.Fatalf("recovery root=(%d,%x,%v), want (1,%x,true)", floor, root, ok, seal.RootHash)
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 || entries[0].Type != qlog.EntryCheckpoint || entries[0].Slot != 1 || entries[0].Hash != seal.RootHash {
		t.Fatalf("compacted WAL does not begin with exact checkpoint base: %+v", entries)
	}
}

func TestReclaimLocalCheckpointPreflightDoesNotWriteOnRefusal(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if _, _, err := core.Propose(context.Background(), []byte("checkpoint refusal seed")); err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, core, 1)
	before, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	used := wal.Bytes()
	if err := wal.SetMaxBytes(used); err != nil {
		t.Fatal(err)
	}
	_, err = core.ReclaimLocalCheckpoint(context.Background(), seal, func(context.Context, Slot) error {
		t.Fatal("apply callback ran after prewrite capacity refusal")
		return nil
	})
	if !IsLocalAdmissionDenial(err) {
		t.Fatalf("reclaim error=%v, want dedicated prewrite denial", err)
	}
	after, readErr := wal.Read()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(after) != len(before) || core.Tip() != 1 {
		t.Fatalf("capacity refusal changed state: entries %d=>%d tip=%d", len(before), len(after), core.Tip())
	}
	if _, _, ok := core.LatestPreparedCheckpoint(); ok {
		t.Fatal("capacity refusal left a prepared checkpoint")
	}
}

func TestReclaimLocalCheckpointDoesNotPrepareWhenRootValidationFails(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if _, _, err := core.Propose(context.Background(), []byte("root validation seed")); err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, core, 1)
	before := wal.Bytes()
	rootErr := errors.New("missing exact checkpoint root")
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return rootErr })
	if _, err := core.ReclaimLocalCheckpoint(context.Background(), seal, func(context.Context, Slot) error {
		t.Fatal("apply callback ran with a missing root")
		return nil
	}); !errors.Is(err, rootErr) {
		t.Fatalf("reclaim error=%v, want exact-root failure", err)
	}
	if wal.Bytes() != before {
		t.Fatalf("root validation failure appended WAL bytes: %d=>%d", before, wal.Bytes())
	}
}

func TestLocalCheckpointRootsIncludesPreparedAndSealedRoots(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if _, _, err := core.Propose(context.Background(), []byte("root inventory seed")); err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, core, 1)
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	roots, err := core.LocalCheckpointRoots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, root := range roots {
		if root.RootHash == seal.RootHash && root.Index == seal.Index && root.PrefixHash == seal.PrefixHash && root.StateHash == seal.StateHash && root.ConfigID == seal.ConfigID {
			found = true
		}
	}
	if !found {
		t.Fatalf("prepared root %x missing from live roots %x", seal.RootHash, roots)
	}
}

func TestLocalCheckpointRootsRetainsHistoricalPreparedMarkers(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if _, _, err := core.Propose(context.Background(), []byte("first checkpoint prefix")); err != nil {
		t.Fatal(err)
	}
	first := testLocalSeal(t, core, 1)
	if err := core.PrepareCheckpoint(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), []byte("second checkpoint prefix")); err != nil {
		t.Fatal(err)
	}
	second := testLocalSeal(t, core, 2)
	second.RootHash = sha256.Sum256([]byte("second local root"))
	second.StateHash = sha256.Sum256([]byte("second local state"))
	if err := core.PrepareCheckpoint(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	roots, err := core.LocalCheckpointRoots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[[32]byte]bool{first.RootHash: false, second.RootHash: false}
	for _, root := range roots {
		if _, ok := want[root.RootHash]; ok {
			want[root.RootHash] = true
		}
	}
	for root, found := range want {
		if !found {
			t.Errorf("checkpoint root %x referenced by historical WAL marker was omitted", root)
		}
	}
}

func TestReclaimLocalCheckpointResumesExistingSealWithoutReplacement(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if _, _, err := core.Propose(context.Background(), []byte("resume checkpoint seed")); err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, core, 1)
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	sealValue, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	sealSlot, _, err := core.Propose(context.Background(), sealValue)
	if err != nil {
		t.Fatal(err)
	}
	entriesBefore, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	decisionsBefore := 0
	for _, entry := range entriesBefore {
		if entry.Type == qlog.EntryDecide && entry.Slot == uint64(sealSlot) {
			decisionsBefore++
		}
	}
	if decisionsBefore != 1 {
		t.Fatalf("initial seal decision records=%d, want 1", decisionsBefore)
	}
	if _, err := core.ReclaimLocalCheckpoint(context.Background(), seal, func(context.Context, Slot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if core.Tip() != sealSlot {
		t.Fatalf("resume allocated replacement slot: tip=%d original=%d", core.Tip(), sealSlot)
	}
	entriesAfter, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entriesAfter {
		if entry.Type == qlog.EntryDecide && entry.Slot > uint64(sealSlot) {
			t.Fatalf("resume appended a later replacement seal decision: %+v", entry)
		}
	}
}

func TestReclaimLocalCheckpointPreflightsExactRewriteAndRemainingBound(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if _, _, err := core.Propose(context.Background(), []byte("space estimate checkpoint seed")); err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, core, 1)
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	expectedRewrite, err := core.estimateLocalCompactionOwned(seal)
	if err != nil {
		t.Fatal(err)
	}
	var gotRewrite, gotRemaining uint64
	_, err = core.ReclaimLocalCheckpointWithSpacePreflight(context.Background(), seal, func(rewrite, remaining uint64) error {
		gotRewrite, gotRemaining = rewrite, remaining
		return nil
	}, func(context.Context, Slot) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if gotRewrite != expectedRewrite || gotRemaining == 0 {
		t.Fatalf("physical preflight got rewrite=%d remaining=%d, want exact rewrite=%d and positive remaining bound", gotRewrite, gotRemaining, expectedRewrite)
	}
}
