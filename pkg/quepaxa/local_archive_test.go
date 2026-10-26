package quepaxa

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
)

func archiveFixture(t *testing.T) (CheckpointSeal, DecidedValue, []DecidedValue) {
	t.Helper()
	c, w := localReserveCore(t, t.TempDir(), "local")
	defer w.Close()
	ctx := context.Background()
	if _, _, err := c.Propose(ctx, []byte("base")); err != nil {
		t.Fatal(err)
	}
	seal := testLocalSeal(t, c, 1)
	c.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	slot, err := c.ReclaimLocalCheckpoint(ctx, seal, func(context.Context, Slot) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := c.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing seal decision")
	}
	if _, _, err := c.Propose(ctx, []byte("suffix")); err != nil {
		t.Fatal(err)
	}
	page, _, err := c.DecisionsFrom(2, 10)
	if err != nil {
		t.Fatal(err)
	}
	return seal, decision, page
}

func TestLocalArchiveBaseSuffixReopen(t *testing.T) {
	seal, decision, page := archiveFixture(t)
	dir := t.TempDir()
	c, w := localReserveCore(t, dir, "local")
	c.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	ctx := context.Background()
	if err := c.RestoreLocalArchiveBase(ctx, seal, decision); err != nil {
		t.Fatal(err)
	}
	before := w.Bytes()
	if err := c.RestoreLocalArchiveBase(ctx, seal, decision); err != nil {
		t.Fatal(err)
	}
	if w.Bytes() != before {
		t.Fatal("retry rewrote base")
	}
	if err := c.ImportLocalArchive(ctx, page); err != nil {
		t.Fatal(err)
	}
	for _, want := range page {
		got, ok := c.CertifiedValue(want.Slot)
		if !ok || !bytes.Equal(got.Certificate, want.Certificate) {
			t.Fatal("original certificate lost")
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, rw := localReserveCore(t, dir, "local")
	defer rw.Close()
	if reopened.Tip() != page[len(page)-1].Slot || reopened.CompactionFloor() != seal.Index {
		t.Fatal("reopened prefix differs")
	}
	for _, want := range page {
		got, ok := reopened.CertifiedValue(want.Slot)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatal("reopened decision differs")
		}
	}
}

func TestLocalArchiveRefusalsDoNotMutate(t *testing.T) {
	seal, decision, page := archiveFixture(t)
	for _, mode := range []string{"late_invalid", "late_gap", "capacity", "pending", "canceled", "nonlocal", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			c, w := localReserveCore(t, t.TempDir(), "local")
			defer w.Close()
			c.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
			ctx := context.Background()
			if err := c.RestoreLocalArchiveBase(ctx, seal, decision); err != nil {
				t.Fatal(err)
			}
			input := append([]DecidedValue(nil), page...)
			switch mode {
			case "late_invalid":
				input[len(input)-1].Certificate = []byte("invalid")
			case "late_gap":
				input[len(input)-1].Slot++
			case "capacity":
				if err := w.SetMaxBytes(w.Bytes() + 1); err != nil {
					t.Fatal(err)
				}
			case "pending":
				if _, err := localRecordForTest(t, c, RecordRequest{Slot: 2, Step: 4, Proposal: newProposal(highestPriority, "local", []byte("pending"))}); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nonlocal":
				c.localMode = false
			case "conflict":
				if err := c.ImportLocalArchive(ctx, page); err != nil {
					t.Fatal(err)
				}
			}
			before := localReserveEntries(t, w)
			tip := c.Tip()
			err := c.ImportLocalArchive(ctx, input)
			if err == nil {
				t.Fatal("expected refusal")
			}
			if mode == "capacity" && !IsLocalAdmissionDenial(err) {
				t.Fatalf("not admission denial: %v", err)
			}
			if mode == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if c.Tip() != tip || !reflect.DeepEqual(before, localReserveEntries(t, w)) {
				t.Fatal("refusal mutated state")
			}
		})
	}
}

func TestLocalArchiveBaseRejectsPendingState(t *testing.T) {
	seal, decision, _ := archiveFixture(t)
	for _, mode := range []string{"recorder", "prepared", "proposal", "decided", "allocated", "unlogged", "canceled", "nonlocal"} {
		t.Run(mode, func(t *testing.T) {
			c, w := localReserveCore(t, t.TempDir(), "local")
			defer w.Close()
			c.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
			ctx := context.Background()
			switch mode {
			case "recorder":
				if _, err := localRecordForTest(t, c, RecordRequest{Slot: 1, Step: 4, Proposal: newProposal(highestPriority, "local", []byte("pending"))}); err != nil {
					t.Fatal(err)
				}
			case "prepared":
				c.preparedCheckpoints[1] = seal.RootHash
			case "proposal":
				c.values[decision.Hash] = []byte("pending")
			case "decided":
				c.decided[decision.Slot] = decision
			case "allocated":
				c.nextSlot = 2
			case "unlogged":
				c.localUnloggedCount = 1
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nonlocal":
				c.localMode = false
			}
			before := localReserveEntries(t, w)
			if err := c.RestoreLocalArchiveBase(ctx, seal, decision); err == nil {
				t.Fatal("expected refusal")
			}
			if c.Tip() != 0 || !reflect.DeepEqual(before, localReserveEntries(t, w)) {
				t.Fatal("base refusal mutated state")
			}
		})
	}
}
