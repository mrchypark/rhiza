package recovery

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// TestArchiveTrimPublicationAdmissionKeepsCleanupBusy proves the missing
// contention edge of the Trim publication-admission contract: Trim retains
// GC_LOCK while it waits for the real PUBLISH_LOCK, so a second Manager's
// Cleanup remains immediately busy instead of entering destructive work.
func TestArchiveTrimPublicationAdmissionKeepsCleanupBusy(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	for i := 0; i < 3; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 2)}); err != nil {
			t.Fatal(err)
		}
	}
	prefix, ok := core.PrefixHash(2)
	if !ok {
		t.Fatal("missing checkpoint prefix")
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: 2, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: prefix,
		NextLeaderOrder: []quepaxa.NodeID{"n1"},
	}
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing certified checkpoint decision")
	}
	if err := seed.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	before := seed.head.Generation

	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	lease, err := holder.acquireArchiveLock(ctx, holder.publicationLockKey(), "trim-publication-holder", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			_ = holder.releaseArchiveLock(context.Background(), holder.publicationLockKey(), lease)
		}
	}()
	probe := &sealAdmissionBucket{Bucket: base, observed: make(chan struct{})}
	trimmer := NewManager(probe, "cluster", 1)
	defer trimmer.Close()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	trimDone := make(chan error, 1)
	go func() {
		trimDone <- trimmer.TrimThrough(waitCtx, quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: slot}, decision)
	}()
	select {
	case <-probe.observed:
	case err := <-trimDone:
		t.Fatalf("TrimThrough did not reach publication admission: %v", err)
	case <-waitCtx.Done():
		t.Fatalf("TrimThrough admission not observed: %v", waitCtx.Err())
	}

	gcLease, err := trimmer.readGCLock(ctx)
	if err != nil || gcLease == nil || gcLease.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatalf("GC lease while TrimThrough waits: lease=%v err=%v", gcLease, err)
	}
	cleanup := NewManager(base, "cluster", 1)
	defer cleanup.Close()
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, time.Second)
	defer cleanupCancel()
	if err := cleanup.Cleanup(cleanupCtx, time.Hour); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("Cleanup while TrimThrough owns GC lease=%v, want ErrArchiveBusy", err)
	}
	if err := holder.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if holder.head.Generation != before || holder.head.Base != 0 {
		t.Fatalf("waiting trim mutated HEAD generation=%d base=%d, want %d/0", holder.head.Generation, holder.head.Base, before)
	}

	if err := holder.releaseArchiveLock(ctx, holder.publicationLockKey(), lease); err != nil {
		t.Fatal(err)
	}
	released = true
	select {
	case err := <-trimDone:
		if err != nil {
			t.Fatalf("TrimThrough after publication release: %v", err)
		}
	case <-waitCtx.Done():
		t.Fatalf("TrimThrough did not finish after publication release: %v", waitCtx.Err())
	}

	independent := NewManager(base, "cluster", 1)
	defer independent.Close()
	if err := independent.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if independent.head.Base != 2 || independent.head.Generation != before+1 {
		t.Fatalf("trimmed HEAD base=%d generation=%d, want 2/%d", independent.head.Base, independent.head.Generation, before+1)
	}
	if len(independent.extents) == 0 || independent.extents[0].Start != 3 {
		t.Fatalf("trimmed refs=%+v, want tail starting at slot 3", independent.extents)
	}
	values, tip, err := independent.DecisionsFrom(ctx, 3, int(core.Tip()-2))
	if err != nil || tip != core.Tip() || len(values) != int(core.Tip()-2) {
		t.Fatalf("trimmed independent tail tip=%d want=%d values=%d err=%v", tip, core.Tip(), len(values), err)
	}
	for i, got := range values {
		want, ok := core.CertifiedValue(quepaxa.Slot(i + 3))
		if !ok || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("trimmed tail slot=%d bytes mismatch", i+3)
		}
	}
}
