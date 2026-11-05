package quepaxa

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalRecordRefusesPendingCanceledCheckpointPrepare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	for i := 0; i < 2; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatalf("certify local prefix slot %d: %v", i+1, err)
		}
	}
	oldSeal, higherSeal := testLocalSeal(t, core, 1), testLocalSeal(t, core, 2)
	core.SetCheckpointValidator(func(_ context.Context, seal CheckpointSeal) error {
		if seal.Index == oldSeal.Index && seal.RootHash == oldSeal.RootHash || seal.Index == higherSeal.Index && seal.RootHash == higherSeal.RootHash {
			return nil
		}
		return errors.New("unexpected local checkpoint candidate")
	})
	oldValue, err := EncodeCheckpointSeal(oldSeal)
	if err != nil {
		t.Fatal(err)
	}

	flushEntered, releaseFlush, flushDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var flushOnce sync.Once
	defer flushOnce.Do(func() { close(releaseFlush) })
	var syncCalls atomic.Int32
	core.commits = newGroupCommit(func() error {
		call := syncCalls.Add(1)
		if call == 1 {
			close(flushEntered)
			<-releaseFlush
		}
		err := wal.Sync()
		if call == 1 {
			close(flushDone)
		}
		return err
	})

	prepareCtx, cancelPrepare := context.WithCancel(ctx)
	prepareResult := make(chan error, 1)
	go func() { prepareResult <- core.PrepareCheckpoint(prepareCtx, higherSeal) }()
	select {
	case <-flushEntered:
	case <-ctx.Done():
		t.Fatal("checkpoint marker Sync did not block")
	}
	cancelPrepare()
	select {
	case err := <-prepareResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled local PrepareCheckpoint returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled local PrepareCheckpoint did not return")
	}

	appendEntered, releaseAppend := make(chan struct{}), make(chan struct{})
	var appendOnce sync.Once
	defer appendOnce.Do(func() { close(releaseAppend) })
	core.recordBeforeAppend = func() {
		close(appendEntered)
		<-releaseAppend
	}
	type proposalResult struct {
		slot Slot
		err  error
	}
	proposalDone := make(chan proposalResult, 1)
	go func() {
		slot, _, err := core.Propose(ctx, oldValue)
		proposalDone <- proposalResult{slot: slot, err: err}
	}()
	proposalWaited := false
	defer func() {
		flushOnce.Do(func() { close(releaseFlush) })
		appendOnce.Do(func() { close(releaseAppend) })
		if !proposalWaited {
			select {
			case <-proposalDone:
			case <-time.After(time.Second):
				t.Error("Local proposal did not return during cleanup")
			}
		}
	}()
	select {
	case <-appendEntered:
		t.Fatal("Local API appended an older checkpoint receipt while the higher marker Sync was unresolved")
	case result := <-proposalDone:
		proposalWaited = true
		if !errors.Is(result.err, ErrCheckpointPreparePending) {
			t.Fatalf("Local API returned non-typed pending refusal: slot=%d err=%v", result.slot, result.err)
		}
	case <-ctx.Done():
		t.Fatal("Local API neither refused nor reached the receipt boundary")
	}

	flushOnce.Do(func() { close(releaseFlush) })
	select {
	case <-flushDone:
	case <-ctx.Done():
		t.Fatal("canceled caller's actual checkpoint marker flush did not finish")
	}
	if err := core.PrepareCheckpoint(ctx, higherSeal); err != nil {
		t.Fatalf("exact candidate did not finish its late durable marker: %v", err)
	}
	if index, root, ok := core.LatestPreparedCheckpoint(); !ok || index != higherSeal.Index || root != higherSeal.RootHash {
		t.Fatalf("late flush did not publish exact prepared candidate: index=%d ok=%t", index, ok)
	}
	sameCutB := higherSeal
	sameCutB.RootHash = [32]byte{0xB}
	sameCutB.StateHash = [32]byte{0xB, 0x1}
	if err := core.PrepareCheckpoint(ctx, sameCutB); err == nil {
		t.Fatal("different root at A's prepared cut was accepted")
	}
	core.recordBeforeAppend = nil
	if _, _, err := core.Propose(ctx, oldValue); err == nil {
		t.Fatal("Local API accepted the old checkpoint after the candidate became durably prepared")
	}
	core.mu.RLock()
	state := core.recorders[3]
	core.mu.RUnlock()
	if state.Step != 0 || state.FirstCurrent != nil {
		t.Fatalf("Local API left an older checkpoint receipt behind: %+v", state)
	}
	if _, _, err := core.Propose(ctx, []byte("new certified suffix")); err != nil {
		t.Fatalf("valid higher-cut progress after A: %v", err)
	}
	higherCut := testLocalSeal(t, core, 3)
	if err := core.PrepareCheckpoint(ctx, higherCut); err != nil {
		t.Fatalf("valid higher cut did not proceed after A: %v", err)
	}
	if index, root, ok := core.LatestPreparedCheckpoint(); !ok || index != higherCut.Index || root != higherCut.RootHash {
		t.Fatalf("valid higher cut did not replace A: index=%d ok=%t", index, ok)
	}
}
