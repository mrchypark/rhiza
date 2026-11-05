//go:build rhiza_local_testhooks

package quepaxa

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestCheckpointRecordRechecksPreparedFenceAfterValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cores, _ := newTestCluster(t)
	core := cores["n1"]
	for i := 0; i < 7; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatalf("certify prefix slot %d: %v", i+1, err)
		}
	}
	makeSeal := func(index Slot, label string) CheckpointSeal {
		t.Helper()
		prefix, ok := core.PrefixHash(index)
		if !ok {
			t.Fatalf("prefix %d is unavailable", index)
		}
		next, following, err := core.CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatal(err)
		}
		return CheckpointSeal{ConfigID: 1, Index: index, RootHash: sha256.Sum256([]byte(label)), StateHash: sha256.Sum256([]byte("state-" + label)), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	}
	oldSeal, higherSeal := makeSeal(5, "old-record"), makeSeal(7, "higher-fence")
	core.SetCheckpointValidator(func(_ context.Context, candidate CheckpointSeal) error {
		if candidate.Index == oldSeal.Index && candidate.RootHash == oldSeal.RootHash || candidate.Index == higherSeal.Index && candidate.RootHash == higherSeal.RootHash {
			return nil
		}
		return errors.New("unexpected checkpoint candidate")
	})
	if err := core.PrepareCheckpoint(ctx, oldSeal); err != nil {
		t.Fatalf("prepare old checkpoint candidate: %v", err)
	}
	value, err := EncodeCheckpointSeal(oldSeal)
	if err != nil {
		t.Fatal(err)
	}
	validated, release := make(chan struct{}), make(chan struct{})
	restore := localtesthooks.Set(func(event string) {
		if event == "core:after-checkpoint-record-validation" {
			close(validated)
			<-release
		}
	})
	defer restore()
	result := make(chan error, 1)
	go func() {
		_, err := core.Record(ctx, RecordRequest{Slot: 8, Step: 4, ConfigID: 1, Proposal: Proposal{Hash: sha256.Sum256(value), Value: value}})
		result <- err
	}()
	select {
	case <-validated:
	case err := <-result:
		t.Fatalf("Record rejected before the validation boundary: %v", err)
	case <-ctx.Done():
		t.Fatal("Record did not finish external checkpoint validation")
	}
	if err := core.PrepareCheckpoint(ctx, higherSeal); err != nil {
		t.Fatalf("publish higher prepared fence: %v", err)
	}
	close(release)
	if err := <-result; err == nil {
		t.Fatal("stale checkpoint Record passed after higher prepared fence")
	}
	core.mu.RLock()
	state := core.recorders[8]
	core.mu.RUnlock()
	if state.Step != 0 || state.FirstCurrent != nil || state.AggregateCurrent != nil {
		t.Fatalf("stale checkpoint receipt was appended after fence: %+v", state)
	}
}
