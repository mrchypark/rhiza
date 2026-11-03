package quepaxa

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

type atomicOrderStepFiveGate struct {
	*clusterTransport
	sealHash ValueHash
	accepted chan NodeID
}

func (gate *atomicOrderStepFiveGate) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	summary, err := gate.clusterTransport.SendRecord(ctx, to, request)
	if err != nil || request.Step != 5 || request.Proposal.Hash != gate.sealHash {
		return summary, err
	}
	gate.accepted <- to
	<-ctx.Done()
	return Summary{}, ctx.Err()
}

// A higher prepared marker must not supersede a durable, uncertified Step-5
// seal already present in the live recorder quorum.
func TestHigherPrepareDoesNotFenceDurablePendingStepFiveSeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cores, transport := newTestCluster(t)
	for i := 0; i < 7; i++ {
		if _, _, err := cores["n1"].Propose(ctx, []byte("certified-prefix")); err != nil {
			t.Fatalf("certify prefix slot %d: %v", i+1, err)
		}
	}
	for id, core := range cores {
		if err := core.WaitTip(ctx, 7); err != nil {
			t.Fatalf("core %s did not learn certified prefix: %v", id, err)
		}
	}
	makeSeal := func(index Slot, label string) CheckpointSeal {
		t.Helper()
		prefix, ok := cores["n1"].PrefixHash(index)
		if !ok {
			t.Fatalf("certified prefix %d is missing", index)
		}
		next, following, err := cores["n1"].CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatal(err)
		}
		return CheckpointSeal{ConfigID: 1, Index: index, RootHash: sha256.Sum256([]byte(label)), StateHash: sha256.Sum256([]byte("state-" + label)), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	}
	oldSeal := makeSeal(5, "step-five-old-seal")
	higherSeal := makeSeal(7, "step-five-higher-fence")
	for id, core := range cores {
		core.SetCheckpointValidator(func(_ context.Context, seal CheckpointSeal) error {
			if seal.Index == oldSeal.Index && seal.RootHash == oldSeal.RootHash && seal.StateHash == oldSeal.StateHash || seal.Index == higherSeal.Index && seal.RootHash == higherSeal.RootHash && seal.StateHash == higherSeal.StateHash {
				return nil
			}
			return errors.New("unknown test checkpoint")
		})
		if err := core.PrepareCheckpoint(ctx, oldSeal); err != nil {
			t.Fatalf("prepare old seal at %s: %v", id, err)
		}
	}
	oldValue, err := EncodeCheckpointSeal(oldSeal)
	if err != nil {
		t.Fatal(err)
	}
	oldHash := ValueHash(sha256.Sum256(oldValue))
	leaders, err := cores["n1"].LeaderOrder(8)
	if err != nil || len(leaders) != 3 {
		t.Fatalf("slot-8 leader order=%v err=%v", leaders, err)
	}
	ownerID := NodeID("n1")
	if leaders[0] == ownerID {
		ownerID = "n2"
	}
	gate := &atomicOrderStepFiveGate{clusterTransport: transport, sealHash: oldHash, accepted: make(chan NodeID, 3)}
	cores[ownerID].transport = gate
	ownerCtx, cancelOwner := context.WithCancel(ctx)
	type proposeResult struct {
		slot Slot
		err  error
	}
	ownerDone := make(chan proposeResult, 1)
	go func() {
		slot, _, err := cores[ownerID].Propose(ownerCtx, oldValue)
		ownerDone <- proposeResult{slot: slot, err: err}
	}()
	accepted := make(map[NodeID]bool, 2)
	for len(accepted) < 2 {
		select {
		case id := <-gate.accepted:
			accepted[id] = true
		case <-ctx.Done():
			t.Fatal("old seal did not reach two actual peer Step-5 Record calls")
		}
	}
	deadline := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		count := 0
		for _, core := range cores {
			core.mu.RLock()
			state := core.recorders[8]
			actual := state.Step == 5 && state.FirstCurrent != nil && state.FirstCurrent.Hash == oldHash && core.valueDurable[oldHash]
			core.mu.RUnlock()
			if actual {
				count++
			}
		}
		if count == len(cores) {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("durable actual Step-5 recorder count=%d, want %d", count, len(cores))
		case <-ctx.Done():
			t.Fatal("context ended before Step-5 recorder state was confirmed")
		}
	}
	for id, core := range cores {
		if _, ok := core.CertifiedValue(8); ok {
			t.Fatalf("old seal unexpectedly certified at %s before cancellation", id)
		}
	}
	cancelOwner()
	result := <-ownerDone
	if result.slot != 8 || !errors.Is(result.err, context.Canceled) {
		t.Fatalf("old-seal owner result: slot=%d err=%v", result.slot, result.err)
	}
	cores[ownerID].transport = transport

	for id, core := range cores {
		if err := core.PrepareCheckpoint(ctx, higherSeal); err == nil {
			t.Fatalf("higher prepare unexpectedly superseded pending old seal at %s", id)
		} else if !errors.Is(err, ErrCheckpointPreparePending) {
			t.Fatalf("higher prepare at %s returned non-typed refusal: %v", id, err)
		}
		core.mu.RLock()
		fence := core.preparedCheckpoints[7]
		core.mu.RUnlock()
		if fence == higherSeal.RootHash {
			t.Fatalf("higher marker was published despite pending old seal at %s", id)
		}
	}

	var nonce [ReadBarrierNonceSize]byte
	nonce[0] = 0x87
	barrier := EncodeReadBarrier(nonce)
	barrierSlot, _, err := cores[ownerID].Propose(ctx, barrier)
	if err != nil {
		t.Fatalf("owned barrier did not finish pending seal: %v", err)
	}
	oldDecision, ok := cores[ownerID].CertifiedValue(8)
	if !ok || oldDecision.Hash != oldHash || string(oldDecision.Value) != string(oldValue) {
		t.Fatal("barrier did not preserve the exact pending old-seal decision")
	}
	barrierDecision, ok := cores[ownerID].CertifiedValue(barrierSlot)
	if !ok || barrierDecision.Hash != sha256.Sum256(barrier) {
		t.Fatal("owned barrier did not return its exact certified decision")
	}
	for id, core := range cores {
		if err := core.PrepareCheckpoint(ctx, higherSeal); err != nil {
			t.Fatalf("higher prepare after exact old-seal certification at %s: %v", id, err)
		}
	}
	stored, ok := cores[ownerID].CertifiedValue(8)
	if !ok {
		t.Fatal("exact old-seal certificate disappeared")
	}
	decision, err := decodeDecision(stored.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = stored.Value
	if _, err := cores[ownerID].Record(ctx, RecordRequest{Slot: 8, Step: decision.Step + 4, ConfigID: 1, Proposal: decision.Proposal}); err != nil {
		t.Fatalf("exact certified checkpoint redelivery after higher prepare: %v", err)
	}
}
