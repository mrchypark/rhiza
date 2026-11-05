//go:build rhiza_local_testhooks

package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"
)

func TestManagedControlObservationRejectsMutation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cores, transport := newManagedControlRaceCluster(t, nil)
	core := cores["n1"]
	managedControlRacePrefix(t, core)

	observedValue := EncodeReadBarrier([ReadBarrierNonceSize]byte{51, 52, 53})
	proposal := startManagedControlProposal(t, ctx, core, transport, observedValue)
	observation, err := core.ObserveManagedAppendOnlyControl(ctx)
	if err != nil || !observation.BelongsTo(core) || observation.CommittedTip != 8 || observation.ObservedSlot != 9 {
		t.Fatalf("create actual valid R9 observation: observation=%+v err=%v", observation, err)
	}
	inPlaceMutation := observation
	inPlaceMutation.Proposals = append([]ManagedAppendOnlyControlProposal(nil), observation.Proposals...)
	inPlaceMutation.Proposals[0].Hash[0] ^= 0xff
	beforeInPlaceMutation := core.wal.Bytes()
	if inPlaceMutation.BelongsTo(core) {
		t.Fatal("observation with an in-place public proposal mutation was accepted")
	}
	if !observation.BelongsTo(core) || core.wal.Bytes() != beforeInPlaceMutation || core.Tip() != 8 || core.RecorderTip() != 9 {
		t.Fatalf("in-place mutation invalidated the original or changed Core/WAL: originalValid=%t bytes=%d/%d C=%d R=%d", observation.BelongsTo(core), beforeInPlaceMutation, core.wal.Bytes(), core.Tip(), core.RecorderTip())
	}

	transport.releaseHeld()
	if result := proposal.wait(ctx); result.err != nil || result.slot != 9 {
		t.Fatalf("actual R9 read-barrier proposer did not certify after release: %+v", result)
	}
	decision, ok := core.CertifiedValue(9)
	if !ok || decision.Hash != ValueHash(sha256.Sum256(observedValue)) || !bytes.Equal(decision.Value, observedValue) {
		t.Fatalf("actual R9 read-barrier certificate differs: found=%t decision=%+v", ok, decision)
	}

	futureValue := EncodeReadBarrier([ReadBarrierNonceSize]byte{61, 62, 63})
	if slot, _, err := core.Propose(ctx, futureValue); err != nil || slot != 10 {
		t.Fatalf("certify actual future R10 slot=%d err=%v", slot, err)
	}
	future, ok := core.CertifiedValue(10)
	futureHash := ValueHash(sha256.Sum256(futureValue))
	if !ok || future.Hash != futureHash || !bytes.Equal(future.Value, futureValue) {
		t.Fatalf("future value lacks its actual Core certificate: found=%t value=%x", ok, future.Value)
	}

	mutated := observation
	mutated.CommittedTip = 9
	mutated.ObservedSlot = 10
	mutated.ConfigID = core.ConfigIDForSlot(10)
	mutated.Proposals = append([]ManagedAppendOnlyControlProposal(nil), observation.Proposals...)
	for index := range mutated.Proposals {
		mutated.Proposals[index].Hash = futureHash
	}
	beforeMutationAttempt := core.wal.Bytes()
	if mutated.BelongsTo(core) {
		t.Fatal("mutated stale observation still passed its private integrity seal")
	}
	if core.wal.Bytes() != beforeMutationAttempt || core.Tip() != 10 || core.RecorderTip() != 10 {
		t.Fatalf("mutated-observation rejection changed Core or WAL: bytes=%d/%d C=%d R=%d", beforeMutationAttempt, core.wal.Bytes(), core.Tip(), core.RecorderTip())
	}
	if _, ok := core.CertifiedValue(11); ok {
		t.Fatal("mutated observation created another certified slot")
	}
}

func TestManagedControlRequiresOwnCertificateAfterObservedBarrierCertifies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cores, transport := newManagedControlRaceCluster(t, nil)
	core := cores["n1"]
	managedControlRacePrefix(t, core)

	observedValue := EncodeReadBarrier([ReadBarrierNonceSize]byte{81, 82, 83})
	foreignOwner := startManagedControlProposal(t, ctx, core, transport, observedValue)
	observation, err := core.ObserveManagedAppendOnlyControl(ctx)
	if err != nil || !observation.BelongsTo(core) || observation.CommittedTip != 8 || observation.ObservedSlot != 9 {
		t.Fatalf("capture actual pending R=9 observation: observation=%+v err=%v", observation, err)
	}
	transport.releaseHeld()
	if result := foreignOwner.wait(ctx); result.err != nil || result.slot != 9 {
		t.Fatalf("actual foreign R=9 barrier did not certify: %+v", result)
	}

	ownedValue := EncodeReadBarrier([ReadBarrierNonceSize]byte{91, 92, 93})
	slot, _, err := core.Propose(ctx, ownedValue)
	if err != nil || slot <= observation.ObservedSlot {
		t.Fatalf("observed foreign R=9 certificate was returned instead of certifying the caller's barrier: slot=%d err=%v", slot, err)
	}
	owned, ok := core.CertifiedValue(slot)
	if !ok || owned.Hash != ValueHash(sha256.Sum256(ownedValue)) || !bytes.Equal(owned.Value, ownedValue) {
		t.Fatalf("ordinary proposal path did not certify its own exact nonce at S=%d: found=%t value=%x", slot, ok, owned.Value)
	}
	observed, ok := core.CertifiedValue(observation.ObservedSlot)
	if !ok || observed.Hash != ValueHash(sha256.Sum256(observedValue)) || !bytes.Equal(observed.Value, observedValue) {
		t.Fatalf("own barrier certification changed the original observed R=9 decision: found=%t value=%x", ok, observed.Value)
	}
}
