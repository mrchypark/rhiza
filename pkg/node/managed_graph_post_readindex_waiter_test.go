//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphAdmissionPostReadIndexJoinsValidatedRemoteOwner(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	readTipRelease := make(chan struct{})
	var readTipReleaseOnce sync.Once
	releaseReadTips := func() { readTipReleaseOnce.Do(func() { close(readTipRelease) }) }
	t.Cleanup(releaseReadTips)
	readTipGate := &graphAdmissionReadTipGate{blockCalls: 2, entered: make(chan struct{}, 2), release: readTipRelease}
	fixture.transport.readTipGate.Store(readTipGate)

	// The existing external slot-9 proposal is allowed through. The later
	// private barrier proposal is held synchronously after its owner operation
	// has been registered, but before it can reach the real Core proposal path.
	pendingControl := managedGraphRaceControlValue()
	ownerProposalEntered := make(chan []byte, 1)
	ownerProposalRelease := make(chan struct{})
	var ownerReleaseOnce sync.Once
	releaseOwnerProposal := func() { ownerReleaseOnce.Do(func() { close(ownerProposalRelease) }) }
	t.Cleanup(releaseOwnerProposal)
	ownerWaitEntered := make(chan struct{})
	ownerWaitRelease := make(chan struct{})
	var ownerWaitOnce sync.Once
	releaseOwnerWait := func() { ownerWaitOnce.Do(func() { close(ownerWaitRelease) }) }
	t.Cleanup(releaseOwnerWait)
	var waiterHookSeen atomic.Bool
	restoreEvents := localtesthooks.Set(func(event string) {
		if event == "node:managed-control:post-readindex-owner-selected" && waiterHookSeen.CompareAndSwap(false, true) {
			close(ownerWaitEntered)
			<-ownerWaitRelease
		}
	})
	t.Cleanup(restoreEvents)
	previousProposalHook := localtesthooks.SetProposalEntry(func(value []byte) {
		if bytes.Equal(value, pendingControl) {
			return
		}
		if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
			return
		}
		ownerProposalEntered <- bytes.Clone(value)
		<-ownerProposalRelease
	})
	t.Cleanup(previousProposalHook)

	// Caller A passes pre-control with a clean local prefix, then is held inside
	// its actual two-peer ReadIndex requests.
	callerA := make(chan error, 1)
	go func() { callerA <- fixture.node.admitInternalApplication(ctx) }()
	for range 2 {
		select {
		case <-readTipGate.entered:
		case <-ctx.Done():
			t.Fatal("caller A did not reach both real ReadIndex peer requests", ctx.Err())
		}
	}

	// Create a real uncertified slot-9 barrier behind A's ReadIndex. Its actual
	// proposal remains held at the existing recorder-quorum gate.
	foreignOwner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}

	// Caller B validates the actual R=9 observation, registers its own nonce,
	// and reaches the pre-proposal gate. The gate proves B owns a different
	// value while the original slot-9 certificate is still pending.
	callerB := make(chan error, 1)
	go func() { callerB <- fixture.node.admitInternalApplication(ctx) }()
	awaitManagedGraphAdmissionObservation(t, fixture, callerB, 9)
	var ownerBarrier []byte
	select {
	case ownerBarrier = <-ownerProposalEntered:
	case <-ctx.Done():
		t.Fatal("validated owner did not reach its actual pre-proposal gate", ctx.Err())
	}
	if bytes.Equal(ownerBarrier, pendingControl) {
		t.Fatal("private owner reused the foreign slot-9 proposal value")
	}
	fixture.node.managedGraphControlMu.Lock()
	registered := fixture.node.managedGraphControl
	registeredMatches := registered != nil && registered.observation != nil &&
		registered.observation.ObservedSlot == 9 && registered.observation.BelongsTo(fixture.node.core) &&
		bytes.Equal(registered.value, ownerBarrier)
	fixture.node.managedGraphControlMu.Unlock()
	if !registeredMatches {
		t.Fatalf("pre-proposal gate lacks the actual registered R=9 owner: op=%+v", registered)
	}

	// Let A's actual ReadIndex return while the original recorder gap is still
	// pending (C=8, R=9). This is the critical schedule: the old waiter compares
	// the foreign recorder hash with B's distinct private nonce and refuses.
	releaseReadTips()
	select {
	case <-ownerWaitEntered:
	case err := <-callerA:
		t.Fatalf("caller A returned before selecting the matching C=8/R=9 owner: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("caller A did not select the validated remote owner after ReadIndex")
	}

	// Only after A has selected B's operation do we certify the original slot 9
	// and permit B's private proposal to continue.
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, foreignOwner); err != nil {
		t.Fatalf("actual slot-9 control did not certify: %v", err)
	}
	releaseOwnerProposal()
	select {
	case err := <-callerB:
		if err != nil {
			t.Fatalf("caller B failed after its owned barrier completed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("caller B did not finish after its owned barrier completed", ctx.Err())
	}
	releaseOwnerWait()
	select {
	case err := <-callerA:
		if err != nil {
			t.Fatalf("caller A failed to join the matching remote observation owner: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("caller A did not finish after the matching owner completed", ctx.Err())
	}

	if isBarrier, err := types.DecodeReadBarrier(ownerBarrier); err != nil || !isBarrier {
		t.Fatalf("registered owner value is not a pure barrier: barrier=%t err=%v", isBarrier, err)
	}
	if decision, ok := fixture.node.core.CertifiedValue(9); !ok || !bytes.Equal(decision.Value, pendingControl) {
		t.Fatalf("the exact observed foreign control was not retained at slot 9: ok=%t decision=%+v", ok, decision)
	}
	if decision, ok := fixture.node.core.CertifiedValue(10); ok && !bytes.Equal(decision.Value, ownerBarrier) {
		t.Fatalf("a second barrier was not the registered owner's exact value: %+v", decision)
	}
	if fixture.node.core.Tip() < 9 || fixture.node.core.Tip() > 10 || fixture.node.material.Tip() != uint64(fixture.node.core.Tip()) {
		t.Fatalf("joined admission did not finish only the actual observed/owner suffix: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	if _, ok := fixture.node.core.CertifiedValue(11); ok {
		t.Fatal("caller A submitted a second barrier after joining B's owner")
	}
}

func TestManagedGraphAdmissionPostReadIndexDoesNotReuseOwnerForFreshGap(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	readTipRelease := make(chan struct{})
	var readTipReleaseOnce sync.Once
	t.Cleanup(func() { readTipReleaseOnce.Do(func() { close(readTipRelease) }) })
	readTipGate := &graphAdmissionReadTipGate{blockCalls: 2, entered: make(chan struct{}, 2), release: readTipRelease}
	fixture.transport.readTipGate.Store(readTipGate)

	pendingControl := managedGraphRaceControlValue()
	ownerProposalEntered := make(chan []byte, 1)
	ownerProposalRelease := make(chan struct{})
	var ownerReleaseOnce sync.Once
	t.Cleanup(func() { ownerReleaseOnce.Do(func() { close(ownerProposalRelease) }) })
	previousProposalHook := localtesthooks.SetProposalEntry(func(value []byte) {
		if bytes.Equal(value, pendingControl) {
			return
		}
		if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
			return
		}
		ownerProposalEntered <- bytes.Clone(value)
		<-ownerProposalRelease
	})
	t.Cleanup(previousProposalHook)

	callerA := make(chan error, 1)
	go func() { callerA <- fixture.node.admitInternalApplication(ctx) }()
	for range 2 {
		select {
		case <-readTipGate.entered:
		case <-ctx.Done():
			t.Fatal("caller A did not reach both real ReadIndex peer requests", ctx.Err())
		}
	}
	foreignOwner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	callerB := make(chan error, 1)
	go func() { callerB <- fixture.node.admitInternalApplication(ctx) }()
	awaitManagedGraphAdmissionObservation(t, fixture, callerB, 9)
	var ownerBarrier []byte
	select {
	case ownerBarrier = <-ownerProposalEntered:
	case <-ctx.Done():
		t.Fatal("validated owner did not reach its pre-proposal gate", ctx.Err())
	}
	if bytes.Equal(ownerBarrier, pendingControl) {
		t.Fatal("private owner reused the foreign slot-9 proposal value")
	}

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, foreignOwner); err != nil {
		t.Fatalf("actual slot-9 control did not certify: %v", err)
	}
	ordinary, err := types.EncodeManagedGraphExecution(types.GraphCommand{
		RequestID: "post-readindex-fresh-gap",
		Cypher:    "CREATE (:PostReadIndexFreshGap {original:true})",
	})
	if err != nil {
		t.Fatal(err)
	}
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	ordinaryHash := quepaxa.ValueHash(sha256.Sum256(ordinary))
	if _, err := fixture.node.core.Record(ctx, quepaxa.RecordRequest{
		Slot: 10, Step: 4, ConfigID: 1,
		Proposal: quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: ordinaryHash, Value: bytes.Clone(ordinary)},
	}); err != nil {
		t.Fatalf("record the actual fresh ordinary gap at slot 10: %v", err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || progress.RecorderHashIsCertified || progress.RecorderHash != ordinaryHash {
		t.Fatalf("fixture did not create the exact new uncertified ordinary gap: %+v", progress)
	}

	readTipReleaseOnce.Do(func() { close(readTipRelease) })
	select {
	case err := <-callerA:
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("caller A reused the old R=9 observation for fresh ordinary R=10: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("caller A waited on the old owner instead of refusing the fresh R=10 gap")
	}
	if _, ok := fixture.node.core.CertifiedValue(10); ok {
		t.Fatal("the fresh ordinary slot 10 was certified while B's private proposal remained held")
	}
	stored, storedOK := fixture.node.core.Value(ordinaryHash)
	if !storedOK || !bytes.Equal(stored, ordinary) {
		t.Fatalf("refusal did not preserve the exact fresh ordinary bytes: stored=%t exact=%t", storedOK, bytes.Equal(stored, ordinary))
	}

	cancel()
	ownerReleaseOnce.Do(func() { close(ownerProposalRelease) })
	select {
	case err := <-callerB:
		if err == nil {
			t.Fatal("canceled private owner unexpectedly completed after the fresh-gap refusal")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("private owner did not stop after cancellation")
	}
	progress = fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || progress.RecorderHashIsCertified || progress.RecorderHash != ordinaryHash {
		t.Fatalf("fresh gap changed after owner cancellation: %+v", progress)
	}
}
