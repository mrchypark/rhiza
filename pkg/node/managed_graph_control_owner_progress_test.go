//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphAdmissionAcceptsSuccessfulOwnedBarrierBelowLaterCertifiedTip(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	durabilityReached := make(chan struct{})
	releaseDurability := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseDurability) }) }
	t.Cleanup(release)
	fixture.node.server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if slot != 9 {
			return nil
		}
		close(durabilityReached)
		select {
		case <-releaseDurability:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeld()
	select {
	case <-durabilityReached:
	case <-ctx.Done():
		t.Fatal("owned control did not reach actual durability boundary", ctx.Err())
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("durability hold is not post-certificate/apply: C=%d A=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	ownedDecision, ok := fixture.node.core.CertifiedValue(9)
	if !ok {
		t.Fatal("actual n1 slot-9 owner was not certified before durability hold")
	}
	if isBarrier, err := types.DecodeReadBarrier(ownedDecision.Value); err != nil || !isBarrier {
		t.Fatalf("slot-9 owner is not a certified pure barrier: barrier=%t err=%v", isBarrier, err)
	}

	var nextNonce [types.ReadBarrierNonceSize]byte
	for i := range nextNonce {
		nextNonce[i] = byte(90 + i)
	}
	nextValue := types.EncodeReadBarrier(nextNonce)
	type proposalResult struct {
		slot quepaxa.Slot
		err  error
	}
	remoteNext := make(chan proposalResult, 1)
	go func() {
		slot, _, err := fixture.transport.graphReviewTransport["n2"].Propose(ctx, nextValue)
		remoteNext <- proposalResult{slot: slot, err: err}
	}()
	select {
	case result := <-remoteNext:
		if result.err != nil || result.slot != 10 {
			t.Fatalf("other voter did not certify the actual next slot: slot=%d err=%v", result.slot, result.err)
		}
	case <-ctx.Done():
		t.Fatal("other voter did not finish slot-10 barrier", ctx.Err())
	}
	if fixture.node.core.Tip() != 10 {
		t.Fatalf("n1 did not learn the later certificate before owner completion: C=%d", fixture.node.core.Tip())
	}
	later, ok := fixture.node.core.CertifiedValue(10)
	if !ok || !bytes.Equal(later.Value, nextValue) {
		t.Fatalf("n1 did not retain the exact actual slot-10 certificate: ok=%t decision=%+v", ok, later)
	}
	shortDone := make(chan struct{})
	close(shortDone)
	shortRemoteOperation := &managedGraphControlOperation{
		value: bytes.Clone(ownedDecision.Value), hash: sha256.Sum256(ownedDecision.Value),
		done: shortDone, slot: 9,
	}
	if err := fixture.node.waitManagedAppendOnlyBarrier(ctx, 10, shortRemoteOperation, shortDone, false); err == nil {
		t.Fatal("a valid slot-9 certificate satisfied the actual slot-10 remote minimum")
	}

	foreground := make(chan error, 1)
	go func() { foreground <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case err := <-foreground:
		t.Fatalf("foreground did not remain joined to the successful slot-9 owner: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case err := <-owner:
		if err != nil {
			t.Fatalf("successful slot-9 owner failed after durability release: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("slot-9 owner did not finish after durability release", ctx.Err())
	}
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatalf("foreground rejected successful owner slot 9 after later certified slot 10: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("foreground did not finish after owner completion", ctx.Err())
	}
	if fixture.node.core.Tip() != 10 || fixture.node.material.Tip() != 10 {
		t.Fatalf("successful owner join failed to preserve and replay the later certificate: C=%d A=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionReobservesAfterCompletedRemoteControlBeforeNewGap(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	firstOperation := completeRemoteObservedBarrierForStalePointerTest(t, fixture, ctx)

	var nextNonce [types.ReadBarrierNonceSize]byte
	for i := range nextNonce {
		nextNonce[i] = byte(111 + i)
	}
	nextValue := types.EncodeReadBarrier(nextNonce)
	recordRemotePendingControl(t, fixture, ctx, 11, nextValue)
	awaitManagedGraphActualRecorderGap(t, fixture, 11, nextValue)

	var observationChecks atomic.Int32
	observedFresh := make(chan struct{})
	var observedOnce sync.Once
	restoreObserver := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" {
			observationChecks.Add(1)
			observedOnce.Do(func() { close(observedFresh) })
		}
	})
	t.Cleanup(restoreObserver)
	secondOperationReady := make(chan struct{})
	var secondOperationMu sync.Mutex
	var secondOperation *managedGraphControlOperation
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		isBarrier, err := types.DecodeReadBarrier(value)
		if err != nil || !isBarrier {
			return
		}
		fixture.node.managedGraphControlMu.Lock()
		candidate := fixture.node.managedGraphControl
		fixture.node.managedGraphControlMu.Unlock()
		if candidate == nil || candidate.observation == nil || candidate.observation.ObservedSlot != 11 {
			return
		}
		secondOperationMu.Lock()
		if secondOperation == nil {
			secondOperation = candidate
			close(secondOperationReady)
		}
		secondOperationMu.Unlock()
	})
	t.Cleanup(restoreProposal)
	secondAdmission := make(chan error, 1)
	go func() { secondAdmission <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-observedFresh:
	case <-ctx.Done():
		t.Fatal("second admission did not execute Core's fresh R=11 observation", ctx.Err())
	}
	if observationChecks.Load() == 0 {
		t.Fatal("second admission retained R=11 without executing Core's actual managed-control observation")
	}
	select {
	case <-secondOperationReady:
	case <-ctx.Done():
		t.Fatal("fresh R=11 observation did not launch a Node-owned barrier", ctx.Err())
	}
	secondOperationMu.Lock()
	operation := secondOperation
	secondOperationMu.Unlock()
	select {
	case err := <-secondAdmission:
		if err != nil {
			t.Fatalf("second admission failed after its fresh R=11 observation and certified continuation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("second admission did not complete after R=11 certification", ctx.Err())
	}
	decision11, ok := fixture.node.core.CertifiedValue(11)
	if !ok {
		t.Fatal("second observation did not close the actual R=11 certificate")
	}
	isBarrier, decodeErr := types.DecodeReadBarrier(decision11.Value)
	if decodeErr != nil || !isBarrier || operation.slot < operation.observation.ObservedSlot || operation.observation.ObservedSlot != 11 {
		t.Fatalf("second observation's exact-slot/later barrier contract differs: decode_err=%v is_barrier=%t operation=%+v", decodeErr, isBarrier, operation)
	}
	decision, ok := fixture.node.core.CertifiedValue(operation.slot)
	if !ok || decision.Hash != operation.hash || !bytes.Equal(decision.Value, operation.value) {
		t.Fatalf("second observation's Node-owned barrier lacks its exact certificate: ok=%t decision=%+v operation=%+v", ok, decision, operation)
	}
	if fixture.node.material.Tip() < uint64(operation.slot) {
		t.Fatalf("second admission returned before replaying its Node-owned continuation: applied=%d", fixture.node.material.Tip())
	}
	if fixture.node.core.Tip() < 10 || fixture.node.material.Tip() < 10 || firstOperation.slot != 10 {
		t.Fatalf("completed first prefix was not preserved: first=%+v certified=%d applied=%d", firstOperation, fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionRejectsOrdinaryGapAfterCompletedRemoteControl(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	firstOperation := completeRemoteObservedBarrierForStalePointerTest(t, fixture, ctx)

	ordinaryValue, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "stale-operation-ordinary-gap", Cypher: "CREATE (:NeverAdmitted {id:'ordinary'})"})
	if err != nil {
		t.Fatal(err)
	}
	recordRemotePendingControl(t, fixture, ctx, 11, ordinaryValue)
	awaitManagedGraphActualRecorderGap(t, fixture, 11, ordinaryValue)

	var observationLogs bytes.Buffer
	previousLogOutput := log.Writer()
	log.SetOutput(&observationLogs)
	t.Cleanup(func() { log.SetOutput(previousLogOutput) })
	var barrierProposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if barrier, err := types.DecodeReadBarrier(value); err == nil && barrier {
			barrierProposals.Add(1)
		}
	})
	t.Cleanup(restoreProposal)
	result := make(chan error, 1)
	go func() { result <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("ordinary R=11 recorder value was accepted")
		}
	case <-ctx.Done():
		t.Fatal("ordinary R=11 observation did not fail closed", ctx.Err())
	}
	if !strings.Contains(observationLogs.String(), "MANAGED_CONTROL_OBSERVATION_REJECTED reason=reference_validation") {
		t.Fatalf("ordinary-gap rejection lacks Core's fixed reference-validation observation diagnostic: %q", observationLogs.String())
	}
	if barrierProposals.Load() != 0 {
		t.Fatalf("ordinary-gap rejection proposed %d Node-owned barriers", barrierProposals.Load())
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 10 || progress.RecorderTip != 11 || !progress.RecorderHashPresent || progress.RecorderHashIsCertified || fixture.node.material.Tip() != 10 {
		t.Fatalf("ordinary-gap rejection changed or applied the pending value: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
	retained, found := fixture.node.core.Value(progress.RecorderHash)
	if !found || !bytes.Equal(retained, ordinaryValue) {
		t.Fatal("ordinary-gap rejection did not preserve the exact original pending bytes")
	}
	if firstOperation.slot != 10 {
		t.Fatalf("initial completed-operation schedule was not actual R=9/S=10: %+v", firstOperation)
	}
}

func completeRemoteObservedBarrierForStalePointerTest(t *testing.T, fixture *managedGraphAdmissionRaceFixture, ctx context.Context) *managedGraphControlOperation {
	t.Helper()
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	durabilityReached := make(chan struct{})
	releaseDurability := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseDurability) }) })
	fixture.node.server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if slot != 10 {
			return nil
		}
		close(durabilityReached)
		select {
		case <-releaseDurability:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	remoteOwner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	firstAdmission := make(chan error, 1)
	go func() { firstAdmission <- fixture.node.admitInternalApplication(ctx) }()
	awaitManagedGraphAdmissionObservation(t, fixture, firstAdmission, 9)
	fixture.node.managedGraphControlMu.Lock()
	firstOperation := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if firstOperation == nil || firstOperation.observation == nil || firstOperation.observation.ObservedSlot != 9 {
		t.Fatalf("first admission did not retain its actual R=9 observation: %+v", firstOperation)
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, remoteOwner); err != nil {
		t.Fatalf("actual remote slot-9 control did not certify: %v", err)
	}
	select {
	case <-durabilityReached:
	case <-ctx.Done():
		t.Fatal("Node-owned slot-10 barrier did not reach its actual durability boundary", ctx.Err())
	}
	firstDecision, ok := fixture.node.core.CertifiedValue(10)
	isBarrier, decodeErr := types.DecodeReadBarrier(firstDecision.Value)
	if !ok || decodeErr != nil || !isBarrier {
		t.Fatalf("first observation's Node-owned slot-10 barrier is not certified: ok=%t decision=%+v", ok, firstDecision)
	}
	// Unblock the real Node transport request before waiting for its owner.
	releaseOnce.Do(func() { close(releaseDurability) })
	select {
	case err := <-firstAdmission:
		if err != nil {
			t.Fatalf("first admission did not finish after actual R=9/S=10 certification: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("first admission did not finish after the slot-10 durability release", ctx.Err())
	}
	if firstOperation.slot != 10 || firstOperation.err != nil || firstOperation.observation == nil || firstOperation.observation.ObservedSlot != 9 {
		t.Fatalf("first success did not complete its actual R=9/S=10 observation: %+v", firstOperation)
	}
	select {
	case <-firstOperation.done:
	default:
		t.Fatal("first successful operation was not completed")
	}
	fixture.node.managedGraphControlMu.Lock()
	registered := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if registered != nil && registered != firstOperation {
		t.Fatalf("first completion was replaced by an unrelated registered operation: old=%p current=%p", firstOperation, registered)
	}
	return firstOperation
}

func awaitManagedGraphActualRecorderGap(t *testing.T, fixture *managedGraphAdmissionRaceFixture, slot quepaxa.Slot, want []byte) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		progress := fixture.node.core.RuntimeProgressSnapshot()
		if progress.RecorderTip == slot && progress.CertifiedTip+1 == slot && progress.RecorderHashPresent {
			value, found := fixture.node.core.Value(progress.RecorderHash)
			if found && bytes.Equal(value, want) && !progress.RecorderHashIsCertified {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	t.Fatalf("n1 did not receive the exact actual remote slot-%d recorder value: progress=%+v", slot, progress)
}

func recordRemotePendingControl(t *testing.T, fixture *managedGraphAdmissionRaceFixture, ctx context.Context, slot quepaxa.Slot, value []byte) {
	t.Helper()
	proposal := quepaxa.Proposal{
		ProposerID: "n2", Hash: quepaxa.ValueHash(sha256.Sum256(value)), Value: bytes.Clone(value),
	}
	if _, err := fixture.transport.SendRecord(ctx, "n1", quepaxa.RecordRequest{Slot: slot, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatalf("record actual remote control at slot %d through n1's Record endpoint: %v", slot, err)
	}
}
