//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedGraphAdmissionWaitsForOwnedControlBeforeRecord(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	entered, resume := make(chan struct{}), make(chan struct{})
	var captured atomic.Bool
	var valueMu sync.Mutex
	var ownedValue []byte
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		isBarrier, decodeErr := types.DecodeReadBarrier(value)
		if decodeErr == nil && isBarrier && captured.CompareAndSwap(false, true) {
			valueMu.Lock()
			ownedValue = append([]byte(nil), value...)
			valueMu.Unlock()
			close(entered)
			<-resume
		}
	})
	t.Cleanup(restore)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("owned control did not reach pre-record proposal entry")
	}
	if fixture.node.core.Tip() != 8 || fixture.node.core.RecorderTip() != 8 {
		t.Fatalf("pre-record hold changed Core progress: C=%d R=%d", fixture.node.core.Tip(), fixture.node.core.RecorderTip())
	}
	foreground := make(chan error, 1)
	go func() {
		if err := fixture.node.admitInternalApplication(ctx); err != nil {
			foreground <- err
			return
		}
		_, err := fixture.node.server.GraphExecute(ctx, types.GraphCommand{RequestID: "pre-record-owned-control", Cypher: "CREATE (:PreRecordOwnedControl {ok: true})"})
		foreground <- err
	}()
	select {
	case err := <-foreground:
		t.Fatalf("foreground passed while owned control was before Core.Record: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if fixture.node.core.Tip() != 8 {
		t.Fatalf("foreground submitted during pre-record hold: C=%d", fixture.node.core.Tip())
	}
	close(resume)
	fixture.awaitPendingControl(t)
	valueMu.Lock()
	value := append([]byte(nil), ownedValue...)
	valueMu.Unlock()
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if len(value) == 0 || progress.RecorderHash != sha256.Sum256(value) || progress.RecorderHashIsCertified {
		t.Fatalf("held slot did not match the exact pending owned value: progress=%+v", progress)
	}
	if isBarrier, decodeErr := types.DecodeReadBarrier(value); decodeErr != nil || !isBarrier {
		t.Fatalf("owned encoded value is not a read-barrier: barrier=%t err=%v", isBarrier, decodeErr)
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("owned control failed after pre-record release: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != sha256.Sum256(value) || !bytes.Equal(decision.Value, value) {
		t.Fatalf("certified value did not match the exact owned read-barrier at slot 9: ok=%t", ok)
	}
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatalf("foreground failed after owner completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("foreground remained blocked after owner completion")
	}
	if fixture.node.core.Tip() != 10 {
		t.Fatalf("foreground did not submit once after pre-record owner completion: C=%d", fixture.node.core.Tip())
	}
}

func TestManagedGraphAdmissionOwnerFailureBeforeRecordDoesNotPoisonFutureRequest(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	entered, resume := make(chan struct{}), make(chan struct{})
	var captured atomic.Bool
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		isBarrier, decodeErr := types.DecodeReadBarrier(value)
		if decodeErr == nil && isBarrier && captured.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	})
	t.Cleanup(restore)
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("owner did not reach pre-record boundary")
	}
	waiter := make(chan error, 1)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()
	go func() { waiter <- fixture.node.admitInternalApplication(waitCtx) }()
	select {
	case err := <-waiter:
		t.Fatalf("waiter passed before owner terminal result: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	serverClosing := make(chan struct{})
	serverClosed := make(chan struct{})
	go func() {
		close(serverClosing)
		server.Close()
		close(serverClosed)
	}()
	<-serverClosing
	time.Sleep(100 * time.Millisecond)
	close(resume)
	if err := awaitProposal(t, owner); err == nil {
		t.Fatal("server shutdown did not fail the pre-record owner")
	}
	select {
	case <-serverClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not finish shutdown after pre-record proposal canceled")
	}
	select {
	case err := <-waiter:
		if err == nil {
			t.Fatal("current waiter accepted owner failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("current waiter did not return after owner failure")
	}
	if fixture.node.core.Tip() != 8 || fixture.node.core.RecorderTip() != 8 {
		t.Fatalf("pre-record failure unexpectedly created a recorder entry: C=%d R=%d", fixture.node.core.Tip(), fixture.node.core.RecorderTip())
	}
	if err := fixture.node.admitInternalApplication(waitCtx); err != nil {
		t.Fatalf("future request was poisoned by pre-record owner failure: %v", err)
	}
}

func TestManagedGraphAdmissionRejectsWrongHashDuringOwnedOperation(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	releaseResume := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(releaseResume)
	t.Cleanup(fixture.transport.releaseHeld)
	var captured atomic.Bool
	var ownerValueMu sync.Mutex
	var ownerValue []byte
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		isBarrier, decodeErr := types.DecodeReadBarrier(value)
		if decodeErr == nil && isBarrier && captured.CompareAndSwap(false, true) {
			ownerValueMu.Lock()
			ownerValue = append([]byte(nil), value...)
			ownerValueMu.Unlock()
			close(entered)
			<-resume
		}
	})
	t.Cleanup(restore)
	ownerCtx, cancelOwner := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelOwner()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ownerCtx) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("owned control did not reach pre-record proposal entry")
	}
	competitorCtx, cancelCompetitor := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCompetitor()
	var competitorNonce [types.ReadBarrierNonceSize]byte
	for i := range competitorNonce {
		competitorNonce[i] = byte(77 + i)
	}
	competitorValue := types.EncodeReadBarrier(competitorNonce)
	competitor := make(chan error, 1)
	go func() {
		_, _, err := fixture.node.core.Propose(competitorCtx, competitorValue)
		competitor <- err
	}()
	fixture.awaitPendingControl(t)
	ownerValueMu.Lock()
	owned := append([]byte(nil), ownerValue...)
	ownerValueMu.Unlock()
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if len(owned) == 0 || bytes.Equal(owned, competitorValue) || progress.RecorderHash != sha256.Sum256(competitorValue) || progress.RecorderHashIsCertified {
		t.Fatalf("actual pending recorder state is not the distinct competing proposal: owner=%x competitor=%x progress=%+v", owned, competitorValue, progress)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	admissionErr := fixture.node.admitInternalApplication(ctx)
	if admissionErr == nil || !strings.Contains(admissionErr.Error(), "target=9 recorded=9 certified=8 applied=8") {
		progress := fixture.node.core.RuntimeProgressSnapshot()
		t.Fatalf("active operation accepted a different pending recorder hash or reported an unexpected gate: err=%v progress=%+v", admissionErr, progress)
	}
	if fixture.node.core.Tip() != 8 {
		t.Fatalf("wrong-hash refusal submitted foreground work: C=%d", fixture.node.core.Tip())
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, competitor); err != nil {
		t.Fatalf("actual competing proposal failed after release: %v", err)
	}
	releaseResume()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("owner did not complete after its candidate was displaced: %v", err)
	}
	if fixture.node.core.Tip() != 10 || fixture.node.material.Tip() != 10 {
		t.Fatalf("displaced owner schedule did not complete at the later slot: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	if err := fixture.node.admitInternalApplication(ctx); err != nil {
		t.Fatalf("future request remained poisoned after displaced owner reached valid state: %v", err)
	}
}

func TestManagedGraphAdmissionRejectsActiveControlFromOldBinding(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	entered, resume := make(chan struct{}), make(chan struct{})
	var captured atomic.Bool
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		isBarrier, decodeErr := types.DecodeReadBarrier(value)
		if decodeErr == nil && isBarrier && captured.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	})
	t.Cleanup(restore)
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("owned control did not reach pre-record proposal entry")
	}
	_ = fixture.node.stopInternalApplication()
	if err := fixture.node.installInternalApplicationBinding(context.Background()); err != nil {
		t.Fatalf("install replacement binding: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fixture.node.admitInternalApplication(ctx); err == nil {
		t.Fatal("new binding joined the previous binding's active owner")
	}
	close(resume)
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("old owner cleanup failed: %v", err)
	}
	if err := fixture.node.admitInternalApplication(ctx); err != nil {
		t.Fatalf("future request remained poisoned by the old binding: %v", err)
	}
}

func TestManagedGraphAdmissionReobservesActualProgressBetweenCoreAndMaterializer(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proposal := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	pause, resume := fixture.pauseRuntimeStateObservation(t, 1)
	defer resume()

	gate := make(chan error, 1)
	go func() { gate <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-pause:
	case <-ctx.Done():
		t.Fatal("admission did not reach the certified-tip observation seam")
	}
	if fixture.node.core.Tip() != 8 || fixture.node.material.Tip() != 8 {
		t.Fatalf("observation seam was not at the actual pre-progress cut: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, proposal); err != nil {
		t.Fatalf("actual control did not certify: %v", err)
	}
	if err := fixture.node.replayLocalDecisions(ctx); err != nil {
		t.Fatalf("actual certified control did not apply: %v", err)
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("publisher progress did not advance actual pair: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	resume()
	select {
	case err := <-gate:
		if err != nil {
			t.Fatalf("progressed C=A=9 was rejected from a torn C=8/A=9 sample: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission failed to reobserve after actual Core progress")
	}
}

func TestManagedGraphAdmissionProgressReobservationHonorsCallerCancellation(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proposal := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	pause, resume := fixture.pauseRuntimeStateObservation(t, 1)
	defer resume()

	gateCtx, cancelGate := context.WithCancel(ctx)
	defer cancelGate()
	gate := make(chan error, 1)
	go func() { gate <- fixture.node.admitInternalApplication(gateCtx) }()
	select {
	case <-pause:
	case <-ctx.Done():
		t.Fatal("admission did not reach the certified-tip observation seam")
	}

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, proposal); err != nil {
		t.Fatalf("actual control did not certify: %v", err)
	}
	if err := fixture.node.replayLocalDecisions(ctx); err != nil {
		t.Fatalf("actual certified control did not apply: %v", err)
	}
	cancelGate()
	resume()
	select {
	case err := <-gate:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("progress reobservation did not preserve caller cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not return after caller cancellation")
	}
	if fixture.node.core.Tip() != 9 {
		t.Fatalf("cancelled reobservation submitted another proposal: certified tip=%d", fixture.node.core.Tip())
	}
}

func TestManagedStateWithoutContextRejectsUnstableProgressOnce(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proposal := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	pause, resume := fixture.pauseRuntimeStateObservation(t, 1)
	defer resume()

	stateResult := make(chan error, 1)
	go func() {
		_, err := fixture.node.ManagedState()
		stateResult <- err
	}()
	select {
	case <-pause:
	case <-ctx.Done():
		t.Fatal("contextless ManagedState did not reach the bounded observation seam")
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, proposal); err != nil {
		t.Fatalf("actual control did not certify: %v", err)
	}
	if err := fixture.node.replayLocalDecisions(ctx); err != nil {
		t.Fatalf("actual certified control did not apply: %v", err)
	}
	resume()
	select {
	case err := <-stateResult:
		if !errors.Is(err, network.ErrNotReady) {
			t.Fatalf("contextless ManagedState did not fail closed on one unstable observation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("contextless ManagedState retried indefinitely instead of returning NotReady")
	}
}

func (f *managedGraphAdmissionRaceFixture) pauseRuntimeStateObservation(t *testing.T, target int64) (<-chan struct{}, func()) {
	t.Helper()
	paused := make(chan struct{})
	resumeCh := make(chan struct{})
	var once sync.Once
	var count atomic.Int64
	restore := localtesthooks.Set(func(event string) {
		if event == "node:managed-runtime-state:after-certified-tip" && count.Add(1) == target {
			close(paused)
			<-resumeCh
		}
	})
	resume := func() { once.Do(func() { close(resumeCh) }) }
	t.Cleanup(func() {
		resume()
		restore()
	})
	return paused, resume
}
