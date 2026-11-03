//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type managedGraphAdmissionLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *managedGraphAdmissionLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *managedGraphAdmissionLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestManagedControlObservationBudgetIsSharedAndOneShot(t *testing.T) {
	ctx := WithManagedControlObservationBudget(context.Background())
	budget := managedControlObservationBudgetFromContext(ctx)
	if budget == nil {
		t.Fatal("managed control observation budget was not installed")
	}
	if nested := WithManagedControlObservationBudget(ctx); managedControlObservationBudgetFromContext(nested) != budget {
		t.Fatal("nested initial/execution scope replaced the public operation budget")
	}
	if !budget.consume() || budget.consume() {
		t.Fatal("managed control observation fallback budget was not one-shot")
	}
}

func TestManagedGraphAdmissionDoesNotReobserveAfterBudgetIsSpent(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "second-gap-ordinary", Cypher: "CREATE (:SecondGap {n:1})"})
	if err != nil {
		t.Fatal(err)
	}
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	proposal := quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: quepaxa.ValueHash(sha256.Sum256(value)), Value: bytes.Clone(value)}
	if _, err := fixture.node.core.Record(ctx, quepaxa.RecordRequest{Slot: 9, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatalf("record second-gap ordinary proposal locally: %v", err)
	}
	ctx = WithManagedControlObservationBudget(ctx)
	if !managedControlObservationBudgetFromContext(ctx).consume() {
		t.Fatal("failed to spend operation budget")
	}
	var observerRechecks atomic.Int32
	restoreObserver := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" {
			observerRechecks.Add(1)
		}
	})
	t.Cleanup(restoreObserver)
	var barriers atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if isBarrier, err := types.DecodeReadBarrier(value); err == nil && isBarrier {
			barriers.Add(1)
		}
	})
	t.Cleanup(restoreProposal)
	response, err := fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{RequestID: "must-not-pass-second-gap", Cypher: "CREATE (:MustNotPass {n:1})"})
	if err == nil || response.MutationReceipt.Slot != 0 {
		t.Fatalf("second unresolved gap was admitted: response=%+v err=%v", response, err)
	}
	if observerRechecks.Load() != 0 || barriers.Load() != 0 || fixture.node.core.Tip() != 8 || fixture.node.material.Tip() != 8 {
		t.Fatalf("spent budget caused another observer/control round: observer_rechecks=%d barriers=%d certified=%d applied=%d", observerRechecks.Load(), barriers.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionRefusesSecondGapAfterActualPrefixFallback(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = WithManagedControlObservationBudget(ctx)
	budget := managedControlObservationBudgetFromContext(ctx)
	logOutput := &managedGraphAdmissionLogBuffer{}
	previousLogOutput := log.Writer()
	log.SetOutput(logOutput)
	defer log.SetOutput(previousLogOutput)
	barrier := managedGraphRaceControlValue()
	owner := fixture.startHeldValue(ctx, barrier)
	fixture.awaitPendingControl(t)
	pending := fixture.node.core.RuntimeProgressSnapshot()
	if pending.CertifiedTip != 8 || pending.RecorderTip != 9 || pending.RecorderHash != quepaxa.ValueHash(sha256.Sum256(barrier)) {
		t.Fatalf("fixture lacks actual pending slot-9 control: %+v", pending)
	}

	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	defer resumeOnce.Do(func() { close(resume) })
	var observerCalls, barriers, graphProposals atomic.Int32
	var injected atomic.Bool
	var injectedErr error
	var ordinary []byte
	restoreObserver := localtesthooks.Set(func(event string) {
		switch event {
		case "core:managed-control:before-snapshot-recheck":
			observerCalls.Add(1)
			select {
			case <-entered:
			default:
				close(entered)
				<-resume
			}
		case "node:managed-control:validated-prefix-rechecked":
			if !injected.CompareAndSwap(false, true) {
				return
			}
			var err error
			ordinary, err = types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "second-gap-original", Cypher: "CREATE (:SecondGap {original:true})"})
			if err != nil {
				injectedErr = err
				return
			}
			var priority quepaxa.Priority
			for i := range priority {
				priority[i] = 0xff
			}
			_, injectedErr = fixture.node.core.Record(ctx, quepaxa.RecordRequest{Slot: 10, Step: 4, ConfigID: 1, Proposal: quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: quepaxa.ValueHash(sha256.Sum256(ordinary)), Value: bytes.Clone(ordinary)}})
		}
	})
	t.Cleanup(restoreObserver)
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if isBarrier, err := types.DecodeReadBarrier(value); err == nil && isBarrier {
			barriers.Add(1)
		}
		if _, graph, err := types.DecodeManagedGraphExecution(value); err == nil && graph {
			graphProposals.Add(1)
		}
	})
	t.Cleanup(restoreProposal)

	type outcome struct {
		response network.GraphExecuteResponse
		err      error
	}
	result := make(chan outcome, 1)
	go func() {
		response, err := fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{RequestID: "must-stop-after-second-gap", Cypher: "CREATE (:MustNotSubmit {n:1})"})
		result <- outcome{response: response, err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("actual observer did not reach its final validation boundary", ctx.Err())
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual slot-9 control did not certify: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != pending.RecorderHash || !bytes.Equal(decision.Value, barrier) {
		t.Fatalf("slot 9 did not certify the exact original control: found=%t hash=%x", ok, decision.Hash)
	}
	resumeOnce.Do(func() { close(resume) })
	var got outcome
	select {
	case got = <-result:
	case <-ctx.Done():
		t.Fatal("admission did not refuse the second gap", ctx.Err())
	}
	if injectedErr != nil || !injected.Load() || !budget.used.Load() {
		t.Fatalf("actual fallback or ordinary second-gap injection did not complete: injected=%t budget_used=%t err=%v", injected.Load(), budget.used.Load(), injectedErr)
	}
	if got.err == nil || errors.Is(got.err, network.ErrCommitUnknown) || got.response.MutationReceipt.Slot != 0 {
		t.Fatalf("second-gap refusal was reported as success or UNKNOWN: response=%+v err=%v", got.response, got.err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	ordinaryHash := quepaxa.ValueHash(sha256.Sum256(ordinary))
	stored, storedOK := fixture.node.core.Value(ordinaryHash)
	_, secondGapDecided := fixture.node.core.CertifiedValue(10)
	if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || !progress.RecorderHashPresent || progress.RecorderHash != ordinaryHash || progress.RecorderHashIsCertified || secondGapDecided || !storedOK || !bytes.Equal(stored, ordinary) {
		t.Fatalf("second-gap original was lost or recast: progress=%+v stored=%t exact=%t", progress, storedOK, bytes.Equal(stored, ordinary))
	}
	if observerCalls.Load() != 1 || barriers.Load() != 0 || graphProposals.Load() != 0 || fixture.transport.readTipCalls.Load() != 0 || fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 8 {
		t.Fatalf("spent fallback performed another observation/quorum/barrier/application or replay: observers=%d read_tips=%d barriers=%d graph_proposals=%d certified=%d applied=%d", observerCalls.Load(), fixture.transport.readTipCalls.Load(), barriers.Load(), graphProposals.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	logs := logOutput.String()
	if strings.Count(logs, "MANAGED_CONTROL_OBSERVATION_REJECTED reason=validated_prefix_advance") != 1 || strings.Contains(logs, "MANAGED_CONTROL_OBSERVATION_REJECTED reason=reference_validation") || strings.Count(logs, "reason=observation_recheck_budget_exhausted") != 1 {
		t.Fatalf("refusal did not come from the spent-budget guard before another observer: %s", logs)
	}
}

func TestManagedGraphAdmissionLaterWaitFallbackRefusesSecondGapBeforeAnotherReadIndex(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = WithManagedControlObservationBudget(ctx)

	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	defer resumeOnce.Do(func() { close(resume) })
	var runtimeSamples, observerCalls, barriers, graphProposals atomic.Int32
	var owner <-chan error
	var ownerErr, injectedErr error
	var ordinary []byte
	var readTipsAtFallback int32
	var fallback atomic.Bool
	restoreObserver := localtesthooks.Set(func(event string) {
		switch event {
		case "node:managed-runtime-state:after-certified-tip":
			if runtimeSamples.Add(1) == 2 {
				owner = fixture.startHeldControl(ctx)
			}
		case "core:managed-control:before-snapshot-recheck":
			observerCalls.Add(1)
			select {
			case <-entered:
			default:
				close(entered)
				<-resume
			}
		case "node:managed-control:validated-prefix-rechecked":
			if !fallback.CompareAndSwap(false, true) {
				return
			}
			readTipsAtFallback = fixture.transport.readTipCalls.Load()
			var err error
			ordinary, err = types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "later-second-gap", Cypher: "CREATE (:LaterGap {original:true})"})
			if err != nil {
				injectedErr = err
				return
			}
			var priority quepaxa.Priority
			for i := range priority {
				priority[i] = 0xff
			}
			_, injectedErr = fixture.node.core.Record(ctx, quepaxa.RecordRequest{Slot: 10, Step: 4, ConfigID: 1, Proposal: quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: quepaxa.ValueHash(sha256.Sum256(ordinary)), Value: bytes.Clone(ordinary)}})
		}
	})
	t.Cleanup(restoreObserver)
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		if isBarrier, err := types.DecodeReadBarrier(value); err == nil && isBarrier {
			barriers.Add(1)
		}
		if _, graph, err := types.DecodeManagedGraphExecution(value); err == nil && graph {
			graphProposals.Add(1)
		}
	})
	t.Cleanup(restoreProposal)

	type outcome struct {
		response network.GraphExecuteResponse
		err      error
	}
	result := make(chan outcome, 1)
	go func() {
		response, err := fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{RequestID: "later-second-gap-stop", Cypher: "CREATE (:MustNotSubmitLater {n:1})"})
		result <- outcome{response: response, err: err}
	}()
	select {
	case <-fixture.transport.held:
	case <-ctx.Done():
		t.Fatal("later wait did not create the actual held control", ctx.Err())
	case <-time.After(3 * time.Second):
		t.Fatal("later wait did not create the actual held control")
	}
	fixture.awaitPendingControl(t)
	pending := fixture.node.core.RuntimeProgressSnapshot()
	if pending.CertifiedTip != 8 || pending.RecorderTip != 9 || pending.RecorderHash != quepaxa.ValueHash(sha256.Sum256(managedGraphRaceControlValue())) || pending.RecorderHashIsCertified {
		t.Fatalf("later wait lacks exact actual pending control: %+v", pending)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("later observer did not reach its final validation boundary", ctx.Err())
	}
	fixture.transport.releaseHeld()
	select {
	case ownerErr = <-owner:
	case <-ctx.Done():
		t.Fatal("actual slot-9 control did not certify", ctx.Err())
	}
	if ownerErr != nil {
		t.Fatalf("actual slot-9 control did not certify: %v", ownerErr)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	barrier := managedGraphRaceControlValue()
	if !ok || decision.Hash != pending.RecorderHash || !bytes.Equal(decision.Value, barrier) {
		t.Fatalf("later fallback lacks exact slot-9 certificate: found=%t hash=%x", ok, decision.Hash)
	}
	resumeOnce.Do(func() { close(resume) })
	var got outcome
	select {
	case got = <-result:
	case <-ctx.Done():
		t.Fatal("later admission did not refuse the second gap", ctx.Err())
	}
	if !fallback.Load() || injectedErr != nil {
		t.Fatalf("actual fallback or second-gap recording failed: fallback=%t err=%v", fallback.Load(), injectedErr)
	}
	if got.err == nil || errors.Is(got.err, network.ErrCommitUnknown) || got.response.MutationReceipt.Slot != 0 {
		t.Fatalf("later second-gap refusal was reported as success or UNKNOWN: response=%+v err=%v", got.response, got.err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	ordinaryHash := quepaxa.ValueHash(sha256.Sum256(ordinary))
	stored, storedOK := fixture.node.core.Value(ordinaryHash)
	_, decided := fixture.node.core.CertifiedValue(10)
	if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || progress.RecorderHash != ordinaryHash || progress.RecorderHashIsCertified || decided || !storedOK || !bytes.Equal(stored, ordinary) {
		t.Fatalf("later second-gap bytes were lost or certified: progress=%+v stored=%t exact=%t decided=%t", progress, storedOK, bytes.Equal(stored, ordinary), decided)
	}
	if observerCalls.Load() != 1 || barriers.Load() != 1 || graphProposals.Load() != 0 || fixture.transport.readTipCalls.Load() != readTipsAtFallback || fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 8 {
		t.Fatalf("later spent fallback performed another round or replay: observers=%d read_tips=%d baseline=%d barriers=%d graph_proposals=%d certified=%d applied=%d", observerCalls.Load(), fixture.transport.readTipCalls.Load(), readTipsAtFallback, barriers.Load(), graphProposals.Load(), fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func managedGraphSpentBudgetOwnerFixture(t *testing.T) (*managedGraphAdmissionRaceFixture, context.Context, *internalApplicationBinding, *managedGraphControlOperation) {
	t.Helper()
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	ctx = WithManagedControlObservationBudget(ctx)
	budget := managedControlObservationBudgetFromContext(ctx)
	barrier := managedGraphRaceControlValue()
	owner := fixture.startHeldValue(ctx, barrier)
	fixture.awaitPendingControl(t)
	observation, err := fixture.node.core.ObserveManagedAppendOnlyControl(ctx)
	if err != nil || observation.ObservedSlot != 9 || !observation.BelongsTo(fixture.node.core) {
		t.Fatalf("capture actual slot-9 observation before certification: observation=%+v err=%v", observation, err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	t.Cleanup(func() { resumeOnce.Do(func() { close(resume) }) })
	var enteredOnce sync.Once
	restore := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" {
			enteredOnce.Do(func() { close(entered) })
			<-resume
		}
	})
	t.Cleanup(restore)
	result := make(chan error, 1)
	binding := fixture.node.internalApplication.Load()
	go func() { result <- fixture.node.waitManagedAppendOnlyControl(ctx, binding) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("actual fallback observer did not reach its final validation boundary", ctx.Err())
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual slot-9 control did not certify: %v", err)
	}
	resumeOnce.Do(func() { close(resume) })
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("actual prefix fallback failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("actual prefix fallback did not finish", ctx.Err())
	}
	if !budget.used.Load() {
		t.Fatal("actual typed-prefix fallback did not consume the operation budget")
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != quepaxa.ValueHash(sha256.Sum256(barrier)) || !bytes.Equal(decision.Value, barrier) {
		t.Fatalf("fallback did not leave the exact slot-9 certificate: found=%t decision=%+v", ok, decision)
	}
	ownerOperation := &managedGraphControlOperation{
		value: append([]byte(nil), barrier...), hash: sha256.Sum256(barrier), binding: binding,
		observation: &observation, done: make(chan struct{}), slot: 9,
	}
	return fixture, ctx, binding, ownerOperation
}

func installManagedGraphRaceOwner(fixture *managedGraphAdmissionRaceFixture, operation *managedGraphControlOperation, done bool) {
	fixture.node.managedGraphControlMu.Lock()
	fixture.node.managedGraphControl = operation
	fixture.node.managedGraphControlMu.Unlock()
	if done {
		close(operation.done)
	}
}

func TestManagedGraphAdmissionSpentBudgetRejectsFreshGapWithMatchingOwner(t *testing.T) {
	fixture, ctx, binding, owner := managedGraphSpentBudgetOwnerFixture(t)
	installManagedGraphRaceOwner(fixture, owner, false)
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	ordinary, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "matching-owner-fresh-gap", Cypher: "CREATE (:MatchingOwnerGap {original:true})"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.node.core.Record(ctx, quepaxa.RecordRequest{Slot: 10, Step: 4, ConfigID: 1, Proposal: quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: quepaxa.ValueHash(sha256.Sum256(ordinary)), Value: bytes.Clone(ordinary)}})
	if err != nil {
		t.Fatalf("record actual uncertified next-slot application: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	err = fixture.node.waitManagedAppendOnlyControl(waitCtx, binding)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("spent fallback waited on a matching owner despite a fresh gap: %v", err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	stored, storedOK := fixture.node.core.Value(quepaxa.ValueHash(sha256.Sum256(ordinary)))
	if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || progress.RecorderHashIsCertified || !storedOK || !bytes.Equal(stored, ordinary) {
		t.Fatalf("fresh gap was not preserved as exact uncertified recorder data: progress=%+v stored=%t", progress, storedOK)
	}
}

func TestManagedGraphAdmissionSpentBudgetStillJoinsExactOwnerCertificate(t *testing.T) {
	fixture, ctx, binding, owner := managedGraphSpentBudgetOwnerFixture(t)
	installManagedGraphRaceOwner(fixture, owner, true)
	if err := fixture.node.waitManagedGraphOwnedControl(ctx, binding, 9); err != nil {
		t.Fatalf("spent budget rejected the exact completed owner certificate without a new recorder gap: %v", err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 9 || progress.RecorderTip != 9 {
		t.Fatalf("exact owner join did not preserve settled prefix: %+v", progress)
	}
}
