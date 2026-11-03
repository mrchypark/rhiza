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

func TestManagedGraphAdmissionAfterControlCertifiesDuringObservation(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entered, resume := make(chan struct{}), make(chan struct{})
	var enteredOnce, resumeOnce sync.Once
	releaseObserver := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(releaseObserver)
	var observed atomic.Bool
	restoreObserver := localtesthooks.Set(func(event string) {
		if event == "core:managed-control:before-snapshot-recheck" && observed.CompareAndSwap(false, true) {
			enteredOnce.Do(func() { close(entered) })
			<-resume
		}
	})
	t.Cleanup(restoreObserver)

	var graphProposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		_, graph, err := types.DecodeManagedGraphExecution(value)
		if err == nil && graph {
			graphProposals.Add(1)
		}
	})
	t.Cleanup(restoreProposal)

	barrier := managedGraphRaceControlValue()
	owner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	pending := fixture.node.core.RuntimeProgressSnapshot()
	if pending.CertifiedTip != 8 || pending.RecorderTip != 9 || !pending.RecorderHashPresent || pending.RecorderHashIsCertified || pending.RecorderHash != quepaxa.ValueHash(sha256.Sum256(barrier)) {
		t.Fatalf("fixture lacks the exact real R=C+1 pending control: %+v", pending)
	}
	if _, decided := fixture.node.core.CertifiedValue(9); decided {
		t.Fatal("pending control was already certified before observation")
	}

	type executeResult struct {
		response types.MutationReceipt
		err      error
	}
	executed := make(chan executeResult, 1)
	go func() {
		response, err := fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{
			RequestID: "observation-certified-race",
			Cypher:    "CREATE (:ObservedControlRace {once:true})",
		})
		executed <- executeResult{response: response.MutationReceipt, err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("Core observer did not reach its deterministic final-snapshot boundary", ctx.Err())
	}
	if progress := fixture.node.core.RuntimeProgressSnapshot(); progress.CertifiedTip != 8 || progress.RecorderTip != 9 || progress.RecorderHash != pending.RecorderHash || progress.RecorderHashIsCertified {
		t.Fatalf("observer gate did not retain the exact uncertified control: %+v", progress)
	}

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("actual original control did not certify while observation was paused: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != pending.RecorderHash || !bytes.Equal(decision.Value, barrier) {
		t.Fatalf("slot 9 did not certify the exact observed control: found=%t hash=%x", ok, decision.Hash)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 9 || progress.RecorderTip != 9 || fixture.node.material.Tip() > uint64(progress.CertifiedTip) {
		t.Fatalf("the observed recorder gap did not close cleanly: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}

	releaseObserver()
	select {
	case result := <-executed:
		if result.err != nil {
			t.Fatalf("public Graph admission rejected after the exact pending value became certified: %v", result.err)
		}
		if result.response.Slot != 10 || result.response.Status != types.MutationCommitted {
			t.Fatalf("Graph request did not commit once after fresh admission: %+v", result.response)
		}
	case <-ctx.Done():
		t.Fatal("public Graph execution did not finish after observer revalidation", ctx.Err())
	}
	if graphProposals.Load() != 1 {
		t.Fatalf("public Graph request proposal count=%d, want exactly one", graphProposals.Load())
	}
}

func TestManagedGraphAdmissionKeepsUncertifiedOrdinaryAndCanceledRequestsClosed(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{
		RequestID: "ordinary-not-control",
		Cypher:    "CREATE (:OrdinaryNotControl {once:true})",
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(value)
	var priority quepaxa.Priority
	for i := range priority {
		priority[i] = 0xff
	}
	proposal := quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: quepaxa.ValueHash(hash), Value: bytes.Clone(value)}
	if _, err := fixture.node.core.Record(ctx, quepaxa.RecordRequest{Slot: 9, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatalf("record actual unquorate Graph proposal locally: %v", err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 8 || progress.RecorderTip != 9 || !progress.RecorderHashPresent || progress.RecorderHashIsCertified || progress.RecorderHash != quepaxa.ValueHash(hash) {
		t.Fatalf("ordinary Graph value did not remain an uncertified next-slot record: %+v", progress)
	}
	if _, decided := fixture.node.core.CertifiedValue(9); decided {
		t.Fatal("ordinary Graph proposal unexpectedly has a certificate")
	}

	var graphProposals atomic.Int32
	restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
		_, graph, decodeErr := types.DecodeManagedGraphExecution(value)
		if decodeErr == nil && graph {
			graphProposals.Add(1)
		}
	})
	t.Cleanup(restoreProposal)
	response, executeErr := fixture.node.ExecuteManagedGraph(ctx, types.GraphCommand{
		RequestID: "foreground-after-ordinary-record",
		Cypher:    "CREATE (:ForegroundMustNotPass {once:true})",
	})
	if executeErr == nil || response.MutationReceipt.Slot != 0 {
		t.Fatalf("uncertified ordinary recorder value admitted a Graph request: response=%+v error=%v", response, executeErr)
	}
	if graphProposals.Load() != 0 {
		t.Fatalf("Graph application proposed despite an uncertified ordinary record: count=%d", graphProposals.Load())
	}

	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	response, executeErr = fixture.node.ExecuteManagedGraph(canceled, types.GraphCommand{
		RequestID: "canceled-after-ordinary-record",
		Cypher:    "CREATE (:CanceledMustNotPass {once:true})",
	})
	if !errors.Is(executeErr, context.Canceled) || response.MutationReceipt.Slot != 0 {
		t.Fatalf("cancellation did not remain fail-closed: response=%+v error=%v", response, executeErr)
	}
	if progress := fixture.node.core.RuntimeProgressSnapshot(); progress.CertifiedTip != 8 || progress.RecorderTip != 9 || progress.RecorderHashIsCertified || fixture.node.material.Tip() != 8 {
		t.Fatalf("ordinary unquorate state advanced during rejected/canceled admission: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
	if graphProposals.Load() != 0 {
		t.Fatalf("canceled Graph request was proposed: count=%d", graphProposals.Load())
	}
}
