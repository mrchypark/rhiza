//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedObservedBarrierDoesNotAdoptUnobservedNextSlot(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(fixture.transport.releaseHeld)

	observed := managedGraphRaceControlValue()
	proposal := quepaxa.Proposal{Hash: sha256.Sum256(observed), Value: observed}
	for id, core := range fixture.transport.graphReviewTransport {
		if _, err := core.Record(ctx, quepaxa.RecordRequest{Slot: 9, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
			t.Fatalf("record actual observed control at %s: %v", id, err)
		}
	}
	if progress := fixture.node.core.RuntimeProgressSnapshot(); progress.RecorderTip != 9 || progress.CertifiedTip != 8 || !progress.RecorderHashPresent || progress.RecorderHash != proposal.Hash {
		t.Fatalf("fixture did not establish exact actual pending R9: %+v", progress)
	}

	controlEntered := make(chan struct{})
	barrierEntered := make(chan struct{})
	resumeBarrier := make(chan struct{})
	var controlOnce, barrierOnce, resumeOnce sync.Once
	releaseBarrier := func() { resumeOnce.Do(func() { close(resumeBarrier) }) }
	t.Cleanup(releaseBarrier)
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		if bytes.Equal(value, observed) {
			controlOnce.Do(func() { close(controlEntered) })
			return
		}
		isBarrier, err := types.DecodeReadBarrier(value)
		if err == nil && isBarrier {
			barrierOnce.Do(func() {
				close(barrierEntered)
				<-resumeBarrier
			})
		}
	})
	t.Cleanup(restore)

	// Capture the validated observation and stop its own proposal before slot
	// reservation before the second voter updates the existing R9 summary.
	admission := make(chan error, 1)
	go func() { admission <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-barrierEntered:
	case err := <-admission:
		t.Fatalf("admission returned before observed barrier reached Core reservation: %v", err)
	case <-ctx.Done():
		t.Fatal("observed barrier did not reach Core reservation")
	}
	// The second voter now runs the real proposer for the already-recorded R9.
	// This ordering keeps its same-slot ISR update after the snapshot is sealed;
	// the fixture still holds its recorder calls until the observed barrier is
	// paused at the pre-reservation seam above.
	otherVoter := make(chan error, 1)
	go func() {
		slot, _, err := fixture.transport.graphReviewTransport["n2"].Propose(ctx, observed)
		if err == nil && slot != 9 {
			err = fmt.Errorf("actual R9 proposer returned slot %d", slot)
		}
		otherVoter <- err
	}()
	select {
	case <-controlEntered:
	case <-ctx.Done():
		t.Fatal("second voter did not enter the actual R9 proposal")
	}

	fixture.transport.releaseHeld()
	select {
	case err := <-otherVoter:
		if err != nil {
			t.Fatalf("second voter did not certify the actual R9: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("second voter did not finish the R9 proposal")
	}
	if decision, ok := fixture.node.core.CertifiedValue(9); !ok || !bytes.Equal(decision.Value, observed) {
		t.Fatalf("slot 9 lacks the exact actual read-barrier certificate: found=%t value=%x", ok, decision.Value)
	}

	ordinary, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "unobserved-slot-ten", Cypher: "CREATE (:UnobservedSlotTen {ok: true})"})
	if err != nil {
		t.Fatal(err)
	}
	ordinaryProposal := quepaxa.Proposal{Hash: sha256.Sum256(ordinary), Value: ordinary}
	for id, core := range fixture.transport.graphReviewTransport {
		summary, err := core.Record(ctx, quepaxa.RecordRequest{Slot: 10, Step: 5, ConfigID: 1, Proposal: ordinaryProposal})
		if err != nil {
			t.Fatalf("record actual ordinary slot-10 value at %s: %v", id, err)
		}
		if summary.Step != 5 || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != ordinaryProposal.Hash {
			t.Fatalf("actual slot-10 record at %s did not retain X as step-5 FirstCurrent: %+v", id, summary)
		}
		stored, ok := core.Value(ordinaryProposal.Hash)
		if !ok || !bytes.Equal(stored, ordinary) {
			t.Fatalf("actual slot-10 Step5 FirstCurrent bytes are unavailable at %s: found=%t value=%x", id, ok, stored)
		}
	}
	for id, core := range fixture.transport.graphReviewTransport {
		progress := core.RuntimeProgressSnapshot()
		if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || !progress.RecorderHashPresent || progress.RecorderHash != ordinaryProposal.Hash {
			t.Fatalf("fixture lacks actual uncommitted ordinary X10 at %s: %+v", id, progress)
		}
	}

	releaseBarrier()
	select {
	case err := <-admission:
		if err == nil {
			t.Fatal("admission accepted a barrier that adopted unobserved X10")
		}
	case <-ctx.Done():
		t.Fatal("admission did not return after the observed-slot proposal resumed")
	}
	if decision, ok := fixture.node.core.CertifiedValue(10); ok {
		t.Fatalf("observed R9 barrier drove an unobserved slot-10 decision: hash=%x value=%x", decision.Hash, decision.Value)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 9 || progress.RecorderTip != 10 || !progress.RecorderHashPresent || progress.RecorderHash != ordinaryProposal.Hash {
		t.Fatalf("slot-10 ordinary recorder value was changed or driven: %+v", progress)
	}
}

func TestManagedObservedBarrierLearnsCertifiedLaterOrdinary(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(fixture.transport.releaseHeld)

	observed := managedGraphRaceControlValue()
	proposal := quepaxa.Proposal{Hash: sha256.Sum256(observed), Value: observed}
	for id, core := range fixture.transport.graphReviewTransport {
		if _, err := core.Record(ctx, quepaxa.RecordRequest{Slot: 9, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
			t.Fatalf("record actual observed control at %s: %v", id, err)
		}
	}
	if progress := fixture.node.core.RuntimeProgressSnapshot(); progress.RecorderTip != 9 || progress.CertifiedTip != 8 || !progress.RecorderHashPresent || progress.RecorderHash != proposal.Hash {
		t.Fatalf("fixture did not establish exact pending R9: %+v", progress)
	}

	ordinary, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "certified-unobserved-slot-ten", Cypher: "CREATE (:CertifiedBeforeBarrier {id:'once'})"})
	if err != nil {
		t.Fatal(err)
	}
	ordinaryHash := sha256.Sum256(ordinary)
	barrierEntered := make(chan struct{})
	resumeBarrier := make(chan struct{})
	var barrierOnce, resumeOnce sync.Once
	releaseBarrier := func() { resumeOnce.Do(func() { close(resumeBarrier) }) }
	t.Cleanup(releaseBarrier)
	var ordinaryProposalEntries atomic.Int32
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		if bytes.Equal(value, ordinary) {
			ordinaryProposalEntries.Add(1)
		}
		isBarrier, decodeErr := types.DecodeReadBarrier(value)
		if decodeErr == nil && isBarrier && !bytes.Equal(value, observed) {
			barrierOnce.Do(func() {
				close(barrierEntered)
				<-resumeBarrier
			})
		}
	})
	t.Cleanup(restore)

	admission := make(chan error, 1)
	go func() { admission <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-barrierEntered:
	case err := <-admission:
		t.Fatalf("admission returned before its observed barrier reached proposal entry: %v", err)
	case <-ctx.Done():
		t.Fatal("observed barrier did not reach proposal entry")
	}
	fixture.node.managedGraphControlMu.Lock()
	operation := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if operation == nil || operation.observation == nil || operation.observation.ObservedSlot != 9 {
		t.Fatalf("admission did not retain the exact R9 observation before the certified suffix: %+v", operation)
	}

	ownerResult := make(chan error, 1)
	go func() {
		slot, _, err := fixture.transport.graphReviewTransport["n2"].Propose(ctx, observed)
		if err == nil && slot != 9 {
			err = fmt.Errorf("actual observed control proposer returned slot %d", slot)
		}
		ownerResult <- err
	}()
	fixture.transport.releaseHeld()
	select {
	case err := <-ownerResult:
		if err != nil {
			t.Fatalf("actual observed control did not certify at R9: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("actual observed control did not finish at R9")
	}
	if decision, ok := fixture.node.core.CertifiedValue(9); !ok || !bytes.Equal(decision.Value, observed) {
		t.Fatalf("R9 lacks the exact observed control certificate: found=%t value=%x", ok, decision.Value)
	}

	ordinarySlot, _, err := fixture.transport.graphReviewTransport["n2"].Propose(ctx, ordinary)
	if err != nil || ordinarySlot != 10 {
		t.Fatalf("actual later ordinary proposal returned slot=%d err=%v, want certified X10", ordinarySlot, err)
	}
	ordinaryDecision, ok := fixture.node.core.CertifiedValue(10)
	if !ok || ordinaryDecision.Hash != quepaxa.ValueHash(ordinaryHash) || !bytes.Equal(ordinaryDecision.Value, ordinary) {
		t.Fatalf("later ordinary proposal lacks its exact actual certificate: found=%t decision=%+v", ok, ordinaryDecision)
	}
	entriesBeforeBarrierRelease := ordinaryProposalEntries.Load()
	if entriesBeforeBarrierRelease != 1 {
		t.Fatalf("ordinary proposal entry count before Node resumes=%d, want one actual proposer", entriesBeforeBarrierRelease)
	}

	releaseBarrier()
	select {
	case err := <-admission:
		if err != nil {
			t.Fatalf("admission did not learn certified X10 and complete its later barrier: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish after the certified X10")
	}
	if ordinaryProposalEntries.Load() != entriesBeforeBarrierRelease {
		t.Fatalf("observed barrier resubmitted certified X10: proposal entries before=%d after=%d", entriesBeforeBarrierRelease, ordinaryProposalEntries.Load())
	}
	if operation == nil || operation.observation == nil || operation.observation.ObservedSlot != 9 || operation.slot != 11 || operation.err != nil {
		t.Fatalf("admission did not retain its exact R9 observation and later S11 barrier: %+v", operation)
	}
	barrier, ok := fixture.node.core.CertifiedValue(11)
	if !ok || barrier.Hash != operation.hash || !bytes.Equal(barrier.Value, operation.value) {
		t.Fatalf("S11 does not contain the exact owned barrier: found=%t decision=%+v", ok, barrier)
	}
	decisions, _, err := fixture.node.core.DecisionsFrom(10, 12)
	if err != nil {
		t.Fatal("read actual certified X10/S11 suffix", err)
	}
	ordinaryCount := 0
	for _, decision := range decisions {
		if bytes.Equal(decision.Value, ordinary) {
			ordinaryCount++
		}
	}
	if ordinaryCount != 1 {
		t.Fatalf("certified ordinary bytes appeared %d times after admission", ordinaryCount)
	}
	query, err := fixture.node.material.GraphQuery(ctx, "MATCH (n:CertifiedBeforeBarrier) RETURN n.id", nil)
	if err != nil || len(query.Rows) != 1 || len(query.Rows[0]) != 1 || query.Rows[0][0] != "once" {
		t.Fatalf("certified X10 was not replayed exactly once: rows=%v err=%v", query.Rows, err)
	}
}
