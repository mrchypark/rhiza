//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
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

	// A second voter runs the real proposer for the already-recorded R9. Its
	// slot-9 recorder calls are held by the fixture until the observed barrier
	// has reached the pre-reservation seam below.
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

	admission := make(chan error, 1)
	go func() { admission <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case <-barrierEntered:
	case err := <-admission:
		t.Fatalf("admission returned before observed barrier reached Core reservation: %v", err)
	case <-ctx.Done():
		t.Fatal("observed barrier did not reach Core reservation")
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
		if summary.Step != 5 || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != ordinaryProposal.Hash || !bytes.Equal(summary.FirstCurrent.Value, ordinary) {
			t.Fatalf("actual slot-10 record at %s did not retain X as step-5 FirstCurrent: %+v", id, summary)
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
