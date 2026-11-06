//go:build rhiza_local_testhooks

package quepaxa

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestObserveManagedControlAllowsCoveredSameStepAggregateAdvance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cores, base := newManagedControlRaceCluster(t, nil)
	observer := cores["n1"]
	proposer := cores["n2"]
	gate := &managedControlStepGate{
		clusterTransport: base.clusterTransport,
		step4Held:        make(chan struct{}, 2), step4Release: make(chan struct{}),
		step5Held: make(chan struct{}, 2), step5Release: make(chan struct{}),
	}
	for _, core := range cores {
		core.transport = gate
	}
	managedControlRacePrefix(t, observer)
	requireManagedControlSharedPrefix(t, ctx, observer, proposer)
	if observer.Tip() != 8 || proposer.Tip() != 8 {
		t.Fatalf("observer/proposer do not share certified prefix 8: observer=%d proposer=%d", observer.Tip(), proposer.Tip())
	}
	t.Cleanup(func() {
		select {
		case <-gate.step4Release:
		default:
			close(gate.step4Release)
		}
		select {
		case <-gate.step5Release:
		default:
			close(gate.step5Release)
		}
	})
	value := EncodeReadBarrier([ReadBarrierNonceSize]byte{91, 92, 93})
	type proposalResult struct {
		slot Slot
		err  error
	}
	proposalDone := make(chan proposalResult, 1)
	go func() {
		slot, _, err := proposer.Propose(ctx, value)
		proposalDone <- proposalResult{slot: slot, err: err}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-gate.step4Held:
		case <-ctx.Done():
			t.Fatalf("actual Step-4 responses were not held: %v", ctx.Err())
		}
	}
	close(gate.step4Release)
	for i := 0; i < 2; i++ {
		select {
		case <-gate.step5Held:
		case <-ctx.Done():
			t.Fatalf("actual Step-5 records were not held: %v", ctx.Err())
		}
	}

	var hookHit atomic.Bool
	var injectedErr error
	var observed ManagedAppendOnlyControlObservation
	var observationErr error
	var original managedControlSnapshot
	var fresh managedControlSnapshot
	var freshErr error
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || !hookHit.CompareAndSwap(false, true) {
			return
		}
		var err error
		original, err = observer.managedControlSnapshot()
		if err != nil {
			injectedErr = err
			return
		}
		if original.committed != 8 || original.observed != 9 || original.step != 5 || len(original.refs) != 3 {
			injectedErr = errors.New("unexpected original same-step ISR")
			return
		}
		// This is a real durable Record at the existing step. Reusing the
		// already-validated control bytes keeps exact value coverage while the
		// proposal ordering advances only AggregateCurrent.
		proposal := newProposal(highestPriority, "n1", value)
		if _, err := observer.Record(ctx, RecordRequest{Slot: 9, Step: 5, ConfigID: 1, Proposal: proposal}); err != nil {
			injectedErr = err
			return
		}
		fresh, freshErr = observer.managedControlSnapshot()
	})
	observed, observationErr = observer.ObserveManagedAppendOnlyControl(ctx)
	restore()
	if !hookHit.Load() || injectedErr != nil {
		t.Fatalf("same-step durable Record hook=%v injectionErr=%v", hookHit.Load(), injectedErr)
	}
	if freshErr != nil || fresh.committed != original.committed || fresh.observed != original.observed || fresh.step != original.step {
		t.Fatalf("fresh same-step snapshot=%+v original=%+v err=%v", fresh, original, freshErr)
	}
	if !sameProposal(&original.refs[0].proposal, &fresh.refs[0].proposal) {
		// The first-current reference is immutable within a step.
		t.Fatalf("same-step Record changed FirstCurrent: before=%+v after=%+v", original.refs[0], fresh.refs[0])
	}
	if !sameProposal(&original.refs[2].proposal, &fresh.refs[2].proposal) {
		t.Fatalf("same-step Record changed AggregatePrior: before=%+v after=%+v", original.refs[2], fresh.refs[2])
	}
	if compareProposal(&fresh.refs[1].proposal, &original.refs[1].proposal) <= 0 || fresh.refs[1].proposal.Priority != highestPriority || fresh.refs[1].proposal.ProposerID != "n1" || fresh.refs[1].proposal.Hash != fresh.refs[0].proposal.Hash {
		t.Fatalf("actual same-step Record did not advance only AggregateCurrent: %+v", fresh.refs)
	}
	if observationErr != nil || observed.CommittedTip != 8 || observed.ObservedSlot != 9 || observed.Step != 5 || len(observed.Proposals) != len(fresh.refs) {
		t.Fatalf("covered same-step advance was not returned as a current observation: observation=%+v refs=%+v err=%v", observed, fresh.refs, observationErr)
	}
	for i, ref := range fresh.refs {
		got := observed.Proposals[i]
		if got.Role != ref.role || got.Hash != ref.proposal.Hash || got.Priority != ref.proposal.Priority || got.ProposerID != ref.proposal.ProposerID {
			t.Fatalf("returned proposal %d does not carry fresh ISR metadata: got=%+v want=%+v", i, got, ref)
		}
	}
	close(gate.step5Release)
	select {
	case result := <-proposalDone:
		if result.err != nil || result.slot != 9 {
			t.Fatalf("actual pending control proposal=%+v", result)
		}
	case <-ctx.Done():
		t.Fatalf("pending control did not finish after releasing recorders: %v", ctx.Err())
	}
}

func requireManagedControlSharedPrefix(t *testing.T, ctx context.Context, source, target *Core) {
	t.Helper()
	// Proposal completion covers a quorum, not every remote learner delivery.
	if err := target.WaitTip(ctx, 8); err != nil {
		t.Fatalf("wait for actual proposer prefix 8: %v", err)
	}
	sourceHash, sourceOK := source.PrefixHash(8)
	targetHash, targetOK := target.PrefixHash(8)
	if !sourceOK || !targetOK || sourceHash != targetHash {
		t.Fatal("observer/proposer certified prefix 8 differs")
	}
	for slot := Slot(1); slot <= 8; slot++ {
		want, wantOK := source.CertifiedValue(slot)
		got, gotOK := target.CertifiedValue(slot)
		if !wantOK || !gotOK || len(want.Certificate) == 0 || !bytes.Equal(want.Certificate, got.Certificate) || !bytes.Equal(want.Value, got.Value) {
			t.Fatalf("observer/proposer certified value differs at slot %d", slot)
		}
	}
}

func TestManagedControlQuorumPrefixRequiresProposerDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cores, transport := newManagedControlRaceCluster(t, nil)
	transport.mu.Lock()
	transport.dropDecision["n2"] = true
	transport.mu.Unlock()
	observer, proposer := cores["n1"], cores["n2"]
	managedControlRacePrefix(t, observer)
	if err := cores["n3"].WaitTip(ctx, 8); err != nil {
		t.Fatalf("actual n1/n3 completion quorum: %v", err)
	}
	if proposer.Tip() >= 8 {
		t.Fatal("undelivered proposer unexpectedly has certified prefix 8")
	}
	if _, ok := proposer.PrefixHash(8); ok {
		t.Fatal("undelivered proposer unexpectedly has prefix hash 8")
	}
	values, tip, err := observer.DecisionsFrom(1, 8)
	if err != nil || tip != 8 || len(values) != 8 {
		t.Fatalf("actual quorum-certified prefix: values=%d tip=%d err=%v", len(values), tip, err)
	}
	if err := proposer.AcceptCertifiedValues(values); err != nil {
		t.Fatalf("deliver actual certified prefix: %v", err)
	}
	requireManagedControlSharedPrefix(t, ctx, observer, proposer)
}
