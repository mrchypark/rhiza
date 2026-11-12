//go:build rhiza_local_testhooks

package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

type managedControlStepGate struct {
	*clusterTransport
	step4Held    chan struct{}
	step4Release chan struct{}
	step5Held    chan struct{}
	step5Release chan struct{}
}

func (t *managedControlStepGate) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	summary, err := t.clusterTransport.SendRecord(ctx, to, request)
	if err != nil || request.Slot != 9 {
		return summary, err
	}
	var held, release chan struct{}
	switch request.Step {
	case 4:
		held, release = t.step4Held, t.step4Release
	case 5:
		held, release = t.step5Held, t.step5Release
	default:
		return summary, err
	}
	held <- struct{}{}
	select {
	case <-release:
		return summary, nil
	case <-ctx.Done():
		return Summary{}, ctx.Err()
	}
}

func TestObserveManagedControlAllowsSameValidatedValueISRStepAdvance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cores, base := newManagedControlRaceCluster(t, nil)
	core := cores["n1"] // observe the actual remote recorder, not the proposer-local Record path
	proposer := cores["n2"]
	gate := &managedControlStepGate{
		clusterTransport: base.clusterTransport,
		step4Held:        make(chan struct{}, 2), step4Release: make(chan struct{}),
		step5Held: make(chan struct{}, 2), step5Release: make(chan struct{}),
	}
	for _, memberCore := range cores {
		memberCore.transport = gate
	}
	managedControlRacePrefix(t, core)
	requireManagedControlSharedPrefix(t, ctx, core, proposer)
	if core.Tip() != 8 || proposer.Tip() != 8 {
		t.Fatalf("observer/proposer do not share certified prefix 8: observer=%d proposer=%d", core.Tip(), proposer.Tip())
	}
	leaders, err := proposer.LeaderOrder(9)
	if err != nil || len(leaders) == 0 || leaders[0] == proposer.nodeID {
		t.Fatalf("slot 9 must use the actual non-leader proposal path: leaders=%v err=%v", leaders, err)
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
	var nonce [ReadBarrierNonceSize]byte
	for i := range nonce {
		nonce[i] = byte(91 + i)
	}
	value := EncodeReadBarrier(nonce)
	hash := ValueHash(sha256.Sum256(value))
	type proposalResult struct {
		slot Slot
		err  error
	}
	proposalDone := make(chan proposalResult, 1)
	go func() { slot, _, err := proposer.Propose(ctx, value); proposalDone <- proposalResult{slot, err} }()
	for i := 0; i < 2; i++ {
		select {
		case <-gate.step4Held:
		case <-ctx.Done():
			t.Fatalf("both Step-4 responses were not held: %v", ctx.Err())
		}
	}
	var hookHit atomic.Bool
	var observed ManagedAppendOnlyControlObservation
	var observationErr error
	var freshSnapshot managedControlSnapshot
	var freshSnapshotErr error
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || !hookHit.CompareAndSwap(false, true) {
			return
		}
		close(gate.step4Release)
		for i := 0; i < 2; i++ {
			select {
			case <-gate.step5Held:
			case <-ctx.Done():
				t.Errorf("both actual Step-5 Records did not reach held responses: %v", ctx.Err())
			}
		}
		core.mu.RLock()
		state := core.recorders[9]
		durable := core.valueDurable[hash]
		core.mu.RUnlock()
		if state.Step != 5 || state.FirstCurrent == nil || state.FirstCurrent.Hash != hash || !durable {
			t.Errorf("Step-5 state is not durable at the observer core: step=%d first=%v durable=%v", state.Step, state.FirstCurrent, durable)
		}
		if _, certified := core.CertifiedValue(9); certified {
			t.Error("slot 9 certified while both actual Step-5 responses were still held")
		}
		freshSnapshot, freshSnapshotErr = core.managedControlSnapshot()
	})
	observed, observationErr = core.ObserveManagedAppendOnlyControl(ctx)
	restore()
	if !hookHit.Load() {
		t.Fatal("observer did not reach the final snapshot hook")
	}
	if freshSnapshotErr != nil || freshSnapshot.committed != 8 || freshSnapshot.observed != 9 || freshSnapshot.step != 5 || len(freshSnapshot.refs) != 3 {
		t.Fatalf("held actual Step-5 ISR snapshot=%+v err=%v", freshSnapshot, freshSnapshotErr)
	}
	if observationErr != nil || observed.ObservedSlot != freshSnapshot.observed || observed.CommittedTip != freshSnapshot.committed || observed.ConfigID != freshSnapshot.configID || observed.Step != freshSnapshot.step || len(observed.Proposals) != len(freshSnapshot.refs) {
		t.Fatalf("fresh Step-5 observation differs from held ISR: observation=%+v refs=%+v err=%v", observed, freshSnapshot.refs, observationErr)
	}
	for index, ref := range freshSnapshot.refs {
		proposal := observed.Proposals[index]
		if ref.proposal.Hash != hash || !bytes.Equal(ref.value, value) || proposal.Role != ref.role || proposal.Hash != ref.proposal.Hash || proposal.Priority != ref.proposal.Priority || proposal.ProposerID != ref.proposal.ProposerID {
			t.Fatalf("fresh observation reference %d does not match validated actual ISR: got=%+v want=%+v bytes_match=%v", index, proposal, ref, bytes.Equal(ref.value, value))
		}
	}
	close(gate.step5Release)
	select {
	case result := <-proposalDone:
		if result.err != nil || result.slot != 9 {
			t.Fatalf("actual original control proposal=%+v", result)
		}
	case <-ctx.Done():
		t.Fatalf("proposal did not certify after gates released: %v", ctx.Err())
	}
	certified, ok := core.CertifiedValue(9)
	if !ok || certified.Hash != hash || observationErr != nil {
		t.Fatalf("original control lacks actual slot-9 certificate: present=%v hash=%x observerErr=%v", ok, certified.Hash, observationErr)
	}
}

func TestObserveManagedControlRejectsNewOrdinaryReferenceDuringISRAdvance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cores, base := newManagedControlRaceCluster(t, nil)
	core := cores["n1"] // observe a remote recorder so gate completion establishes its local ISR
	proposer := cores["n2"]
	gate := &managedControlStepGate{
		clusterTransport: base.clusterTransport,
		step4Held:        make(chan struct{}, 2), step4Release: make(chan struct{}),
		step5Held: make(chan struct{}, 2), step5Release: make(chan struct{}),
	}
	for _, memberCore := range cores {
		memberCore.transport = gate
	}
	managedControlRacePrefix(t, core)
	requireManagedControlSharedPrefix(t, ctx, core, proposer)
	proposalCtx, cancelProposal := context.WithCancel(ctx)
	t.Cleanup(func() {
		cancelProposal()
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
	control := EncodeReadBarrier([ReadBarrierNonceSize]byte{17, 18, 19})
	proposalDone := make(chan error, 1)
	go func() { _, _, err := proposer.Propose(proposalCtx, control); proposalDone <- err }()
	for i := 0; i < 2; i++ {
		select {
		case <-gate.step4Held:
		case <-ctx.Done():
			t.Fatalf("actual Step-4 responses were not held: %v", ctx.Err())
		}
	}
	select {
	case <-base.localSynced:
	case <-ctx.Done():
		t.Fatalf("observer local Step-4 ISR was not durable: %v", ctx.Err())
	}
	var injected atomic.Bool
	var observationErr error
	var freshSnapshot managedControlSnapshot
	var freshSnapshotErr error
	var ordinaryHash ValueHash
	ordinary := []byte("new ordinary value during validated control observation")
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || !injected.CompareAndSwap(false, true) {
			return
		}
		proposal := newProposal(highestPriority, "n3", ordinary)
		ordinaryHash = proposal.Hash
		if err := core.StageValue(proposal.Hash, ordinary); err != nil {
			t.Errorf("stage actual ordinary value: %v", err)
			return
		}
		if _, err := core.Record(ctx, RecordRequest{Slot: 9, Step: 5, ConfigID: 1, Proposal: proposal}); err != nil {
			t.Errorf("record actual ordinary Step-5 reference: %v", err)
		}
		freshSnapshot, freshSnapshotErr = core.managedControlSnapshot()
	})
	_, observationErr = core.ObserveManagedAppendOnlyControl(ctx)
	restore()
	if !injected.Load() || observationErr == nil || !errors.Is(observationErr, ErrManagedControlObservationUnavailable) || IsManagedControlPrefixAdvance(observationErr) {
		t.Fatalf("new ordinary reference was not rejected generically: injected=%v err=%v", injected.Load(), observationErr)
	}
	ordinaryPresent := false
	for _, ref := range freshSnapshot.refs {
		if ref.proposal.Hash == ordinaryHash && bytes.Equal(ref.value, ordinary) {
			ordinaryPresent = true
		}
	}
	if freshSnapshotErr != nil || freshSnapshot.committed != 8 || freshSnapshot.observed != 9 || freshSnapshot.step != 5 || !ordinaryPresent {
		t.Fatalf("ordinary-value negative did not produce a valid same-C/S Step-5 ISR: snapshot=%+v ordinary_present=%v err=%v", freshSnapshot, ordinaryPresent, freshSnapshotErr)
	}
	cancelProposal()
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
	select {
	case <-proposalDone:
	case <-ctx.Done():
		t.Fatalf("held original proposal did not terminate after cleanup: %v", ctx.Err())
	}
}

func TestObserveManagedControlRecheckDiagnosticIsFixedAndBounded(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	diagnostic := managedControlRecheckDiagnostic{
		cause: "refs_uncovered", oldCommitted: 8, oldObserved: 9, oldStep: 4,
		freshCommitted: 8, freshObserved: 9, freshStep: 5, freshKnown: true, freshStepKnown: true,
		authoritySame: true, authoritySameKnown: true, refsCovered: false, refsCoveredKnown: true,
	}
	logManagedControlRecheckDiagnostic(diagnostic)
	line := output.String()
	for _, field := range []string{
		"MANAGED_CONTROL_OBSERVATION_REJECTED reason=refs_uncovered",
		"prefix_recheck=false authority_same_known=true authority_same=true refs_covered_known=true refs_covered=false",
		"old_known=true old_c=8 old_r=9 old_step=4 fresh_known=true fresh_c=8 fresh_r=9 fresh_step_known=true fresh_step=5",
	} {
		if !strings.Contains(line, field) {
			t.Fatalf("bounded diagnostic omitted %q: %q", field, line)
		}
	}
	for _, reason := range []string{"runtime_role_or_profile", "slot_not_next", "isr_shape", "voter_configuration", "proposal_value"} {
		if managedControlReaderCause(reason) != reason {
			t.Fatalf("fixed reader cause %q was not preserved", reason)
		}
	}
	if managedControlReaderCause("raw private error text") != "final_snapshot_mismatch" {
		t.Fatal("unrecognized reader text escaped fixed cause classification")
	}
}
