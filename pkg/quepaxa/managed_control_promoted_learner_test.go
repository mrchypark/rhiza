//go:build rhiza_local_testhooks

package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

type promotedLearnerHeldTransport struct {
	*clusterTransport
	slot        atomic.Uint64
	held        chan struct{}
	release     chan struct{}
	request     chan RecordRequest
	summary     chan Summary
	heldOnce    sync.Once
	releaseOnce sync.Once
	recordOnce  sync.Once
}

type promotedLearnerFixture struct {
	initial       Cluster
	target        Cluster
	cores         map[NodeID]*Core
	learner       *Core
	transport     *promotedLearnerHeldTransport
	committedSlot Slot
}

func (t *promotedLearnerHeldTransport) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	holdSlot := Slot(t.slot.Load())
	if request.Slot == holdSlot && to == "n3" {
		select {
		case <-t.release:
		case <-ctx.Done():
			return Summary{}, ctx.Err()
		}
	}
	summary, err := t.clusterTransport.SendRecord(ctx, to, request)
	if err != nil {
		return summary, err
	}
	if request.Slot == holdSlot && to == "n4" {
		t.recordOnce.Do(func() {
			request.Proposal = *cloneProposal(&request.Proposal)
			t.request <- request
		})
		t.summary <- summary
		t.heldOnce.Do(func() { close(t.held) })
		select {
		case <-t.release:
		case <-ctx.Done():
			return Summary{}, ctx.Err()
		}
	}
	return summary, nil
}

func (t *promotedLearnerHeldTransport) releaseHeld() {
	t.releaseOnce.Do(func() { close(t.release) })
}

func newPromotedLearnerFixture(t *testing.T, ctx context.Context) *promotedLearnerFixture {
	initial := Cluster{ConfigID: 1}
	base := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool)}
	transport := &promotedLearnerHeldTransport{clusterTransport: base, held: make(chan struct{}), release: make(chan struct{}), request: make(chan RecordRequest, 1), summary: make(chan Summary, 1)}
	t.Cleanup(transport.releaseHeld)
	cores := make(map[NodeID]*Core, 3)
	wals := make(map[NodeID]*qlog.WAL, 4)
	deferCloseWAL := func(wal *qlog.WAL) {
		t.Helper()
		t.Cleanup(func() { _ = wal.Close() })
	}
	identityFor := func(id NodeID) []byte {
		identity := make([]byte, 32)
		for i := range identity {
			identity[i] = byte(len(id) + i + int(id[len(id)-1]))
		}
		return identity
	}
	openRegisteredWAL := func(id NodeID) (*qlog.WAL, []byte) {
		t.Helper()
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(id)))
		if err != nil {
			t.Fatal(err)
		}
		deferCloseWAL(wal)
		identity := identityFor(id)
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		return wal, identity
	}
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		wal, identity := openRegisteredWAL(id)
		wals[id] = wal
		initial.Members = append(initial.Members, Member{ID: id, WALIdentity: hex.EncodeToString(identity)})
	}
	for _, member := range initial.Members {
		wal := wals[member.ID]
		identity, registered := wal.Identity()
		if !registered {
			t.Fatalf("voter %s WAL identity was not registered", member.ID)
		}
		managed := managedNoPVCBinding(identity, managedGraphRequestAppendOnlyApplicationProtocol)
		managed.Binding.Generation = 0
		core, err := New(Config{
			NodeID: member.ID, Cluster: initial, WAL: wal, Transport: transport,
			EnableReconfiguration: true, ManagedAppendOnly: managed,
		})
		if err != nil {
			t.Fatalf("create voter %s: %v", member.ID, err)
		}
		cores[member.ID] = core
		base.cores[member.ID] = core
	}
	learnerWAL, learnerIdentity := openRegisteredWAL("n4")
	wals["n4"] = learnerWAL
	learnerManaged := managedNoPVCBinding(learnerIdentity, managedGraphRequestAppendOnlyApplicationProtocol)
	learnerManaged.Binding.Generation = 0
	learner, err := NewLearner(Config{
		NodeID: "n4", Cluster: initial, WAL: learnerWAL, Transport: transport,
		EnableReconfiguration: true,
		ManagedAppendOnly:     learnerManaged,
	})
	if err != nil {
		t.Fatalf("create real managed learner: %v", err)
	}
	base.cores["n4"] = learner
	for _, core := range cores {
		core.reconfigAdmission = func(ctx context.Context, target Cluster, through Slot, prefix [32]byte) error {
			var added *Member
			for i := range target.Members {
				if target.Members[i].ID == "n4" {
					added = &target.Members[i]
					break
				}
			}
			if added != nil {
				if added.WALIdentity != learner.WALIdentity() {
					return fmt.Errorf("target lacks the actual learner WAL identity")
				}
				if err := learner.WaitTip(ctx, through); err != nil {
					return err
				}
				if err := learner.EnsureDurableThrough(ctx, through); err != nil {
					return err
				}
				got, ok := learner.PrefixHash(through)
				if !ok || got != prefix {
					return fmt.Errorf("actual learner prefix differs from reconfiguration prefix")
				}
				return nil
			}
			for _, member := range target.Members {
				voter := cores[member.ID]
				if voter == nil || voter.WALIdentity() != member.WALIdentity {
					return fmt.Errorf("removal target lacks the existing voter WAL identity")
				}
				if err := voter.WaitTip(ctx, through); err != nil {
					return err
				}
				if err := voter.EnsureDurableThrough(ctx, through); err != nil {
					return err
				}
				got, ok := voter.PrefixHash(through)
				if !ok || got != prefix {
					return fmt.Errorf("removal target prefix differs from reconfiguration prefix")
				}
			}
			return nil
		}
	}

	target := Cluster{ConfigID: 2, Members: append(append([]Member(nil), initial.Members...), Member{
		ID: "n4", WALIdentity: learner.WALIdentity(),
	})}
	if _, err := cores["n1"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("begin actual learner promotion: %v", err)
	}
	if err := cores["n1"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish actual learner promotion: %v", err)
	}
	terminal := cores["n1"].Tip()
	if err := learner.WaitTip(ctx, terminal); err != nil {
		t.Fatalf("promoted learner did not apply terminal at %d: %v", terminal, err)
	}
	if err := learner.EnsureDurableThrough(ctx, terminal); err != nil {
		t.Fatalf("promoted learner terminal was not durable: %v", err)
	}
	history, err := learner.MembershipHistory()
	if err != nil || len(history.Transitions) != 1 || history.Transitions[0].Terminal.Slot != terminal {
		t.Fatalf("actual durable promotion history=%+v err=%v", history, err)
	}
	if !learner.IsVoter() || learner.ConfigID() != target.ConfigID {
		t.Fatalf("actual promotion not reflected: voter=%t config=%d", learner.IsVoter(), learner.ConfigID())
	}

	committedSlot := terminal + 1
	var committedNonce [ReadBarrierNonceSize]byte
	for i := range committedNonce {
		committedNonce[i] = byte(80 + i)
	}
	if got, _, err := learner.Propose(ctx, EncodeReadBarrier(committedNonce)); err != nil || got != committedSlot {
		t.Fatalf("establish actual post-promotion prefix slot=%d got=%d err=%v", committedSlot, got, err)
	}
	return &promotedLearnerFixture{initial: initial, target: target, cores: cores, learner: learner, transport: transport, committedSlot: committedSlot}
}

func startPromotedLearnerPendingControl(t *testing.T, ctx context.Context, fixture *promotedLearnerFixture, nonce [ReadBarrierNonceSize]byte) (Slot, *managedControlProposal, RecordRequest, Summary) {
	t.Helper()
	pendingSlot := fixture.committedSlot + 1
	fixture.transport.slot.Store(uint64(pendingSlot))
	if got := fixture.learner.ConfigIDForSlot(pendingSlot); got != fixture.target.ConfigID {
		t.Fatalf("pending slot configuration=%d, want promoted config %d", got, fixture.target.ConfigID)
	}
	proposal := &managedControlProposal{done: make(chan managedControlProposalResult, 1)}
	go func() {
		slot, _, err := fixture.cores["n1"].Propose(ctx, EncodeReadBarrier(nonce))
		proposal.done <- managedControlProposalResult{slot: slot, err: err}
	}()
	t.Cleanup(func() {
		fixture.transport.releaseHeld()
		if !proposal.got.Load() {
			select {
			case proposal.result = <-proposal.done:
				proposal.got.Store(true)
			case <-time.After(5 * time.Second):
				t.Error("held promoted-learner proposal did not terminate during cleanup")
			}
		}
	})
	select {
	case <-fixture.transport.held:
	case <-ctx.Done():
		t.Fatalf("proposal did not reach held n4 recorder response: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not reach held n4 recorder response")
	}
	var request RecordRequest
	select {
	case request = <-fixture.transport.request:
	case <-ctx.Done():
		t.Fatalf("pending record request was not captured: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("pending record request was not captured")
	}
	var summary Summary
	select {
	case summary = <-fixture.transport.summary:
	case <-ctx.Done():
		t.Fatalf("n4 actual Record response was not captured: %v", ctx.Err())
	}
	return pendingSlot, proposal, request, summary
}

func TestObserveManagedControlAcceptsDurablyPromotedLearner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixture := newPromotedLearnerFixture(t, ctx)
	initial, target, cores, learner, transport, committedSlot := fixture.initial, fixture.target, fixture.cores, fixture.learner, fixture.transport, fixture.committedSlot
	var nonce [ReadBarrierNonceSize]byte
	for i := range nonce {
		nonce[i] = byte(200 + i)
	}
	pendingSlot, proposal, pendingRequest, localSummary := startPromotedLearnerPendingControl(t, ctx, fixture, nonce)
	// n4 has completed the real Record call, but its response is withheld. n3 is
	// held before Record, so n1 cannot form the four-voter quorum before Observe.
	learner.mu.RLock()
	localState, localRecorded := learner.recorders[pendingSlot]
	localCommitted, localObserved := learner.tip, learner.highestKnownSlotLocked()
	learner.mu.RUnlock()
	expectedHash := ValueHash(sha256.Sum256(pendingRequest.Proposal.Value))
	if pendingRequest.Slot != pendingSlot || pendingRequest.ConfigID != target.ConfigID || !localRecorded || localCommitted != committedSlot || localObserved != pendingSlot || localState.Step != pendingRequest.Step || !sameProposal(localState.FirstCurrent, &pendingRequest.Proposal) || !sameProposal(localState.AggregateCurrent, &pendingRequest.Proposal) || localSummary.FirstCurrent == nil || !sameProposal(localSummary.FirstCurrent, &pendingRequest.Proposal) || pendingRequest.Proposal.Hash != expectedHash {
		t.Fatalf("n4 pending recorder state lacks exact durable request: recorded=%t C=%d R=%d state=%+v requestHash=%x want=%x", localRecorded, localCommitted, localObserved, localState, pendingRequest.Proposal.Hash, expectedHash)
	}
	var hookHit atomic.Bool
	var proposalResult managedControlProposalResult
	var deliveryErr error
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || !hookHit.CompareAndSwap(false, true) {
			return
		}
		// A proposer quorum need not include n4. Hold its delivery until the
		// proposer returns, then install that exact certificate on the observer.
		transport.mu.Lock()
		transport.dropDecision["n4"] = true
		transport.mu.Unlock()
		defer func() {
			transport.mu.Lock()
			delete(transport.dropDecision, "n4")
			transport.mu.Unlock()
		}()
		transport.releaseHeld()
		proposalResult = proposal.wait(ctx)
		if proposalResult.err != nil || proposalResult.slot != pendingSlot {
			return
		}
		value, ok := cores["n1"].CertifiedValue(pendingSlot)
		if !ok || value.Hash != expectedHash || learner.Tip() != committedSlot {
			deliveryErr = fmt.Errorf("expected exact source certificate and unchanged n4 tip %d, got certified=%t hash=%x tip=%d", committedSlot, ok, value.Hash, learner.Tip())
			return
		}
		decision, err := cores["n1"].certifiedDecision(value)
		if err != nil {
			deliveryErr = err
			return
		}
		deliveryErr = learner.AcceptDecision(decision)
	})
	_, observationErr := learner.ObserveManagedAppendOnlyControl(ctx)
	restore()
	if !hookHit.Load() || proposalResult.err != nil || proposalResult.slot != pendingSlot || deliveryErr != nil {
		t.Fatalf("certification during promoted-learner validation: hook=%t proposal=%+v learner delivery=%v", hookHit.Load(), proposalResult, deliveryErr)
	}
	if !IsManagedControlPrefixAdvance(observationErr) || !errors.Is(observationErr, ErrManagedControlObservationUnavailable) {
		t.Fatalf("promoted learner observation error=%v, want typed certified-prefix advance", observationErr)
	}
	if _, ok := learner.CertifiedValue(pendingSlot); !ok {
		t.Fatal("pending control slot was not actually certified after release")
	}
	if ok, err := learner.RecheckManagedControlPrefixAdvance(ctx, observationErr, committedSlot, pendingSlot, managedGraphRequestAppendOnlyApplicationProtocol, 0); err != nil || !ok {
		t.Fatalf("same-authority promoted prefix recheck=%v err=%v", ok, err)
	}
	if ok, err := cores["n1"].RecheckManagedControlPrefixAdvance(ctx, observationErr, committedSlot, pendingSlot, managedGraphRequestAppendOnlyApplicationProtocol, 0); err != nil || ok {
		t.Fatalf("different-Core promoted prefix recheck=%v err=%v", ok, err)
	}
	removed := Cluster{ConfigID: target.ConfigID + 1, Members: append([]Member(nil), initial.Members...)}
	if _, err := cores["n1"].BeginReconfiguration(ctx, removed); err != nil {
		t.Fatalf("begin actual removal after typed prefix observation: %v", err)
	}
	if err := cores["n1"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish actual removal after typed prefix observation: %v", err)
	}
	removalTerminal := cores["n1"].Tip()
	if err := learner.WaitTip(ctx, removalTerminal); err != nil {
		t.Fatalf("promoted learner did not apply actual removal terminal %d: %v", removalTerminal, err)
	}
	if ok, err := learner.RecheckManagedControlPrefixAdvance(ctx, observationErr, committedSlot, pendingSlot, managedGraphRequestAppendOnlyApplicationProtocol, 0); err != nil || ok {
		t.Fatalf("removed learner accepted old typed prefix observation: %v err=%v", ok, err)
	}
}

func TestObserveManagedControlRejectsRemovalDuringPromotedValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixture := newPromotedLearnerFixture(t, ctx)
	var nonce [ReadBarrierNonceSize]byte
	nonce[0] = 31
	pendingSlot, proposal, _, _ := startPromotedLearnerPendingControl(t, ctx, fixture, nonce)
	var hookHit atomic.Bool
	var proposalResult managedControlProposalResult
	var removalErr error
	var removalTerminal Slot
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || !hookHit.CompareAndSwap(false, true) {
			return
		}
		fixture.transport.releaseHeld()
		proposalResult = proposal.wait(ctx)
		removed := Cluster{ConfigID: fixture.target.ConfigID + 1, Members: append([]Member(nil), fixture.initial.Members...)}
		if _, removalErr = fixture.cores["n1"].BeginReconfiguration(ctx, removed); removalErr == nil {
			removalErr = fixture.cores["n1"].FinishReconfiguration(ctx)
		}
		if removalErr == nil {
			removalTerminal = fixture.cores["n1"].Tip()
			removalErr = fixture.learner.WaitTip(ctx, removalTerminal)
		}
	})
	_, observationErr := fixture.learner.ObserveManagedAppendOnlyControl(ctx)
	restore()
	if !hookHit.Load() || proposalResult.err != nil || proposalResult.slot != pendingSlot || removalErr != nil {
		t.Fatalf("removal-during-validation hook=%t proposal=%+v removalErr=%v", hookHit.Load(), proposalResult, removalErr)
	}
	if removalTerminal == 0 || fixture.learner.IsVoter() {
		t.Fatalf("actual REMOVE did not retire promoted learner: terminal=%d voter=%t", removalTerminal, fixture.learner.IsVoter())
	}
	if observationErr == nil || IsManagedControlPrefixAdvance(observationErr) {
		t.Fatalf("observer accepted promoted learner after REMOVE during validation: %v", observationErr)
	}
}

func TestObserveManagedControlRejectsPromotedLearnerOrdinaryNextSlotGap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixture := newPromotedLearnerFixture(t, ctx)
	var nonce [ReadBarrierNonceSize]byte
	nonce[0] = 42
	pendingSlot, proposal, _, _ := startPromotedLearnerPendingControl(t, ctx, fixture, nonce)
	var hookHit atomic.Bool
	var proposalResult managedControlProposalResult
	var learnerWaitErr error
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || !hookHit.CompareAndSwap(false, true) {
			return
		}
		fixture.transport.releaseHeld()
		proposalResult = proposal.wait(ctx)
		if proposalResult.err == nil && proposalResult.slot == pendingSlot {
			learnerWaitErr = fixture.learner.WaitTip(ctx, pendingSlot)
		}
	})
	_, observationErr := fixture.learner.ObserveManagedAppendOnlyControl(ctx)
	restore()
	if !hookHit.Load() || proposalResult.err != nil || proposalResult.slot != pendingSlot || learnerWaitErr != nil || !IsManagedControlPrefixAdvance(observationErr) {
		t.Fatalf("actual promoted prefix advance hook=%t proposal=%+v learner wait=%v observationErr=%v", hookHit.Load(), proposalResult, learnerWaitErr, observationErr)
	}
	ordinary := newProposal(highestPriority, "n4", []byte("ordinary R+1 application value"))
	ordinarySlot := pendingSlot + 1
	summary, err := fixture.learner.Record(ctx, RecordRequest{Slot: ordinarySlot, Step: 4, ConfigID: fixture.target.ConfigID, Proposal: ordinary})
	if err != nil || summary.RecorderID != "n4" || summary.FirstCurrent == nil || !sameProposal(summary.FirstCurrent, &ordinary) {
		t.Fatalf("actual promoted learner ordinary R+1 Record summary=%+v err=%v", summary, err)
	}
	fixture.learner.mu.RLock()
	state, recorded := fixture.learner.recorders[ordinarySlot]
	committed, observed := fixture.learner.tip, fixture.learner.highestKnownSlotLocked()
	fixture.learner.mu.RUnlock()
	if !recorded || committed != pendingSlot || observed != ordinarySlot || !sameProposal(state.FirstCurrent, &ordinary) {
		t.Fatalf("ordinary next-slot gap not present on promoted learner: recorded=%t C=%d R=%d state=%+v", recorded, committed, observed, state)
	}
	if ok, err := fixture.learner.RecheckManagedControlPrefixAdvance(ctx, observationErr, fixture.committedSlot, pendingSlot, managedGraphRequestAppendOnlyApplicationProtocol, 0); err != nil || ok {
		t.Fatalf("promoted learner accepted original prefix after ordinary R+1 gap: %v err=%v", ok, err)
	}
}
