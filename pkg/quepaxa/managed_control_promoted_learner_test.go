//go:build rhiza_local_testhooks

package quepaxa

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

type promotedLearnerHeldTransport struct {
	*clusterTransport
	slot        atomic.Uint64
	held        chan struct{}
	release     chan struct{}
	heldOnce    sync.Once
	releaseOnce sync.Once
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
	if request.Slot == holdSlot && to == "n2" {
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

func TestObserveManagedControlAcceptsDurablyPromotedLearner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	initial := Cluster{ConfigID: 1}
	base := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool)}
	transport := &promotedLearnerHeldTransport{clusterTransport: base, held: make(chan struct{}), release: make(chan struct{})}
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
	learner, err := NewLearner(Config{
		NodeID: "n4", Cluster: initial, WAL: learnerWAL, Transport: transport,
		EnableReconfiguration: true,
		ManagedAppendOnly:     managedNoPVCBinding(learnerIdentity, managedGraphRequestAppendOnlyApplicationProtocol),
	})
	if err != nil {
		t.Fatalf("create real managed learner: %v", err)
	}
	base.cores["n4"] = learner

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
	pendingSlot := committedSlot + 1
	transport.slot.Store(uint64(pendingSlot))
	if got := learner.ConfigIDForSlot(pendingSlot); got != target.ConfigID {
		t.Fatalf("pending slot configuration=%d, want promoted config %d", got, target.ConfigID)
	}
	var nonce [ReadBarrierNonceSize]byte
	for i := range nonce {
		nonce[i] = byte(200 + i)
	}
	proposal := &managedControlProposal{done: make(chan managedControlProposalResult, 1)}
	go func() {
		slot, _, err := learner.Propose(ctx, EncodeReadBarrier(nonce))
		proposal.done <- managedControlProposalResult{slot: slot, err: err}
	}()
	t.Cleanup(func() {
		transport.releaseHeld()
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
	case <-transport.held:
	case <-ctx.Done():
		t.Fatalf("proposal did not reach held recorder response: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not reach held recorder response")
	}
	observation, err := learner.ObserveManagedAppendOnlyControl(ctx)
	if err != nil {
		t.Fatalf("promoted managed learner refused actual pending control: %v", err)
	}
	if observation.CommittedTip != committedSlot || observation.ObservedSlot != pendingSlot || observation.ConfigID != target.ConfigID || !observation.BelongsTo(learner) {
		t.Fatalf("unexpected promoted learner observation: %+v", observation)
	}
	transport.releaseHeld()
	var result managedControlProposalResult
	select {
	case result = <-proposal.done:
		proposal.got.Store(true)
	case <-time.After(5 * time.Second):
		t.Fatal("pending promoted-learner proposal did not finish after release")
	}
	if result.err != nil || result.slot != pendingSlot {
		t.Fatalf("actual pending control proposal=%+v", result)
	}
	if _, ok := learner.CertifiedValue(pendingSlot); !ok {
		t.Fatal("pending control slot was not actually certified after release")
	}
}
