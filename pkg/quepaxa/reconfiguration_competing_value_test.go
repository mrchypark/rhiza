package quepaxa

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type freezeRecordGate struct {
	base    *clusterTransport
	once    sync.Once
	seen    chan struct{}
	release chan struct{}
	slot    atomic.Uint64
}

func (g *freezeRecordGate) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	if control, ok, err := decodeReconfiguration(request.Proposal.Value); err == nil && ok && !control.Terminal {
		g.once.Do(func() {
			g.slot.Store(uint64(request.Slot))
			close(g.seen)
			select {
			case <-g.release:
			case <-ctx.Done():
			}
		})
	}
	return g.base.SendRecord(ctx, to, request)
}

func (g *freezeRecordGate) SendDecision(ctx context.Context, decision Decision) error {
	return g.base.SendDecision(ctx, decision)
}

func (g *freezeRecordGate) ReadTip(ctx context.Context, to NodeID) (Slot, error) {
	return g.base.ReadTip(ctx, to)
}

func (g *freezeRecordGate) StageValue(ctx context.Context, to NodeID, hash ValueHash, value []byte) error {
	return g.base.StageValue(ctx, to, hash, value)
}

func (g *freezeRecordGate) FetchValue(ctx context.Context, from NodeID, hash ValueHash) ([]byte, error) {
	return g.base.FetchValue(ctx, from, hash)
}

func (g *freezeRecordGate) FetchFrozenTargetDecisions(ctx context.Context, from NodeID, slot Slot, limit int, target Cluster) ([]DecidedValue, Slot, error) {
	return g.base.FetchFrozenTargetDecisions(ctx, from, slot, limit, target)
}

func TestBeginReconfigurationRetriesAfterCertifiedOrdinaryCompetingValue(t *testing.T) {
	cores, base := reconfigCluster(t)
	gate := &freezeRecordGate{base: base, seen: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(gate.release) })
	for _, core := range cores {
		core.transport = gate
	}
	proposer := cores["a"]
	for attempts := 0; ; attempts++ {
		slot := proposer.Tip() + 1
		if attempts == 32 {
			t.Fatal("could not place freeze outside a schedule boundary with a non-proposer leader")
		}
		if proposer.isLeaderScheduleSlot(slot) || proposer.isLeaderScheduleSlot(slot+16) {
			if _, _, err := proposer.Propose(context.Background(), []byte("advance-schedule")); err != nil {
				t.Fatal(err)
			}
			continue
		}
		order, err := proposer.LeaderOrder(slot)
		if err != nil {
			t.Fatal(err)
		}
		if order[0] != proposer.nodeID {
			break
		}
		if _, _, err := proposer.Propose(context.Background(), []byte("advance-leader")); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	type beginResult struct {
		slot Slot
		err  error
	}
	result := make(chan beginResult, 1)
	go func() {
		slot, err := proposer.BeginReconfiguration(ctx, target)
		result <- beginResult{slot: slot, err: err}
	}()

	select {
	case <-gate.seen:
	case r := <-result:
		t.Fatalf("begin returned before the competing value window: slot=%d err=%v", r.slot, r.err)
	case <-ctx.Done():
		t.Fatal("freeze proposal was not observed")
	}
	competingSlot := Slot(gate.slot.Load())
	order, err := proposer.LeaderOrder(competingSlot)
	if err != nil {
		t.Fatal(err)
	}
	leader := order[0]
	if leader == proposer.nodeID {
		t.Fatal("fixture did not select a non-proposer leader")
	}
	competing := []byte("actual competing ordinary value")
	if slot, _, err := cores[leader].Propose(ctx, competing); err != nil || slot != competingSlot {
		t.Fatalf("certify competing value: slot=%d want=%d err=%v", slot, competingSlot, err)
	}
	release.Do(func() { close(gate.release) })

	select {
	case r := <-result:
		if r.err != nil {
			t.Fatalf("begin after competing value: %v", r.err)
		}
		if r.slot <= competingSlot {
			t.Fatalf("freeze reused competing slot %d: returned %d", competingSlot, r.slot)
		}
		decided, ok := proposer.CertifiedValue(competingSlot)
		if !ok || string(decided.Value) != string(competing) {
			t.Fatalf("competing value was overwritten: %+v", decided)
		}
		if err := proposer.FinishReconfigurationAt(ctx, target, 0); err != nil {
			t.Fatal(err)
		}
		if got := proposer.CurrentCluster(); got.ConfigID != 2 || len(got.Members) != 2 {
			t.Fatalf("target was not installed after retry: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("begin did not retry after certified competing value")
	}
}

func TestOrdinaryCompetingValueRejectsMembershipAndLeaderControls(t *testing.T) {
	freeze, err := encodeReconfiguration(reconfigurationValue{Freeze: 2, TerminalSlot: 18, Target: Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}})
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := EncodeLeaderSchedule([]NodeID{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	seal, err := EncodeCheckpointSeal(CheckpointSeal{ConfigID: 1, Index: 1, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: [32]byte{3}, NextLeaderOrder: []NodeID{"a", "b", "c"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		value []byte
		want  bool
	}{
		{"ordinary", []byte("ordinary application value"), true},
		{"checkpoint", seal, true},
		{"read barrier", EncodeReadBarrier([ReadBarrierNonceSize]byte{1}), true},
		{"membership control", freeze, false},
		{"leader control", schedule, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ordinaryCompetingValue(test.value); got != test.want {
				t.Fatalf("ordinaryCompetingValue=%t want=%t", got, test.want)
			}
		})
	}
}
