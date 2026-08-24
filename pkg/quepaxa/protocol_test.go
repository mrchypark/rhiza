package quepaxa

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func BenchmarkCoreProposeThreePeers(b *testing.B) {
	benchmarkCorePropose(b, false)
}

func BenchmarkCoreProposeOnePeerDown(b *testing.B) {
	benchmarkCorePropose(b, true)
}

func benchmarkCorePropose(b *testing.B, oneDown bool) {
	cores, transport := newTestCluster(b)
	if oneDown {
		transport.fail("n1")
	}
	value := make([]byte, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		if _, _, err := cores["n2"].Propose(context.Background(), value); err != nil {
			b.Fatal(err)
		}
	}
}

func TestISRMatchesAlgorithm3(t *testing.T) {
	low := newProposal(Priority{31: 1}, "n1", []byte("low"))
	high := newProposal(Priority{31: 2}, "n2", []byte("high"))

	state, reply := (ISR{}).Record(4, low)
	if reply.Step != 4 || !sameProposal(reply.FirstCurrent, &low) || reply.AggregatePrior != nil {
		t.Fatalf("first record: %+v", reply)
	}
	state, reply = state.Record(4, high)
	if !sameProposal(reply.FirstCurrent, &low) || !sameProposal(state.AggregateCurrent, &high) {
		t.Fatalf("same-step aggregation: state=%+v reply=%+v", state, reply)
	}
	state, reply = state.Record(5, low)
	if !sameProposal(reply.AggregatePrior, &high) {
		t.Fatalf("adjacent step did not carry aggregate: %+v", reply)
	}
	before := state
	state, reply = state.Record(4, high)
	if state.Step != before.Step || !sameProposal(state.FirstCurrent, before.FirstCurrent) || !sameProposal(reply.AggregatePrior, before.AggregatePrior) {
		t.Fatalf("stale record mutated ISR: before=%+v after=%+v", before, state)
	}
	_, reply = state.Record(8, low)
	if reply.AggregatePrior != nil {
		t.Fatalf("skipped step retained prior aggregate: %+v", reply)
	}
}

func TestProposalOrderIsPriorityProposerValue(t *testing.T) {
	priority := Priority{31: 1}
	low := newProposal(priority, "n1", []byte("a"))
	highValue := newProposal(priority, "n1", []byte("b"))
	highProposer := newProposal(priority, "n2", []byte("a"))
	highPriority := newProposal(Priority{31: 2}, "n1", []byte("a"))
	if compareProposal(&low, &highValue) >= 0 || compareProposal(&highValue, &highProposer) >= 0 || compareProposal(&highProposer, &highPriority) >= 0 {
		t.Fatal("proposal order does not match <priority, proposer, value>")
	}
}

func TestLeaderScheduleExploresThenUsesAgreedOrder(t *testing.T) {
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	core := newCore("n1", &Cluster{Members: members}, nil, nil)
	wantExploration := []NodeID{"n2", "n3", "n1"}
	got, err := core.LeaderOrder(leaderEpochFirst(1))
	if err != nil || !slices.Equal(got, wantExploration) {
		t.Fatalf("exploration order=%v err=%v", got, err)
	}

	wantLearned := []NodeID{"n3", "n2", "n1"}
	value, err := EncodeLeaderSchedule(wantLearned)
	if err != nil {
		t.Fatal(err)
	}
	controlEpoch := core.explorationEpochs() - 1
	controlSlot := leaderEpochFirst(controlEpoch)
	core.mu.Lock()
	core.decided[controlSlot] = DecidedValue{Slot: controlSlot, Value: value}
	core.mu.Unlock()
	got, err = core.LeaderOrder(leaderEpochFirst(core.explorationEpochs()))
	if err != nil || !slices.Equal(got, wantLearned) {
		t.Fatalf("learned order=%v err=%v", got, err)
	}
}

func TestLeaderScheduleSortsByAverageEpochCompletion(t *testing.T) {
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	core := newCore("n1", &Cluster{Members: members}, nil, nil)
	core.timings["n1"] = leaderTiming{average: 30 * time.Millisecond, samples: 2}
	core.timings["n2"] = leaderTiming{average: 5 * time.Millisecond, samples: 2}
	core.timings["n3"] = leaderTiming{average: 10 * time.Millisecond, samples: 2}
	if got, want := core.calculateLeaderSchedule(), []NodeID{"n2", "n3", "n1"}; !slices.Equal(got, want) {
		t.Fatalf("schedule=%v, want %v", got, want)
	}
}

func TestLeaderTimingUsesRecentEpochEWMA(t *testing.T) {
	core := newCore("n1", &Cluster{Members: []Member{{ID: "n1"}}}, nil, nil)
	base := time.Unix(0, 0)
	now := base.Add(100 * time.Millisecond)
	core.now = func() time.Time { return now }
	core.epochStart[0] = base
	for slot := Slot(1); slot <= leaderEpochSize; slot++ {
		core.decided[slot] = DecidedValue{Slot: slot}
	}
	core.advanceTipLocked()
	if got := core.timings["n1"].average; got != 100*time.Millisecond {
		t.Fatalf("first average=%v", got)
	}
	core.epochStart[1] = now
	now = now.Add(200 * time.Millisecond)
	for slot := leaderEpochSize + 1; slot <= 2*leaderEpochSize; slot++ {
		core.decided[slot] = DecidedValue{Slot: slot}
	}
	core.advanceTipLocked()
	if got := core.timings["n1"].average; got != 150*time.Millisecond {
		t.Fatalf("EWMA average=%v, want 150ms", got)
	}
}

func TestFastPathUsesEpochLeader(t *testing.T) {
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	core := newCore("n1", &Cluster{Members: members}, nil, nil)
	slot := leaderEpochFirst(1)
	proposal := newProposal(highestPriority, "n2", []byte("value"))
	decision := Decision{Slot: slot, Step: 4, Proposal: proposal, Summaries: []Summary{
		{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)},
		{RecorderID: "n2", Step: 4, FirstCurrent: cloneProposal(&proposal)},
	}}
	if err := core.validateDecision(decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.ProposerID = "n1"
	if err := core.validateDecision(decision); err == nil {
		t.Fatal("accepted fast-path decision from a non-leader")
	}
}

type clusterTransport struct {
	mu           sync.RWMutex
	cores        map[NodeID]*Core
	down         map[NodeID]bool
	dropDecision map[NodeID]bool
	delay        map[Slot]time.Duration
}

func (transport *clusterTransport) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	transport.mu.RLock()
	core, down, delay := transport.cores[to], transport.down[to], transport.delay[request.Slot]
	transport.mu.RUnlock()
	if down {
		return Summary{}, errors.New("replica down")
	}
	if delay > 0 {
		select {
		case <-ctx.Done():
			return Summary{}, ctx.Err()
		case <-time.After(delay):
		}
	}
	return core.Record(ctx, request)
}

func (transport *clusterTransport) SendDecision(_ context.Context, decision Decision) error {
	transport.mu.RLock()
	defer transport.mu.RUnlock()
	for id, core := range transport.cores {
		if !transport.down[id] && !transport.dropDecision[id] {
			if err := core.AcceptDecision(decision); err != nil {
				return err
			}
		}
	}
	return nil
}

func (transport *clusterTransport) fail(ids ...NodeID) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for _, id := range ids {
		transport.down[id] = true
	}
}

func (transport *clusterTransport) recover(ids ...NodeID) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for _, id := range ids {
		delete(transport.down, id)
	}
}

func newTestCluster(t testing.TB) (map[NodeID]*Core, *clusterTransport) {
	t.Helper()
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	config := &Cluster{ConfigID: 1, Members: members}
	transport := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool), delay: make(map[Slot]time.Duration)}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core := newCore(member.ID, config, wal, transport)
		counter := byte(0)
		seed := member.ID[len(member.ID)-1]
		core.priority = func() (Priority, error) {
			counter++
			var priority Priority
			priority[30], priority[31] = seed, counter
			return priority, nil
		}
		transport.cores[member.ID] = core
	}
	return transport.cores, transport
}

func TestPipelineAllowsLaterSlotToFinishFirst(t *testing.T) {
	cores, transport := newTestCluster(t)
	transport.delay[1] = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	type result struct {
		slot Slot
		err  error
	}
	results := make(chan result, 2)
	go func() {
		slot, _, err := cores["n2"].Propose(ctx, []byte("first"))
		results <- result{slot: slot, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		cores["n2"].slotMu.Lock()
		reserved := cores["n2"].nextSlot > 1
		cores["n2"].slotMu.Unlock()
		if reserved || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	go func() {
		slot, _, err := cores["n2"].Propose(ctx, []byte("second"))
		results <- result{slot: slot, err: err}
	}()
	firstDone := <-results
	if firstDone.err != nil || firstDone.slot != 2 {
		t.Fatalf("later slot did not finish first: %+v", firstDone)
	}
	if result := <-results; result.err != nil || result.slot != 1 {
		t.Fatalf("first slot failed: %+v", result)
	}
	if err := cores["n2"].WaitTip(ctx, 2); err != nil {
		t.Fatal(err)
	}
}

func TestNonLeaderDecidesWithPreferredReplicaDown(t *testing.T) {
	cores, transport := newTestCluster(t)
	transport.fail("n1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	slot, receipts, err := cores["n2"].Propose(ctx, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if slot != 1 || len(receipts) != 2 {
		t.Fatalf("slot=%d receipts=%d", slot, len(receipts))
	}
	deadline := time.Now().Add(time.Second)
	for cores["n3"].Tip() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for _, id := range []NodeID{"n2", "n3"} {
		decisions, tip, err := cores[id].DecisionsFrom(1, 1)
		if err != nil || tip != 1 || len(decisions) != 1 || string(decisions[0].Value) != "value" {
			t.Fatalf("%s did not learn decision: tip=%d decisions=%+v err=%v", id, tip, decisions, err)
		}
	}
}

func TestOneOfThreeCannotDecide(t *testing.T) {
	cores, transport := newTestCluster(t)
	transport.fail("n1", "n3")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := cores["n2"].Propose(ctx, []byte("minority"))
	if !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("expected quorum failure, got %v", err)
	}
}

func TestQuorumFailureSlotIsReusedAfterRecovery(t *testing.T) {
	cores, transport := newTestCluster(t)
	transport.fail("n1", "n3")
	if _, _, err := cores["n2"].Propose(context.Background(), []byte("failed")); !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("expected quorum failure, got %v", err)
	}
	transport.recover("n1", "n3")
	slot, _, err := cores["n2"].Propose(context.Background(), []byte("recovered"))
	if err != nil || slot != 1 {
		t.Fatalf("slot=%d err=%v", slot, err)
	}
}

func TestConcurrentProposersPreserveBothCommands(t *testing.T) {
	cores, _ := newTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type result struct {
		slot Slot
		err  error
	}
	results := make(chan result, 2)
	for id, value := range map[NodeID]string{"n2": "alpha", "n3": "beta"} {
		go func(id NodeID, value string) {
			slot, _, err := cores[id].Propose(ctx, []byte(value))
			results <- result{slot: slot, err: err}
		}(id, value)
	}
	seenSlots := map[Slot]bool{}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		seenSlots[result.slot] = true
	}
	if !seenSlots[1] || !seenSlots[2] {
		t.Fatalf("commands did not occupy distinct contiguous slots: %+v", seenSlots)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cores["n1"].Tip() == 2 && cores["n2"].Tip() == 2 && cores["n3"].Tip() == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	var baseline []DecidedValue
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		decisions, tip, err := cores[id].DecisionsFrom(1, 2)
		if err != nil || tip != 2 || len(decisions) != 2 {
			t.Fatalf("%s tip=%d decisions=%+v err=%v", id, tip, decisions, err)
		}
		if baseline == nil {
			baseline = decisions
		} else if baseline[0].Hash != decisions[0].Hash || baseline[1].Hash != decisions[1].Hash {
			t.Fatalf("agreement failure on %s", id)
		}
	}
}

func TestUniqueReadBarrierOnStaleReplicaFollowsCompletedWrite(t *testing.T) {
	cores, transport := newTestCluster(t)
	transport.mu.Lock()
	transport.dropDecision["n3"] = true
	transport.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	writeSlot, _, err := cores["n1"].Propose(ctx, []byte("completed write"))
	if err != nil || writeSlot != 1 {
		t.Fatalf("write slot=%d err=%v", writeSlot, err)
	}
	if cores["n3"].Tip() != 0 {
		t.Fatalf("stale replica unexpectedly learned write: tip=%d", cores["n3"].Tip())
	}
	var nonce [ReadBarrierNonceSize]byte
	nonce[0] = 1
	barrier := EncodeReadBarrier(nonce)
	barrierSlot, _, err := cores["n3"].Propose(ctx, barrier)
	if err != nil || barrierSlot != 2 {
		t.Fatalf("barrier slot=%d err=%v, want exact barrier after write at slot 2", barrierSlot, err)
	}
	decisions, tip, err := cores["n3"].DecisionsFrom(1, 2)
	if err != nil || tip != 2 || len(decisions) != 2 || string(decisions[0].Value) != "completed write" || !slices.Equal(decisions[1].Value, barrier) {
		t.Fatalf("stale replica did not recover write before barrier: tip=%d decisions=%+v err=%v", tip, decisions, err)
	}
}

func TestRecorderStateRecoversDurably(t *testing.T) {
	dir := t.TempDir()
	config := &Cluster{Members: []Member{{ID: "n1"}}}
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	core := newCore("n1", config, wal, nil)
	high := newProposal(Priority{31: 2}, "n1", []byte("high"))
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 1, Step: 4, Proposal: high}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wAL2, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wAL2.Close()
	recovered := newCore("n1", config, wAL2, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	low := newProposal(Priority{31: 1}, "n1", []byte("low"))
	summary, err := recovered.Record(context.Background(), RecordRequest{Slot: 1, Step: 5, Proposal: low})
	if err != nil {
		t.Fatal(err)
	}
	if !sameProposal(summary.AggregatePrior, &high) {
		t.Fatalf("recovered ISR lost prior aggregate: %+v", summary)
	}
}
