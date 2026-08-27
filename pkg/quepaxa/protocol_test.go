package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/rand"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
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

func BenchmarkCoreReadIndexThreePeers(b *testing.B)  { benchmarkCoreReadIndex(b, false) }
func BenchmarkCoreReadIndexOnePeerDown(b *testing.B) { benchmarkCoreReadIndex(b, true) }

func BenchmarkCoreLocalReadIndex(b *testing.B) {
	cores, _ := newTestCluster(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = cores["n2"].Tip()
	}
}

func benchmarkCoreReadIndex(b *testing.B, oneDown bool) {
	cores, transport := newTestCluster(b)
	if _, _, err := cores["n2"].Propose(context.Background(), []byte("seed")); err != nil {
		b.Fatal(err)
	}
	if oneDown {
		transport.fail("n1")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := cores["n2"].ReadIndex(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
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

func TestISRMonotonicProperties(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	state := ISR{}
	for range 10_000 {
		step := Step(random.Intn(64) + 4)
		var priority Priority
		priority[31] = byte(random.Intn(255))
		proposal := newProposal(priority, NodeID("n"+string(rune('1'+random.Intn(3)))), []byte{byte(random.Intn(255))})
		before := state
		next, summary := state.Record(step, proposal)
		if next.Step < before.Step || summary.Step != next.Step {
			t.Fatalf("step regressed: before=%d input=%d after=%d", before.Step, step, next.Step)
		}
		switch {
		case step < before.Step:
			if !sameProposal(next.FirstCurrent, before.FirstCurrent) || !sameProposal(next.AggregateCurrent, before.AggregateCurrent) || !sameProposal(next.AggregatePrior, before.AggregatePrior) {
				t.Fatal("stale input mutated ISR")
			}
		case step == before.Step:
			if !sameProposal(next.FirstCurrent, before.FirstCurrent) || compareProposal(next.AggregateCurrent, before.AggregateCurrent) < 0 {
				t.Fatal("same-step aggregate regressed")
			}
		case step == before.Step+1:
			if !sameProposal(next.AggregatePrior, before.AggregateCurrent) {
				t.Fatal("adjacent step lost prior aggregate")
			}
		case step > before.Step+1:
			if next.AggregatePrior != nil {
				t.Fatal("step gap retained prior aggregate")
			}
		}
		state = next
	}
}

func TestProposalOrderIsPriorityProposerHash(t *testing.T) {
	priority := Priority{31: 1}
	left := newProposal(priority, "n1", []byte("a"))
	right := newProposal(priority, "n1", []byte("b"))
	highProposer := newProposal(priority, "n2", []byte("a"))
	highPriority := newProposal(Priority{31: 2}, "n1", []byte("a"))
	if got, want := compareProposal(&left, &right), bytes.Compare(left.Hash[:], right.Hash[:]); got != want {
		t.Fatalf("hash order=%d, want %d", got, want)
	}
	if compareProposal(&left, &highProposer) >= 0 || compareProposal(&highProposer, &highPriority) >= 0 {
		t.Fatal("proposal order does not match <priority, proposer, hash>")
	}
}

func TestCertificateContainsOnlyValueHash(t *testing.T) {
	value := bytes.Repeat([]byte("certificate-must-not-repeat-this-value"), 32)
	proposal := newProposal(highestPriority, "n1", value)
	decision := Decision{Slot: 1, Step: 4, Proposal: proposal, Summaries: []Summary{
		{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)},
		{RecorderID: "n2", Step: 4, FirstCurrent: cloneProposal(&proposal)},
	}}
	certificate, err := encodeCertificate(7, decision)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(certificate, value) {
		t.Fatal("certificate contains the decided value")
	}
	configID, decoded, err := decodeCertificate(certificate)
	if err != nil || configID != 7 || decoded.Proposal.Hash != proposal.Hash || len(decoded.Proposal.Value) != 0 {
		t.Fatalf("decoded certificate=%+v config=%d err=%v", decoded, configID, err)
	}
}

func TestCertificateRejectsNumberedObject(t *testing.T) {
	proposal := newProposal(highestPriority, "n1", []byte("value"))
	decision := Decision{Slot: 1, Step: 4, Proposal: proposal, Summaries: []Summary{{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)}}}
	data, err := encodeCertificate(1, decision)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	object["version"] = 2
	data, err = json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeCertificate(data); err == nil {
		t.Fatal("accepted numbered certificate")
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
	targets := make([]*Core, 0, len(transport.cores))
	for id, core := range transport.cores {
		if !transport.down[id] && !transport.dropDecision[id] {
			targets = append(targets, core)
		}
	}
	transport.mu.RUnlock()
	results := make(chan error, len(targets))
	for _, core := range targets {
		go func() { results <- core.AcceptDecision(decision) }()
	}
	successes := 0
	for range targets {
		if err := <-results; err == nil {
			successes++
			if successes >= len(transport.cores)/2+1 {
				return nil
			}
		}
	}
	return ErrQuorumUnavailable
}

func (transport *clusterTransport) ReadTip(_ context.Context, to NodeID) (Slot, error) {
	transport.mu.Lock()
	down := transport.down[to]
	core := transport.cores[to]
	transport.mu.Unlock()
	if down || core == nil {
		return 0, errors.New("down")
	}
	return core.Tip(), nil
}

func (transport *clusterTransport) StageValue(_ context.Context, to NodeID, hash ValueHash, value []byte) error {
	transport.mu.Lock()
	down := transport.down[to]
	core := transport.cores[to]
	transport.mu.Unlock()
	if down || core == nil {
		return errors.New("down")
	}
	return core.StageValue(hash, value)
}

func (transport *clusterTransport) FetchValue(_ context.Context, from NodeID, hash ValueHash) ([]byte, error) {
	transport.mu.Lock()
	down := transport.down[from]
	core := transport.cores[from]
	transport.mu.Unlock()
	if down || core == nil {
		return nil, errors.New("down")
	}
	value, ok := core.Value(hash)
	if !ok {
		return nil, errors.New("missing")
	}
	return value, nil
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
		var counter atomic.Uint32
		seed := member.ID[len(member.ID)-1]
		core.priority = func() (Priority, error) {
			var priority Priority
			priority[30], priority[31] = seed, byte(counter.Add(1))
			return priority, nil
		}
		transport.cores[member.ID] = core
	}
	return transport.cores, transport
}

func TestAcceptCertifiedValuesAppliesLeaderScheduleBeforeFollowingDecisions(t *testing.T) {
	cores, _ := newTestCluster(t)
	source := cores["n2"]
	for i := range 160 {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, uint64(i))
		if _, _, err := source.Propose(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}

	values := make([]DecidedValue, 0, source.Tip())
	for slot := Slot(1); slot <= source.Tip(); slot++ {
		value, ok := source.CertifiedValue(slot)
		if !ok {
			t.Fatalf("missing source slot %d", slot)
		}
		values = append(values, value)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	target := newCore("n1", source.config, wal, nil)
	if err := target.AcceptCertifiedValues(values); err != nil {
		t.Fatal(err)
	}
	if target.Tip() != source.Tip() {
		t.Fatalf("tip=%d want %d", target.Tip(), source.Tip())
	}
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

func TestReadIndexSurvivesOneFailureAndFailsWithoutQuorum(t *testing.T) {
	cores, transport := newTestCluster(t)
	if _, _, err := cores["n1"].Propose(context.Background(), []byte("write")); err != nil {
		t.Fatal(err)
	}
	transport.fail("n1")
	index, _, err := cores["n2"].ReadIndex(context.Background())
	if err != nil || index != 1 || cores["n2"].Tip() != 1 {
		t.Fatalf("read-index=%d tip=%d err=%v", index, cores["n2"].Tip(), err)
	}
	transport.fail("n3")
	if _, _, err := cores["n2"].ReadIndex(context.Background()); !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("minority read-index error=%v", err)
	}
}

func TestCheckpointSealRequiresPrefixAndObjectQuorum(t *testing.T) {
	cores, _ := newTestCluster(t)
	if _, _, err := cores["n1"].Propose(context.Background(), []byte("state")); err != nil {
		t.Fatal(err)
	}
	prefix, ok := cores["n1"].PrefixHash(1)
	if !ok {
		t.Fatal("missing decision prefix")
	}
	root := sha256.Sum256([]byte("root"))
	state := sha256.Sum256([]byte("snapshot"))
	var mu sync.Mutex
	verified := 0
	for _, core := range cores {
		core.SetCheckpointValidator(func(_ context.Context, seal CheckpointSeal) error {
			if seal.RootHash != root || seal.StateHash != state {
				return errors.New("wrong root")
			}
			mu.Lock()
			verified++
			mu.Unlock()
			return nil
		})
	}
	order, _ := cores["n1"].LeaderOrder(2)
	checkpoint := CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root, StateHash: state, PrefixHash: prefix, NextLeaderOrder: order}
	for _, core := range cores {
		if err := core.PrepareCheckpoint(context.Background(), checkpoint); err != nil {
			t.Fatal(err)
		}
	}
	value, err := EncodeCheckpointSeal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := cores["n1"].Propose(context.Background(), value); err != nil || slot != 2 {
		t.Fatalf("seal slot=%d err=%v", slot, err)
	}
	mu.Lock()
	count := verified
	mu.Unlock()
	if count != len(cores) {
		t.Fatalf("checkpoint validator calls=%d, want prepare-only %d", count, len(cores))
	}
}

func TestCompactedLearnerCountsAsCoveredQuorum(t *testing.T) {
	cores, transport := newTestCluster(t)
	if _, _, err := cores["n1"].Propose(context.Background(), []byte("covered write")); err != nil {
		t.Fatal(err)
	}
	certified, ok := cores["n1"].CertifiedValue(1)
	if !ok {
		t.Fatal("slot 1 is not certified")
	}
	decision, err := cores["n1"].certifiedDecision(certified)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := cores["n1"].PrefixHash(1)
	root := sha256.Sum256([]byte("covered-root"))
	state := sha256.Sum256([]byte("covered-state"))
	for _, core := range cores {
		core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	}
	order, _ := cores["n1"].LeaderOrder(2)
	checkpointSeal := CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root, StateHash: state, PrefixHash: prefix, NextLeaderOrder: order}
	for _, core := range cores {
		if err := core.PrepareCheckpoint(context.Background(), checkpointSeal); err != nil {
			t.Fatal(err)
		}
	}
	seal, err := EncodeCheckpointSeal(checkpointSeal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cores["n1"].Propose(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	if err := cores["n2"].CompactThrough(1, root); err != nil {
		t.Fatal(err)
	}
	if err := cores["n2"].AcceptDecision(decision); err != nil {
		t.Fatalf("covered learner rejected valid decision: %v", err)
	}
	if _, ok := cores["n2"].CertifiedValue(1); ok {
		t.Fatal("covered learner reinserted compacted decision")
	}
	malformed := decision
	malformed.Proposal.Value = []byte("tampered")
	if err := cores["n2"].AcceptDecision(malformed); err == nil {
		t.Fatal("covered learner accepted malformed decision")
	}
	transport.fail("n3")
	if _, err := cores["n1"].CompleteDecision(context.Background(), 1); err != nil {
		t.Fatalf("retained plus covered learner did not form quorum: %v", err)
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

func TestDecisionsRecoverDurablyAfterCrash(t *testing.T) {
	dir := t.TempDir()
	config := &Cluster{Members: []Member{{ID: "n1"}}}
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	core := newCore("n1", config, wal, nil)
	for slot := Slot(1); slot <= 20; slot++ {
		got, _, err := core.Propose(context.Background(), []byte{byte(slot)})
		if err != nil || got != slot {
			t.Fatalf("slot=%d got=%d err=%v", slot, got, err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("n1", config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	decisions, tip, err := recovered.DecisionsFrom(1, 20)
	if err != nil || tip != 20 || len(decisions) != 20 {
		t.Fatalf("tip=%d decisions=%d err=%v", tip, len(decisions), err)
	}
	for i, decision := range decisions {
		if decision.Slot != Slot(i+1) || len(decision.Value) != 1 || decision.Value[0] != byte(i+1) {
			t.Fatalf("decision[%d]=%+v", i, decision)
		}
	}
	if len(recovered.recorders) != 0 {
		t.Fatalf("decided recorder states retained after recovery: %d", len(recovered.recorders))
	}
	proposal := newProposal(highestPriority, "n1", []byte{1})
	if _, err := recovered.Record(context.Background(), RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if len(recovered.recorders) != 0 {
		t.Fatal("duplicate record recreated decided ISR state")
	}
}

func TestRecoveryDefersMissingLeaderScheduleToPeerCatchUp(t *testing.T) {
	dir := t.TempDir()
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	config := &Cluster{Members: members}
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	proposal := newProposal(highestPriority, "n1", []byte("durable-with-schedule-gap"))
	decision := Decision{Slot: 130, Step: 4, Proposal: proposal, Summaries: []Summary{
		{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)},
		{RecorderID: "n2", Step: 4, FirstCurrent: cloneProposal(&proposal)},
	}}
	certificate, err := encodeCertificate(config.ConfigID, decision)
	if err != nil {
		t.Fatal(err)
	}
	record, err := encodeDecisionRecord(proposal.Value, certificate)
	if err != nil {
		t.Fatal(err)
	}
	payload := append(append([]byte(nil), decisionEntryMagic...), record...)
	if err := wal.Append(qlog.Entry{Slot: uint64(decision.Slot), Hash: proposal.Hash, Type: qlog.EntryDecide, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("n1", config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if _, ok := recovered.CertifiedValue(decision.Slot); !ok {
		t.Fatal("durable decision was not recovered")
	}
	if err := recovered.validateDecision(decision); err == nil {
		t.Fatal("peer input bypassed missing leader schedule validation")
	}
}

func TestDecidedSlotStableAcrossRestartWithDuplicateValue(t *testing.T) {
	dir := t.TempDir()
	config := &Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}}}
	value := []byte("same value at two slots")
	proposal := newProposal(highestPriority, "n1", value)

	liveWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	live := newCore("n1", config, liveWAL, nil)
	live.updateHashIndexLocked(proposal.Hash, 1)
	live.updateHashIndexLocked(proposal.Hash, 2)
	if slot := live.byHash[proposal.Hash]; slot != 1 {
		t.Fatalf("live hash index slot=%d, want 1", slot)
	}
	_ = liveWAL.Close()

	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range []Slot{1, 2} {
		decision := Decision{Slot: slot, Step: 4, Proposal: proposal, Summaries: []Summary{{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)}}}
		certificate, err := encodeCertificate(config.ConfigID, decision)
		if err != nil {
			t.Fatal(err)
		}
		record, err := encodeDecisionRecord(value, certificate)
		if err != nil {
			t.Fatal(err)
		}
		payload := append(append([]byte(nil), decisionEntryMagic...), record...)
		if err := wal.Append(qlog.Entry{Slot: uint64(slot), Hash: proposal.Hash, Type: qlog.EntryDecide, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("n1", config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if slot, ok := recovered.DecidedSlot(value); !ok || slot != 1 {
		t.Fatalf("recovered decided slot=%d ok=%v, want 1", slot, ok)
	}
}

func TestClusterDoesNotAckWithoutCommitQuorumAndRecoversRecorderState(t *testing.T) {
	cores, transport := newTestCluster(t)
	for id := range cores {
		transport.dropDecision[id] = true
	}
	if slot, _, err := cores["n1"].Propose(context.Background(), []byte("quorum-only")); !errors.Is(err, ErrQuorumUnavailable) || slot != 0 {
		t.Fatalf("propose slot=%d err=%v", slot, err)
	}
	proposerEntries, err := cores["n1"].wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(proposerEntries, func(entry qlog.Entry) bool { return entry.Type == qlog.EntryDecide }) {
		t.Fatal("clustered fast path returned without a durable commit marker")
	}

	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	config := &Cluster{ConfigID: 1, Members: members}
	recoveredTransport := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool), delay: make(map[Slot]time.Duration)}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		entries, err := cores[member.ID].wal.Read()
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.Type == qlog.EntryReceipt || entry.Type == qlog.EntryProposal {
				if err := wal.Append(entry); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := wal.Sync(); err != nil {
			t.Fatal(err)
		}
		core := newCore(member.ID, config, wal, recoveredTransport)
		if err := core.recover(); err != nil {
			t.Fatal(err)
		}
		recoveredTransport.cores[member.ID] = core
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	recovered := recoveredTransport.cores["n1"]
	if recovered.RecorderTip() != 1 {
		t.Fatalf("recorder tip=%d, want 1", recovered.RecorderTip())
	}
	if err := recovered.RecoverThrough(ctx, 1); err != nil {
		t.Fatal(err)
	}
	decision, ok := recovered.CertifiedValue(1)
	if !ok || string(decision.Value) != "quorum-only" {
		t.Fatalf("recovered decision=%q ok=%v", decision.Value, ok)
	}
	entries, err := recovered.wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(entries, func(entry qlog.Entry) bool { return entry.Type == qlog.EntryDecide }) {
		t.Fatal("recovered decision was not persisted")
	}
}
