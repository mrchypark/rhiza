package quepaxa

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

type mockTransport struct{}

func (m *mockTransport) SendRecord(_ context.Context, to NodeID, request RecordRequest) (Summary, error) {
	return Summary{RecorderID: to, Step: request.Step, FirstCurrent: cloneProposal(&request.Proposal)}, nil
}

func (m *mockTransport) SendDecision(context.Context, Decision) error {
	return nil
}

type blockingDecisionTransport struct {
	once    sync.Once
	mu      sync.Mutex
	slots   []Slot
	started chan struct{}
	release chan struct{}
}

func (t *blockingDecisionTransport) SendRecord(_ context.Context, to NodeID, request RecordRequest) (Summary, error) {
	return Summary{RecorderID: to, Step: request.Step, FirstCurrent: cloneProposal(&request.Proposal)}, nil
}

func (t *blockingDecisionTransport) SendDecision(_ context.Context, decision Decision) error {
	t.mu.Lock()
	t.slots = append(t.slots, decision.Slot)
	t.mu.Unlock()
	t.once.Do(func() { close(t.started) })
	<-t.release
	return nil
}

func TestCorePropose(t *testing.T) {
	dir, err := os.MkdirTemp("", "consensus-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	config := &Cluster{
		ConfigID: 1,
		Members: []Member{
			{ID: "node-1", URL: "http://localhost:8081"},
			{ID: "node-2", URL: "http://localhost:8082"},
			{ID: "node-3", URL: "http://localhost:8083"},
		},
	}

	core := newCore("node-1", config, wal, &mockTransport{})

	// Propose a value
	slot, receipts, err := core.Propose(context.Background(), []byte("CREATE TABLE test (id INT)"))
	if err != nil {
		t.Fatalf("propose error: %v", err)
	}

	if slot != 1 {
		t.Errorf("expected slot 1, got %d", slot)
	}

	if len(receipts) != 2 {
		t.Errorf("expected quorum of 2 receipts, got %d", len(receipts))
	}

	// Check quorum
	if !core.IsQuorum(receipts) {
		t.Error("expected quorum to be reached")
	}
}

func TestCoreMayDecideSameValueInDifferentSlots(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	config := &Cluster{Members: []Member{{ID: "node-1"}}}
	core := newCore("node-1", config, wal, nil)
	for want := Slot(1); want <= 2; want++ {
		slot, _, err := core.Propose(context.Background(), []byte("same value"))
		if err != nil || slot != want {
			t.Fatalf("proposal %d: slot=%d err=%v", want, slot, err)
		}
	}
}

func TestCoreDoesNotReuseFastPathPriorityAfterCanceledSlot(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}}}, wal, nil)
	core.priority = func() (Priority, error) { return Priority{1}, nil }
	core.releaseSlot(1) // A canceled attempt may already exist on a recorder.

	slot, _, err := core.Propose(context.Background(), []byte("retry"))
	if err != nil {
		t.Fatal(err)
	}
	certified, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatal("retried slot was not certified")
	}
	decision, err := decodeDecision(certified.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Proposal.Priority == highestPriority {
		t.Fatal("reused slot incorrectly took the unique fast path")
	}
}

func TestCoreReservesEpochBoundaryForAgreedSchedule(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}}}, wal, nil)
	var last Slot
	for i := 0; i < 33; i++ {
		last, _, err = core.Propose(context.Background(), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if last != 34 {
		t.Fatalf("last user slot=%d, want 34", last)
	}
	decision, ok := core.CertifiedValue(33)
	if !ok {
		t.Fatal("missing agreed schedule at epoch boundary")
	}
	order, schedule, err := DecodeLeaderSchedule(decision.Value)
	if err != nil || !schedule || len(order) != 1 || order[0] != "node-1" {
		t.Fatalf("schedule=%v order=%v err=%v", schedule, order, err)
	}
}

func TestCoreRecord(t *testing.T) {
	dir, err := os.MkdirTemp("", "consensus-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	config := &Cluster{
		ConfigID: 1,
		Members: []Member{
			{ID: "node-1", URL: "http://localhost:8081"},
		},
	}

	core := newCore("node-1", config, wal, &mockTransport{})

	// Record a proposal
	proposal := newProposal(highestPriority, "node-1", []byte("test"))
	summary, err := core.Record(context.Background(), RecordRequest{Slot: 1, Step: 4, Proposal: proposal})
	if err != nil {
		t.Fatalf("record error: %v", err)
	}

	if summary.Step != 4 || !sameProposal(summary.FirstCurrent, &proposal) {
		t.Fatalf("unexpected ISR summary: %+v", summary)
	}
}

func TestRecorderDurablyPromotesLearnedHintBeforeReply(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	config := &Cluster{Members: []Member{{ID: "n1"}}}
	core := newCore("n1", config, wal, nil)
	proposal := newProposal(highestPriority, "n1", []byte("hint"))
	decision := Decision{Slot: 1, Step: 4, Proposal: proposal, Summaries: []Summary{{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)}}}
	if err := core.AcceptDecisionHint(decision); err != nil {
		t.Fatal(err)
	}
	if core.durable[1] {
		t.Fatal("learned hint unexpectedly marked durable")
	}
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if !core.durable[1] {
		t.Fatal("recorder replied before durable promotion")
	}
}

func TestDecisionDisseminationQueueIsBounded(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	transport := &blockingDecisionTransport{started: make(chan struct{}), release: make(chan struct{})}
	config := &Cluster{Members: []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	core := newCore("n1", config, wal, transport)
	for i := 0; i < decisionQueueSize+10; i++ {
		core.enqueueDecision(Decision{Slot: Slot(i + 1)})
	}
	<-transport.started
	if queued := len(core.decisionQ); queued > decisionQueueSize {
		t.Fatalf("queued decisions=%d, want at most %d", queued, decisionQueueSize)
	}
	close(transport.release)
	deadline := time.Now().Add(time.Second)
	for {
		core.decisionMu.Lock()
		sending := core.sending
		core.decisionMu.Unlock()
		if !sending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("decision worker did not drain")
		}
		time.Sleep(time.Millisecond)
	}
	transport.mu.Lock()
	last := transport.slots[len(transport.slots)-1]
	transport.mu.Unlock()
	if want := Slot(decisionQueueSize + 10); last != want {
		t.Fatalf("last disseminated slot=%d, want newest %d", last, want)
	}
}

func TestCoreAcceptLearned(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("node-2", &Cluster{Members: []Member{{ID: "node-2"}}}, wal, &mockTransport{})
	value := []byte("CREATE TABLE learned (id INT)")
	hash := sha256.Sum256(value)
	if err := core.AcceptLearned(1, value, hash); err != nil {
		t.Fatal(err)
	}
	if !core.IsDecided(1) || core.Tip() != 1 {
		t.Fatalf("learned slot not recorded: tip=%d", core.Tip())
	}
	hash[0]++
	if err := core.AcceptLearned(8, value, hash); err == nil {
		t.Fatal("expected hash mismatch")
	}
	decisions, tip, err := core.DecisionsFrom(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if tip != 1 || len(decisions) != 1 || decisions[0].Slot != 1 || string(decisions[0].Value) != string(value) {
		t.Fatalf("unexpected decisions: tip=%d values=%+v", tip, decisions)
	}
}

func TestDecisionsFromStopsAtGap(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}}}, wal, &mockTransport{})
	value := []byte("SELECT 2")
	hash := sha256.Sum256(value)
	if err := core.AcceptLearned(2, value, hash); err != nil {
		t.Fatal(err)
	}
	decisions, tip, err := core.DecisionsFrom(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if tip != 0 || len(decisions) != 0 {
		t.Fatalf("exposed non-contiguous decision: tip=%d decisions=%+v", tip, decisions)
	}
}
