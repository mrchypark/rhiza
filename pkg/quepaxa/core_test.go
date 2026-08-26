package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

type mockTransport struct{}

func (m *mockTransport) SendRecord(_ context.Context, to NodeID, request RecordRequest) (Summary, error) {
	return Summary{RecorderID: to, Step: request.Step, FirstCurrent: cloneProposal(&request.Proposal)}, nil
}

func (m *mockTransport) SendDecision(context.Context, Decision) error {
	return nil
}

func (m *mockTransport) ReadTip(context.Context, NodeID) (Slot, error)               { return 0, nil }
func (m *mockTransport) StageValue(context.Context, NodeID, ValueHash, []byte) error { return nil }
func (m *mockTransport) FetchValue(context.Context, NodeID, ValueHash) ([]byte, error) {
	return nil, errors.New("value unavailable")
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

func TestCoreRejectsOversizedValue(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}}}, wal, nil)
	if _, _, err := core.Propose(context.Background(), make([]byte, MaxReplicatedValueBytes+1)); err == nil {
		t.Fatal("oversized value was accepted")
	}
}

func TestValueIsNotAvailableUntilRetrySyncSucceeds(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{Members: []Member{{ID: "n1"}}}, wal, nil)
	value := []byte("durable-only")
	hash := sha256.Sum256(value)
	core.commits = newGroupCommit(func() error { return errors.New("sync failed") })
	if err := core.StoreValue(hash, value); err == nil {
		t.Fatal("failed sync was acknowledged")
	}
	if _, ok := core.Value(hash); ok {
		t.Fatal("unsynced value became available")
	}
	core.commits = newGroupCommit(wal.Sync)
	if err := core.StoreValue(hash, value); err != nil {
		t.Fatal(err)
	}
	if got, ok := core.Value(hash); !ok || !bytes.Equal(got, value) {
		t.Fatal("synced retry did not publish value")
	}
}

func TestCertifiedCompactionRestartsFromBaseAndSuffix(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := &Cluster{ConfigID: 3, Members: []Member{{ID: "n1"}}}
	core := newCore("n1", config, wal, nil)
	for i := byte(1); i <= 5; i++ {
		if _, _, err := core.Propose(context.Background(), []byte{i}); err != nil {
			t.Fatal(err)
		}
	}
	wantPrefix, _ := core.PrefixHash(5)
	root := sha256.Sum256([]byte("certified-root"))
	state := sha256.Sum256([]byte("snapshot"))
	prefix3, _ := core.PrefixHash(3)
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	seal, err := EncodeCheckpointSeal(CheckpointSeal{ConfigID: 3, Index: 3, RootHash: root, StateHash: state, PrefixHash: prefix3})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(3, root); err != nil {
		t.Fatal(err)
	}
	if core.Tip() != 6 {
		t.Fatalf("live compacted tip=%d, want 6", core.Tip())
	}
	if _, _, err := core.DecisionsFrom(1, 10); !errors.Is(err, ErrCompacted) {
		t.Fatalf("compacted read error=%v", err)
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
	if recovered.Tip() != 6 {
		t.Fatalf("recovered tip=%d, want 6", recovered.Tip())
	}
	if got, ok := recovered.PrefixHash(5); !ok || got != wantPrefix {
		t.Fatal("recovered suffix prefix hash mismatch")
	}
	if slot, _, err := recovered.Propose(context.Background(), []byte("next")); err != nil || slot != 7 {
		t.Fatalf("next slot=%d err=%v", slot, err)
	}
}

func TestRecorderCapPreservesLegacyRecovery(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}}}, wal, nil)
	proposal := newProposal(Priority{1}, "node-1", make([]byte, MaxReplicatedValueBytes+1))
	request := RecordRequest{Slot: 1, Step: 4, Proposal: proposal}
	if _, err := core.Record(context.Background(), request); err == nil {
		t.Fatal("new oversized recorder value was accepted")
	}
	core.recorders[1] = ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}
	if _, err := core.Record(context.Background(), request); err != nil {
		t.Fatalf("legacy recorder recovery was blocked: %v", err)
	}
}

func TestEnsureDurableWritesDecisionMarker(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	config := &Cluster{Members: []Member{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}}
	core := newCore("node-1", config, wal, &mockTransport{})
	slot, _, err := core.Propose(context.Background(), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if err := core.EnsureDurable(slot); err != nil {
		t.Fatal(err)
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Slot == uint64(slot) && entry.Type == qlog.EntryDecide {
			return
		}
	}
	t.Fatal("durability barrier did not persist the certified decision")
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
