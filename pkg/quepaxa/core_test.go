package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

type mockTransport struct {
	sendDecisionCalls int
	sendDecisionErr   error
	sendDecision      func(context.Context, Decision) error
}

func (m *mockTransport) SendRecord(_ context.Context, to NodeID, request RecordRequest) (Summary, error) {
	return Summary{RecorderID: to, Step: request.Step, FirstCurrent: cloneProposal(&request.Proposal)}, nil
}

func (m *mockTransport) SendDecision(ctx context.Context, decision Decision) error {
	m.sendDecisionCalls++
	if m.sendDecision != nil {
		return m.sendDecision(ctx, decision)
	}
	return m.sendDecisionErr
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

func TestCompleteDecisionRetriesLearnerQuorum(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	transport := &mockTransport{sendDecisionErr: errors.New("learners unavailable")}
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}}, wal, transport)
	value := []byte("retry learner dissemination")
	if _, _, err := core.Propose(context.Background(), value); !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("first proposal error=%v, want quorum unavailable", err)
	}
	slot, ok := core.DecidedSlot(value)
	if !ok {
		t.Fatal("failed dissemination lost the local decision")
	}
	transport.sendDecisionErr = nil
	if _, err := core.CompleteDecision(context.Background(), slot); err != nil {
		t.Fatal(err)
	}
	if transport.sendDecisionCalls != 2 {
		t.Fatalf("SendDecision calls=%d, want 2", transport.sendDecisionCalls)
	}
}

func TestCompleteDecisionAfterRestartResendsCertificate(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	transport := &mockTransport{}
	config := &Cluster{Members: []Member{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}}
	core := newCore("node-1", config, wal, transport)
	value := []byte("restart dissemination")
	if _, _, err := core.Propose(context.Background(), value); err != nil {
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
	recovered := newCore("node-1", config, reopened, transport)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	slot, ok := recovered.DecidedSlot(value)
	if !ok {
		t.Fatal("restarted core lost the decision")
	}
	if _, err := recovered.CompleteDecision(context.Background(), slot); err != nil {
		t.Fatal(err)
	}
	if transport.sendDecisionCalls != 2 {
		t.Fatalf("SendDecision calls=%d, want initial and restart resend", transport.sendDecisionCalls)
	}
}

func TestCompleteDecisionAndCompactionLockBoundary(t *testing.T) {
	newCompactable := func(t *testing.T) (*Core, *mockTransport, [32]byte) {
		t.Helper()
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		transport := &mockTransport{}
		core := newCore("node-1", &Cluster{ConfigID: 11, Members: []Member{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}}, wal, transport)
		for _, value := range [][]byte{[]byte("floor"), []byte("retained")} {
			if _, _, err := core.Propose(context.Background(), value); err != nil {
				t.Fatal(err)
			}
		}
		root := sha256.Sum256([]byte("lock-root"))
		state := sha256.Sum256([]byte("lock-state"))
		prefix, _ := core.PrefixHash(1)
		core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		order, _ := core.LeaderOrder(2)
		checkpoint := CheckpointSeal{ConfigID: 11, Index: 1, RootHash: root, StateHash: state, PrefixHash: prefix, NextLeaderOrder: order}
		if err := core.PrepareCheckpoint(context.Background(), checkpoint); err != nil {
			t.Fatal(err)
		}
		seal, err := EncodeCheckpointSeal(checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := core.Propose(context.Background(), seal); err != nil {
			t.Fatal(err)
		}
		return core, transport, root
	}

	t.Run("sync blocks compaction", func(t *testing.T) {
		core, _, root := newCompactable(t)
		core.mu.Lock()
		core.durable[2] = false
		core.logged[2] = false
		core.mu.Unlock()
		syncStarted := make(chan struct{})
		releaseSync := make(chan struct{})
		core.commits = newGroupCommit(func() error {
			close(syncStarted)
			<-releaseSync
			return core.wal.Sync()
		})
		completeDone := make(chan error, 1)
		go func() {
			_, err := core.CompleteDecision(context.Background(), 2)
			completeDone <- err
		}()
		<-syncStarted
		compactDone := make(chan error, 1)
		go func() { compactDone <- core.CompactThrough(1, root) }()
		select {
		case err := <-compactDone:
			t.Fatalf("compaction crossed an in-flight sync: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		close(releaseSync)
		if err := <-completeDone; err != nil {
			t.Fatal(err)
		}
		if err := <-compactDone; err != nil {
			t.Fatal(err)
		}
	})

	t.Run("network does not block compaction", func(t *testing.T) {
		core, transport, root := newCompactable(t)
		sendStarted := make(chan struct{})
		releaseSend := make(chan struct{})
		transport.sendDecision = func(context.Context, Decision) error {
			close(sendStarted)
			<-releaseSend
			return nil
		}
		completeDone := make(chan error, 1)
		go func() {
			_, err := core.CompleteDecision(context.Background(), 2)
			completeDone <- err
		}()
		<-sendStarted
		compactDone := make(chan error, 1)
		go func() { compactDone <- core.CompactThrough(1, root) }()
		select {
		case err := <-compactDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("network dissemination held a Core lock")
		}
		close(releaseSend)
		if err := <-completeDone; err != nil {
			t.Fatal(err)
		}
	})
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
	order, _ := core.LeaderOrder(4)
	checkpoint := CheckpointSeal{ConfigID: 3, Index: 3, RootHash: root, StateHash: state, PrefixHash: prefix3, NextLeaderOrder: order}
	if err := core.PrepareCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatal(err)
	}
	seal, err := EncodeCheckpointSeal(checkpoint)
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

func TestCompactionPreservesFollowingEpochLeaderOrder(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := &Cluster{ConfigID: 9, Members: []Member{{ID: "n1"}}}
	core := newCore("n1", config, wal, nil)
	for core.Tip() < 36 {
		if _, _, err := core.Propose(context.Background(), []byte(fmt.Sprintf("value-%d", core.Tip()+1))); err != nil {
			t.Fatal(err)
		}
	}
	prefix, ok := core.PrefixHash(36)
	if !ok {
		t.Fatal("checkpoint prefix is unavailable")
	}
	next, following, err := core.CheckpointLeaderOrders(36)
	if err != nil {
		t.Fatal(err)
	}
	if len(following) == 0 {
		t.Fatal("checkpoint omitted the compacted following epoch leader order")
	}
	root := sha256.Sum256([]byte("following-order-root"))
	seal := CheckpointSeal{
		ConfigID: config.ConfigID, Index: 36, RootHash: root,
		StateHash: sha256.Sum256([]byte("following-order-state")), PrefixHash: prefix,
		NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), encoded); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(36, root); err != nil {
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
	for recovered.Tip() < 50 {
		if _, _, err := recovered.Propose(context.Background(), []byte(fmt.Sprintf("recovered-%d", recovered.Tip()+1))); err != nil {
			t.Fatalf("propose across following epoch at tip %d: %v", recovered.Tip(), err)
		}
	}
}

func TestRestoreCheckpointBaseReplacesLaggingPrefix(t *testing.T) {
	config := &Cluster{ConfigID: 12, Members: []Member{{ID: "n1"}}}
	sourceWAL, err := qlog.Open(t.TempDir() + "/source")
	if err != nil {
		t.Fatal(err)
	}
	defer sourceWAL.Close()
	source := newCore("n1", config, sourceWAL, nil)
	for i := 1; i <= 5; i++ {
		if _, _, err := source.Propose(context.Background(), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	prefix, _ := source.PrefixHash(5)
	next, following, err := source.CheckpointLeaderOrders(5)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: config.ConfigID, Index: 5,
		RootHash: sha256.Sum256([]byte("recovery-root")), StateHash: sha256.Sum256([]byte("recovery-state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
	source.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := source.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(context.Background(), encoded); err != nil {
		t.Fatal(err)
	}
	baseDecision, ok := source.CertifiedValue(6)
	if !ok {
		t.Fatal("checkpoint decision is unavailable")
	}
	first, _ := source.CertifiedValue(1)

	targetWAL, err := qlog.Open(t.TempDir() + "/target")
	if err != nil {
		t.Fatal(err)
	}
	defer targetWAL.Close()
	target := newCore("n1", config, targetWAL, nil)
	if err := target.AcceptCertifiedValues([]DecidedValue{first}); err != nil {
		t.Fatal(err)
	}
	target.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := target.RestoreCheckpointBase(context.Background(), seal, baseDecision); err != nil {
		t.Fatal(err)
	}
	if target.Tip() != 5 || target.CompactionFloor() != 5 {
		t.Fatalf("tip=%d floor=%d, want 5/5", target.Tip(), target.CompactionFloor())
	}
	if err := target.AcceptCertifiedValues([]DecidedValue{baseDecision}); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareCheckpointLocksOneRootPerIndexAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := &Cluster{ConfigID: 3, Members: []Member{{ID: "n1"}}}
	core := newCore("n1", config, wal, nil)
	if _, _, err := core.Propose(context.Background(), []byte("value")); err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(1)
	order, _ := core.LeaderOrder(2)
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	first := CheckpointSeal{ConfigID: 3, Index: 1, RootHash: sha256.Sum256([]byte("first")), StateHash: sha256.Sum256([]byte("state")), PrefixHash: prefix, NextLeaderOrder: order}
	if err := core.PrepareCheckpoint(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.RootHash = sha256.Sum256([]byte("second"))
	if err := core.PrepareCheckpoint(context.Background(), second); err == nil {
		t.Fatal("second root was prepared at the same index")
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
	if err := recovered.RequirePreparedCheckpoint(first); err != nil {
		t.Fatal(err)
	}
	if err := recovered.RequirePreparedCheckpoint(second); err == nil {
		t.Fatal("restart lost the one-root prepare lock")
	}
}

func TestCompactionPrunesAllocatorAndRejectsClosedSlots(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{ConfigID: 7, Members: []Member{{ID: "n1"}}}, wal, nil)
	for range 2 {
		if _, _, err := core.Propose(context.Background(), []byte("same")); err != nil {
			t.Fatal(err)
		}
	}
	if slot, ok := core.DecidedSlot([]byte("same")); !ok || slot != 1 {
		t.Fatalf("canonical byHash slot=%d ok=%v, want 1", slot, ok)
	}
	closed, ok := core.CertifiedValue(1)
	if !ok {
		t.Fatal("slot 1 is not certified")
	}
	root := sha256.Sum256([]byte("root"))
	state := sha256.Sum256([]byte("state"))
	prefix, _ := core.PrefixHash(1)
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	order, _ := core.LeaderOrder(2)
	checkpoint := CheckpointSeal{ConfigID: 7, Index: 1, RootHash: root, StateHash: state, PrefixHash: prefix, NextLeaderOrder: order}
	if err := core.PrepareCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatal(err)
	}
	seal, err := EncodeCheckpointSeal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	core.vacant = []Slot{1}
	core.nextSlot = 1
	if err := core.CompactThrough(1, root); err != nil {
		t.Fatal(err)
	}
	if slot, ok := core.DecidedSlot([]byte("same")); !ok || slot != 2 {
		t.Fatalf("retained byHash slot=%d ok=%v, want 2", slot, ok)
	}
	if err := core.AcceptCertifiedValue(closed); !errors.Is(err, ErrCompacted) {
		t.Fatalf("catch-up error=%v, want ErrCompacted", err)
	}
	if err := core.EnsureDurable(1); err != nil {
		t.Fatalf("covered durability error=%v", err)
	}
	if _, err := core.CompleteDecision(context.Background(), 1); !errors.Is(err, ErrCompacted) {
		t.Fatalf("complete error=%v, want ErrCompacted cache miss", err)
	}
	covered, err := core.certifiedDecision(closed)
	if err != nil {
		t.Fatal(err)
	}
	if err := core.AcceptDecision(covered); err != nil {
		t.Fatalf("covered learned decision error=%v", err)
	}
	if _, ok := core.CertifiedValue(1); ok {
		t.Fatal("covered learned decision was reinserted")
	}
	sourceWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sourceWAL.Close()
	source := newCore("n1", &Cluster{ConfigID: 7, Members: []Member{{ID: "n1"}}}, sourceWAL, nil)
	for slot := Slot(1); slot <= 4; slot++ {
		if _, _, err := source.Propose(context.Background(), []byte{byte(slot)}); err != nil {
			t.Fatal(err)
		}
	}
	newer, ok := source.CertifiedValue(4)
	if !ok {
		t.Fatal("source slot 4 is not certified")
	}
	if err := core.AcceptCertifiedValues([]DecidedValue{newer, closed}); !errors.Is(err, ErrCompacted) {
		t.Fatalf("mixed catch-up error=%v, want ErrCompacted", err)
	}
	if _, ok := core.CertifiedValue(4); ok {
		t.Fatal("mixed catch-up partially installed slot 4")
	}
	proposal := newProposal(highestPriority, "n1", []byte("stale"))
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 1, Step: 4, Proposal: proposal}); !errors.Is(err, ErrCompacted) {
		t.Fatalf("record error=%v, want ErrCompacted", err)
	}
	if slot, _, err := core.Propose(context.Background(), []byte("next")); err != nil || slot != 4 {
		t.Fatalf("next slot=%d err=%v, want 4", slot, err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("n1", &Cluster{ConfigID: 7, Members: []Member{{ID: "n1"}}}, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if slot, ok := recovered.DecidedSlot([]byte("same")); !ok || slot != 2 {
		t.Fatalf("recovered byHash slot=%d ok=%v, want 2", slot, ok)
	}
}

func TestRecorderCapRejectsOversizedValue(t *testing.T) {
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

func TestDecisionsFromStopsAtGap(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}}}, wal, &mockTransport{})
	value := []byte("SELECT 2")
	proposal := newProposal(highestPriority, "node-1", value)
	decision := Decision{Slot: 2, Step: 4, Proposal: proposal, Summaries: []Summary{{RecorderID: "node-1", Step: 4, FirstCurrent: cloneProposal(&proposal)}}}
	if err := core.AcceptDecision(decision); err != nil {
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
