package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

type mockTransport struct {
	sendDecisionCalls int
	sendDecisionErr   error
	sendDecision      func(context.Context, Decision) error
	stageCalls        atomic.Uint64
	inlineValues      atomic.Uint64
}

type cancelReadTransport struct {
	mockTransport
	started  chan struct{}
	canceled chan struct{}
}

type phaseFaultTransport struct {
	cores  map[NodeID]*Core
	n2Down atomic.Bool
}

type parallelFetchTransport struct {
	mockTransport
	value    []byte
	canceled chan struct{}
}

func (t *parallelFetchTransport) FetchValue(ctx context.Context, from NodeID, _ ValueHash) ([]byte, error) {
	if from == "n1" {
		<-ctx.Done()
		close(t.canceled)
		return nil, ctx.Err()
	}
	return append([]byte(nil), t.value...), nil
}

func (t *phaseFaultTransport) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	if to == "n2" {
		if request.Step != 4 || t.n2Down.Load() {
			return Summary{}, errors.New("n2 down")
		}
		summary, err := t.cores[to].Record(ctx, request)
		t.n2Down.Store(true)
		return summary, err
	}
	if to == "n3" && request.Step == 4 {
		return Summary{}, errors.New("n3 missed first record")
	}
	return t.cores[to].Record(ctx, request)
}

func (t *phaseFaultTransport) SendDecision(context.Context, Decision) error { return nil }
func (t *phaseFaultTransport) ReadTip(context.Context, NodeID) (Slot, error) {
	return 0, errors.New("not used")
}
func (t *phaseFaultTransport) StageValue(context.Context, NodeID, ValueHash, []byte) error {
	return errors.New("not used")
}
func (t *phaseFaultTransport) FetchValue(_ context.Context, from NodeID, hash ValueHash) ([]byte, error) {
	value, ok := t.cores[from].Value(hash)
	if !ok {
		return nil, errors.New("value unavailable")
	}
	return value, nil
}

func (t *cancelReadTransport) ReadTip(ctx context.Context, to NodeID) (Slot, error) {
	if to == "node-2" {
		return 0, nil
	}
	close(t.started)
	<-ctx.Done()
	close(t.canceled)
	return 0, ctx.Err()
}

func (m *mockTransport) SendRecord(_ context.Context, to NodeID, request RecordRequest) (Summary, error) {
	if len(request.Proposal.Value) != 0 {
		m.inlineValues.Add(1)
	}
	return Summary{RecorderID: to, Step: request.Step, FirstCurrent: cloneProposal(&request.Proposal)}, nil
}

func (m *mockTransport) SendDecision(ctx context.Context, decision Decision) error {
	m.sendDecisionCalls++
	if m.sendDecision != nil {
		return m.sendDecision(ctx, decision)
	}
	return m.sendDecisionErr
}

func (m *mockTransport) ReadTip(context.Context, NodeID) (Slot, error) { return 0, nil }
func (m *mockTransport) StageValue(context.Context, NodeID, ValueHash, []byte) error {
	m.stageCalls.Add(1)
	return nil
}
func (m *mockTransport) FetchValue(context.Context, NodeID, ValueHash) ([]byte, error) {
	return nil, errors.New("value unavailable")
}

func TestHydrateProposalUsesFirstAvailableSource(t *testing.T) {
	value := []byte("value")
	transport := &parallelFetchTransport{value: value, canceled: make(chan struct{})}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n2", &Cluster{Members: []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}, wal, transport)
	proposal := Proposal{Hash: sha256.Sum256(value)}
	if err := core.hydrateProposal(context.Background(), &proposal, "n1", "n3"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(proposal.Value, value) {
		t.Fatalf("value=%q", proposal.Value)
	}
	select {
	case <-transport.canceled:
	case <-time.After(time.Second):
		t.Fatal("slower fetch was not canceled")
	}
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

	transport := &mockTransport{}
	core := newCore("node-1", config, wal, transport)

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
	if got := transport.stageCalls.Load(); got != 0 {
		t.Fatalf("normal proposal used %d separate StageValue RPCs", got)
	}
	if got := transport.inlineValues.Load(); got == 0 {
		t.Fatal("first Record round omitted the proposal value")
	}
}

func TestSlowPathSurvivesLossOfInitialValueHolder(t *testing.T) {
	cores, _ := newTestCluster(t)
	transport := &phaseFaultTransport{cores: cores}
	cores["n1"].transport = transport
	cores["n2"].transport = transport
	cores["n3"].transport = transport

	seed := newProposal(Priority{31: 1}, "n2", []byte("n2 slow-path seed"))
	if _, err := cores["n2"].Record(context.Background(), RecordRequest{Slot: 1, Step: 4, Proposal: seed}); err != nil {
		t.Fatal(err)
	}
	offered := []byte("n1 offered value")
	decision, err := cores["n1"].runSlot(context.Background(), 1, offered, true)
	if err != nil {
		t.Fatalf("slow path lost quorum after n2 failed: %v", err)
	}
	if decision.Step != 6 || !bytes.Equal(decision.Proposal.Value, offered) {
		t.Fatalf("slow-path decision=%+v, want n1 value at phase 2", decision)
	}
	hash := sha256.Sum256(offered)
	if value, ok := cores["n3"].Value(hash); !ok || !bytes.Equal(value, offered) {
		t.Fatalf("n3 did not receive the value after missing phase 0: %q ok=%v", value, ok)
	}
}

func TestReadIndexCancelsOutstandingPeerRPCs(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	transport := &cancelReadTransport{started: make(chan struct{}), canceled: make(chan struct{})}
	core := newCore("node-1", &Cluster{ConfigID: 1, Members: []Member{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}}, wal, transport)
	if _, _, err := core.ReadIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-transport.canceled:
	case <-time.After(time.Second):
		t.Fatal("read-index left a losing peer RPC running")
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
	failedSlot, _, err := core.Propose(context.Background(), value)
	if !errors.Is(err, ErrQuorumUnavailable) || failedSlot != 1 {
		t.Fatalf("first proposal slot=%d error=%v, want slot 1 with quorum unavailable", failedSlot, err)
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

func TestFreshClusterDecisionSkipsRedundantLocalSync(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	transport := &mockTransport{}
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}}, wal, transport)
	proposal := newProposal(highestPriority, "node-1", []byte("fresh decision"))
	decision := Decision{Slot: 1, Step: 4, Proposal: proposal, Summaries: []Summary{
		{RecorderID: "node-1", Step: 4, FirstCurrent: cloneProposal(&proposal)},
		{RecorderID: "node-2", Step: 4, FirstCurrent: cloneProposal(&proposal)},
	}}
	if err := core.acceptDecision(decision); err != nil {
		t.Fatal(err)
	}
	syncs := 0
	core.commits = newGroupCommit(func() error {
		syncs++
		return wal.Sync()
	})
	if _, err := core.completeDecision(context.Background(), 1, false); err != nil {
		t.Fatal(err)
	}
	if syncs != 0 {
		t.Fatalf("fresh completion syncs=%d, want 0", syncs)
	}
	if !core.logged[1] || core.durable[1] {
		t.Fatalf("fresh completion logged=%v durable=%v, want logged without a second barrier", core.logged[1], core.durable[1])
	}
	if _, err := core.CompleteDecision(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if syncs != 1 {
		t.Fatalf("retry completion syncs=%d, want 1", syncs)
	}
}

func TestAcceptCertifiedValueForAckReusesRecorderDurability(t *testing.T) {
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	config := &Cluster{ConfigID: 1, Members: members}
	proposal := newProposal(highestPriority, "n1", []byte("proxied value"))
	decision := Decision{Slot: 1, Step: 4, Proposal: proposal, Summaries: []Summary{
		{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)},
		{RecorderID: "n2", Step: 4, FirstCurrent: cloneProposal(&proposal)},
	}}
	certificate, err := encodeCertificate(config.ConfigID, decision)
	if err != nil {
		t.Fatal(err)
	}
	value := DecidedValue{Slot: decision.Slot, Hash: proposal.Hash, Value: proposal.Value, Certificate: certificate}
	for _, id := range []NodeID{"n2", "n3"} {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer wal.Close()
		core := newCore(id, config, wal, nil)
		if id == "n2" {
			if _, err := core.Record(context.Background(), RecordRequest{Slot: decision.Slot, Step: decision.Step, Proposal: proposal}); err != nil {
				t.Fatal(err)
			}
		}
		syncs := 0
		core.commits = newGroupCommit(func() error {
			syncs++
			return core.wal.Sync()
		})
		if err := core.AcceptCertifiedValueForAck(value); err != nil {
			t.Fatal(err)
		}
		want := 1
		if id == "n2" {
			want = 0
		}
		if syncs != want {
			t.Fatalf("node %s syncs=%d, want %d", id, syncs, want)
		}
	}
}

func TestProposeCertifiedDefersLearnerCompletion(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	transport := &mockTransport{}
	core := newCore("node-1", &Cluster{Members: []Member{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}}, wal, transport)
	slot, _, err := core.ProposeCertified(context.Background(), []byte("certified response"))
	if err != nil {
		t.Fatal(err)
	}
	if transport.sendDecisionCalls != 0 {
		t.Fatalf("learner sends=%d, want 0 before the caller installs the certificate", transport.sendDecisionCalls)
	}
	if _, ok := core.CertifiedValue(slot); !ok {
		t.Fatal("recorder-quorum decision is unavailable")
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
		// Both recorders are required for quorum, so no recorder goroutine can
		// outlive setup while the test swaps the group-commit hook.
		core := newCore("node-1", &Cluster{ConfigID: 11, Members: []Member{{ID: "node-1"}, {ID: "node-2"}}}, wal, transport)
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
		var syncStartedOnce sync.Once
		core.commits = newGroupCommit(func() error {
			syncStartedOnce.Do(func() { close(syncStarted) })
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
		barrierDone := make(chan struct{})
		go func() {
			core.lockCompactionBarrier()
			core.unlockCompactionBarrier()
			close(barrierDone)
		}()
		select {
		case <-barrierDone:
		case <-time.After(5 * time.Second):
			t.Fatal("network dissemination held a Core lock")
		}
		close(releaseSend)
		if err := <-completeDone; err != nil {
			t.Fatal(err)
		}
		if err := core.CompactThrough(1, root); err != nil {
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
	if index, prepared, ok := core.LatestPreparedCheckpoint(); !ok || index != 3 || prepared != root {
		t.Fatalf("prepared checkpoint index=%d root=%x ok=%v", index, prepared, ok)
	}
	seal, err := EncodeCheckpointSeal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	oldRoot := sha256.Sum256([]byte("old-root"))
	core.preparedCheckpoints[1] = oldRoot
	core.sealedRoots[oldRoot] = SealedCheckpoint{CheckpointSeal: CheckpointSeal{Index: 1, RootHash: oldRoot}}
	if err := core.CompactThrough(3, root); err != nil {
		t.Fatal(err)
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type == qlog.EntryProposal {
			t.Fatalf("compacted decided suffix retained duplicate proposal payload for %x", entry.Hash[:8])
		}
	}
	if _, ok := core.preparedCheckpoints[1]; ok {
		t.Fatal("compaction retained obsolete prepared checkpoint")
	}
	if _, ok := core.sealedRoots[oldRoot]; ok {
		t.Fatal("compaction retained obsolete sealed checkpoint")
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
	if got := len(recovered.values); got != 3 {
		t.Fatalf("recovered live values=%d, want 3 suffix values", got)
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

func testCheckpointRecoveryBase(t *testing.T) (*Core, CheckpointSeal, DecidedValue) {
	t.Helper()
	config := &Cluster{ConfigID: 12, Members: []Member{{ID: "n1"}}}
	sourceWAL, err := qlog.Open(t.TempDir() + "/source")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceWAL.Close() })
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
	return source, seal, baseDecision
}

func TestRestoreCheckpointBaseReplacesLaggingPrefix(t *testing.T) {
	source, seal, baseDecision := testCheckpointRecoveryBase(t)
	config := source.config
	first, _ := source.CertifiedValue(1)
	if _, _, err := source.Propose(context.Background(), []byte("retained suffix")); err != nil {
		t.Fatal(err)
	}
	suffix, ok := source.CertifiedValue(7)
	if !ok {
		t.Fatal("suffix decision is unavailable")
	}

	targetWAL, err := qlog.Open(t.TempDir() + "/target")
	if err != nil {
		t.Fatal(err)
	}
	defer targetWAL.Close()
	target := newCore("n1", config, targetWAL, nil)
	if err := target.AcceptCertifiedValues([]DecidedValue{first, suffix, baseDecision}); err != nil {
		t.Fatal(err)
	}
	target.releaseSlot(2)
	target.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	failingWAL, err := qlog.Open(t.TempDir() + "/failing-target")
	if err != nil {
		t.Fatal(err)
	}
	defer failingWAL.Close()
	if err := failingWAL.SetMaxBytes(1); err != nil {
		t.Fatal(err)
	}
	failing := newCore("n1", config, failingWAL, nil)
	failing.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := failing.RestoreCheckpointBase(context.Background(), seal, baseDecision); !errors.Is(err, qlog.ErrCapacity) {
		t.Fatalf("restore error=%v, want WAL capacity", err)
	}
	if failing.Tip() != 0 || failing.CompactionFloor() != 0 {
		t.Fatalf("failed restore changed memory state: tip=%d floor=%d", failing.Tip(), failing.CompactionFloor())
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	woken := make(chan error, 1)
	go func() { woken <- target.WaitTip(waitCtx, 7) }()
	if err := target.RestoreCheckpointBase(context.Background(), seal, baseDecision); err != nil {
		t.Fatal(err)
	}
	if target.Tip() != 7 || target.CompactionFloor() != 5 {
		t.Fatalf("tip=%d floor=%d, want 7/5", target.Tip(), target.CompactionFloor())
	}
	target.mu.Lock()
	target.floorRoot = sha256.Sum256([]byte("wrong recovery root"))
	target.mu.Unlock()
	if err := target.ValidateCheckpointBase(context.Background(), seal, baseDecision); err == nil || !strings.Contains(err.Error(), "recovery root") {
		t.Fatalf("warm checkpoint validation error=%v", err)
	}
	select {
	case err := <-woken:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("checkpoint base did not wake a retained-suffix waiter")
	}
	target.slotMu.Lock()
	for _, slot := range target.vacant {
		if slot <= 5 {
			target.slotMu.Unlock()
			t.Fatalf("checkpoint base retained compacted vacant slot %d", slot)
		}
	}
	target.slotMu.Unlock()
}

func TestRestoreCheckpointBasePreservesUnresolvedRecorderValue(t *testing.T) {
	source, seal, baseDecision := testCheckpointRecoveryBase(t)
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	target := newCore("n1", source.config, wal, nil)
	target.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := target.AcceptCertifiedValue(baseDecision); err != nil {
		t.Fatal(err)
	}
	proposal := newProposal(Priority{31: 1}, "n1", []byte("unresolved recorder value"))
	if _, err := target.Record(context.Background(), RecordRequest{Slot: 20, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if err := target.RestoreCheckpointBase(context.Background(), seal, baseDecision); err != nil {
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
	recovered := newCore("n1", source.config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if value, ok := recovered.Value(proposal.Hash); !ok || !bytes.Equal(value, proposal.Value) {
		t.Fatalf("recovered unresolved value=%q ok=%v", value, ok)
	}
	if err := recovered.RecoverThrough(context.Background(), 20); err != nil {
		t.Fatalf("recover unresolved recorder state: %v", err)
	}
	decision, ok := recovered.CertifiedValue(20)
	if !ok || !bytes.Equal(decision.Value, proposal.Value) {
		t.Fatalf("recovered slot 20 value=%q ok=%v", decision.Value, ok)
	}
}

func TestRestoreCheckpointBasePreservesUnloggedHintValue(t *testing.T) {
	source, seal, baseDecision := testCheckpointRecoveryBase(t)
	if _, _, err := source.Propose(context.Background(), []byte("hint-only decision")); err != nil {
		t.Fatal(err)
	}
	hint, ok := source.CertifiedValue(7)
	if !ok {
		t.Fatal("hint decision is unavailable")
	}
	decision, err := decodeDecision(hint.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = hint.Value

	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	target := newCore("n1", source.config, wal, nil)
	target.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := target.AcceptCertifiedValue(baseDecision); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Record(context.Background(), RecordRequest{Slot: decision.Slot, Step: decision.Step, Proposal: decision.Proposal}); err != nil {
		t.Fatal(err)
	}
	if err := target.AcceptCertifiedHints([]DecidedValue{hint}); err != nil {
		t.Fatal(err)
	}
	if target.logged[hint.Slot] {
		t.Fatal("hint unexpectedly reached the decision WAL")
	}
	if err := target.RestoreCheckpointBase(context.Background(), seal, baseDecision); err != nil {
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
	recovered := newCore("n1", source.config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if value, ok := recovered.Value(hint.Hash); !ok || !bytes.Equal(value, hint.Value) {
		t.Fatalf("recovered hint value=%q ok=%v", value, ok)
	}
	if err := recovered.RecoverThrough(context.Background(), hint.Slot); err != nil {
		t.Fatalf("recover hint recorder state: %v", err)
	}
}

func TestCompactionRewritesValueReusedAcrossFloors(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := &Cluster{ConfigID: 13, Members: []Member{{ID: "n1"}}}
	core := newCore("n1", config, wal, nil)
	for i := 1; i <= 5; i++ {
		if _, _, err := core.Propose(context.Background(), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	reused, ok := core.CertifiedValue(5)
	if !ok {
		t.Fatal("reused decision is unavailable")
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	prepare := func(index Slot, name string) [32]byte {
		prefix, _ := core.PrefixHash(index)
		next, following, err := core.CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatal(err)
		}
		root := sha256.Sum256([]byte(name))
		seal := CheckpointSeal{ConfigID: config.ConfigID, Index: index, RootHash: root, StateHash: sha256.Sum256([]byte(name + "-state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
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
		return root
	}
	if err := core.CompactThrough(3, prepare(3, "first-floor")); err != nil {
		t.Fatal(err)
	}
	proposal := newProposal(Priority{31: 1}, "n1", reused.Value)
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 20, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if err := core.CompactThrough(5, prepare(5, "second-floor")); err != nil {
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
	if value, ok := recovered.Value(reused.Hash); !ok || !bytes.Equal(value, reused.Value) {
		t.Fatalf("recovered reused value=%q ok=%v", value, ok)
	}
	if err := recovered.RecoverThrough(context.Background(), 20); err != nil {
		t.Fatalf("recover reused recorder state: %v", err)
	}
}

func TestCompactionRetainsHashReusedAfterFence(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("reused after compaction fence")
	hash := sha256.Sum256(value)
	core := newCore("n1", &Cluster{ConfigID: 14, Members: []Member{{ID: "n1"}}}, wal, nil)
	if err := core.StoreValue(hash, value); err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	plan, err := core.beginCompactionLocked(qlog.Entry{Slot: 1, Type: qlog.EntryCheckpoint, Hash: [32]byte{1}, Payload: []byte("base")}, nil)
	core.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	finished := false
	defer func() {
		if !finished {
			core.finishCompaction()
		}
	}()
	if err := core.StageValue(hash, value); err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); err != nil {
		t.Fatal(err)
	}
	core.finishCompaction()
	finished = true
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Read()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if entry.Type == qlog.EntryProposal && entry.Hash == hash && bytes.Equal(entry.Payload, value) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("reused proposal count=%d, want 1", count)
	}
}

func TestCompactionDoesNotDuplicateLoggedSuffixReusedAfterFence(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := &Cluster{ConfigID: 15, Members: []Member{{ID: "n1"}}}
	core := newCore("n1", config, wal, nil)
	if _, _, err := core.Propose(context.Background(), []byte("base")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), []byte("logged suffix")); err != nil {
		t.Fatal(err)
	}
	suffix, ok := core.CertifiedValue(2)
	if !ok {
		t.Fatal("logged suffix is unavailable")
	}
	prefix, _ := core.PrefixHash(1)
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	root := sha256.Sum256([]byte("fence-root"))
	base := consensusBase{ConfigID: config.ConfigID, ClosedThrough: 1, PrefixHash: prefix, RecoveryRoot: root, LeaderEpoch: leaderEpoch(2), NextLeaderOrder: next, FollowingLeaderOrder: following}
	payload, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	plan, err := core.beginCompactionLocked(qlog.Entry{Slot: 1, Type: qlog.EntryCheckpoint, Hash: root, Payload: payload}, nil)
	core.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	finished := false
	defer func() {
		if !finished {
			core.finishCompaction()
		}
	}()
	proposal := newProposal(Priority{31: 1}, "n1", suffix.Value)
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 20, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Commit(); err != nil {
		t.Fatal(err)
	}
	core.finishCompaction()
	finished = true
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	entries, err := reopened.Read()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type == qlog.EntryProposal && entry.Hash == suffix.Hash {
			t.Fatal("logged suffix was duplicated as a raw proposal")
		}
	}
	recovered := newCore("n1", config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if err := recovered.RecoverThrough(context.Background(), 20); err != nil {
		t.Fatalf("recover reused suffix recorder state: %v", err)
	}
}

func TestRestoreCheckpointBaseDropsProposalDuplicatedByDecidedSuffix(t *testing.T) {
	source, seal, baseDecision := testCheckpointRecoveryBase(t)
	if _, _, err := source.Propose(context.Background(), []byte("retained slot 7")); err != nil {
		t.Fatal(err)
	}
	shared := []byte("decided suffix also held by unresolved recorder")
	if _, _, err := source.Propose(context.Background(), shared); err != nil {
		t.Fatal(err)
	}
	slot7, ok := source.CertifiedValue(7)
	if !ok {
		t.Fatal("slot 7 decision is unavailable")
	}
	slot8, ok := source.CertifiedValue(8)
	if !ok {
		t.Fatal("slot 8 decision is unavailable")
	}

	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	target := newCore("n1", source.config, wal, nil)
	target.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	proposal := newProposal(Priority{31: 1}, "n1", shared)
	if err := target.StageValue(proposal.Hash, proposal.Value); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Record(context.Background(), RecordRequest{Slot: 20, Step: 4, Proposal: proposal}); err != nil {
		t.Fatal(err)
	}
	if err := target.AcceptCertifiedValues([]DecidedValue{baseDecision, slot7, slot8}); err != nil {
		t.Fatal(err)
	}
	if err := target.RestoreCheckpointBase(context.Background(), seal, baseDecision); err != nil {
		t.Fatal(err)
	}
	entries, err := wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type == qlog.EntryProposal && entry.Hash == proposal.Hash {
			t.Fatal("compacted WAL retained proposal already carried by decided suffix")
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
	recovered := newCore("n1", source.config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if value, ok := recovered.Value(proposal.Hash); !ok || !bytes.Equal(value, proposal.Value) {
		t.Fatalf("recovered decided value=%q ok=%v", value, ok)
	}
	if err := recovered.RecoverThrough(context.Background(), 20); err != nil {
		t.Fatalf("recover unresolved recorder state: %v", err)
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

func TestPrepareCheckpointPersistsLearnedPrefixAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := &Cluster{ConfigID: 3, Members: []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}
	core := newCore("n2", config, wal, nil)
	for slot := Slot(1); slot <= 3; slot++ {
		proposal := newProposal(highestPriority, "n1", []byte(fmt.Sprintf("learned-%d", slot)))
		recorded, err := core.Record(context.Background(), RecordRequest{Slot: slot, Step: 4, Proposal: proposal})
		if err != nil {
			t.Fatal(err)
		}
		decision := Decision{Slot: slot, Step: 4, Proposal: proposal, Summaries: []Summary{{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)}, recorded}}
		if err := core.AcceptDecisionHint(decision); err != nil {
			t.Fatal(err)
		}
		if core.logged[slot] || core.durable[slot] {
			t.Fatalf("learned decision %d was already durable before checkpoint prepare", slot)
		}
	}
	prefix, ok := core.PrefixHash(3)
	if !ok {
		t.Fatal("learned decision did not advance the certified prefix")
	}
	order, err := core.LeaderOrder(4)
	if err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	seal := CheckpointSeal{ConfigID: 3, Index: 3, RootHash: sha256.Sum256([]byte("root")), StateHash: sha256.Sum256([]byte("state")), PrefixHash: prefix, NextLeaderOrder: order}
	core.commits = newGroupCommit(func() error { return errors.New("sync failed") })
	if err := core.PrepareCheckpoint(context.Background(), seal); err == nil {
		t.Fatal("failed prefix sync was acknowledged")
	}
	for slot := Slot(1); slot <= 3; slot++ {
		if core.durable[slot] {
			t.Fatalf("learned decision %d became durable after failed sync", slot)
		}
	}
	core.commits = newGroupCommit(wal.Sync)
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	for slot := Slot(1); slot <= 3; slot++ {
		if !core.logged[slot] || !core.durable[slot] {
			t.Fatalf("checkpoint prepare did not make certified prefix slot %d durable", slot)
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
	recovered := newCore("n2", config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if err := recovered.RequirePreparedCheckpoint(seal); err != nil {
		t.Fatal(err)
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

func TestSlowPathDrawsOnePriorityPerRound(t *testing.T) {
	cores, _ := newTestCluster(t)
	core := cores["n1"]
	core.releaseSlot(1)
	draws := 0
	core.priority = func() (Priority, error) {
		draws++
		return Priority{1}, nil
	}
	if _, _, err := core.Propose(context.Background(), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if draws != 1 {
		t.Fatalf("priority draws=%d, want 1 per proposal round", draws)
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
	if cap(decisions) != 0 {
		t.Fatalf("empty decision page capacity=%d, want 0", cap(decisions))
	}
}

func TestDecisionsFromBoundsPageCapacity(t *testing.T) {
	core := &Core{decided: map[Slot]DecidedValue{1: {Slot: 1}}, tip: 1}
	decisions, tip, err := core.DecisionsFrom(1, 256)
	if err != nil || tip != 1 || len(decisions) != 1 || cap(decisions) != 1 {
		t.Fatalf("tip=%d len=%d cap=%d err=%v", tip, len(decisions), cap(decisions), err)
	}
}

func TestCompleteDecisionWaitsForContiguousPrefix(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{Members: []Member{{ID: "n1"}}}, wal, nil)
	decision := func(slot Slot, value string) Decision {
		proposal := newProposal(highestPriority, "n1", []byte(value))
		return Decision{Slot: slot, Step: 4, Proposal: proposal, Summaries: []Summary{{RecorderID: "n1", Step: 4, FirstCurrent: cloneProposal(&proposal)}}}
	}
	if err := core.AcceptDecision(decision(2, "two")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := core.CompleteDecision(context.Background(), 2)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("slot 2 completed before slot 1: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := core.AcceptDecision(decision(1, "one")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("slot 2 did not complete after prefix became contiguous")
	}
}

func TestPreparedCheckpointIsMonotonicFence(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{ConfigID: 3, Members: []Member{{ID: "n1"}}}, wal, nil)
	for i := byte(1); i <= 4; i++ {
		if _, _, err := core.Propose(context.Background(), []byte{i}); err != nil {
			t.Fatal(err)
		}
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	makeSeal := func(index Slot, root [32]byte) CheckpointSeal {
		prefix, _ := core.PrefixHash(index)
		order, following, err := core.CheckpointLeaderOrders(index)
		if err != nil {
			t.Fatal(err)
		}
		return CheckpointSeal{ConfigID: 3, Index: index, RootHash: root, StateHash: sha256.Sum256([]byte{byte(index)}), PrefixHash: prefix, NextLeaderOrder: order, FollowingLeaderOrder: following}
	}
	root3 := sha256.Sum256([]byte("root-3"))
	seal3 := makeSeal(3, root3)
	if err := core.PrepareCheckpoint(context.Background(), seal3); err != nil {
		t.Fatal(err)
	}
	if err := core.PrepareCheckpoint(context.Background(), makeSeal(2, sha256.Sum256([]byte("root-2")))); err == nil {
		t.Fatal("lower checkpoint crossed prepared fence")
	}
	conflict := sha256.Sum256([]byte("conflict"))
	if err := core.PrepareCheckpoint(context.Background(), makeSeal(3, conflict)); err == nil {
		t.Fatal("same-index conflicting root crossed prepared fence")
	}
	root4 := sha256.Sum256([]byte("root-4"))
	if err := core.PrepareCheckpoint(context.Background(), makeSeal(4, root4)); err != nil {
		t.Fatal(err)
	}
	if index, root, ok := core.LatestPreparedCheckpoint(); !ok || index != 4 || root != root4 || len(core.preparedCheckpoints) != 1 {
		t.Fatalf("active fence index=%d root=%x entries=%d", index, root, len(core.preparedCheckpoints))
	}
	core.sealedRoots[root3] = SealedCheckpoint{CheckpointSeal: seal3}
	core.sealedRoots[conflict] = SealedCheckpoint{CheckpointSeal: makeSeal(3, conflict)}
	if _, _, err := core.LatestCheckpointSeal(); err == nil {
		t.Fatal("same-index sealed root conflict was hidden")
	}
}
