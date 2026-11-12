package quepaxa

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// trackingTransport wraps a Transport and records every ReadTip call by node ID.
type trackingTransport struct {
	inner     Transport
	readTipMu sync.Mutex
	readTips  map[NodeID]int
}

func newTrackingTransport(inner Transport) *trackingTransport {
	return &trackingTransport{inner: inner, readTips: make(map[NodeID]int)}
}

func (t *trackingTransport) ReadTip(ctx context.Context, to NodeID) (Slot, error) {
	t.readTipMu.Lock()
	t.readTips[to]++
	t.readTipMu.Unlock()
	return t.inner.ReadTip(ctx, to)
}

func (t *trackingTransport) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	return t.inner.SendRecord(ctx, to, request)
}

func (t *trackingTransport) SendDecision(ctx context.Context, decision Decision) error {
	return t.inner.SendDecision(ctx, decision)
}

func (t *trackingTransport) StageValue(ctx context.Context, to NodeID, hash ValueHash, value []byte) error {
	return t.inner.StageValue(ctx, to, hash, value)
}

func (t *trackingTransport) FetchValue(ctx context.Context, from NodeID, hash ValueHash) ([]byte, error) {
	return t.inner.FetchValue(ctx, from, hash)
}

func (t *trackingTransport) readTipCount(id NodeID) int {
	t.readTipMu.Lock()
	defer t.readTipMu.Unlock()
	return t.readTips[id]
}

// TestReadIndexExcludingExcludedVoterNeverContacted verifies that the excluded
// voter is never contacted via transport.ReadTip and does not count toward quorum.
// A 3-node cluster (quorum=2) excludes n3: only n1(self) + n2 can succeed.
func TestReadIndexExcludingExcludedVoterNeverContacted(t *testing.T) {
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	config := &Cluster{ConfigID: 1, Members: members}

	inner := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool)}
	tracking := newTrackingTransport(inner)

	for _, member := range members {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core := newCore(member.ID, config, wal, tracking)
		inner.cores[member.ID] = core
	}

	// Advance all tips to slot 1 via a consensus round.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := inner.cores["n1"].Propose(ctx, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	for _, core := range inner.cores {
		if err := core.WaitTip(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}

	// ReadIndexExcluding n3: n1(self) + n2 = 2 = quorum. n3 must not be contacted.
	slot, id, err := inner.cores["n1"].ReadIndexExcluding(ctx, "n3")
	if err != nil {
		t.Fatal(err)
	}
	if slot < 1 {
		t.Fatalf("expected tip >= 1, got %d", slot)
	}
	if id == "" {
		t.Fatal("expected a non-empty responder ID")
	}
	if got := tracking.readTipCount("n3"); got != 0 {
		t.Fatalf("excluded voter n3 was contacted %d times via ReadTip", got)
	}
}

// TestReadIndexExcludingBridgeDependingOnExcludedCannotPass verifies that when
// the only remaining reachable voter besides self is excluded, quorum cannot be
// reached. In a 3-node cluster (quorum=2) with n3 down, excluding n2 leaves only
// n1(self) = 1 < 2.
func TestReadIndexExcludingBridgeDependingOnExcludedCannotPass(t *testing.T) {
	cores, transport := newTestCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := cores["n1"].Propose(ctx, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	for _, core := range cores {
		if err := core.WaitTip(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}

	// Take n3 down and exclude n2: only n1(self) succeeds = 1 < 2.
	transport.fail("n3")
	_, _, err := cores["n1"].ReadIndexExcluding(ctx, "n2")
	if err != ErrQuorumUnavailable {
		t.Fatalf("excluded bridge returned %v, want unchanged ErrQuorumUnavailable identity", err)
	}
}

// TestReadIndexExcludingNormalRemainingQuorumPasses verifies that excluding one
// voter from a healthy 3-node cluster still allows quorum via self + one remote.
func TestReadIndexExcludingNormalRemainingQuorumPasses(t *testing.T) {
	cores, _ := newTestCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := cores["n1"].Propose(ctx, []byte("seed")); err != nil {
		t.Fatal(err)
	}
	for _, core := range cores {
		if err := core.WaitTip(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}

	slot, id, err := cores["n1"].ReadIndexExcluding(ctx, "n3")
	if err != nil {
		t.Fatal(err)
	}
	if slot < 1 {
		t.Fatalf("expected tip >= 1, got %d", slot)
	}
	if id == "" {
		t.Fatal("expected a non-empty responder ID")
	}
}

// TestReadIndexExcludingLocalNodeFails verifies that excluding the calling
// node itself returns ErrQuorumUnavailable immediately.
func TestReadIndexExcludingLocalNodeFails(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{Members: []Member{{ID: "n1"}, {ID: "n2"}}}, wal, nil)
	if _, _, err := core.ReadIndexExcluding(context.Background(), "n1"); err != ErrQuorumUnavailable {
		t.Fatalf("local exclusion returned %v, want ErrQuorumUnavailable", err)
	}
}

type membershipAdvanceReadTransport struct {
	mu          sync.Mutex
	readTips    map[NodeID]int
	n2Entered   chan struct{}
	n2Once      sync.Once
	oldBlocked  chan struct{}
	oldCanceled chan struct{}
	cancelOnce  sync.Once
	blocked     atomic.Int32
}

func (t *membershipAdvanceReadTransport) ReadTip(ctx context.Context, to NodeID) (Slot, error) {
	t.mu.Lock()
	t.readTips[to]++
	call := t.readTips[to]
	t.mu.Unlock()
	switch to {
	case "n2":
		t.n2Once.Do(func() { close(t.n2Entered) })
		return Slot(call), nil
	case "n3", "n4":
		if t.blocked.Add(1) == 1 {
			close(t.oldBlocked)
		}
		<-ctx.Done()
		if t.blocked.Add(-1) == 0 {
			t.cancelOnce.Do(func() { close(t.oldCanceled) })
		}
		return 0, ctx.Err()
	default:
		return 0, errors.New("unexpected read-index peer")
	}
}

func (t *membershipAdvanceReadTransport) SendRecord(context.Context, NodeID, RecordRequest) (Summary, error) {
	return Summary{}, errors.New("unexpected record")
}

func (t *membershipAdvanceReadTransport) SendDecision(context.Context, Decision) error {
	return errors.New("unexpected decision")
}

func (t *membershipAdvanceReadTransport) StageValue(context.Context, NodeID, ValueHash, []byte) error {
	return errors.New("unexpected stage")
}

func (t *membershipAdvanceReadTransport) FetchValue(context.Context, NodeID, ValueHash) ([]byte, error) {
	return nil, errors.New("unexpected fetch")
}

// A certified configuration advance invalidates every response from the old
// roster. The read must cancel that round and issue a fresh round against the
// installed roster instead of waiting for unavailable old members or mixing
// quorum evidence across snapshots.
func TestReadIndexRestartsAfterCertifiedMembershipAdvance(t *testing.T) {
	oldMembers := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}, {ID: "n4"}}
	newMembers := []Member{{ID: "n1"}, {ID: "n2"}}
	coreTransport := &membershipAdvanceReadTransport{
		readTips:    make(map[NodeID]int),
		n2Entered:   make(chan struct{}),
		oldBlocked:  make(chan struct{}),
		oldCanceled: make(chan struct{}),
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{ConfigID: 1, Members: oldMembers}, wal, coreTransport)
	core.mu.Lock()
	core.configHistory = append(core.configHistory, configEpoch{start: 2, cluster: cloneCluster(Cluster{ConfigID: 2, Members: newMembers})})
	core.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		<-coreTransport.n2Entered
		<-coreTransport.oldBlocked
		core.mu.Lock()
		core.tip = 1
		core.durable[1] = true
		close(core.tipChanged)
		core.tipChanged = make(chan struct{})
		core.mu.Unlock()
	}()

	slot, id, err := core.ReadIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slot != 2 || id != "n2" {
		t.Fatalf("read returned slot=%d id=%s, want the fresh config-2 response from n2", slot, id)
	}
	coreTransport.mu.Lock()
	n2Calls := coreTransport.readTips["n2"]
	coreTransport.mu.Unlock()
	if n2Calls != 2 {
		t.Fatalf("n2 ReadTip calls=%d, want one discarded old-roster call and one fresh call", n2Calls)
	}
	select {
	case <-coreTransport.oldCanceled:
	case <-ctx.Done():
		t.Fatal("old-roster ReadTip calls were not canceled")
	}
}

func TestReadIndexExcludingDoesNotFollowMembershipAdvance(t *testing.T) {
	oldMembers := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}, {ID: "n4"}}
	newMembers := []Member{{ID: "n1"}, {ID: "n2"}}
	coreTransport := &membershipAdvanceReadTransport{
		readTips:    make(map[NodeID]int),
		n2Entered:   make(chan struct{}),
		oldBlocked:  make(chan struct{}),
		oldCanceled: make(chan struct{}),
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{ConfigID: 1, Members: oldMembers}, wal, coreTransport)
	core.mu.Lock()
	core.configHistory = append(core.configHistory, configEpoch{start: 2, cluster: cloneCluster(Cluster{ConfigID: 2, Members: newMembers})})
	core.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		<-coreTransport.n2Entered
		<-coreTransport.oldBlocked
		core.mu.Lock()
		core.tip = 1
		core.durable[1] = true
		close(core.tipChanged)
		core.tipChanged = make(chan struct{})
		core.mu.Unlock()
	}()

	if _, _, err := core.ReadIndexExcluding(ctx, "n4"); err != ErrQuorumUnavailable {
		t.Fatalf("exact-roster exclusion followed a membership advance: %v", err)
	}
}

func TestReadIndexWaitsThroughFreezeWithoutCountingQuorum(t *testing.T) {
	oldMembers := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}, {ID: "n4"}}
	newMembers := []Member{{ID: "n1"}, {ID: "n2"}}
	coreTransport := &membershipAdvanceReadTransport{
		readTips:    make(map[NodeID]int),
		n2Entered:   make(chan struct{}),
		oldBlocked:  make(chan struct{}),
		oldCanceled: make(chan struct{}),
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{ConfigID: 1, Members: oldMembers}, wal, coreTransport)
	core.reconfigEnabled = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-coreTransport.n2Entered
		<-coreTransport.oldBlocked
		core.mu.Lock()
		core.reconfiguration = &reconfigurationState{target: Cluster{ConfigID: 2, Members: newMembers}}
		close(core.tipChanged)
		core.tipChanged = make(chan struct{})
		core.mu.Unlock()
		<-coreTransport.oldCanceled
		core.mu.Lock()
		core.reconfiguration = nil
		core.configHistory = append(core.configHistory, configEpoch{start: 2, cluster: Cluster{ConfigID: 2, Members: newMembers}})
		core.tip = 1
		core.durable[1] = true
		close(core.tipChanged)
		core.tipChanged = make(chan struct{})
		core.mu.Unlock()
	}()

	if slot, id, err := core.ReadIndex(ctx); err != nil || slot != 2 || id != "n2" {
		t.Fatalf("read through freeze: slot=%d id=%s err=%v", slot, id, err)
	}
}

func TestReadIndexFrozenBackgroundFailsClosed(t *testing.T) {
	members := []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{ConfigID: 1, Members: members}, wal, nil)
	core.reconfigEnabled = true
	core.mu.Lock()
	core.reconfiguration = &reconfigurationState{target: Cluster{ConfigID: 2, Members: members[:2]}}
	core.mu.Unlock()
	if _, _, err := core.ReadIndex(context.Background()); err != ErrQuorumUnavailable {
		t.Fatalf("unbounded frozen read returned %v, want ErrQuorumUnavailable", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := core.ReadIndex(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled frozen read returned %v, want context.Canceled", err)
	}
}

type tipNotificationReadTransport struct {
	n2Entered chan struct{}
	n2Release chan struct{}
	once      sync.Once
}

func (t *tipNotificationReadTransport) ReadTip(ctx context.Context, to NodeID) (Slot, error) {
	switch to {
	case "n2":
		t.once.Do(func() { close(t.n2Entered) })
		select {
		case <-t.n2Release:
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	case "n3":
		<-ctx.Done()
		return 0, ctx.Err()
	default:
		return 0, errors.New("unexpected read-index peer")
	}
}

func (t *tipNotificationReadTransport) SendRecord(context.Context, NodeID, RecordRequest) (Summary, error) {
	return Summary{}, errors.New("unexpected record")
}

func (t *tipNotificationReadTransport) SendDecision(context.Context, Decision) error {
	return errors.New("unexpected decision")
}

func (t *tipNotificationReadTransport) StageValue(context.Context, NodeID, ValueHash, []byte) error {
	return errors.New("unexpected stage")
}

func (t *tipNotificationReadTransport) FetchValue(context.Context, NodeID, ValueHash) ([]byte, error) {
	return nil, errors.New("unexpected fetch")
}

func TestReadIndexIgnoresTipWakeupsUntilResponse(t *testing.T) {
	coreTransport := &tipNotificationReadTransport{n2Entered: make(chan struct{}), n2Release: make(chan struct{})}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core := newCore("n1", &Cluster{ConfigID: 1, Members: []Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}}, wal, coreTransport)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		<-coreTransport.n2Entered
		for range 8 {
			core.mu.Lock()
			close(core.tipChanged)
			core.tipChanged = make(chan struct{})
			core.mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
		close(coreTransport.n2Release)
	}()

	if slot, id, err := core.ReadIndex(ctx); err != nil || slot != 1 || id != "n2" {
		t.Fatalf("read after ordinary tip wakeups: slot=%d id=%s err=%v", slot, id, err)
	}
}
