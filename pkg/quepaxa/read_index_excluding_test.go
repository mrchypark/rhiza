package quepaxa

import (
	"context"
	"sync"
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
	if err == nil {
		t.Fatal("quorum passed when the excluded voter was the only bridge to quorum")
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
