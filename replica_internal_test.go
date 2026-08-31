package rhiza

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestSyncPeerStopsAtFirstObservedTipAndRejectsNoProgress(t *testing.T) {
	sourceWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sourceWAL.Close()
	cluster := quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1"}}}
	source, err := quepaxa.New(quepaxa.Config{NodeID: "n1", WAL: sourceWAL, Cluster: cluster})
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "one", Operation: "put", Key: "key", Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Propose(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	decision, ok := source.CertifiedValue(1)
	if !ok {
		t.Fatal("source decision missing")
	}

	newReplica := func(t *testing.T) *ReadReplica {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "read-1", WAL: wal, Cluster: cluster})
		if err != nil {
			t.Fatal(err)
		}
		material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
		if err != nil {
			t.Fatal(err)
		}
		r := &ReadReplica{mode: ReplicaModeLearner, config: ReplicaConfig{Members: []network.PeerIdentity{{ID: "n1"}}}, core: core, material: material, wal: wal, ctx: context.Background()}
		t.Cleanup(func() {
			_ = material.Close()
			_ = wal.Close()
		})
		return r
	}

	r := newReplica(t)
	calls := 0
	r.fetch = func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (network.DecisionsResponse, error) {
		calls++
		if calls > 1 {
			return network.DecisionsResponse{}, errors.New("unexpected extra fetch")
		}
		return network.DecisionsResponse{Tip: 1, Decisions: []quepaxa.DecidedValue{decision}}, nil
	}
	if err := r.syncPeer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || r.core.Tip() != 1 || r.material.Tip() != 1 {
		t.Fatalf("calls=%d core=%d material=%d", calls, r.core.Tip(), r.material.Tip())
	}

	failover := newReplica(t)
	failover.config.Members = []network.PeerIdentity{{ID: "n0"}, {ID: "n1"}}
	failover.fetch = func(_ context.Context, id quepaxa.NodeID, _ quepaxa.Slot, _ int) (network.DecisionsResponse, error) {
		if id == "n0" {
			return network.DecisionsResponse{}, errors.New("peer unavailable")
		}
		return network.DecisionsResponse{Tip: 1, Decisions: []quepaxa.DecidedValue{decision}}, nil
	}
	if err := failover.syncPeer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := failover.Status(); status.Source != "peer:n1" || failover.material.Tip() != 1 {
		t.Fatalf("peer failover status=%+v tip=%d", status, failover.material.Tip())
	}

	stalled := newReplica(t)
	if err := stalled.core.AcceptCertifiedValues([]quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	stalled.fetch = func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (network.DecisionsResponse, error) {
		return network.DecisionsResponse{Tip: 2, Decisions: []quepaxa.DecidedValue{decision}}, nil
	}
	if err := stalled.syncPeer(context.Background()); err == nil || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("duplicate page error=%v", err)
	}
}

func TestReadReplicaCloseCancelsActiveSync(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.NewObserver(quepaxa.Config{NodeID: "read-1", WAL: wal, Cluster: quepaxa.Cluster{Members: []quepaxa.Member{{ID: "n1"}}}})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	r := &ReadReplica{
		mode: ReplicaModeLearner, config: ReplicaConfig{Members: []network.PeerIdentity{{ID: "n1"}}},
		core: core, material: material, wal: wal, ctx: lifetime, cancel: cancel,
		fetch: func(ctx context.Context, _ quepaxa.NodeID, _ quepaxa.Slot, _ int) (network.DecisionsResponse, error) {
			close(started)
			<-ctx.Done()
			return network.DecisionsResponse{}, ctx.Err()
		},
	}
	r.ready.Store(true)
	syncDone := make(chan error, 1)
	go func() { syncDone <- r.Sync(context.Background()) }()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- r.Close() }()
	select {
	case err := <-syncDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("sync error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active sync ignored replica shutdown")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close waited for canceled sync")
	}
}

func TestLearnerFallsBackToArchiveWhenPeerHistoryIsCompacted(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	voter, err := Open(ctx, Config{ClusterID: "compacted-fallback", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir, ObjStoreDurability: ObjectStoreDurabilityBeforeAck})
	if err != nil {
		t.Fatal(err)
	}
	defer voter.Close()
	if _, err := voter.Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	replica, err := OpenReadReplica(ctx, ReplicaConfig{ClusterID: "compacted-fallback", ReplicaID: "read-1", DataDir: t.TempDir(),
		Members: []network.PeerIdentity{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: storeDir, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	if _, err := voter.Execute(ctx, ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
	replica.mode = ReplicaModeLearner
	replica.fetch = func(context.Context, quepaxa.NodeID, quepaxa.Slot, int) (network.DecisionsResponse, error) {
		return network.DecisionsResponse{}, quepaxa.ErrCompacted
	}
	if err := replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := replica.Query(ctx, QueryRequest{SQL: "SELECT id FROM items"})
	if err != nil || len(rows.Rows) != 1 || replica.Status().Source != "object-store" {
		t.Fatalf("fallback rows=%#v status=%+v err=%v", rows.Rows, replica.Status(), err)
	}
}
