package rhiza

import (
	"context"
	"errors"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"os"
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

func TestLearnerPreservesPeerPolicyError(t *testing.T) {
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := quepaxa.Member{ID: "n1", PublicKey: quepaxa.PublicKey(network.PeerPublicKey("policy-peer", "n1", "voter"))}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", WAL: wal, Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}})
	if err != nil {
		t.Fatal(err)
	}
	server := network.NewServer(core, nil, "policy-peer", true, nil)
	defer server.Close()
	peer, err := network.StartPeerServer(ctx, "127.0.0.1:0", server, []quepaxa.Member{member}, "voter", "admin")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	config := ReplicaConfig{ClusterID: "policy-peer", ReplicaID: "reader", DataDir: t.TempDir(), AdminToken: "admin", Members: []network.PeerIdentity{{ID: "n1", PeerURL: "quic://" + peer.Addr(), PublicKey: [32]byte(member.PublicKey)}}, ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), SyncInterval: time.Hour}
	running, err := OpenLearner(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer running.Close()
	if status := running.Status(); status.Source != "peer:n1" {
		t.Fatalf("fixed-membership learner did not sync through its pinned peer identity: %+v", status)
	}
	legacy, err := os.ReadFile("pkg/materializer/testdata/sql-policy-legacy/slot2.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	if err := running.Sync(ctx); !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatalf("sync: %v", err)
	}
	if running.material.Tip() != 0 || running.Status().LastError == "" {
		t.Fatalf("status: %+v", running.Status())
	}
	config.DataDir = t.TempDir()
	config.ReplicaID = "new-reader"
	opened, err := OpenLearner(ctx, config)
	if opened != nil {
		opened.Close()
		t.Fatal("incompatible learner became ready")
	}
	if !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatalf("startup: %v", err)
	}
}

func TestRelativeDataDirReopen(t *testing.T) {
	dir, err := os.MkdirTemp(".", "relative-policy-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	config := Config{ClusterID: "relative-policy", NodeID: "n1", DataDir: dir, ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir()}
	node, err := Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = node.Execute(context.Background(), ExecuteRequest{RequestID: "table", SQL: "CREATE TABLE items(id INTEGER PRIMARY KEY)"}); err != nil {
		node.Close()
		t.Fatal(err)
	}
	if err = node.Close(); err != nil {
		t.Fatal(err)
	}
	node, err = Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if _, err = node.Query(context.Background(), QueryRequest{SQL: "SELECT * FROM items"}); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryAwareReplicaRequiresExactPublicBootstrapIdentity(t *testing.T) {
	member := Member{ID: "n1", URL: "http://n1", PeerURL: "quic://n1:9090", LogURL: "http://n1/log", PublicKey: PublicKey{1}}
	peer, err := NewReplicaMember("history", member)
	if err != nil {
		t.Fatal(err)
	}
	base := ReplicaConfig{ClusterID: "history", ReplicaID: "reader", DataDir: t.TempDir(), Members: []ReplicaMember{peer},
		ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir()}
	verification := newReplicaHistoryVerification(ReplicaHistoryVerificationConfig{BootstrapMembers: []Member{member}})
	if err := ensureReplicaIdentity(base, verification); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*replicaHistoryVerification){
		"mode":        func(config *replicaHistoryVerification) { config.enabled = false },
		"peer URL":    func(config *replicaHistoryVerification) { config.bootstrapMembers[0].PeerURL = "quic://other:9090" },
		"public key":  func(config *replicaHistoryVerification) { config.bootstrapMembers[0].PublicKey = PublicKey{2} },
		"genesis URL": func(config *replicaHistoryVerification) { config.bootstrapMembers[0].URL = "http://other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := verification
			changed.bootstrapMembers = append([]Member(nil), verification.bootstrapMembers...)
			mutate(&changed)
			if err := ensureReplicaIdentity(base, changed); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
				t.Fatalf("identity drift error=%v", err)
			}
		})
	}
}

func TestHistoryAwareReplicaRejectsMissingOrMismatchedBootstrap(t *testing.T) {
	member := Member{ID: "n1", PeerURL: "quic://n1:9090", PublicKey: PublicKey{1}}
	peer, err := NewReplicaMember("history", member)
	if err != nil {
		t.Fatal(err)
	}
	base := ReplicaConfig{ClusterID: "history", ReplicaID: "reader", DataDir: t.TempDir(), Members: []ReplicaMember{peer},
		ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir()}
	if replica, err := OpenReadReplicaWithHistory(context.Background(), base, ReplicaHistoryVerificationConfig{}); replica != nil || err == nil || !strings.Contains(err.Error(), "requires public bootstrap") {
		t.Fatalf("missing bootstrap replica=%v error=%v", replica, err)
	}
	verification := ReplicaHistoryVerificationConfig{BootstrapMembers: []Member{{ID: "n1", PeerURL: "quic://other:9090", PublicKey: member.PublicKey}}}
	if replica, err := OpenReadReplicaWithHistory(context.Background(), base, verification); replica != nil || err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched bootstrap replica=%v error=%v", replica, err)
	}
}

func TestHistoryVerificationMatchesTransportPinsByID(t *testing.T) {
	members := []Member{
		{ID: "a", PeerURL: "quic://a:9090", PublicKey: PublicKey{1}},
		{ID: "b", PeerURL: "quic://b:9090", PublicKey: PublicKey{2}},
	}
	peers := make([]ReplicaMember, len(members))
	for i, member := range members {
		peer, err := NewReplicaMember("history-order", member)
		if err != nil {
			t.Fatal(err)
		}
		peers[len(members)-1-i] = peer
	}
	config := ReplicaConfig{ClusterID: "history-order", ReplicaID: "reader", DataDir: t.TempDir(), Members: peers,
		ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir()}
	open := func(t *testing.T, bootstrap []Member) error {
		t.Helper()
		candidate := config
		candidate.DataDir = t.TempDir()
		replica, err := OpenReadReplicaWithHistory(context.Background(), candidate, ReplicaHistoryVerificationConfig{BootstrapMembers: bootstrap})
		if replica != nil {
			_ = replica.Close()
		}
		return err
	}
	if err := open(t, members); err != nil {
		t.Fatalf("reordered identical pins were rejected: %v", err)
	}
	for name, bootstrap := range map[string][]Member{
		"changed key": {{ID: "a", PeerURL: "quic://a:9090", PublicKey: PublicKey{9}}, members[1]},
		"changed URL": {{ID: "a", PeerURL: "quic://other:9090", PublicKey: PublicKey{1}}, members[1]},
		"missing ID":  {members[0]},
		"extra ID":    {members[0], {ID: "c", PeerURL: "quic://c:9090", PublicKey: PublicKey{3}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := open(t, bootstrap); err == nil || !strings.Contains(err.Error(), "bootstrap") {
				t.Fatalf("invalid bootstrap error=%v", err)
			}
		})
	}
}
