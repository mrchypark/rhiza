package rhiza_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

func TestObjectStoreReadReplicaRestoresAndStaysReadOnly(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	voter, err := rhiza.Open(ctx, rhiza.Config{
		ClusterID: "replica", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1, 'first')"}); err != nil {
		t.Fatal(err)
	}
	if _, err := voter.KVPut(ctx, rhiza.KVMutationRequest{RequestID: "kv", Key: "key", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if _, err := voter.GraphExecute(ctx, rhiza.GraphCommand{RequestID: "graph", Cypher: "CREATE (:Item {name: 'graph'})"}); err != nil {
		t.Fatal(err)
	}

	replica, err := rhiza.OpenReadReplica(ctx, rhiza.ReplicaConfig{
		ClusterID: "replica", ReplicaID: "read-1", DataDir: t.TempDir(),
		Members: []rhiza.ReplicaMember{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		SyncInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	if !replica.Ready() || replica.Status().Mode != rhiza.ReplicaModeObjectStore {
		t.Fatalf("replica is not ready: %+v", replica.Status())
	}
	rows, err := replica.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM items ORDER BY id"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "first" {
		t.Fatalf("replica SQL rows=%#v err=%v", rows.Rows, err)
	}
	kv, err := replica.KVGet(ctx, rhiza.KVGetRequest{Key: "key"})
	if err != nil || !kv.Found || string(kv.Value) != "value" {
		t.Fatalf("replica KV=%q found=%v err=%v", kv.Value, kv.Found, err)
	}
	graph, err := replica.GraphQuery(ctx, rhiza.GraphQueryRequest{Cypher: "MATCH (n:Item) RETURN n.name"})
	if err != nil || len(graph.Rows) != 1 || graph.Rows[0][0] != "graph" {
		t.Fatalf("replica graph rows=%#v err=%v", graph.Rows, err)
	}
	if _, err := replica.Query(ctx, rhiza.QueryRequest{SQL: "SELECT 1", Consistency: rhiza.ConsistencyLinearizable}); !errors.Is(err, rhiza.ErrQuorumUnavailable) {
		t.Fatalf("linearizable replica query error=%v", err)
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/sql/execute", strings.NewReader(`{"request_id":"blocked","sql":"INSERT INTO items VALUES (2, 'blocked')"}`))
	req.Header.Set("Content-Type", "application/json")
	replica.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("replica mutation status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert-2", SQL: "INSERT INTO items VALUES (2, 'second')"}); err != nil {
		t.Fatal(err)
	}
	if err := replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err = replica.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM items ORDER BY id"})
	if err != nil || len(rows.Rows) != 2 || rows.Rows[1][0] != "second" {
		t.Fatalf("updated replica rows=%#v err=%v", rows.Rows, err)
	}
	if err := voter.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPeerLearnerUsesVoterWithoutJoiningQuorum(t *testing.T) {
	ctx := context.Background()
	peerAddr := freeUDPAddr(t)
	storeDir := t.TempDir()
	members := []rhiza.Member{{ID: "n1", PeerURL: "quic://" + peerAddr, Token: "voter-token"}}
	voter, err := rhiza.Open(ctx, rhiza.Config{
		ClusterID: "learner", NodeID: "n1", DataDir: t.TempDir(), PeerAddr: peerAddr,
		AdminToken: "learner-token", Members: members,
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer voter.Close()
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	learner, err := rhiza.OpenLearner(ctx, rhiza.ReplicaConfig{
		ClusterID: "learner", ReplicaID: "learner-1", DataDir: t.TempDir(), AdminToken: "learner-token",
		Members: replicaMembers(t, "learner", members), ObjStoreProvider: "filesystem", ObjStoreDir: storeDir, SyncInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
	if err := learner.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	status := learner.Status()
	if status.Mode != rhiza.ReplicaModeLearner || status.Source != "peer:n1" {
		t.Fatalf("learner did not use peer source: %+v", status)
	}
	rows, err := learner.Query(ctx, rhiza.QueryRequest{SQL: "SELECT id FROM items"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("learner rows=%#v err=%v", rows.Rows, err)
	}
	if _, err := learner.Query(ctx, rhiza.QueryRequest{SQL: "SELECT 1", Consistency: rhiza.ConsistencyLinearizable}); !errors.Is(err, rhiza.ErrQuorumUnavailable) {
		t.Fatalf("learner linearizable query error=%v", err)
	}
}

func TestElevenObjectStoreReadReplicas(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	voter, err := rhiza.Open(ctx, rhiza.Config{
		ClusterID: "fanout", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer voter.Close()
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1)"}); err != nil {
		t.Fatal(err)
	}
	var replicas []*rhiza.ReadReplica
	defer func() {
		for _, replica := range replicas {
			_ = replica.Close()
		}
	}()
	for i := 0; i < 11; i++ {
		replica, err := rhiza.OpenReadReplica(ctx, rhiza.ReplicaConfig{
			ClusterID: "fanout", ReplicaID: fmt.Sprintf("read-%d", i), DataDir: t.TempDir(),
			Members: []rhiza.ReplicaMember{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
			SyncInterval: time.Hour,
		})
		if err != nil {
			t.Fatalf("open replica %d: %v", i, err)
		}
		replicas = append(replicas, replica)
		rows, err := replica.Query(ctx, rhiza.QueryRequest{SQL: "SELECT count(*) FROM items"})
		if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != int64(1) {
			t.Fatalf("replica %d rows=%#v err=%v", i, rows.Rows, err)
		}
	}
}

func TestReadReplicaBootstrapsFromCertifiedCheckpoint(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	voter, err := rhiza.Open(ctx, rhiza.Config{
		ClusterID: "checkpoint-replica", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1, 'checkpoint')"}); err != nil {
		t.Fatal(err)
	}
	if err := voter.Close(); err != nil {
		t.Fatal(err)
	}
	replicaDir := t.TempDir()
	replicaConfig := rhiza.ReplicaConfig{
		ClusterID: "checkpoint-replica", ReplicaID: "read-1", DataDir: replicaDir,
		Members: []rhiza.ReplicaMember{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		SyncInterval: time.Hour,
	}
	replica, err := rhiza.OpenReadReplica(ctx, replicaConfig)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := replica.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM items"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "checkpoint" {
		t.Fatalf("checkpoint replica rows=%#v err=%v status=%+v", rows.Rows, err, replica.Status())
	}
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	wrongIdentity := replicaConfig
	wrongIdentity.ClusterID = "another-cluster"
	if wrong, err := rhiza.OpenReadReplica(ctx, wrongIdentity); wrong != nil || err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("reused replica directory result=%v error=%v", wrong, err)
	}
	for _, name := range []string{"sqlite.db", "sqlite.db-wal", "sqlite.db-shm"} {
		if err := os.Remove(filepath.Join(replicaDir, name)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if err := os.RemoveAll(filepath.Join(replicaDir, "latticedb")); err != nil {
		t.Fatal(err)
	}
	replica, err = rhiza.OpenReadReplica(ctx, replicaConfig)
	if err != nil {
		t.Fatalf("restore materializer behind observer WAL: %v", err)
	}
	defer replica.Close()
	rows, err = replica.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM items"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "checkpoint" {
		t.Fatalf("re-restored checkpoint rows=%#v err=%v status=%+v", rows.Rows, err, replica.Status())
	}
}

func TestReadReplicaRejectsExistingStateWithoutIdentity(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "sqlite.db"), []byte("old state"), 0o600); err != nil {
		t.Fatal(err)
	}
	replica, err := rhiza.OpenReadReplica(context.Background(), rhiza.ReplicaConfig{
		ClusterID: "cluster", ReplicaID: "read-1", DataDir: dataDir,
		Members: []rhiza.ReplicaMember{{ID: "n1"}}, ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(),
	})
	if replica != nil || err == nil || !strings.Contains(err.Error(), "without identity manifest") {
		t.Fatalf("replica=%v error=%v", replica, err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "replica-identity.json")); !os.IsNotExist(err) {
		t.Fatalf("identity manifest created over unbound state: %v", err)
	}
}

func freeUDPAddr(t testing.TB) string {
	t.Helper()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.LocalAddr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func replicaMembers(t testing.TB, clusterID string, members []rhiza.Member) []rhiza.ReplicaMember {
	t.Helper()
	peers := make([]rhiza.ReplicaMember, 0, len(members))
	for _, member := range members {
		peer, err := rhiza.NewReplicaMember(clusterID, member)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peer)
	}
	return peers
}

func BenchmarkReplicaCatchUp(b *testing.B) {
	for _, mode := range []rhiza.ReplicaMode{rhiza.ReplicaModeObjectStore, rhiza.ReplicaModeLearner} {
		b.Run(string(mode), func(b *testing.B) {
			ctx := context.Background()
			peerAddr := freeUDPAddr(b)
			storeDir := b.TempDir()
			members := []rhiza.Member{{ID: "n1", PeerURL: "quic://" + peerAddr, Token: "voter-token"}}
			voter, err := rhiza.Open(ctx, rhiza.Config{
				ClusterID: "bench-" + string(mode), NodeID: "n1", DataDir: b.TempDir(), PeerAddr: peerAddr,
				AdminToken: "learner-token", Members: members,
				ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
				ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer voter.Close()
			if _, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY)"}); err != nil {
				b.Fatal(err)
			}
			config := rhiza.ReplicaConfig{
				ClusterID: "bench-" + string(mode), ReplicaID: "read-1", DataDir: b.TempDir(),
				AdminToken: "learner-token", Members: replicaMembers(b, "bench-"+string(mode), members),
				ObjStoreProvider: "filesystem", ObjStoreDir: storeDir, SyncInterval: time.Hour,
			}
			var replica *rhiza.ReadReplica
			if mode == rhiza.ReplicaModeLearner {
				replica, err = rhiza.OpenLearner(ctx, config)
			} else {
				replica, err = rhiza.OpenReadReplica(ctx, config)
			}
			if err != nil {
				b.Fatal(err)
			}
			defer replica.Close()
			before := replica.ObjectStoreStats()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				_, err := voter.Execute(ctx, rhiza.ExecuteRequest{RequestID: fmt.Sprintf("insert-%d", i), SQL: "INSERT INTO items VALUES (?)", Args: []any{i}})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := replica.Sync(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			after := replica.ObjectStoreStats()
			b.ReportMetric(float64(after.Gets-before.Gets)/float64(b.N), "obj_gets/op")
			b.ReportMetric(float64(after.Heads-before.Heads)/float64(b.N), "obj_heads/op")
		})
	}
}

func TestEmbeddedGoAPI(t *testing.T) {
	db, err := rhiza.Open(context.Background(), rhiza.Config{NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.Ready() {
		t.Fatal("opened single-peer DB is not locally ready")
	}
	if _, err := db.Execute(context.Background(), rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(context.Background(), rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (?, ?)", Args: []any{1, "tea"}}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(context.Background(), rhiza.QueryRequest{SQL: "SELECT name FROM items WHERE id = ?", Args: []any{1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "tea" {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
	if _, err := db.GraphExecute(context.Background(), rhiza.GraphCommand{RequestID: "graph-insert", Cypher: "CREATE (:Item {name: 'graph'})"}); err != nil {
		t.Fatal(err)
	}
	graph, err := db.GraphQuery(context.Background(), rhiza.GraphQueryRequest{Cypher: "MATCH (n:Item) RETURN n.name"})
	if err != nil || len(graph.Rows) != 1 || graph.Rows[0][0] != "graph" {
		t.Fatalf("unexpected graph rows: %#v, err=%v", graph.Rows, err)
	}
	if _, err := db.KVPut(context.Background(), rhiza.KVMutationRequest{RequestID: "kv-put", Key: "kind", Value: []byte("combined")}); err != nil {
		t.Fatal(err)
	}
	kv, err := db.KVGet(context.Background(), rhiza.KVGetRequest{Key: "kind"})
	if err != nil || !kv.Found || string(kv.Value) != "combined" {
		t.Fatalf("unexpected KV value: %q found=%v err=%v", kv.Value, kv.Found, err)
	}
}

func TestExecuteContractAndEncodedSizeBoundary(t *testing.T) {
	if rhiza.MaxReplicatedMutationBytes != 128<<10 || rhiza.MaxHTTPBodyBytes != 1<<20 {
		t.Fatalf("limits consensus=%d HTTP=%d", rhiza.MaxReplicatedMutationBytes, rhiza.MaxHTTPBodyBytes)
	}
	if err := rhiza.ValidateExecuteRequest(rhiza.ExecuteRequest{RequestID: "rows", SQL: "SELECT 1", WantRows: true}); !errors.Is(err, rhiza.ErrInvalidRequest) {
		t.Fatalf("want_rows validation error=%v", err)
	}

	valid := func(n int) error {
		return rhiza.ValidateExecuteRequest(rhiza.ExecuteRequest{
			RequestID: "size", SQL: "CREATE TABLE size_limit (id INTEGER) /*" + strings.Repeat("x", n) + "*/",
		})
	}
	low, high := 0, rhiza.MaxReplicatedMutationBytes
	for low < high {
		mid := low + (high-low+1)/2
		if valid(mid) == nil {
			low = mid
		} else {
			high = mid - 1
		}
	}
	if err := valid(low); err != nil {
		t.Fatalf("largest accepted mutation rejected: %v", err)
	}
	if err := valid(low + 1); !errors.Is(err, rhiza.ErrInvalidRequest) {
		t.Fatalf("oversized mutation error=%v", err)
	}
}

func TestEmbeddedObjectStoreBeforeAckRecoveryWithoutClose(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	config := rhiza.Config{
		ClusterID: "strict", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		ObjStoreDurability:   rhiza.ObjectStoreDurabilityBeforeAck,
		ObjStoreSyncInterval: time.Hour, CheckpointInterval: time.Hour,
	}
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	insert, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1, 'published')"})
	if err != nil {
		t.Fatal(err)
	}
	if tip := objectStoreTip(t, storeDir, "strict", "n1"); tip < insert.Slot {
		t.Fatalf("published tip=%d, insert slot=%d", tip, insert.Slot)
	}

	config.DataDir = t.TempDir()
	restored, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	result, err := restored.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM items WHERE id = 1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "published" {
		t.Fatalf("unexpected restored rows: %#v", result.Rows)
	}
}

func TestEmbeddedObjectStoreAsyncInterval(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	config := rhiza.Config{
		ClusterID: "async", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
		ObjStoreSyncInterval: 10 * time.Millisecond, CheckpointInterval: time.Hour,
	}
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	insert, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1, 'periodic')"})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tip, err := readObjectStoreTip(storeDir, "async", "n1"); err == nil && tip >= insert.Slot {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("async sync did not publish slot %d", insert.Slot)
}

func TestBeforeAckRequiresObjectStore(t *testing.T) {
	_, err := rhiza.Open(context.Background(), rhiza.Config{
		NodeID: "n1", DataDir: t.TempDir(), ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	})
	if err == nil {
		t.Fatal("expected before-ack configuration error")
	}
}

func TestInvalidObjectStoreDurability(t *testing.T) {
	_, err := rhiza.Open(context.Background(), rhiza.Config{
		NodeID: "n1", DataDir: t.TempDir(), ObjStoreDurability: "sometimes",
	})
	if err == nil {
		t.Fatal("expected invalid durability error")
	}
}

func objectStoreTip(t *testing.T, dir, cluster, node string) uint64 {
	t.Helper()
	tip, err := readObjectStoreTip(dir, cluster, node)
	if err != nil {
		t.Fatal(err)
	}
	return tip
}

func readObjectStoreTip(dir, cluster, _ string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(dir, cluster, "archive", "head.bin"))
	if err != nil {
		return 0, err
	}
	if len(data) < 80 || string(data[:8]) != "RHZAHEAD" {
		return 0, fmt.Errorf("invalid archive head")
	}
	return binary.BigEndian.Uint64(data[72:80]), nil
}

func TestEmbeddedObjectStoreRecovery(t *testing.T) {
	ctx := context.Background()
	storeDir := t.TempDir()
	config := rhiza.Config{
		ClusterID: "restore", NodeID: "n1", DataDir: t.TempDir(),
		ObjStoreProvider: "filesystem", ObjStoreDir: storeDir,
	}
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO items VALUES (1, 'tea')"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	config.DataDir = t.TempDir()
	restored, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	result, err := restored.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM items WHERE id = 1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "tea" {
		t.Fatalf("unexpected restored rows: %#v", result.Rows)
	}
	if stats, ok := restored.ObjectStoreStats(); !ok || stats.Gets == 0 {
		t.Fatalf("object store metrics unavailable: ok=%v stats=%+v", ok, stats)
	}
}
