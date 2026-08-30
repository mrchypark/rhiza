package rhiza_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
)

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

func TestEmbeddedSpeculativeSQLAlwaysRollsBack(t *testing.T) {
	db, err := rhiza.Open(context.Background(), rhiza.Config{NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Execute(context.Background(), rhiza.ExecuteRequest{RequestID: "schema-speculate", Statements: []rhiza.SQLStatement{
		{SQL: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"},
		{SQL: "CREATE TABLE item_children (id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL REFERENCES items(id))"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := db.SpeculateSQL(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO items VALUES (1, 'tea')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := db.OpenLocalSQLReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var count int
	if err := reader.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("speculative row count=%d err=%v", count, err)
	}
	if err := db.SpeculateSQL(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO item_children VALUES (1, 999)`)
		return err
	}); err == nil {
		t.Fatal("speculative SQL did not enforce foreign keys")
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
