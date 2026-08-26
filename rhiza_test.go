//go:build !graph

package rhiza_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

func readObjectStoreTip(dir, cluster, node string) (uint64, error) {
	manifestDir := filepath.Join(dir, cluster, node, "qlog", "manifests")
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		return 0, os.ErrNotExist
	}
	data, err := os.ReadFile(filepath.Join(manifestDir, entries[len(entries)-1].Name()))
	if err != nil {
		return 0, err
	}
	var manifest struct {
		Tip uint64 `json:"tip_slot"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return 0, err
	}
	return manifest.Tip, nil
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
