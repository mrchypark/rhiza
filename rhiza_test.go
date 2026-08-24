//go:build !graph

package rhiza_test

import (
	"context"
	"testing"

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
