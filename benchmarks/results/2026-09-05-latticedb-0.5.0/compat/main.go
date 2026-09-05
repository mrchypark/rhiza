package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mrchypark/rhiza"
)

const (
	oldSQLRequest   = "compat-sql-insert-v03"
	oldKVRequest    = "compat-kv-put-v03"
	oldGraphRequest = "compat-graph-insert-v03"
)

func main() {
	if len(os.Args) < 3 {
		fail("usage: compat <write|verify|reopen|checkpoint-write|checkpoint-restore> <data-dir> [object-store-dir]")
	}
	ctx := context.Background()
	mode, dataDir := os.Args[1], os.Args[2]
	storeDir := ""
	if mode == "checkpoint-write" || mode == "checkpoint-restore" {
		if len(os.Args) != 4 {
			fail("%s requires an object-store directory", mode)
		}
		storeDir = os.Args[3]
	} else if len(os.Args) != 3 {
		fail("%s accepts only a data directory", mode)
	}
	config := rhiza.Config{NodeID: "compat-node", DataDir: dataDir}
	if storeDir != "" {
		config.ClusterID = "compat-checkpoint"
		config.ObjStoreProvider = rhiza.ObjectStoreProviderFilesystem
		config.ObjStoreDir = storeDir
		config.ObjStoreDurability = rhiza.ObjectStoreDurabilityBeforeAck
	}
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		fail("open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			fail("close: %v", err)
		}
	}()

	switch mode {
	case "write":
		write(ctx, db)
	case "verify":
		verifyOld(ctx, db)
		writeNew(ctx, db)
	case "reopen":
		verifyOld(ctx, db)
		verifyNew(ctx, db)
	case "checkpoint-write":
		write(ctx, db)
		fmt.Println("checkpoint writer: close will archive and certify a checkpoint")
	case "checkpoint-restore":
		verifyOld(ctx, db)
		fmt.Println("checkpoint reader: v0.3 archive/checkpoint recovered into fresh v0.5 data directory")
	default:
		fail("unknown mode %q", mode)
	}
}

func write(ctx context.Context, db *rhiza.DB) {
	must(db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "compat-sql-schema-v03", SQL: "CREATE TABLE compat_items (id INTEGER PRIMARY KEY, name TEXT)"}))
	must(db.Execute(ctx, rhiza.ExecuteRequest{RequestID: oldSQLRequest, SQL: "INSERT INTO compat_items VALUES (?, ?)", Args: []any{1, "written-by-v03"}}))
	must(db.KVPut(ctx, rhiza.KVMutationRequest{RequestID: oldKVRequest, Key: "compat/key", Value: []byte("written-by-v03")}))
	must(db.GraphExecute(ctx, rhiza.GraphCommand{RequestID: "compat-graph-schema-v03", Cypher: "CREATE NODE TABLE Compat(id STRING, name STRING, PRIMARY KEY(id))"}))
	must(db.GraphExecute(ctx, rhiza.GraphCommand{RequestID: oldGraphRequest, Cypher: "CREATE (:Compat {id: 'graph-1', name: 'written-by-v03'})"}))
	verifyOld(ctx, db)
	fmt.Println("writer: SQL/KV/Graph writes and receipts verified")
}

func verifyOld(ctx context.Context, db *rhiza.DB) {
	query, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM compat_items WHERE id = ?", Args: []any{1}})
	mustErr(err)
	if len(query.Rows) != 1 || query.Rows[0][0] != "written-by-v03" {
		fail("old SQL value: %#v", query.Rows)
	}
	kv, err := db.KVGet(ctx, rhiza.KVGetRequest{Key: "compat/key"})
	mustErr(err)
	if !kv.Found || string(kv.Value) != "written-by-v03" {
		fail("old KV value: found=%v value=%q", kv.Found, kv.Value)
	}
	graph, err := db.GraphQuery(ctx, rhiza.GraphQueryRequest{Cypher: "MATCH (n:Compat) RETURN n.name", Consistency: rhiza.ConsistencyLocal})
	mustErr(err)
	foundOld := false
	for _, row := range graph.Rows {
		foundOld = foundOld || len(row) == 1 && row[0] == "written-by-v03"
	}
	if !foundOld {
		fail("old graph value: %#v", graph.Rows)
	}
	for _, receipt := range []struct{ kind, id string }{{rhiza.RequestKindSQL, oldSQLRequest}, {rhiza.RequestKindKV, oldKVRequest}, {rhiza.RequestKindGraph, oldGraphRequest}} {
		status, err := db.RequestStatus(ctx, rhiza.RequestStatusRequest{Kind: receipt.kind, RequestID: receipt.id})
		mustErr(err)
		if status.State != rhiza.RequestStateCommitted {
			fail("old receipt %s/%s: %#v", receipt.kind, receipt.id, status)
		}
	}
}

func writeNew(ctx context.Context, db *rhiza.DB) {
	must(db.Execute(ctx, rhiza.ExecuteRequest{RequestID: "compat-sql-insert-v05", SQL: "INSERT INTO compat_items VALUES (?, ?)", Args: []any{2, "written-by-v05"}}))
	must(db.KVPut(ctx, rhiza.KVMutationRequest{RequestID: "compat-kv-put-v05", Key: "compat/new-key", Value: []byte("written-by-v05")}))
	must(db.GraphExecute(ctx, rhiza.GraphCommand{RequestID: "compat-graph-insert-v05", Cypher: "CREATE (:Compat {id: 'graph-2', name: 'written-by-v05'})"}))
	fmt.Println("reader: old values/receipts verified; v0.5 writes committed")
}

func verifyNew(ctx context.Context, db *rhiza.DB) {
	query, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT name FROM compat_items WHERE id = ?", Args: []any{2}})
	mustErr(err)
	if len(query.Rows) != 1 || query.Rows[0][0] != "written-by-v05" {
		fail("new SQL value: %#v", query.Rows)
	}
	kv, err := db.KVGet(ctx, rhiza.KVGetRequest{Key: "compat/new-key"})
	mustErr(err)
	if !kv.Found || string(kv.Value) != "written-by-v05" {
		fail("new KV value: found=%v value=%q", kv.Found, kv.Value)
	}
	graph, err := db.GraphQuery(ctx, rhiza.GraphQueryRequest{Cypher: "MATCH (n:Compat) RETURN n.name", Consistency: rhiza.ConsistencyLocal})
	mustErr(err)
	if len(graph.Rows) != 2 {
		fail("new graph values: %#v", graph.Rows)
	}
	for _, receipt := range []struct{ kind, id string }{{rhiza.RequestKindSQL, "compat-sql-insert-v05"}, {rhiza.RequestKindKV, "compat-kv-put-v05"}, {rhiza.RequestKindGraph, "compat-graph-insert-v05"}} {
		status, err := db.RequestStatus(ctx, rhiza.RequestStatusRequest{Kind: receipt.kind, RequestID: receipt.id})
		mustErr(err)
		if status.State != rhiza.RequestStateCommitted {
			fail("new receipt %s/%s: %#v", receipt.kind, receipt.id, status)
		}
	}
	fmt.Println("reopen: old/new SQL/KV/Graph values and receipts verified")
}

func must(_ any, err error) { mustErr(err) }
func mustErr(err error) {
	if err != nil {
		fail("operation: %v", err)
	}
}
func fail(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...); os.Exit(1) }
