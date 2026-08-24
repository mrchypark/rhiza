//go:build graph

package rhiza_test

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestEmbeddedGraphGoAPI(t *testing.T) {
	db, err := rhiza.Open(context.Background(), rhiza.Config{NodeID: "n1", Profile: rhiza.ProfileGraph, DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.GraphExecute(context.Background(), rhiza.GraphCommand{RequestID: "schema", Cypher: "CREATE NODE TABLE Tea(id STRING, name STRING, PRIMARY KEY(id))"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GraphExecute(context.Background(), rhiza.GraphCommand{RequestID: "insert", Cypher: "CREATE (:Tea {id: '1', name: 'green'})"}); err != nil {
		t.Fatal(err)
	}
	result, err := db.GraphQuery(context.Background(), rhiza.GraphQueryRequest{Cypher: "MATCH (n:Tea) RETURN n.name", Consistency: rhiza.ConsistencyLocal})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
}
