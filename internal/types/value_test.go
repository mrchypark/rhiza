package types

import (
	"bytes"
	"testing"
)

func TestTypedBatchDecodersRejectTrailingData(t *testing.T) {
	sql, err := EncodeSQLBatch([]SQLCommand{{RequestID: "sql", SQL: "SELECT 1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeSQLBatch(append(sql, []byte(" true")...)); err == nil {
		t.Fatal("SQL batch trailing value was accepted")
	}
	graph, err := EncodeGraphBatch([]GraphCommand{{RequestID: "graph", Cypher: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeGraphBatch(append(graph, []byte(" {}")...)); err == nil {
		t.Fatal("graph batch trailing value was accepted")
	}
}

func TestSQLBatchPreservesBlobArguments(t *testing.T) {
	want := []byte{0, 1, 2, 255}
	encoded, err := EncodeSQLBatch([]SQLCommand{{
		RequestID: "blob",
		SQL:       "INSERT INTO blobs VALUES (?)",
		Args:      []any{want},
		Statements: []SQLStatement{{
			SQL:  "INSERT INTO blobs VALUES (?)",
			Args: []any{want},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	commands, ok, err := DecodeSQLBatch(encoded)
	if err != nil || !ok {
		t.Fatalf("decode ok=%v err=%v", ok, err)
	}
	if got := commands[0].Args[0].([]byte); !bytes.Equal(got, want) {
		t.Fatalf("top-level blob=%v", got)
	}
	if got := commands[0].Statements[0].Args[0].([]byte); !bytes.Equal(got, want) {
		t.Fatalf("statement blob=%v", got)
	}
	if _, _, err := DecodeSQLBatch(append(append([]byte(nil), sqlBatchMagic...), []byte(`[{"request_id":"bad","sql":"SELECT ?","args":[{"$rhiza_blob":"!"}]}]`)...)); err == nil {
		t.Fatal("invalid blob argument was accepted")
	}
}
