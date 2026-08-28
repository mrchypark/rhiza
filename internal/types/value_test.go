package types

import "testing"

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
