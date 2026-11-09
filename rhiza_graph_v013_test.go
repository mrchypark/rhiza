package rhiza_test

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestEmbeddedGraphOptionalExistsAndUnion(t *testing.T) {
	ctx := t.Context()
	db, err := rhiza.Open(ctx, rhiza.Config{NodeID: "n1", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var tip uint64
	for i, cypher := range []string{
		"CREATE (:Person {name: 'Ada'})",
		"CREATE (:Person {name: 'Grace'})",
		"MATCH (a:Person {name: 'Ada'}), (g:Person {name: 'Grace'}) CREATE (a)-[:KNOWS]->(g)",
	} {
		result, err := db.GraphExecute(ctx, rhiza.GraphCommand{RequestID: "seed-" + strconv.Itoa(i), Cypher: cypher})
		if err != nil || result.Status != rhiza.MutationCommitted || result.Slot <= tip {
			t.Fatalf("seed %q: result=%+v err=%v", cypher, result, err)
		}
		tip = result.Slot
	}

	for _, tc := range []struct {
		name, cypher string
		columns      []string
		rows         [][]any
	}{
		{"optional", "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) RETURN p.name AS person, f.name AS friend ORDER BY person", []string{"person", "friend"}, [][]any{{"Ada", "Grace"}, {"Grace", nil}}},
		{"exists", "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Grace' } RETURN p.name AS person", []string{"person"}, [][]any{{"Ada"}}},
		{"not-exists", "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Grace' } RETURN p.name AS person", []string{"person"}, [][]any{{"Grace"}}},
		{"union", "RETURN 'Ada' AS person UNION RETURN 'Ada' AS person", []string{"person"}, [][]any{{"Ada"}}},
		{"union-all", "RETURN 'Ada' AS person UNION ALL RETURN 'Ada' AS person", []string{"person"}, [][]any{{"Ada"}, {"Ada"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := db.GraphQuery(t.Context(), rhiza.GraphQueryRequest{Cypher: tc.cypher, Consistency: rhiza.ConsistencyLocal})
			if err != nil || !reflect.DeepEqual(result.Columns, tc.columns) || !reflect.DeepEqual(result.Rows, tc.rows) || result.AppliedSlot != tip {
				t.Fatalf("query=%q result=%+v err=%v; want columns=%v rows=%v applied=%d", tc.cypher, result, err, tc.columns, tc.rows, tip)
			}
		})
	}

	if result, err := db.GraphQuery(ctx, rhiza.GraphQueryRequest{
		Cypher:      "RETURN 'Ada' AS person UNION ALL CREATE (:Person {name: 'unwanted'}) RETURN 'unwanted' AS person",
		Consistency: rhiza.ConsistencyLocal,
	}); err == nil || len(result.Rows) != 0 {
		t.Fatalf("read-only UNION allowed a mutation or returned partial rows: result=%+v err=%v", result, err)
	}
	result, err := db.GraphQuery(ctx, rhiza.GraphQueryRequest{Cypher: "MATCH (p:Person) RETURN p.name AS person ORDER BY person", Consistency: rhiza.ConsistencyLocal})
	if err != nil || !reflect.DeepEqual(result.Rows, [][]any{{"Ada"}, {"Grace"}}) || result.AppliedSlot != tip {
		t.Fatalf("read-only rejection changed graph state: result=%+v err=%v", result, err)
	}
}
