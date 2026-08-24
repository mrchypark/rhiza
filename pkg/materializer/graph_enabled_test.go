//go:build graph

package materializer

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestGraphAndKVMaterializer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	applyGraph := func(slot uint64, command types.GraphCommand) {
		t.Helper()
		value, err := types.EncodeGraphCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, slot, value); err != nil {
			t.Fatal(err)
		}
	}
	applyGraph(1, types.GraphCommand{RequestID: "schema", Cypher: `CREATE NODE TABLE Person(id STRING, name STRING, PRIMARY KEY(id))`})
	person := types.GraphCommand{RequestID: "person-1", Cypher: `CREATE (p:Person {id: $id, name: $name}) RETURN p.name`, Args: map[string]any{"id": "1", "name": "Ada"}}
	applyGraph(2, person)
	applyGraph(3, person)
	value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "kv-1", Operation: "put", Key: "mode", Value: []byte("graph")})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 4, value); err != nil {
		t.Fatalf("apply KV: %v", err)
	}
	result, err := m.GraphQuery(ctx, `MATCH (p:Person {id: $id}) RETURN p.name`, map[string]any{"id": "1"})
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "Ada" {
		t.Fatalf("query result=%+v err=%v", result, err)
	}
	got, found, err := m.KVGet(ctx, "mode", time.Now())
	if err != nil || !found || string(got) != "graph" {
		t.Fatalf("KV got=%q found=%v err=%v", got, found, err)
	}
	snapshot, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	applyGraph(5, types.GraphCommand{RequestID: "person-2", Cypher: `CREATE (p:Person {id: '2', name: 'Grace'})`})
	if err := m.Restore(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	result, err = m.GraphQuery(ctx, `MATCH (p:Person) RETURN p.name ORDER BY p.name`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "Ada" || m.Tip() != 4 {
		t.Fatalf("restored graph result=%+v tip=%d err=%v", result, m.Tip(), err)
	}
	got, found, err = m.KVGet(ctx, "mode", time.Now())
	if err != nil || !found || string(got) != "graph" {
		t.Fatalf("restored KV got=%q found=%v err=%v", got, found, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.Tip() != 4 {
		t.Fatalf("tip=%d, want 4", m.Tip())
	}
}

func TestFailedGraphCommandRecordsResultAndAdvancesTip(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	commands := []types.GraphCommand{
		{RequestID: "schema", Cypher: `CREATE NODE TABLE Item(id STRING, PRIMARY KEY(id))`},
		{RequestID: "first", Cypher: `CREATE (:Item {id: '1'})`},
		{RequestID: "duplicate", Cypher: `CREATE (:Item {id: '1'})`},
		{RequestID: "after", Cypher: `CREATE (:Item {id: '2'})`},
	}
	for i, command := range commands {
		value, err := types.EncodeGraphCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
	}
	result, err := m.GraphRequestResult(ctx, "duplicate")
	if err != nil || result.Error == "" || m.Tip() != 4 {
		t.Fatalf("result=%+v tip=%d err=%v", result, m.Tip(), err)
	}
	rows, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id ORDER BY n.id`, nil)
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}
