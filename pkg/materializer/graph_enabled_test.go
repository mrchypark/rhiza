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
	person := types.GraphCommand{RequestID: "person-1", Cypher: `CREATE (p:Person {id: $id, name: $name}) RETURN p.name`, Args: map[string]any{"id": "1", "name": "Ada"}}
	applyGraph(1, person)
	applyGraph(2, person)
	value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "kv-1", Operation: "put", Key: "mode", Value: []byte("graph")})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 3, value); err != nil {
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
	applyGraph(4, types.GraphCommand{RequestID: "person-2", Cypher: `CREATE (p:Person {id: '2', name: 'Grace'})`})
	if err := m.Restore(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	result, err = m.GraphQuery(ctx, `MATCH (p:Person) RETURN p.name ORDER BY p.name`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "Ada" || m.Tip() != 3 {
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
	if m.Tip() != 3 {
		t.Fatalf("tip=%d, want 3", m.Tip())
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
		{RequestID: "first", Cypher: `CREATE (:Item {id: '1'})`},
		{RequestID: "invalid", Cypher: `MATCH (n:Item {id: '1'}) RETURN n`},
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
	result, err := m.GraphRequestResult(ctx, "invalid")
	if err != nil || result.Error == "" || m.Tip() != 3 {
		t.Fatalf("result=%+v tip=%d err=%v", result, m.Tip(), err)
	}
	rows, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id ORDER BY n.id`, nil)
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestGraphQueryWaitsForSQLiteApply(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	m.mu.Lock()
	if err := m.graph.advanceTip(context.Background(), 1); err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	m.graph.tip = 1
	done := make(chan error, 1)
	go func() {
		_, err := m.GraphQuery(context.Background(), `MATCH (n) RETURN n LIMIT 1`, nil)
		done <- err
	}()
	select {
	case err := <-done:
		m.mu.Unlock()
		t.Fatalf("query escaped apply lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	m.tip = 1
	m.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
