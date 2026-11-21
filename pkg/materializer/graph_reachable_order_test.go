package materializer

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestGraphReachableFrontierOrderAndFilters(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "graph.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	ids := make([]uint64, 7)
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
		for i, name := range []string{"start", "blocked", "b", "c", "d", "e", "hidden"} {
			group := "keep"
			if name == "blocked" {
				group = "skip"
			}
			node, err := tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Node"}, Properties: map[string]latticedb.Value{"id": name, "group": group}})
			if err != nil {
				return err
			}
			ids[i] = node.ID
		}
		for _, edge := range [][2]int{{0, 1}, {0, 3}, {0, 2}, {0, 2}, {2, 5}, {2, 4}, {3, 4}, {1, 6}} {
			if _, err := tx.CreateEdge(ids[edge[0]], ids[edge[1]], "REL", latticedb.CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if ids[1] >= ids[2] || ids[2] >= ids[3] {
		t.Fatalf("fixture IDs do not sort blocked before b and c: %v", ids)
	}
	if err := m.ConfigureLocalGraphNodePropertyIndexes([]types.GraphNodePropertyIndex{{Label: "Node", Property: "id"}}); err != nil {
		t.Fatal(err)
	}
	request := types.GraphReachableRequest{
		StartLabel: "Node", StartProperty: "id", StartValue: "start", EdgeType: "REL",
		NodeLabel: "Node", NodeFilters: map[string]any{"group": "keep"}, ResultProperty: "id",
		MaxDepth: 3, MaxResults: 10, MaxScannedEdges: 10, MaxBytes: 1024,
	}
	result, err := m.GraphReachable(context.Background(), request)
	want := []types.GraphReachableNode{{Value: "b", Distance: 1}, {Value: "c", Distance: 1}, {Value: "d", Distance: 2}, {Value: "e", Distance: 2}}
	if err != nil || !result.StartFound || result.ScannedEdges != 7 || !reflect.DeepEqual(result.Nodes, want) {
		t.Fatalf("result=%#v error=%v, want nodes=%#v and 7 scanned edges", result, err, want)
	}
	request.NodeFilters = map[string]any{"group": "none"}
	request.MaxBytes = 2
	if result, err := m.GraphReachable(context.Background(), request); err != nil || len(result.Nodes) != 0 || cap(result.Nodes) != 0 {
		t.Fatalf("fully filtered result=%#v capacity=%d error=%v", result, cap(result.Nodes), err)
	}
	request.NodeFilters = map[string]any{"group": "keep"}
	request.MaxBytes = 2
	if result, err := m.GraphReachable(context.Background(), request); !errors.Is(err, ErrGraphResourceLimit) || len(result.Nodes) != 0 {
		t.Fatalf("byte-limited result=%#v error=%v", result, err)
	}
}
