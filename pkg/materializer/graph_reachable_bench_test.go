package materializer

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/types"
)

func BenchmarkGraphReachableFanout4096(b *testing.B) {
	m, err := Open(filepath.Join(b.TempDir(), "graph.db"), 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = m.Close() })

	const fanout = 4096
	ids := make([]uint64, fanout+1)
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
		for i := range ids {
			node, err := tx.CreateNode(latticedb.CreateNodeOptions{
				Labels:     []string{"BenchmarkNode"},
				Properties: map[string]latticedb.Value{"id": fmt.Sprintf("%d", i)},
			})
			if err != nil {
				return err
			}
			ids[i] = node.ID
		}
		for _, target := range ids[1:] {
			if _, err := tx.CreateEdge(ids[0], target, "REL", latticedb.CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	if err := m.ConfigureLocalGraphNodePropertyIndexes([]types.GraphNodePropertyIndex{{Label: "BenchmarkNode", Property: "id"}}); err != nil {
		b.Fatal(err)
	}

	request := types.GraphReachableRequest{
		StartLabel: "BenchmarkNode", StartProperty: "id", StartValue: "0", EdgeType: "REL",
		NodeLabel: "BenchmarkNode", ResultProperty: "id",
		MaxDepth: 1, MaxResults: fanout, MaxScannedEdges: fanout, MaxBytes: 1 << 20,
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := m.GraphReachable(ctx, request)
		if err != nil || len(result.Nodes) != fanout || result.ScannedEdges != fanout {
			b.Fatalf("reachable nodes=%d scanned=%d err=%v", len(result.Nodes), result.ScannedEdges, err)
		}
	}
}
