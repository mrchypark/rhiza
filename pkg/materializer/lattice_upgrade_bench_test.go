package materializer

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
)

func BenchmarkLatticeAppMetadataUpdate4096Keys(b *testing.B) {
	m, err := Open(filepath.Join(b.TempDir(), "sqlite.db"), 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = m.Close() })

	const keyCount = 4096
	hotKey := []byte("benchmark/metadata/0000")
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
		for i := range keyCount {
			if err := tx.PutAppMetadata([]byte(fmt.Sprintf("benchmark/metadata/%04d", i)), []byte("seed")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	values := [2][]byte{[]byte("zero"), []byte("one")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
			return tx.PutAppMetadata(hotKey, values[i&1])
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	got, err := m.graph.getMetadata(hotKey)
	if err != nil || !bytes.Equal(got, values[(b.N-1)&1]) {
		b.Fatalf("hot metadata=%q err=%v", got, err)
	}
}

func BenchmarkGraphQuery4096Nodes(b *testing.B) {
	m, err := Open(filepath.Join(b.TempDir(), "sqlite.db"), 1)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
		for i := range 4096 {
			if _, err := tx.CreateNode(latticedb.CreateNodeOptions{
				Labels:     []string{"BenchmarkNode"},
				Properties: map[string]latticedb.Value{"id": fmt.Sprintf("%d", i)},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	args := map[string]any{"id": "2048"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := m.GraphQuery(ctx, `MATCH (n:BenchmarkNode {id: $id}) RETURN n.id`, args)
		if err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != "2048" {
			b.Fatalf("result=%+v err=%v", result, err)
		}
	}
}
