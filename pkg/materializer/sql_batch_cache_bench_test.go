package materializer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func BenchmarkApplySQLPage(b *testing.B) {
	for _, shape := range []struct {
		name    string
		count   int
		padding int
		replay  bool
	}{
		{"within-count", 16, 0, false},
		{"over-count", 128, 0, false},
		{"over-bytes", 64, 20 << 10, false},
		{"replay", 128, 0, true},
	} {
		b.Run(shape.name, func(b *testing.B) {
			page := make([]quepaxa.DecidedValue, shape.count)
			var sourceBytes int64
			for i := range page {
				value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: fmt.Sprint(i), SQL: "INSERT INTO bench VALUES (1) /*" + strings.Repeat("x", shape.padding) + "*/"}})
				if err != nil {
					b.Fatal(err)
				}
				page[i] = quepaxa.DecidedValue{Slot: quepaxa.Slot(i + 2), Value: value}
				sourceBytes += int64(len(value))
			}
			setup, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "setup", SQL: "CREATE TABLE bench(id INTEGER)"}})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(sourceBytes)
			root := b.TempDir()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				func() {
					dir, err := os.MkdirTemp(root, "iteration-")
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := os.RemoveAll(dir); err != nil {
							b.Errorf("remove iteration directory: %v", err)
							return
						}
						if _, err := os.Lstat(dir); !os.IsNotExist(err) {
							b.Errorf("iteration directory remains or cannot be checked: %v", err)
						}
					}()
					m, err := Open(filepath.Join(dir, "bench.db"), 1)
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := m.Close(); err != nil {
							b.Errorf("close materializer: %v", err)
						}
					}()
					if err := m.Apply(context.Background(), 1, setup); err != nil {
						b.Fatal(err)
					}
					if shape.replay {
						if err := m.ApplyBatch(context.Background(), page); err != nil {
							b.Fatal(err)
						}
					}
					b.StartTimer()
					err = m.ApplyBatch(context.Background(), page)
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
				}()
				b.StartTimer()
			}
		})
	}
}
