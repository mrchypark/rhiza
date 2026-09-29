package quepaxa

import (
	"context"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func BenchmarkEnsureDurableThroughRetainedPrefix(b *testing.B) {
	for _, size := range []int{1, 64, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			wal, err := qlog.Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer wal.Close()
			core := newCore("a", &Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}, wal, nil)
			// Model an already-logged durable prefix to isolate retained-range scanning.
			for i := 1; i <= size; i++ {
				slot := Slot(i)
				value := DecidedValue{Slot: slot, Hash: [32]byte{byte(i)}}
				core.decided[slot] = value
				core.logged[slot] = true
				core.durable[slot] = true
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := core.EnsureDurableThrough(context.Background(), Slot(size)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
