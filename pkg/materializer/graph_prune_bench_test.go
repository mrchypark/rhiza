package materializer

import (
	"fmt"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/types"
)

func BenchmarkGraphPruneSparseSlotsAndBatchSize(b *testing.B) {
	const window uint64 = 256
	for _, gap := range []int{1, 8} {
		for _, batchSize := range []int{1, 8} {
			b.Run(fmt.Sprintf("gap=%d/batch=%d", gap, batchSize), func(b *testing.B) {
				m, err := Open(b.TempDir()+"/graph.db", 1)
				if err != nil {
					b.Fatal(err)
				}
				defer m.Close()
				start, through := window+1, 2*window
				seed := func() error {
					return m.graph.db.Update(func(tx *latticedb.Tx) error {
						for slot := start; slot <= through; slot++ {
							if int(slot-start)%gap != 0 {
								continue
							}
							for item := range batchSize {
								id := fmt.Sprintf("slot-%d-request-%d", slot, item)
								request := graphRequest{Receipt: types.MutationReceipt{Slot: slot, Status: types.MutationCommitted, Applied: true}}
								if err := putRequest(tx, id, request); err != nil {
									return err
								}
							}
						}
						return nil
					})
				}
				if err := seed(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					if err := seed(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
						return pruneGraphRequestsForApply(tx, 3*window, 2*window, window)
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
