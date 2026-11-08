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
				// The first slot outside the prune range must survive, including
				// sparse-slot and multi-item cases. Seed it once outside timing.
				if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
					for item := range batchSize {
						id := fmt.Sprintf("slot-%d-request-%d", through+1, item)
						if err := putRequest(tx, id, graphRequest{Receipt: types.MutationReceipt{Slot: through + 1, Status: types.MutationCommitted, Applied: true}}); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
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
					b.StopTimer()
					if err := m.graph.db.View(func(tx *latticedb.Tx) error {
						for slot := start; slot <= through+1; slot++ {
							boundary := slot == through+1
							if _, found, err := tx.GetAppMetadata(graphSlotKey(slot)); err != nil {
								return err
							} else if found != boundary {
								return fmt.Errorf("slot index %d present=%t want=%t", slot, found, boundary)
							}
							if !boundary && int(slot-start)%gap != 0 {
								continue
							}
							for item := range batchSize {
								id := fmt.Sprintf("slot-%d-request-%d", slot, item)
								request, found, err := requestInTx(tx, id)
								if err != nil {
									return err
								}
								if found != boundary || boundary && (request.Receipt.Slot != slot || request.Receipt.Status != types.MutationCommitted || !request.Receipt.Applied) {
									return fmt.Errorf("request %s present=%t receipt=%+v boundary=%t", id, found, request.Receipt, boundary)
								}
							}
						}
						return nil
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
