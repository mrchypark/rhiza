//go:build rhiza_local_testhooks

package network

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

func TestServerGraphBatcherConstructedExactlyOnce(t *testing.T) {
	if !localtesthooks.Enabled {
		t.Skip("requires rhiza_local_testhooks")
	}
	for _, policy := range []sqlpolicy.MaterializationPolicy{7, 8, 9, 10} {
		t.Run(strconv.Itoa(int(policy)), func(t *testing.T) {
			var ordinary, managed atomic.Int32
			restore := localtesthooks.Set(func(event string) {
				switch event {
				case "network:graph-batcher:ordinary-constructor":
					ordinary.Add(1)
				case "network:graph-batcher:managed-constructor":
					managed.Add(1)
				}
			})
			defer restore()
			s, err := NewServerForPolicy(nil, nil, "cluster", false, nil, policy)
			if err != nil {
				t.Fatal(err)
			}
			// Close drains each owned batcher's real workers, not merely its context.
			s.Close()
			if s.graphBatcher.ctx.Err() == nil || s.sqlBatcher.ctx.Err() == nil || s.kvBatcher.ctx.Err() == nil {
				t.Fatal("owned batcher remains live after Close")
			}
			wantOrdinary, wantManaged := int32(1), int32(0)
			if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				wantOrdinary, wantManaged = 0, 1
			}
			if ordinary.Load() != wantOrdinary || managed.Load() != wantManaged {
				t.Fatalf("constructors ordinary=%d managed=%d; want %d/%d", ordinary.Load(), managed.Load(), wantOrdinary, wantManaged)
			}
		})
	}
}
