package network

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
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

func TestServerGraphBatcherPolicyAndClose(t *testing.T) {
	for _, policy := range []sqlpolicy.MaterializationPolicy{7, 8, 9, 10} {
		t.Run(strconv.Itoa(int(policy)), func(t *testing.T) {
			s, err := NewServerForPolicy(nil, nil, "cluster", false, nil, policy)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.Close)
			graph := s.graphBatcher
			wantValue, wantBatch := quepaxa.MaxReplicatedValueBytes, maxMutationBatch
			if policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
				wantValue, wantBatch = types.MaxManagedGraphExecutionBytes, 1
			}
			if graph.encodedValueLimit() != wantValue || graph.commandBatchLimit() != wantBatch {
				t.Fatalf("wrong batcher limits: %d/%d", graph.encodedValueLimit(), graph.commandBatchLimit())
			}
			closed := make(chan struct{})
			go func() {
				s.Close()
				close(closed)
			}()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("server did not drain owned batchers")
			}
			for name, ctx := range map[string]<-chan struct{}{
				"sql": s.sqlBatcher.ctx.Done(), "graph": graph.ctx.Done(), "kv": s.kvBatcher.ctx.Done(),
			} {
				select {
				case <-ctx:
				default:
					t.Fatalf("%s batcher context remains live after Close", name)
				}
			}
			s.Close()
		})
	}
}
