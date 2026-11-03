package network

import (
	"strconv"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

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
