//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
)

type managedGraphBudgetMarkerKey struct{}

type managedGraphBudgetProbeContext struct {
	context.Context
	mu  sync.Mutex
	key any
}

func (c *managedGraphBudgetProbeContext) Value(key any) any {
	c.mu.Lock()
	if c.key == nil {
		c.key = key
	}
	c.mu.Unlock()
	return c.Context.Value(key)
}

type managedGraphBudgetMarkedContext struct {
	context.Context
	marker *struct{}
}

func (c managedGraphBudgetMarkedContext) Value(key any) any {
	if key == (managedGraphBudgetMarkerKey{}) {
		return c.marker
	}
	return c.Context.Value(key)
}

func TestManagedGraphAdmissionPublicHostSharesObservationBudget(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		h, err := fixture.open(ctx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatalf("open public Graph Host %d: %v", i, err)
		}
		hosts[i] = h
	}

	// Learn the unexported budget key via a disposable Node helper call. The
	// actual public operation below receives a plain marker context, not a
	// context prewrapped by WithManagedControlObservationBudget.
	probe := &managedGraphBudgetProbeContext{Context: context.Background()}
	_ = node.WithManagedControlObservationBudget(probe)
	probe.mu.Lock()
	budgetKey := probe.key
	probe.mu.Unlock()
	if budgetKey == nil {
		t.Fatal("Node budget helper did not expose its private key to the probe context")
	}
	marker := &struct{}{}
	operationCtx := managedGraphBudgetMarkedContext{Context: ctx, marker: marker}
	var observations atomic.Int32
	var mu sync.Mutex
	var firstBudget any
	var mismatch atomic.Bool
	restore := localtesthooks.SetManagedRuntimeStateContext(func(observed context.Context) {
		if observed.Value(managedGraphBudgetMarkerKey{}) != marker {
			return
		}
		budget := observed.Value(budgetKey)
		if budget == nil {
			mismatch.Store(true)
			return
		}
		mu.Lock()
		if firstBudget == nil {
			firstBudget = budget
		} else if firstBudget != budget {
			mismatch.Store(true)
		}
		mu.Unlock()
		observations.Add(1)
	})
	defer restore()
	result, err := hosts[0].GraphExecute(operationCtx, types.GraphCommand{RequestID: "host-shared-observation-budget", Cypher: "CREATE (:SharedBudget {once:true})"})
	if err != nil || result.Status != types.MutationCommitted || result.Slot == 0 {
		t.Fatalf("public Graph operation failed: receipt=%+v err=%v", result, err)
	}
	if observations.Load() < 3 || mismatch.Load() {
		t.Fatalf("Host initial/execution observations did not share one nonnil budget: observations=%d mismatch=%t", observations.Load(), mismatch.Load())
	}
}
