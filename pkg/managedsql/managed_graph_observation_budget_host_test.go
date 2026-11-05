//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

type managedGraphCallerMarkerKey struct{}

type managedGraphCallerMarkedContext struct {
	context.Context
	marker *struct{}
}

func (c managedGraphCallerMarkedContext) Value(key any) any {
	if key == (managedGraphCallerMarkerKey{}) {
		return c.marker
	}
	return c.Context.Value(key)
}

func TestManagedGraphAdmissionPublicHostPreservesCallerContext(t *testing.T) {
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

	marker := &struct{}{}
	operationCtx := managedGraphCallerMarkedContext{Context: ctx, marker: marker}
	var observations atomic.Int32
	restore := localtesthooks.SetManagedRuntimeStateContext(func(observed context.Context) {
		if observed.Value(managedGraphCallerMarkerKey{}) != marker {
			return
		}
		observations.Add(1)
	})
	defer restore()
	result, err := hosts[0].GraphExecute(operationCtx, types.GraphCommand{RequestID: "host-caller-context", Cypher: "CREATE (:CallerContext {once:true})"})
	if err != nil || result.Status != types.MutationCommitted || result.Slot == 0 {
		t.Fatalf("public Graph operation failed: receipt=%+v err=%v", result, err)
	}
	if observations.Load() < 3 {
		t.Fatalf("Host initial/execution observations lost caller context: observations=%d", observations.Load())
	}
}
