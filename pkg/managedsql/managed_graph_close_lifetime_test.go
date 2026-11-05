//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedGraphCloseCancelsActiveBackgroundAdmissionResampling(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	openCtx, openCancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(openCancel)
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		h, err := fixture.open(openCtx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatalf("open Graph Host %d: %v", i, err)
		}
		hosts[i] = h
	}
	// A policy-10 Graph Host is not locally Ready until the actual initial
	// immutable root has been published. Drive that contract through a real
	// first public mutation before testing cancellation of later admission.
	warmup, err := hosts[0].GraphExecute(openCtx, types.GraphCommand{
		RequestID: "close-resampling-initial-root",
		Cypher:    "CREATE (:CloseResamplingInitialRoot {n:1})",
	})
	if err != nil || warmup.Slot == 0 || warmup.Status != types.MutationCommitted {
		t.Fatalf("actual first Graph write did not publish the initial root: slot=%d status=%s err=%v", warmup.Slot, warmup.Status, err)
	}
	if err := hosts[0].Ready(openCtx); err != nil {
		t.Fatalf("Graph Host not ready after its actual initial-root write: %v", err)
	}

	var armed atomic.Bool
	var closing atomic.Bool
	var injecting atomic.Bool
	var observedHooks atomic.Int64
	var progressCount atomic.Int64
	var progressPaused atomic.Bool
	progressed := make(chan struct{}, 1)
	resumeProgress := make(chan struct{})
	var resumeProgressOnce sync.Once
	releaseProgress := func() { resumeProgressOnce.Do(func() { close(resumeProgress) }) }
	var hookErr atomic.Value
	restore := localtesthooks.Set(func(event string) {
		if event != "node:managed-runtime-state:after-certified-tip" || !armed.Load() || closing.Load() || !injecting.CompareAndSwap(false, true) {
			return
		}
		if observedHooks.Add(1) == 1 {
			injecting.Store(false)
			return // GraphExecute's Host-level initial gate precedes the Node operation.
		}
		id := progressCount.Load() + 1
		_, err := hosts[1].GraphExecute(context.Background(), types.GraphCommand{RequestID: fmt.Sprintf("close-progress-%d", id), Cypher: "CREATE (:CloseProgress {id: $id})", Args: map[string]any{"id": id}})
		injecting.Store(false)
		if err != nil {
			hookErr.Store(err)
			return
		}
		count := progressCount.Add(1)
		if count >= 3 && progressPaused.CompareAndSwap(false, true) {
			select {
			case progressed <- struct{}{}:
			default:
			}
			<-resumeProgress
		}
	})
	t.Cleanup(restore)
	t.Cleanup(releaseProgress)

	var captureArmed atomic.Bool
	var captured atomic.Bool
	entered := make(chan context.Context, 1)
	canceled := make(chan struct{})
	releaseOperation := make(chan struct{})
	var releaseOperationOnce sync.Once
	releaseBorrower := func() { releaseOperationOnce.Do(func() { close(releaseOperation) }) }
	restoreContext := localtesthooks.SetManagedRuntimeStateContext(func(operationCtx context.Context) {
		if !captureArmed.Load() || !captured.CompareAndSwap(false, true) {
			return
		}
		entered <- operationCtx
		<-operationCtx.Done()
		close(canceled)
		<-releaseOperation
	})
	t.Cleanup(restoreContext)
	t.Cleanup(releaseBorrower)

	armed.Store(true)
	type executeResult struct {
		response network.GraphExecuteResponse
		err      error
	}
	executed := make(chan executeResult, 1)
	go func() {
		response, err := hosts[0].GraphExecute(context.Background(), types.GraphCommand{RequestID: "close-active-admission", Cypher: "CREATE (:CloseLifetime {id: 1})"})
		executed <- executeResult{response: response, err: err}
	}()

	select {
	case <-progressed:
	case <-time.After(20 * time.Second):
		if err, ok := hookErr.Load().(error); ok {
			t.Fatalf("actual read-barrier progress failed: %v", err)
		}
		t.Fatal("Graph admission did not repeatedly reobserve actual Core progress")
	}
	if progressCount.Load() < 3 {
		t.Fatalf("insufficient actual certified progress before close: %d", progressCount.Load())
	}

	closing.Store(true)
	captureArmed.Store(true)
	releaseProgress()
	var operationCtx context.Context
	select {
	case operationCtx = <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("active Graph admission did not reach the captured runtime-state boundary")
	}

	closed := make(chan error, 1)
	go func() { closed <- hosts[0].Close() }()
	select {
	case <-operationCtx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Host.Close did not cancel the captured operation context")
	}
	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("runtime-state hook did not observe operation-context cancellation")
	}
	releaseBorrower()
	select {
	case err := <-executed:
		if !errors.Is(err.err, context.Canceled) {
			t.Fatalf("Host.Close did not cancel the active Background admission: %v", err.err)
		}
		if err.response.MutationReceipt.Slot != 0 {
			t.Fatalf("active Graph command was submitted after Host.Close began: receipt=%+v", err.response.MutationReceipt)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("active Graph admission did not return after Host.Close canceled its lifetime")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Host.Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Host.Close remained blocked behind active Graph admission")
	}
	armed.Store(false)
}
