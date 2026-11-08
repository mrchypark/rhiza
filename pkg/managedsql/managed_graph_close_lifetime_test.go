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

	type targetRequestKey struct{}
	targetCtx := context.WithValue(context.Background(), targetRequestKey{}, true)
	progressCtx, cancelProgress := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancelProgress)
	var observing atomic.Bool
	var skippedInitialGate, captured, failed bool
	progressCount := 0
	var lastSlot uint64
	progressed := make(chan error, 1)
	resumeProgress := make(chan struct{})
	var resumeProgressOnce sync.Once
	releaseProgress := func() { resumeProgressOnce.Do(func() { close(resumeProgress) }) }
	entered := make(chan context.Context, 1)
	canceled := make(chan struct{})
	releaseOperation := make(chan struct{})
	var releaseOperationOnce sync.Once
	releaseBorrower := func() { releaseOperationOnce.Do(func() { close(releaseOperation) }) }

	advanceTarget := func(id int) (uint64, error) {
		receipt, err := hosts[1].GraphExecute(progressCtx, types.GraphCommand{
			RequestID: fmt.Sprintf("close-progress-%d", id),
			Cypher:    "CREATE (:CloseProgress {id: $id})",
			Args:      map[string]any{"id": id},
		})
		if err != nil || receipt.Slot <= lastSlot || receipt.Status != types.MutationCommitted {
			return 0, fmt.Errorf("peer write did not commit a new slot: slot=%d status=%s err=%v", receipt.Slot, receipt.Status, err)
		}
		// The peer can ACK before Learned reaches the target's Core.
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			state, err := hosts[0].node.ManagedState()
			if err == nil && state.CertifiedTip >= receipt.Slot {
				return receipt.Slot, nil
			}
			if err != nil && !errors.Is(err, network.ErrNotReady) {
				return 0, err
			}
			select {
			case <-progressCtx.Done():
				return 0, fmt.Errorf("target did not certify slot %d (last observed tip %d): %w", receipt.Slot, state.CertifiedTip, progressCtx.Err())
			case <-tick.C:
			}
		}
	}
	restoreContext := localtesthooks.SetManagedRuntimeStateContext(func(operationCtx context.Context) {
		if operationCtx.Value(targetRequestKey{}) != true || !observing.CompareAndSwap(false, true) {
			return
		}
		defer observing.Store(false)
		if captured || failed {
			return
		}
		if !skippedInitialGate {
			skippedInitialGate = true
			return // GraphExecute's Host-level initial gate precedes the Node operation.
		}
		if progressCount < 3 {
			slot, err := advanceTarget(progressCount + 1)
			if err != nil {
				failed = true
				progressed <- err
				return
			}
			lastSlot = slot
			progressCount++
			if progressCount == 3 {
				progressed <- nil
				<-resumeProgress
			}
			return
		}
		captured = true
		entered <- operationCtx
		<-operationCtx.Done()
		close(canceled)
		<-releaseOperation
	})
	t.Cleanup(restoreContext)
	t.Cleanup(releaseProgress)
	t.Cleanup(releaseBorrower)

	type executeResult struct {
		response network.GraphExecuteResponse
		err      error
	}
	executed := make(chan executeResult, 1)
	go func() {
		response, err := hosts[0].GraphExecute(targetCtx, types.GraphCommand{RequestID: "close-active-admission", Cypher: "CREATE (:CloseLifetime {id: 1})"})
		executed <- executeResult{response: response, err: err}
	}()

	select {
	case err := <-progressed:
		if err != nil {
			t.Fatalf("actual read-barrier progress failed: %v", err)
		}
	case result := <-executed:
		t.Fatalf("Graph admission returned before observing repeated Core progress: slot=%d err=%v", result.response.MutationReceipt.Slot, result.err)
	case <-progressCtx.Done():
		t.Fatal("Graph admission did not repeatedly reobserve actual Core progress")
	}
	if progressCount != 3 {
		t.Fatalf("insufficient actual certified progress before close: %d", progressCount)
	}

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
}
