package materializer

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

// Exercise the same lock-owned close/reopen lifecycle as restore rollback,
// with a real driver checkpoint active on the retiring SQLite handle.
func TestManagedApplicationReopenWithActiveCheckpoint(t *testing.T) {
	m := managedMaterializer(t)
	ctx := context.Background()
	managedKVApply(t, m, types.KVCommand{RequestID: "retained", Operation: "put", Key: "key", Value: []byte("value")})
	before, err := m.InspectManagedApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := checkpointBarrier(t, m.db)
	waitCheckpointSignal(t, entered, "actual retiring-handle checkpoint")
	locked, returned := make(chan struct{}), make(chan struct{})
	var reopenErr error
	go func() {
		defer close(returned)
		m.mu.Lock()
		defer m.mu.Unlock()
		close(locked)
		if reopenErr = m.closeConnections(); reopenErr == nil {
			reopenErr = m.reopen()
		}
	}()
	// Cleanup releases the owned driver barrier before joining the lifecycle
	// operation, so any assertion failure cannot strand Close or its worker.
	t.Cleanup(func() { release(); <-returned })
	waitCheckpointSignal(t, locked, "reopen lifecycle write lock")
	select {
	case <-returned:
		t.Fatal("reopen completed while its old checkpoint was held")
	default:
	}
	release()
	waitCheckpointSignal(t, returned, "reopen after checkpoint release")
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	after, err := m.InspectManagedApplicationState(ctx)
	if err != nil || before != after {
		t.Fatal("reopen changed application/receipt state", err)
	}
	resumed, releaseResumed := checkpointBarrier(t, m.db)
	waitCheckpointSignal(t, resumed, "target worker resumed on adopted handle")
	releaseResumed()
	if err := m.Close(); err != nil {
		t.Fatal("close drained resumed worker", err)
	}
}
