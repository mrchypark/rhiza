//go:build rhiza_local_testhooks && rhiza_latency_diag

package latencydiag

// SyncTestEvent is the bounded metadata retained by a diagnostic ring.
type SyncTestEvent struct {
	Stage    string
	ID, Slot uint64
}

// HoldSyncTestFlush is test-only. It stops the background drain, not writers.
func HoldSyncTestFlush() func() {
	flushMu.Lock()
	ring.drain()
	return flushMu.Unlock
}

// SyncTestSnapshot reads events still in the real ring and its loss count.
func SyncTestSnapshot() ([]SyncTestEvent, uint64) {
	ring.mu.Lock()
	defer ring.mu.Unlock()
	result := make([]SyncTestEvent, 0, ring.count)
	for i := 0; i < ring.count; i++ {
		e := ring.items[(ring.start+i)%capacity]
		result = append(result, SyncTestEvent{Stage: e.stage, ID: e.batch, Slot: e.slot})
	}
	return result, ring.dropped.Load()
}
