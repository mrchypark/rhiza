//go:build rhiza_latency_diag

package qlog

// SyncWithDiagnosticBatch links this physical WAL sync to an internal group.
// It is present only in explicitly tagged diagnostic builds.
func (w *WAL) SyncWithDiagnosticBatch(batch uint64) error {
	return w.syncWithDiagnosticBatch(batch)
}
