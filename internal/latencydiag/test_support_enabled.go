//go:build rhiza_latency_diag

package latencydiag

// DisableBackgroundWriterForTest lets a deterministic subprocess test call
// Flush itself. It does not change normal tagged diagnostic execution.
func DisableBackgroundWriterForTest() { startWriter.Do(func() {}) }
