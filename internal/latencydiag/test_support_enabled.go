//go:build rhiza_latency_diag

package latencydiag

// DisableBackgroundWriterForTest lets a deterministic subprocess test call
// Flush itself. It does not change normal tagged diagnostic execution.
func DisableBackgroundWriterForTest() { startWriter.Do(func() {}) }

// SetLosslessForTest makes deterministic fixture assertions block on the ring.
// Production diagnostics retain their nonblocking, loss-reporting behavior.
func SetLosslessForTest(enabled bool) { testLossless.Store(enabled) }
