//go:build !rhiza_latency_diag

package recovery

type archiveBatchTrace struct{}

func (b *syncBatch) initTrace()      {}
func (b *syncBatch) traceID() uint64 { return 0 }
