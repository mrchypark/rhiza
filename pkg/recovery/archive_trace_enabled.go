//go:build rhiza_latency_diag

package recovery

import "github.com/mrchypark/rhiza/internal/latencydiag"

type archiveBatchTrace struct{ id uint64 }

func (b *syncBatch) initTrace()      { b.id = latencydiag.NextBatch() }
func (b *syncBatch) traceID() uint64 { return b.id }
