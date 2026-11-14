//go:build rhiza_latency_diag

package quepaxa

import (
	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

type groupCommitTrace struct{ syncTagged func(uint64) error }
type commitBatchTrace struct{ id uint64 }

func (b *commitBatch) initTrace()      { b.id = latencydiag.NextBatch() }
func (b *commitBatch) traceID() uint64 { return b.id }
func (g *groupCommit) syncForTrace(id uint64) error {
	if g.syncTagged != nil {
		return g.syncTagged(id)
	}
	return g.sync()
}

func newWALGroupCommit(wal *qlog.WAL) *groupCommit {
	g := newGroupCommit(wal.Sync)
	g.syncTagged = wal.SyncWithDiagnosticBatch
	return g
}
