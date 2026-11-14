//go:build !rhiza_latency_diag

package quepaxa

import "github.com/mrchypark/rhiza/pkg/qlog"

type groupCommitTrace struct{}
type commitBatchTrace struct{}

func (b *commitBatch) initTrace()                  {}
func (b *commitBatch) traceID() uint64             { return 0 }
func (g *groupCommit) syncForTrace(_ uint64) error { return g.sync() }

func newWALGroupCommit(wal *qlog.WAL) *groupCommit { return newGroupCommit(wal.Sync) }
