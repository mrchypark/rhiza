package quepaxa

import (
	"context"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/asyncperfprobe"
)

const (
	recorderCommitDelay = 25 * time.Microsecond
	recorderCommitMax   = 64
)

type groupCommit struct {
	sync         func() error
	mu           sync.Mutex
	pending      *commitBatch
	pendingSince time.Time
	running      bool
	wake         chan struct{}
	delay        time.Duration
	max          int
}

type commitBatch struct {
	done    chan struct{}
	err     error
	waiters int
}

func newGroupCommit(syncFn func() error) *groupCommit {
	return &groupCommit{sync: syncFn, wake: make(chan struct{}, 1), delay: recorderCommitDelay, max: recorderCommitMax}
}

func (g *groupCommit) Sync(ctx context.Context) error {
	return g.syncBatch(ctx, false)
}

// syncBatch joins the pending batch before requesting an early flush. The
// existing worker still serializes WAL Sync and publishes the shared result.
func (g *groupCommit) syncBatch(ctx context.Context, immediate bool) error {
	g.mu.Lock()
	if g.pending == nil {
		g.pending = &commitBatch{done: make(chan struct{})}
		g.pendingSince = asyncperfprobe.Stamp()
	}
	batch := g.pending
	batch.waiters++
	if !g.running {
		g.running = true
		go g.flush()
	}
	if immediate || batch.waiters >= g.max {
		select {
		case g.wake <- struct{}{}:
		default:
		}
	}
	g.mu.Unlock()
	select {
	case <-batch.done:
		return batch.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *groupCommit) flush() {
	for {
		timer := time.NewTimer(g.delay)
		select {
		case <-timer.C:
		case <-g.wake:
			timer.Stop()
		}
		g.mu.Lock()
		batch := g.pending
		queued := g.pendingSince
		g.pending = nil
		g.mu.Unlock()
		start := asyncperfprobe.Stamp()
		batch.err = g.sync()
		end := asyncperfprobe.Stamp()
		close(batch.done)
		if !start.IsZero() {
			asyncperfprobe.Slow("group_queue", queued, start)
			asyncperfprobe.Slow("group_flush", start, end)
		}
		g.mu.Lock()
		if g.pending == nil {
			g.running = false
			g.mu.Unlock()
			return
		}
		g.mu.Unlock()
	}
}
