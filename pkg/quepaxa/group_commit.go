package quepaxa

import (
	"context"
	"sync"
	"time"
)

const (
	recorderCommitDelay = 25 * time.Microsecond
	recorderCommitMax   = 64
)

type groupCommit struct {
	sync    func() error
	mu      sync.Mutex
	pending *commitBatch
	running bool
	wake    chan struct{}
	delay   time.Duration
	max     int
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
	g.mu.Lock()
	if g.pending == nil {
		g.pending = &commitBatch{done: make(chan struct{})}
	}
	batch := g.pending
	batch.waiters++
	if !g.running {
		g.running = true
		go g.flush()
	}
	if batch.waiters >= g.max {
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
		g.pending = nil
		g.mu.Unlock()
		batch.err = g.sync()
		close(batch.done)
		g.mu.Lock()
		if g.pending == nil {
			g.running = false
			g.mu.Unlock()
			return
		}
		g.mu.Unlock()
	}
}
