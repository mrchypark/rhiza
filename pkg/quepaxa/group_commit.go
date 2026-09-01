package quepaxa

import (
	"context"
	"sync"
	"time"
)

const (
	recorderCommitDelay = 0
	recorderCommitMax   = 64
)

type groupCommit struct {
	sync    func() error
	mu      sync.Mutex
	waiters []chan error
	running bool
	wake    chan struct{}
	delay   time.Duration
	max     int
}

func newGroupCommit(syncFn func() error) *groupCommit {
	return &groupCommit{sync: syncFn, wake: make(chan struct{}, 1), delay: recorderCommitDelay, max: recorderCommitMax}
}

func (g *groupCommit) Sync(ctx context.Context) error {
	done := make(chan error, 1)
	g.mu.Lock()
	g.waiters = append(g.waiters, done)
	if !g.running {
		g.running = true
		go g.flush()
	}
	if len(g.waiters) >= g.max {
		select {
		case g.wake <- struct{}{}:
		default:
		}
	}
	g.mu.Unlock()
	select {
	case err := <-done:
		return err
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
		waiters := g.waiters
		g.waiters = nil
		g.mu.Unlock()
		err := g.sync()
		for _, waiter := range waiters {
			waiter <- err
		}
		g.mu.Lock()
		if len(g.waiters) == 0 {
			g.running = false
			g.mu.Unlock()
			return
		}
		g.mu.Unlock()
	}
}
