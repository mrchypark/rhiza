package managedsql

import (
	"context"
	"fmt"
	"sync"
)

// Publication registration and quiescence are serialized across all Hosts.
// Workers registered after quiescence wait only on their own lifetime and do
// not enter a publication attempt; previously registered workers settle first.
type internalPublisherGate struct {
	mu             sync.Mutex
	quiescing      bool
	quiesceStarted chan struct{}
	started        [3]bool
	late           [3]bool
	settled        [3]chan error
	parked         [3]chan struct{}
}

func newInternalPublisherGate() *internalPublisherGate {
	g := &internalPublisherGate{quiesceStarted: make(chan struct{})}
	for i := range g.started {
		g.settled[i] = make(chan error, 1)
		g.parked[i] = make(chan struct{}, 1)
	}
	return g
}

func (g *internalPublisherGate) start(i int) {
	g.mu.Lock()
	g.started[i] = true
	g.late[i] = g.quiescing
	g.mu.Unlock()
}

func (g *internalPublisherGate) wasStarted(i int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.started[i]
}

func (g *internalPublisherGate) before(i int, ctx context.Context) {
	g.mu.Lock()
	late := g.late[i]
	g.mu.Unlock()
	if late {
		g.parked[i] <- struct{}{}
		<-ctx.Done()
	}
}

func (g *internalPublisherGate) after(i int, ctx context.Context, err error) {
	g.settled[i] <- err
	<-ctx.Done()
}

func (g *internalPublisherGate) quiesce(ctx context.Context) error {
	g.mu.Lock()
	if !g.quiescing {
		g.quiescing = true
		close(g.quiesceStarted)
	}
	started := g.started
	g.mu.Unlock()
	for i, yes := range started {
		if !yes {
			continue
		}
		select {
		case err := <-g.settled[i]:
			if err != nil {
				return fmt.Errorf("host %d checkpoint attempt: %w", i, err)
			}
		case <-ctx.Done():
			return fmt.Errorf("host %d checkpoint settlement: %w", i, ctx.Err())
		}
	}
	return nil
}
