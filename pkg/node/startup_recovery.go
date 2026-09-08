package node

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const startupRecoveryLease = 2 * time.Minute

// startupRecoveryGuard keeps the archive head and checkpoint root selected at
// startup alive until recovery has consumed them.
type startupRecoveryGuard struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	owner  string

	mu       sync.Mutex
	err      error
	snapshot *recovery.RecoverySnapshot
	root     *checkpoint.RecoveryPin
	close    sync.Once
}

func newStartupRecoveryGuard(ctx context.Context, archive *recovery.Manager, owner string) (*startupRecoveryGuard, error) {
	workCtx, cancel := context.WithCancel(ctx)
	g := &startupRecoveryGuard{ctx: workCtx, cancel: cancel, done: make(chan struct{}), owner: owner}
	// Snapshot even an empty head. BeginRecoverySnapshot reloads under the
	// archive GC lock, closing the race where another voter publishes the first
	// suffix after this node's initial archive Load. Local filesystem archives
	// without CAS are single-voter only and cannot offer this shared fence.
	if archive != nil && archive.CASSupported() {
		snapshot, err := archive.BeginRecoverySnapshot(workCtx, owner, startupRecoveryLease)
		if err != nil {
			cancel()
			return nil, err
		}
		g.snapshot = snapshot
	}
	go g.renew()
	return g, nil
}

func startupRecoveryOwner(nodeID string) (string, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("startup-%s-%x", nodeID, nonce), nil
}

func (g *startupRecoveryGuard) Context() context.Context { return g.ctx }

func (g *startupRecoveryGuard) Owner() string { return g.owner }

func (g *startupRecoveryGuard) Snapshot() *recovery.RecoverySnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snapshot
}

func (g *startupRecoveryGuard) PinRoot(ctx context.Context, manager *checkpoint.Manager, root *checkpoint.Checkpoint, owner string) (*checkpoint.Checkpoint, error) {
	pin, err := manager.PinRecoveryRoot(ctx, root, owner, startupRecoveryLease)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	g.root = pin
	g.mu.Unlock()
	return pin.Root()
}

func (g *startupRecoveryGuard) Check(err error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	return err
}

func (g *startupRecoveryGuard) renew() {
	defer close(g.done)
	ticker := time.NewTicker(startupRecoveryLease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.mu.Lock()
			var err error
			if g.snapshot != nil {
				err = g.snapshot.Renew(g.ctx, startupRecoveryLease)
			}
			if err == nil && g.root != nil {
				err = g.root.Renew(g.ctx, startupRecoveryLease)
			}
			if err != nil {
				g.err = err
				g.cancel()
			}
			g.mu.Unlock()
		}
	}
}

func (g *startupRecoveryGuard) Close() {
	g.close.Do(func() {
		g.cancel()
		<-g.done
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		g.mu.Lock()
		root, snapshot := g.root, g.snapshot
		g.mu.Unlock()
		if root != nil {
			_ = root.Close(closeCtx)
		}
		if snapshot != nil {
			_ = snapshot.Close(closeCtx)
		}
	})
}
