package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const (
	localRestorePinLease         = 2 * time.Minute
	localRestorePinRenewInterval = localRestorePinLease / 6
	localRestorePinRenewBudget   = localRestorePinLease / 6
)

func retryLocalMaintenance(ctx context.Context, work func() error) error {
	delay := 10 * time.Millisecond
	var busyErr error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(context.Cause(ctx), busyErr)
		}
		err := work()
		if !errors.Is(err, checkpoint.ErrLocalMaintenanceBusy) {
			return err
		}
		busyErr = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(context.Cause(ctx), busyErr)
		case <-timer.C:
		}
		delay = min(2*delay, 250*time.Millisecond)
	}
}

func (n *Node) syncLocalBackup(ctx context.Context) error {
	n.localSnapshotMu.Lock()
	defer n.localSnapshotMu.Unlock()
	return n.syncLocalBackupLocked(ctx)
}

func (n *Node) syncLocalBackupLocked(ctx context.Context) error {
	return retryLocalMaintenance(ctx, func() error {
		return n.localBackup.SyncThrough(ctx, n.core, n.core.Tip())
	})
}

func (n *Node) collectLocalBackup(ctx context.Context) {
	if n.localBackup == nil {
		return
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if n.localGCContext != nil {
		stop := context.AfterFunc(n.localGCContext, cancel)
		defer stop()
	}
	n.localSnapshotMu.Lock()
	defer n.localSnapshotMu.Unlock()
	if err := workCtx.Err(); err != nil {
		return
	}
	err := n.localBackup.GarbageCollect(workCtx, n.config.CheckpointRetainCount, n.config.ObjStoreGCGracePeriod)
	switch err {
	case nil, context.Canceled, context.DeadlineExceeded, checkpoint.ErrLocalMaintenanceBusy:
		return
	default:
		// Wrapped cancellation can describe an uncertain mutation and held gate.
		log.Printf("Local backup GC deferred: %v", err)
	}
}

type localRestoreLease interface {
	Renew(context.Context, time.Duration) error
	Close(context.Context) error
}

type localRestorePin struct {
	mu        sync.Mutex
	pin       localRestoreLease
	renewGate chan struct{}
}

func (p *localRestorePin) renew(ctx context.Context) error {
	p.mu.Lock()
	if p.renewGate == nil {
		p.renewGate = make(chan struct{}, 1)
	}
	gate := p.renewGate
	p.mu.Unlock()
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.pin.Renew(ctx, localRestorePinLease)
}

type localRestorePins struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
	pin    *localRestorePin
	owner  *objstore.LocalFilesystemOwner
}

func newLocalRestorePins(ctx context.Context) *localRestorePins {
	workCtx, cancel := context.WithCancelCause(ctx)
	return &localRestorePins{ctx: workCtx, cancel: cancel}
}

func newOwnerLocalRestorePins(ctx context.Context, owner *objstore.LocalFilesystemOwner) *localRestorePins {
	pins := newLocalRestorePins(ctx)
	pins.owner = owner
	return pins
}

func (p *localRestorePins) acquire(archive *recovery.LocalArchive) error {
	if err := context.Cause(p.ctx); err != nil {
		return err
	}
	if p.owner != nil {
		if !p.owner.Held() {
			return fmt.Errorf("Local startup recovery lost filesystem ownership")
		}
		return nil
	}
	if p.pin != nil {
		return nil
	}
	owner := "local-restore-" + rand.Text()
	var pin *recovery.LocalRestorePin
	err := retryLocalMaintenance(p.ctx, func() error {
		var err error
		pin, err = archive.PinView(p.ctx, owner, localRestorePinLease)
		return err
	})
	if err != nil {
		return fmt.Errorf("confirm Local restore pin: %w", err)
	}
	guard := &localRestorePin{pin: pin}
	p.pin = guard
	p.watch(guard)
	return nil
}

func (p *localRestorePins) watch(guard *localRestorePin) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(localRestorePinRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(p.ctx, localRestorePinRenewBudget)
				err := guard.renew(renewCtx)
				cancel()
				if err != nil {
					p.cancel(fmt.Errorf("Local restore pin lost: %w", err))
					return
				}
			}
		}
	}()
}

func (p *localRestorePins) confirm() error {
	if p.owner != nil && !p.owner.Held() {
		return fmt.Errorf("Local startup recovery lost filesystem ownership")
	}
	if p.pin != nil {
		renewCtx, cancel := context.WithTimeout(p.ctx, localRestorePinRenewBudget)
		defer cancel()
		if err := p.pin.renew(renewCtx); err != nil {
			p.cancel(fmt.Errorf("Local restore pin lost: %w", err))
			return context.Cause(p.ctx)
		}
	}
	return context.Cause(p.ctx)
}

func (p *localRestorePins) close() error {
	p.cancel(nil)
	p.wg.Wait()
	if p.pin == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.pin.pin.Close(ctx)
}

func validateLocalGCConfig(config *types.ExecutionConfig) error {
	if config.CheckpointRetainCount < 0 {
		return fmt.Errorf("checkpoint retain count must not be negative")
	}
	if !config.Local {
		return nil
	}
	if config.CheckpointInterval < 0 || config.CheckpointTailBytes < 0 {
		return fmt.Errorf("Local checkpoint interval and tail bytes must not be negative")
	}
	if config.ObjStoreGCInterval < 0 || config.ObjStoreGCGracePeriod < 0 {
		return fmt.Errorf("object-store GC durations must not be negative")
	}
	if config.CheckpointRetainCount == 0 {
		config.CheckpointRetainCount = 3
	}
	if config.ObjStoreGCInterval == 0 {
		config.ObjStoreGCInterval = time.Hour
	}
	if config.ObjStoreGCGracePeriod == 0 {
		config.ObjStoreGCGracePeriod = 24 * time.Hour
	}
	return nil
}
