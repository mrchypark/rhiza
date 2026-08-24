package checkpoint

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

// AutoCheckpointer automatically creates checkpoints.
type AutoCheckpointer struct {
	manager  *Manager
	material *materializer.Materializer
	interval int64 // checkpoint every N slots
	duration time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewAutoCheckpointer creates a new auto checkpointer.
func NewAutoCheckpointer(manager *Manager, material *materializer.Materializer, interval int64, duration time.Duration) *AutoCheckpointer {
	return &AutoCheckpointer{
		manager:  manager,
		material: material,
		interval: interval,
		duration: duration,
		stopCh:   make(chan struct{}),
	}
}

// Start starts the automatic checkpoint loop.
func (a *AutoCheckpointer) Start(ctx context.Context, tipFunc func() uint64, before func(context.Context) error) {
	if a.duration <= 0 || a.interval <= 0 {
		return
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(a.duration)
		defer ticker.Stop()

		var lastCheckpoint uint64

		for {
			select {
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			case <-ticker.C:
				tip := tipFunc()
				if tip == 0 {
					continue
				}

				// Check if we've processed enough new slots
				if tip-lastCheckpoint < uint64(a.interval) {
					continue
				}
				if before != nil {
					if err := before(ctx); err != nil {
						log.Printf("failed to sync WAL before checkpoint: %v", err)
						continue
					}
				}

				log.Printf("creating automatic checkpoint at slot %d", tip)
				snapshot, err := a.material.Snapshot(ctx)
				if err == nil {
					err = a.manager.Create(ctx, snapshot, tip)
				}
				if err != nil {
					log.Printf("failed to create checkpoint: %v", err)
					continue
				}

				lastCheckpoint = tip

				// Cleanup old checkpoints
				if err := a.manager.Cleanup(ctx, 3); err != nil {
					log.Printf("failed to cleanup checkpoints: %v", err)
				}
			}
		}
	}()
}

// Stop stops the automatic checkpoint loop.
func (a *AutoCheckpointer) Stop() {
	a.stopOnce.Do(func() { close(a.stopCh) })
	a.wg.Wait()
}

// CheckpointOnShutdown creates a checkpoint during shutdown.
func (a *AutoCheckpointer) CheckpointOnShutdown(ctx context.Context, tip uint64) error {
	if tip == 0 {
		return nil
	}

	log.Printf("creating shutdown checkpoint at slot %d", tip)
	snapshot, err := a.material.Snapshot(ctx)
	if err != nil {
		return err
	}
	return a.manager.Create(ctx, snapshot, tip)
}

// NewManagerFromEnv creates a checkpoint manager from environment.
func NewManagerFromEnv(bucket objstore.Bucket, localDir string) *Manager {
	prefix := os.Getenv("RHIZA_OBJSTORE_PREFIX")
	return NewManager(bucket, prefix, localDir)
}
