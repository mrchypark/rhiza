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
	eligible func() bool
	publish  func(context.Context, *Checkpoint) error
}

func (a *AutoCheckpointer) ConfigurePublication(eligible func() bool, publish func(context.Context, *Checkpoint) error) {
	a.eligible, a.publish = eligible, publish
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
		if latest := a.manager.Latest(); latest != nil {
			lastCheckpoint = latest.Index
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			case <-ticker.C:
				if a.eligible != nil && !a.eligible() {
					continue
				}
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

				appliedTip, err := a.create(ctx)
				if err != nil {
					log.Printf("failed to create checkpoint: %v", err)
					continue
				}

				lastCheckpoint = appliedTip

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
	if latest := a.manager.Latest(); latest != nil && latest.Index >= tip {
		return nil
	}

	appliedTip, err := a.create(ctx)
	if err != nil {
		return err
	}
	if appliedTip == 0 {
		return nil
	}
	return nil
}

func (a *AutoCheckpointer) create(ctx context.Context) (uint64, error) {
	if latest := a.manager.Latest(); latest != nil && latest.Index >= a.material.Tip() {
		return latest.Index, nil
	}
	path, appliedTip, cleanup, err := a.material.SnapshotFileAt(ctx)
	if err != nil {
		return 0, err
	}
	defer cleanup()
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	log.Printf("creating streaming checkpoint at slot %d", appliedTip)
	root, err := a.manager.CreateReader(ctx, file, appliedTip)
	if err == nil && a.publish != nil {
		err = a.publish(ctx, root)
	}
	return appliedTip, err
}

// NewManagerFromEnv creates a checkpoint manager from environment.
func NewManagerFromEnv(bucket objstore.Bucket, localDir string) *Manager {
	prefix := os.Getenv("RHIZA_OBJSTORE_PREFIX")
	return NewManager(bucket, prefix, localDir)
}
