package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

const publisherLease = 2 * time.Minute

// AutoCheckpointer automatically creates checkpoints.
type AutoCheckpointer struct {
	manager    *Manager
	material   *materializer.Materializer
	interval   int64 // checkpoint every N slots
	duration   time.Duration
	stopCh     chan struct{}
	stopOnce   sync.Once
	wg         sync.WaitGroup
	eligible   func() bool
	publish    func(context.Context, *Checkpoint) error
	tailBytes  func() int64
	tailBudget int64
	candidate  *Checkpoint
	owner      string
	claimFloor func() uint64
	advance    func(context.Context, uint64) error
}

func (a *AutoCheckpointer) ConfigureTail(bytes func() int64, budget int64) {
	a.tailBytes, a.tailBudget = bytes, budget
}

func (a *AutoCheckpointer) ConfigurePublication(eligible func() bool, publish func(context.Context, *Checkpoint) error) {
	a.eligible, a.publish = eligible, publish
}

func (a *AutoCheckpointer) ConfigurePublisher(owner string, floor func() uint64, advance func(context.Context, uint64) error) {
	a.owner, a.claimFloor, a.advance = owner, floor, advance
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
		period := a.duration
		if period > time.Minute {
			period = time.Minute
		}
		ticker := time.NewTicker(period)
		defer ticker.Stop()

		var lastCheckpoint uint64
		if latest := a.manager.Latest(); latest != nil {
			lastCheckpoint = latest.Index
		}
		lastCheckpointAt := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case <-a.stopCh:
				return
			case <-ticker.C:
				timeDue := time.Since(lastCheckpointAt) >= a.duration
				tailDue := a.tailBytes != nil && a.tailBudget > 0 && a.tailBytes() >= a.tailBudget
				if !timeDue && !tailDue {
					continue
				}
				if a.eligible != nil && !a.eligible() {
					continue
				}
				tip := tipFunc()
				if tip == 0 {
					continue
				}

				// Check if we've processed enough new slots
				if tip <= lastCheckpoint || tip-lastCheckpoint < uint64(a.interval) {
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
				lastCheckpointAt = time.Now()

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
	tip := a.material.Tip()
	if latest := a.manager.Latest(); latest != nil && latest.Index >= tip {
		return latest.Index, nil
	}
	if a.candidate != nil {
		if a.candidate.claim != nil {
			claim, err := a.keepPublisherClaim(ctx, a.candidate.claim, func(workCtx context.Context) error {
				if a.publish == nil {
					return fmt.Errorf("checkpoint certification callback is required")
				}
				return a.publish(workCtx, a.candidate)
			})
			if errors.Is(err, ErrPublisherFenced) {
				a.candidate = nil
				return a.create(ctx)
			}
			if err != nil {
				if errors.Is(err, ErrStaleCheckpoint) {
					a.candidate = nil
					if latest := a.manager.Latest(); latest != nil {
						return latest.Index, nil
					}
				}
				return 0, err
			}
			a.candidate.claim = claim
		} else if a.publish == nil {
			return 0, fmt.Errorf("checkpoint certification callback is required")
		} else if err := a.publish(ctx, a.candidate); err != nil {
			if errors.Is(err, ErrStaleCheckpoint) {
				a.candidate = nil
				if latest := a.manager.Latest(); latest != nil {
					return latest.Index, nil
				}
			}
			return 0, err
		}
		if latest := a.manager.Latest(); latest == nil || latest.Index < a.candidate.Index || latest.RootHash != a.candidate.RootHash {
			return 0, fmt.Errorf("checkpoint candidate was not promoted after certification")
		}
		index := a.candidate.Index
		if a.candidate.claim != nil {
			if err := a.manager.ReleasePublisherClaim(ctx, a.candidate.claim); err != nil && !errors.Is(err, ErrPublisherFenced) {
				return 0, err
			}
		}
		a.candidate = nil
		return index, nil
	}
	var claim *PublisherClaim
	releaseClaim := false
	defer func() {
		if releaseClaim {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = a.manager.ReleasePublisherClaim(releaseCtx, claim)
			cancel()
		}
	}()
	if a.owner != "" {
		floor := uint64(0)
		if a.claimFloor != nil {
			floor = a.claimFloor()
		}
		root, err := a.manager.AcquirePublisherClaim(ctx, a.owner, floor, publisherLease)
		if err != nil {
			return 0, err
		}
		claim = root
		releaseClaim = true
	}
	var root *Checkpoint
	var appliedTip uint64
	var err error
	work := func(workCtx context.Context) error {
		if claim != nil && a.advance != nil {
			if err := a.advance(workCtx, claim.ReservedIndex); err != nil {
				return err
			}
		}
		files, index, cleanup, err := a.material.CheckpointFilesAt(workCtx)
		if err != nil {
			return err
		}
		defer cleanup()
		sources := make([]Source, 0, len(files))
		for _, file := range files {
			sources = append(sources, Source{Role: string(file.Role), Path: file.Path})
		}
		appliedTip = index
		log.Printf("creating streaming checkpoint at slot %d", appliedTip)
		root, err = a.manager.CreateFiles(workCtx, claim, sources, appliedTip)
		return err
	}
	if claim != nil {
		claim, err = a.keepPublisherClaim(ctx, claim, work)
	} else {
		err = work(ctx)
	}
	if err != nil {
		return appliedTip, err
	}
	if claim != nil {
		if appliedTip < claim.ReservedIndex {
			return appliedTip, fmt.Errorf("checkpoint index %d is below reserved index %d", appliedTip, claim.ReservedIndex)
		}
		claim, err = a.manager.BindPublisherClaim(ctx, claim, appliedTip, root.RootHash, publisherLease)
		if err != nil {
			return appliedTip, err
		}
		root.claim = claim
		releaseClaim = false
	}
	a.candidate = root
	return a.create(ctx)
}

func (a *AutoCheckpointer) keepPublisherClaim(ctx context.Context, initial *PublisherClaim, work func(context.Context) error) (*PublisherClaim, error) {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	current := initial
	renewErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(publisherLease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				next, err := a.manager.RenewPublisherClaim(workCtx, current, publisherLease)
				if err == nil {
					current = next
				}
				mu.Unlock()
				if err != nil {
					select {
					case renewErr <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	err := work(workCtx)
	cancel()
	<-done
	mu.Lock()
	claim := current
	mu.Unlock()
	select {
	case renew := <-renewErr:
		if err == nil || errors.Is(err, context.Canceled) {
			err = renew
		}
	default:
	}
	return claim, err
}

// NewManagerFromEnv creates a checkpoint manager from environment.
func NewManagerFromEnv(bucket objstore.Bucket, localDir string) *Manager {
	prefix := os.Getenv("RHIZA_OBJSTORE_PREFIX")
	return NewManager(bucket, prefix, localDir)
}
