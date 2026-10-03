// Package foregroundcosttest contains the test-only request sampler shared by
// the network and node foreground cost tests.
package foregroundcosttest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Gate admits calls only before a fixed window deadline and accounts for
// admitted calls through the bounded drain.
type Gate struct {
	mu              sync.Mutex
	deadline        time.Time
	closed          bool
	active          int
	pendingAtCutoff int
}

func NewGate(deadline time.Time) *Gate { return &Gate{deadline: deadline} }

func (g *Gate) BeginAt(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.beginLocked(now)
}

func (g *Gate) begin() (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	started := time.Now()
	return started, g.beginLocked(started)
}

func (g *Gate) beginLocked(now time.Time) bool {
	if g.closed || !now.Before(g.deadline) {
		if !g.closed {
			g.closed = true
			g.pendingAtCutoff = g.active
		}
		return false
	}
	g.active++
	return true
}

func (g *Gate) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		g.pendingAtCutoff = g.active
	}
}

func (g *Gate) Finish() {
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
}

func (g *Gate) Snapshot() (pendingAtCutoff, outstanding int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pendingAtCutoff, g.active
}

// Call runs one admitted operation with an independent maximum duration.
func Call(parent context.Context, gate *Gate, maxDuration time.Duration, call func(context.Context) error, onError func(context.Context, time.Time, time.Time, error)) (started, completed time.Time, err error, admitted bool) {
	if parent.Err() != nil {
		return time.Time{}, time.Time{}, nil, false
	}
	if started, admitted = gate.begin(); !admitted {
		return time.Time{}, time.Time{}, nil, false
	}
	defer gate.Finish()
	callCtx, cancel := context.WithTimeout(parent, maxDuration)
	defer cancel()
	err = call(callCtx)
	completed = time.Now()
	if err != nil && onError != nil {
		onError(callCtx, started, completed, err)
	}
	return started, completed, err, true
}

type Operation string

const (
	Put Operation = "put"
	Get Operation = "get"
)

// Observation describes one completed API operation. Context is the original
// per-call context, retained so existing diagnostics can inspect its deadline
// and cancellation state synchronously in Observe.
type Observation struct {
	Operation Operation
	Context   context.Context
	RequestID string
	Key       string
	Cutoff    time.Time
	Started   time.Time
	Completed time.Time
	Err       error
	Overlap   bool
	InWindow  bool
}

type Options struct {
	Workers, Keys int
	Value         []byte
	ClientID      int64
	Sequence      *atomic.Uint64
	Duration      time.Duration
	CallTimeout   time.Duration
	Validate      bool
	Maintenance   func() (active bool, epoch uint64)
}

type WindowResult struct {
	PendingAtCutoff int
	Outstanding     int
	Started         int
	Completed       int
}

type PutFunc func(context.Context, string, string, []byte) error
type GetFunc func(context.Context, string) error
type ObserveFunc func(Observation)

type SeedOptions struct {
	Workers, Keys, ProgressEvery int
	Value                        []byte
	ClientID                     int64
}

// Seed loads the deterministic key set with bounded concurrency. Callers own
// request serialization and any outer test-runner deadline accounting.
func Seed(ctx context.Context, options SeedOptions, put PutFunc, progress func(int)) error {
	if options.Workers <= 0 || options.Keys <= 0 || put == nil {
		return fmt.Errorf("invalid foreground cost seed options")
	}
	seedCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var workers sync.WaitGroup
	var completed atomic.Uint64
	var errMu sync.Mutex
	var seedErr error
	workers.Add(options.Workers)
	for range options.Workers {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-seedCtx.Done():
					return
				case index, ok := <-jobs:
					if !ok {
						return
					}
					requestID := fmt.Sprintf("foreground-seed-%d-%d", options.ClientID, index)
					key := fmt.Sprintf("key-%04d", index)
					if err := put(seedCtx, requestID, key, options.Value); err != nil {
						errMu.Lock()
						seedErr = errors.Join(seedErr, fmt.Errorf("seed key %d: %w", index, err))
						errMu.Unlock()
						cancel()
						return
					}
					count := int(completed.Add(1))
					if progress != nil && options.ProgressEvery > 0 && (count%options.ProgressEvery == 0 || count == options.Keys) {
						progress(count)
					}
				}
			}
		}()
	}
dispatch:
	for index := range options.Keys {
		select {
		case jobs <- index:
		case <-seedCtx.Done():
			break dispatch
		}
	}
	close(jobs)
	workers.Wait()
	errMu.Lock()
	defer errMu.Unlock()
	return seedErr
}

// RunWindow shares only the fixed-window admission, API-call budget, worker,
// overlap, and drain accounting. API behavior and evidence remain caller-owned.
func RunWindow(parent context.Context, options Options, put PutFunc, get GetFunc, observe ObserveFunc, observePutError ObserveFunc) (WindowResult, error) {
	if options.Workers <= 0 || options.Keys <= 0 || options.Duration <= 0 || options.CallTimeout <= 0 {
		return WindowResult{}, fmt.Errorf("invalid foreground cost window options")
	}
	if put == nil || get == nil {
		return WindowResult{}, fmt.Errorf("foreground cost window requires put and get callbacks")
	}
	sequence := options.Sequence
	if sequence == nil {
		sequence = &atomic.Uint64{}
	}
	maintenance := options.Maintenance
	if maintenance == nil {
		maintenance = func() (bool, uint64) { return false, 0 }
	}
	window, cancel := context.WithTimeout(parent, options.Duration)
	defer cancel()
	deadline, _ := window.Deadline()
	gate := NewGate(deadline)
	var workers sync.WaitGroup
	var started, completed, observed atomic.Int64
	for worker := range options.Workers {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			index := worker
			for {
				key := fmt.Sprintf("key-%04d", index%options.Keys)
				id := sequence.Add(1)
				requestID := fmt.Sprintf("foreground-%d-%d", options.ClientID, id)
				startedActive, startedEpoch := maintenance()
				callStarted, callCompleted, callErr, admitted := Call(parent, gate, options.CallTimeout, func(callCtx context.Context) error {
					return put(callCtx, requestID, key, options.Value)
				}, func(ctx context.Context, callStart, callEnd time.Time, err error) {
					if observePutError != nil {
						observePutError(Observation{Operation: Put, Context: ctx, RequestID: requestID, Key: key, Cutoff: deadline, Started: callStart, Completed: callEnd, Err: err, InWindow: !callEnd.After(deadline)})
					}
				})
				if !admitted {
					return
				}
				started.Add(1)
				completed.Add(1)
				active, epoch := maintenance()
				if observe != nil {
					observe(Observation{Operation: Put, RequestID: requestID, Key: key, Cutoff: deadline, Started: callStarted, Completed: callCompleted, Err: callErr, Overlap: startedActive || active || startedEpoch != epoch, InWindow: !callCompleted.After(deadline)})
					observed.Add(1)
				}
				startedActive, startedEpoch = maintenance()
				callStarted, callCompleted, callErr, admitted = Call(parent, gate, options.CallTimeout, func(callCtx context.Context) error {
					return get(callCtx, key)
				}, nil)
				if !admitted {
					return
				}
				started.Add(1)
				completed.Add(1)
				active, epoch = maintenance()
				if observe != nil {
					observe(Observation{Operation: Get, Key: key, Cutoff: deadline, Started: callStarted, Completed: callCompleted, Err: callErr, Overlap: startedActive || active || startedEpoch != epoch, InWindow: !callCompleted.After(deadline)})
					observed.Add(1)
				}
				index = (index + options.Workers) % options.Keys
			}
		}(worker)
	}
	<-window.Done()
	gate.Close()
	workers.Wait()
	pending, outstanding := gate.Snapshot()
	result := WindowResult{PendingAtCutoff: pending, Outstanding: outstanding, Started: int(started.Load()), Completed: int(completed.Load())}
	if err := parent.Err(); err != nil {
		return result, err
	}
	if options.Validate && (result.Started != result.Completed || result.Started != int(observed.Load()) || outstanding != 0) {
		return result, fmt.Errorf("foreground API window accounting mismatch: started=%d completed=%d outstanding=%d", result.Started, result.Completed, outstanding)
	}
	return result, nil
}
