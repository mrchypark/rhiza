package foregroundcosttest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunWindowDrainsAdmittedCallsAndObservesErrorBeforeCancel(t *testing.T) {
	const workers = 2
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	var sequence atomic.Uint64
	var mu sync.Mutex
	var observations []Observation
	var errorContexts []error
	resultCh := make(chan struct {
		result WindowResult
		err    error
	}, 1)
	go func() {
		result, err := RunWindow(context.Background(), Options{
			Workers: workers, Keys: 4, Value: []byte("value"), ClientID: 7,
			Sequence: &sequence, Duration: 10 * time.Millisecond, CallTimeout: time.Second, Validate: true,
		}, func(ctx context.Context, _, _ string, _ []byte) error {
			started <- struct{}{}
			select {
			case <-release:
				return errors.New("injected put failure")
			case <-ctx.Done():
				return ctx.Err()
			}
		}, func(context.Context, string) error {
			t.Error("GET should not start after the window cutoff")
			return nil
		}, func(observation Observation) {
			mu.Lock()
			observations = append(observations, observation)
			mu.Unlock()
		}, func(observation Observation) {
			mu.Lock()
			defer mu.Unlock()
			errorContexts = append(errorContexts, observation.Context.Err())
		})
		resultCh <- struct {
			result WindowResult
			err    error
		}{result, err}
	}()
	for range workers {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("PUT did not start")
		}
	}
	time.Sleep(40 * time.Millisecond)
	close(release)
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("RunWindow: %v", got.err)
	}
	if got.result.PendingAtCutoff != workers || got.result.Outstanding != 0 || got.result.Started != workers || got.result.Completed != workers {
		t.Fatalf("window totals=%+v; want all calls drained after cutoff", got.result)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(observations) != workers {
		t.Fatalf("observations=%d, want %d", len(observations), workers)
	}
	for _, observation := range observations {
		if observation.Operation != Put || observation.Err == nil || observation.InWindow {
			t.Fatalf("drain observation=%+v", observation)
		}
	}
	if len(errorContexts) != workers {
		t.Fatalf("error contexts=%d, want %d", len(errorContexts), workers)
	}
	for _, err := range errorContexts {
		if err != nil {
			t.Fatalf("per-call context canceled before error observation: %v", err)
		}
	}
}
