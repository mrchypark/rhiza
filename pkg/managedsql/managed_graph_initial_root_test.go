//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedGraphInitialRootCloseCancelsBackgroundWait(t *testing.T) {
	for _, operation := range []string{"execute", "query"} {
		t.Run(operation, func(t *testing.T) {
			configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestApplicationProtocol)
			rootRelease := make(chan struct{})
			var releaseOnce sync.Once
			releaseRoot := func() { releaseOnce.Do(func() { close(rootRelease) }) }
			fixture.source.putReady = make(chan string, 16)
			fixture.source.putRelease = rootRelease
			fixture.source.putPrefix = "/checkpoint/roots/"
			fixture.source.putSuffix = ".json"
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			t.Cleanup(cancel)
			hosts := make([]*Host, len(configs))
			for i, cfg := range configs {
				h, err := fixture.open(ctx, cfg)
				if h != nil {
					t.Cleanup(func() { _ = h.Close() })
				}
				if err != nil {
					releaseRoot()
					t.Fatal(err)
				}
				hosts[i] = h
			}
			t.Cleanup(releaseRoot)
			var proposals atomic.Int32
			restore := localtesthooks.SetProposalEntry(func(value []byte) {
				_, graph, err := types.DecodeManagedGraphExecution(value)
				if err == nil && graph {
					proposals.Add(1)
				}
			})
			t.Cleanup(restore)
			result := make(chan error, 1)
			go func() {
				if operation == "execute" {
					_, err := hosts[0].GraphExecute(context.Background(), types.GraphCommand{RequestID: "root-close", Cypher: "CREATE (:RootClose {n:1})"})
					result <- err
				} else {
					_, err := hosts[0].GraphQuery(context.Background(), network.GraphQueryRequest{Cypher: "RETURN 1", Consistency: "local"})
					result <- err
				}
			}()
			select {
			case <-fixture.source.putReady:
			case err := <-result:
				t.Fatalf("operation ended before real root gate: %v", err)
			case <-ctx.Done():
				t.Fatal("root gate not reached", ctx.Err())
			}
			closed := make(chan error, 1)
			go func() { closed <- hosts[0].Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal("Close failed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close cannot cancel and drain initial-root wait while upload remains blocked")
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("closed initial-root operation succeeded")
				}
			case <-ctx.Done():
				t.Fatal("operation not drained after Close", ctx.Err())
			}
			if proposals.Load() != 0 {
				t.Fatal("application proposed while initial root was blocked")
			}
			// The wire gate is still closed here. Release only during cleanup,
			// after proving that Node lifetime cancellation drained the operation.
		})
	}
}

func TestManagedGraphInitialRootBlocksApplicationAndHonorsCancellation(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestApplicationProtocol)
	rootRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseRoot := func() { releaseOnce.Do(func() { close(rootRelease) }) }
	fixture.source.putReady = make(chan string, 16)
	fixture.source.putRelease = rootRelease
	fixture.source.putPrefix = "/checkpoint/roots/"
	fixture.source.putSuffix = ".json"
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		h, err := fixture.open(ctx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			releaseRoot()
			t.Fatalf("sequential public Open %d: %v", i, err)
		}
		hosts[i] = h
	}
	// Release before the Host cleanup callbacks, including on assertion failure.
	t.Cleanup(releaseRoot)
	var graphProposals atomic.Int32
	restore := localtesthooks.SetProposalEntry(func(value []byte) {
		_, graph, err := types.DecodeManagedGraphExecution(value)
		if err == nil && graph {
			graphProposals.Add(1)
		}
	})
	t.Cleanup(restore)
	requestCtx, stopRequest := context.WithCancel(ctx)
	t.Cleanup(stopRequest)
	finished := make(chan error, 1)
	go func() {
		_, err := hosts[0].GraphExecute(requestCtx, types.GraphCommand{RequestID: "initial-root-cancelled", Cypher: "CREATE (:InitialRoot {n:1})"})
		finished <- err
	}()
	select {
	case <-fixture.source.putReady:
	case err := <-finished:
		t.Fatalf("first operation ended before actual root-upload boundary: %v", err)
	case <-ctx.Done():
		t.Fatal("actual root-upload boundary not reached", ctx.Err())
	}
	select {
	case err := <-finished:
		t.Fatalf("application completed before verified producer root: %v", err)
	default:
	}
	if graphProposals.Load() != 0 {
		t.Fatal("Graph application proposed before initial root completion")
	}
	stopRequest()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("initial-root wait lost caller cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("cancelled root waiter did not return", ctx.Err())
	}
	if graphProposals.Load() != 0 {
		t.Fatal("cancelled initial-root request proposed application data")
	}
	releaseRoot()
	// A different original is a new request, never a resubmission of the
	// cancelled request. It drives the same real initial-root completion.
	result, err := hosts[0].GraphExecute(ctx, types.GraphCommand{RequestID: "initial-root-after-release", Cypher: "CREATE (:InitialRoot {n:2})"})
	if err != nil || result.Slot == 0 || result.Status != types.MutationCommitted || graphProposals.Load() != 1 {
		t.Fatalf("verified-root application result: slot=%d status=%s proposals=%d err=%v", result.Slot, result.Status, graphProposals.Load(), err)
	}
}
