//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
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
			configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
			var closing atomic.Bool
			ownedPrefix := "/bucket/" + fixture.source.finalPrefix + "/"
			fixture.source.putReadFailure = func(r *http.Request, err error) {
				if !initialRootExpectedCanceledPUT(ownedPrefix, r.URL.Path, err, errors.Is(r.Context().Err(), context.Canceled), closing.Load()) {
					t.Errorf("unexpected PUT body failure path=%s: %v", r.URL.Path, err)
				}
			}
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
			closing.Store(true)
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

func initialRootExpectedCanceledPUT(prefix, requestPath string, err error, canceled, closing bool) bool {
	return closing && canceled && errors.Is(err, io.ErrUnexpectedEOF) && strings.HasPrefix(requestPath, prefix) && len(requestPath) > len(prefix)
}

func TestManagedGraphInitialRootPUTReadFailureScope(t *testing.T) {
	for _, tc := range []struct {
		name, path        string
		err               error
		canceled, closing bool
		want              bool
	}{
		{"owned-close-canceled", "/bucket/owned/archive/head.bin", io.ErrUnexpectedEOF, true, true, true},
		{"before-close", "/bucket/owned/archive/head.bin", io.ErrUnexpectedEOF, true, false, false},
		{"live-request", "/bucket/owned/archive/head.bin", io.ErrUnexpectedEOF, false, true, false},
		{"different-namespace", "/bucket/other/archive/head.bin", io.ErrUnexpectedEOF, true, true, false},
		{"prefix-only", "/bucket/owned/", io.ErrUnexpectedEOF, true, true, false},
		{"other-io-failure", "/bucket/owned/archive/head.bin", io.ErrClosedPipe, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := initialRootExpectedCanceledPUT("/bucket/owned/", tc.path, tc.err, tc.canceled, tc.closing); got != tc.want {
				t.Fatalf("unexpected cancellation classification: got=%v want=%v", got, tc.want)
			}
		})
	}
	observed := make(chan bool, 1)
	server, control := sqlWireStore(t)
	control.putReadFailure = func(r *http.Request, err error) {
		observed <- errors.Is(err, io.ErrUnexpectedEOF) && !initialRootExpectedCanceledPUT("/bucket/owned/", r.URL.Path, err, errors.Is(r.Context().Err(), context.Canceled), false)
	}
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.WriteString(conn, "PUT /bucket/owned/archive/head.bin HTTP/1.1\r\nHost: fixture\r\nContent-Length: 10\r\n\r\nx")
	if err := errors.Join(writeErr, conn.Close()); err != nil {
		t.Fatal(err)
	}
	select {
	case rejected := <-observed:
		if !rejected {
			t.Fatal("actual pre-Close truncated PUT was not rejected")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("actual truncated PUT did not reach body-read observer")
	}
}

func TestManagedGraphInitialRootBlocksApplicationAndHonorsCancellation(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
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
