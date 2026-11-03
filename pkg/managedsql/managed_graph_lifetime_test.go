//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedGraphLongPollCloseCancelsBorrow(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel() // Cancel an unfinished read before Host cleanup on any failure.
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		h, err := fixture.open(ctx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if _, err := hosts[0].GraphExecute(ctx, types.GraphCommand{RequestID: "longpoll-admit", Cypher: "CREATE (:Item)"}); err != nil {
		t.Fatal(err)
	}
	entered, waiting := make(chan struct{}), make(chan struct{})
	var once, waitOnce sync.Once
	restore := localtesthooks.Set(func(event string) {
		if event == "node:managed-graph:before-stream-longpoll" {
			once.Do(func() { close(entered) })
		}
		if event == "materializer:graph-stream:before-wait" {
			waitOnce.Do(func() { close(waiting) })
		}
	})
	t.Cleanup(restore)
	read := make(chan error, 1)
	go func() {
		_, err := hosts[0].GraphStreamRead(ctx, network.GraphStreamReadRequest{Stream: "empty-stream", WaitMS: 30000, Consistency: "local"})
		read <- err
	}()
	select {
	case <-entered:
	case err := <-read:
		t.Fatalf("read ended before actual long-poll boundary: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// This is the actual empty ReadStream + captured wake + unlocked native
	// state + timer/blocking-select boundary, not the Node call-site hook.
	select {
	case <-waiting:
	case err := <-read:
		t.Fatalf("read ended before backend wait: %v", err)
	case <-ctx.Done():
		t.Fatal("actual backend wait was not observed")
	}
	closed := make(chan error, 1)
	go func() { closed <- hosts[0].Close() }()
	select {
	case err := <-read:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Close did not cancel the borrowed long-poll: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("long-poll was not canceled by Close")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not drain the actual long-poll")
	}
	if _, err := hosts[0].GraphQuery(ctx, network.GraphQueryRequest{Cypher: "RETURN 1"}); err == nil {
		t.Fatal("closed Host accepted Graph query")
	}
}
