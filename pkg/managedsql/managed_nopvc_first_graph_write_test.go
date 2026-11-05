//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedNoPVCFirstPublicGraphWriteCompletesInitialRoot(t *testing.T) {
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
			t.Fatalf("public Open %d: %v", i, err)
		}
		hosts[i] = h
	}
	t.Cleanup(releaseRoot)

	result := make(chan error, 1)
	go func() {
		ack, err := hosts[0].GraphExecute(ctx, types.GraphCommand{RequestID: "nopvc-first-graph-write", Cypher: "CREATE (:NoPVCFirstWrite {n:1})"})
		if err == nil && ack.Slot == 0 {
			err = context.DeadlineExceeded
		}
		result <- err
	}()
	select {
	case <-fixture.source.putReady:
	case err := <-result:
		t.Fatalf("first public write finished before the actual initial-root upload: %v", err)
	case <-ctx.Done():
		t.Fatal("first public write did not drive initial-root publication", ctx.Err())
	}
	select {
	case err := <-result:
		t.Fatalf("first public write completed before initial-root certification: %v", err)
	default:
	}
	releaseRoot()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("first public write after root release: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("first public write did not finish after initial-root certification", ctx.Err())
	}
}
