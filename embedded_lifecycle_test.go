package rhiza_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestEmbeddedLifecycleCancelsInflightCallBeforeCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	config := rhiza.Config{
		NodeID:             "lifecycle",
		DataDir:            dir,
		MaxConcurrentReads: 1,
		MaxLongPollReads:   1,
	}
	db, err := rhiza.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Execute(context.Background(), rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE lifecycle (value TEXT)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(context.Background(), rhiza.ExecuteRequest{RequestID: "value", SQL: "INSERT INTO lifecycle VALUES ('kept')"}); err != nil {
		t.Fatal(err)
	}

	callCtx, cancelCall := context.WithCancel(context.Background())
	t.Cleanup(cancelCall)
	callDone := make(chan error, 1)
	go func() {
		for {
			_, err := db.GraphStreamRead(callCtx, rhiza.GraphStreamReadRequest{Stream: "lifecycle", WaitMS: 30_000})
			if !errors.Is(err, network.ErrOverloaded) {
				callDone <- err
				return
			}
			runtime.Gosched()
		}
	}()

	deadline := time.Now().Add(time.Second)
	for {
		_, err := db.GraphStreamRead(context.Background(), rhiza.GraphStreamReadRequest{Stream: "lifecycle", WaitMS: 1})
		if errors.Is(err, network.ErrOverloaded) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("long-poll call was not admitted: %v", err)
		}
	}
	cancelCall()
	select {
	case err := <-callDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("inflight call error=%v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("inflight call did not stop after cancellation")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("repeat close: %v", err)
	}

	reopened, err := rhiza.Open(context.Background(), config)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	defer reopened.Close()
	if !reopened.Ready() {
		t.Fatal("reopened DB is not ready")
	}
	rows, err := reopened.Query(context.Background(), rhiza.QueryRequest{SQL: "SELECT value FROM lifecycle"})
	if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "kept" {
		t.Fatalf("reopened rows=%#v err=%v", rows.Rows, err)
	}
}
