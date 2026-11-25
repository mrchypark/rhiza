package network

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestLocalCheckpointFreshBatchWaitsAndRespectsCancellation(t *testing.T) {
	server, _, _ := newLocalLifecycleServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resume, err := server.QuiesceLocalCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { resume() }()
	done := make(chan error, 1)
	go func() {
		_, err := server.proposeBatch(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{93}), time.Second, 0)
		done <- err
	}()
	waitForAdmissionWaiters(t, ctx, server, 1)
	short, stop := context.WithCancel(ctx)
	canceled := make(chan error, 1)
	go func() {
		_, err := server.proposeBatch(short, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{94}), time.Second, 0)
		canceled <- err
	}()
	waitForAdmissionWaiters(t, ctx, server, 2)
	stop()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	resume()
	resume = func() {}
	if err := <-done; err != nil {
		t.Fatalf("resumed batch: %v", err)
	}
}
