package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

func TestRequestLockSameAndDifferentShardBehavior(t *testing.T) {
	const (
		firstID     = "bench-5242"
		sameShardID = "bench-5247"
		differentID = "bench-5243"
	)
	if testRequestShard(firstID) != testRequestShard(sameShardID) {
		t.Fatal("fixture IDs must share a shard")
	}
	if testRequestShard(firstID) == testRequestShard(differentID) {
		t.Fatal("fixture IDs must use different shards")
	}
	server := newRequestLockTestServer()

	unlockFirst, err := server.lockRequest(context.Background(), firstID)
	if err != nil {
		t.Fatal(err)
	}
	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 25*time.Millisecond)
	_, err = server.lockRequest(blockedCtx, sameShardID)
	cancelBlocked()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-shard lock error=%v, want deadline exceeded", err)
	}
	unlockFirst()

	unlockDifferent, err := server.lockRequest(context.Background(), differentID)
	if err != nil {
		t.Fatalf("different-shard lock error=%v", err)
	}
	unlockDifferent()
}

func TestRequestLockCancellationAndServerErrorBehavior(t *testing.T) {
	const (
		firstID  = "bench-5242"
		secondID = "bench-5247"
		otherID  = "bench-5243"
	)
	server := newRequestLockTestServer()
	unlockFirst, err := server.lockRequest(context.Background(), firstID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, lockErr := server.lockRequest(ctx, secondID)
		result <- lockErr
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lock error=%v", err)
	}
	unlockFirst()

	proposalCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	server.proposalCtx = proposalCtx
	cancelServer()
	if _, err := server.lockRequest(context.Background(), otherID); !errors.Is(err, ErrNotReady) {
		t.Fatalf("server cancellation error=%v, want ErrNotReady", err)
	}
}

func newRequestLockTestServer() *Server {
	server := &Server{proposalCtx: context.Background()}
	for i := range server.requestLocks {
		server.requestLocks[i] = make(chan struct{}, 1)
	}
	return server
}

func testRequestShard(id string) uint16 {
	hash := sha256.Sum256([]byte(id))
	return uint16(hash[0])<<4 | uint16(hash[1])>>4
}
