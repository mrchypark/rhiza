//go:build rhiza_local_testhooks

package network

import (
	"context"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestRetentionControlWaitsForAcceptedProposalAfterCancellation(t *testing.T) {
	server, _, _ := newLocalLifecycleServer(t)
	// Exercise the HA work-context path with an actual admitted proposal.
	server.localMode = false
	entered, release := make(chan struct{}), make(chan struct{})
	restore := localtesthooks.Set(func(name string) {
		if name == "network:retention-control:accepted" {
			close(entered)
			<-release
		}
	})
	defer restore()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = server.ProposeRetentionControl(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{}))
	}()
	<-entered
	cancel()
	select {
	case <-done:
		t.Fatal("returned while the accepted proposal was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not finish after release")
	}
}
