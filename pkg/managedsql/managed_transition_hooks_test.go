//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/node"
)

func TestMembershipTransitionWaitDoesNotHoldHostMutex(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	restore := localtesthooks.Set(func(event string) {
		if event == "managedsql:membership-transition:before-node-wait" {
			close(entered)
			<-release
		}
	})
	defer restore()
	h := &Host{node: &node.Node{}}
	done := make(chan error, 1)
	go func() { done <- h.waitForMembershipTransition(context.Background()) }()
	<-entered
	if !h.mu.TryLock() {
		t.Fatal("membership wait retained Host.mu")
	}
	h.mu.Unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
