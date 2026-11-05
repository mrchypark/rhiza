//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
)

func holdReplacementBeforeReady(t *testing.T, ctx context.Context, id types.NodeID) (<-chan struct{}, <-chan struct{}, func(), bool) {
	t.Helper()
	blocked, observed, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var entered, ready, released sync.Once
	release := func() { released.Do(func() { close(resume) }) }
	restore := localtesthooks.Set(func(event string) {
		if event == "node:catchup:after-ready:node="+string(id) {
			ready.Do(func() { close(observed) })
			return
		}
		if event != "node:catchup:before-ready:node="+string(id) {
			return
		}
		entered.Do(func() { close(blocked) })
		select {
		case <-resume:
		case <-ctx.Done():
		}
	})
	t.Cleanup(func() { release(); restore() })
	return blocked, observed, release, true
}
