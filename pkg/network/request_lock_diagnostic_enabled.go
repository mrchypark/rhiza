//go:build rhiza_latency_diag

package network

import (
	"context"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func requestLockAcquired(ctx context.Context) time.Time {
	return latencydiag.StartTagged(ctx)
}

// releaseInstrumentedRequestLock records the conservative end of the held
// interval immediately before releasing the shard. Work performed after release
// cannot extend the recorded hold.
func releaseInstrumentedRequestLock(ctx context.Context, lock chan struct{}, shard uint64, acquiredAt time.Time) {
	holdEnd := latencydiag.StartTagged(ctx)
	<-lock
	if localtesthooks.Enabled {
		localtesthooks.Hit("network:request-lock:released-before-hold-trace")
	}
	latencydiag.TraceRequestLockHold(ctx, shard, acquiredAt, holdEnd)
}
