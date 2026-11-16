//go:build !rhiza_latency_diag

package network

import (
	"context"
	"time"
)

func requestLockAcquired(context.Context) time.Time { return time.Time{} }

func releaseInstrumentedRequestLock(_ context.Context, lock chan struct{}, _ uint64, _ time.Time) {
	<-lock
}
