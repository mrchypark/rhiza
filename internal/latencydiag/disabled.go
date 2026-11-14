//go:build !rhiza_latency_diag

package latencydiag

import (
	"context"
	"time"
)

const Enabled = false

func Start() time.Time                                               { return time.Time{} }
func Observe(_ string, _ uint64, _ time.Time)                        {}
func ObserveBatch(_ string, _ int, _ error, _ time.Time)             {}
func ObservePeer(_ string, _ uint64, _ string, _ error, _ time.Time) {}
func WithSlot(ctx context.Context, _ uint64) context.Context         { return ctx }
func Slot(_ context.Context) uint64                                  { return 0 }
