//go:build rhiza_local_testhooks

package network

import (
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func installWarmRestartReadTipTrace(t *testing.T, trace *warmRestartReadTipTrace) {
	t.Helper()
	restore := localtesthooks.Set(trace.capture)
	t.Cleanup(restore)
}

func warmRestartReadTipTraceEnabled() bool { return true }
