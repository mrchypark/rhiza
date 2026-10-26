//go:build !rhiza_local_testhooks

package network

import "testing"

func installWarmRestartReadTipTrace(t *testing.T, _ *warmRestartReadTipTrace) {
	t.Helper()
}

func warmRestartReadTipTraceEnabled() bool { return false }
