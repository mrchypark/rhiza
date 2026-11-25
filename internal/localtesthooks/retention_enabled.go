//go:build rhiza_local_testhooks

package localtesthooks

import (
	"os"
	"sync/atomic"
	"time"
)

var retentionPaused atomic.Bool
var retentionCycles atomic.Uint64

func SetRetentionPaused(paused bool) { retentionPaused.Store(paused) }
func RetentionPaused() bool          { return retentionPaused.Load() }
func RecordRetentionCycle()          { retentionCycles.Add(1) }
func RetentionCycles() uint64        { return retentionCycles.Load() }

// Explicit test-only opt-in; all processes in the fixture must agree.
func RetentionWindow(window uint64) uint64 {
	if os.Getenv("RHIZA_TEST_RETENTION_1024") == "1" {
		return 1024
	}
	return window
}

func RetentionInterval(interval time.Duration) time.Duration {
	if os.Getenv("RHIZA_TEST_RETENTION_1024") == "1" {
		return time.Millisecond
	}
	return interval
}
