//go:build !rhiza_local_testhooks

package localtesthooks

import "time"

func RetentionWindow(window uint64) uint64                   { return window }
func RetentionInterval(interval time.Duration) time.Duration { return interval }
func SetRetentionPaused(bool)                                {}
func RetentionPaused() bool                                  { return false }
func RecordRetentionCycle()                                  {}
func RetentionCycles() uint64                                { return 0 }
