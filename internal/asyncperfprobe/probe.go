// Package asyncperfprobe records opt-in slow WAL and group-commit stages.
package asyncperfprobe

import (
	"log"
	"os"
	"time"
)

var enabled = os.Getenv("RHIZA_ASYNC_STAGE_PROBE") == "1"

func init() {
	if enabled {
		_, _ = os.Stderr.WriteString("ASYNC_STAGE_PROBE_READY=1\n")
	}
}

// Stamp returns zero when the probe is disabled.
func Stamp() time.Time {
	if enabled {
		return time.Now()
	}
	return time.Time{}
}

// Slow logs the stage only when it takes at least 2ms.
func Slow(stage string, start, end time.Time) {
	if !start.IsZero() && end.Sub(start) >= 2*time.Millisecond {
		log.Printf("ASYNC_STAGE stage=%s end_ns=%d duration_us=%d", stage, end.UnixNano(), end.Sub(start).Microseconds())
	}
}
