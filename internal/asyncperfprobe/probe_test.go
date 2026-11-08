package asyncperfprobe

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

func TestSlowStageLogsOnlyMeasuredSlowIntervals(t *testing.T) {
	var output bytes.Buffer
	original := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(original) })

	start := time.Unix(1, 0)
	Slow("disabled", time.Time{}, start)
	Slow("fast", start, start.Add(2*time.Millisecond-time.Nanosecond))
	Slow("wal_fsync", start, start.Add(2*time.Millisecond))

	want := "ASYNC_STAGE stage=wal_fsync end_ns=1002000000 duration_us=2000"
	if got := strings.TrimSpace(output.String()); strings.Count(got, "ASYNC_STAGE") != 1 || !strings.HasSuffix(got, want) {
		t.Fatalf("expected exactly one slow event ending in %q, got %q", want, got)
	}
}
