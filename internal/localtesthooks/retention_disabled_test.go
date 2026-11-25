//go:build !rhiza_local_testhooks

package localtesthooks

import (
	"testing"
	"time"
)

func TestRetentionHooksCannotAlterProductionDefaults(t *testing.T) {
	SetRetentionPaused(true)
	if RetentionPaused() {
		t.Fatal("production retention worker was paused by test hook")
	}
	t.Setenv("RHIZA_TEST_RETENTION_1024", "1")
	if got := RetentionWindow(65536); got != 65536 {
		t.Fatalf("window = %d", got)
	}
	if got := RetentionInterval(time.Second); got != time.Second {
		t.Fatalf("interval = %v", got)
	}
}
