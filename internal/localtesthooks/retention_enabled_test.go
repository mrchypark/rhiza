//go:build rhiza_local_testhooks

package localtesthooks

import (
	"testing"
	"time"
)

func TestRetentionHooksRequireExplicitOptIn(t *testing.T) {
	t.Cleanup(func() { SetRetentionPaused(false) })
	SetRetentionPaused(true)
	if !RetentionPaused() {
		t.Fatal("retention pause was not applied")
	}
	SetRetentionPaused(false)
	if RetentionPaused() {
		t.Fatal("retention pause was not cleared")
	}
	t.Setenv("RHIZA_TEST_RETENTION_1024", "")
	if RetentionWindow(65536) != 65536 || RetentionInterval(time.Second) != time.Second {
		t.Fatal("unexpected default override")
	}
	t.Setenv("RHIZA_TEST_RETENTION_1024", "1")
	if RetentionWindow(65536) != 1024 || RetentionInterval(time.Second) != time.Millisecond {
		t.Fatal("test override not applied")
	}
}
