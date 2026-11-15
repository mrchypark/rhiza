//go:build rhiza_local_testhooks

package materializer

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestTryTipDuringActualRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(ctx, 1, policySQL(t, "CREATE TABLE probe(id INTEGER PRIMARY KEY)")); err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil || index != 1 {
		t.Fatalf("checkpoint index=%d error=%v", index, err)
	}
	defer cleanup()
	if err := m.Apply(ctx, 2, policySQL(t, "INSERT INTO probe VALUES(1)")); err != nil {
		t.Fatal(err)
	}
	if tip, ok := m.TryTip(); !ok || tip != 2 {
		t.Fatalf("initial tip=%d known=%t", tip, ok)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	restore := localtesthooks.Set(func(event string) {
		if event == "materializer:restore:locked" {
			enterOnce.Do(func() { close(entered) })
			<-release
		}
	})
	defer restore()
	done := make(chan error, 1)
	go func() { done <- m.RestoreCheckpoint(ctx, files) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("restore exited before gate: %v", err)
	case <-ctx.Done():
		t.Fatal("restore gate not reached")
	}
	if tip, ok := m.TryTip(); ok || tip != 0 {
		t.Fatalf("restore lock must report unknown: tip=%d known=%t", tip, ok)
	}
	releaseGate()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("restore failed to exit")
	}
	if tip, ok := m.TryTip(); !ok || tip != 1 {
		t.Fatalf("restored tip=%d known=%t", tip, ok)
	}
}
