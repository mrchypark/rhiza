package quepaxa

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

type managedControlBlockingLogWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *managedControlBlockingLogWriter) Write(value []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(value), nil
}

func TestManagedControlObservationDiagnosticIsFixedAndPreservesError(t *testing.T) {
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	var output bytes.Buffer
	log.SetOutput(&output)
	secret := fmt.Errorf("private proposal value: %w", ErrManagedControlObservationUnavailable)
	got := managedControlObservationRejected("reference_validation", secret)
	if got != secret || got.Error() != secret.Error() || !errors.Is(got, ErrManagedControlObservationUnavailable) {
		t.Fatalf("diagnostic changed original error: got=%v", got)
	}
	line := output.String()
	if !strings.Contains(line, "MANAGED_CONTROL_OBSERVATION_REJECTED reason=reference_validation") || strings.Contains(line, secret.Error()) {
		t.Fatalf("diagnostic was not fixed-label/redacted: %q", line)
	}
}

func TestObserveManagedAppendOnlyControlDiagnosesUnavailableCore(t *testing.T) {
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	var output bytes.Buffer
	log.SetOutput(&output)
	_, err := (*Core)(nil).ObserveManagedAppendOnlyControl(context.Background())
	if err != ErrManagedControlObservationUnavailable || !errors.Is(err, ErrManagedControlObservationUnavailable) {
		t.Fatalf("unavailable Core error changed: %v", err)
	}
	if got := output.String(); got != "MANAGED_CONTROL_OBSERVATION_REJECTED reason=runtime_role_or_profile\n" {
		t.Fatalf("unexpected fixed diagnostic: %q", got)
	}
}

func TestManagedControlSnapshotLogsAfterCoreReadUnlock(t *testing.T) {
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	core.mu.Lock()
	core.localMode = true
	core.mu.Unlock()

	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	writer := &managedControlBlockingLogWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	releaseLogger := func() { releaseOnce.Do(func() { close(writer.release) }) }
	log.SetOutput(writer)
	t.Cleanup(func() {
		releaseLogger()
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	result := make(chan error, 1)
	go func() {
		_, err := core.managedControlSnapshot()
		result <- err
	}()
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot rejection did not reach its diagnostic")
	}

	writerAcquired := make(chan struct{})
	go func() {
		core.mu.Lock()
		core.localMode = false
		core.mu.Unlock()
		close(writerAcquired)
	}()
	select {
	case <-writerAcquired:
	case <-time.After(5 * time.Second):
		releaseLogger()
		<-writerAcquired
		t.Fatal("Core writer could not acquire the lock while diagnostic output was blocked")
	}
	releaseLogger()
	select {
	case err := <-result:
		if err != ErrManagedControlObservationUnavailable {
			t.Fatalf("snapshot rejection changed its original error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not return after diagnostic output resumed")
	}
}
