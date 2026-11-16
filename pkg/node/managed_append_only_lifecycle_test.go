//go:build rhiza_local_testhooks

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedAppendOnlyCandidateVerifierDrainsBeforeShutdown(t *testing.T) {
	n := &Node{}
	entered := make(chan struct{})
	shutdownReady := make(chan struct{})
	lockedArrived := make(chan struct{})
	verifyDrained := make(chan struct{})
	lockedCheck := make(chan error, 1)
	allowShutdownContinue := make(chan struct{})
	release := make(chan struct{})
	restore := localtesthooks.Set(func(event string) {
		switch event {
		case "node:managed-append-only:verifier-archive-read":
			close(entered)
			<-release
		case "node:managed-append-only:shutdown-archive-lock":
			close(shutdownReady)
		case "node:managed-append-only:shutdown-archive-locked":
			close(lockedArrived)
			<-verifyDrained
			if n.archiveMu.TryRLock() {
				n.archiveMu.RUnlock()
				lockedCheck <- errors.New("shutdown did not hold the archive write lock")
			} else {
				lockedCheck <- nil
			}
			<-allowShutdownContinue
		}
	})
	defer restore()

	verifyDone := make(chan error, 1)
	go func() {
		verifyDone <- n.verifyManagedAppendOnlyCandidate(context.Background(), quepaxa.ManagedCheckpointCandidate{})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("verifier did not acquire the archive lifecycle lock")
	}
	if n.archiveMu.TryLock() {
		n.archiveMu.Unlock()
		t.Fatal("verifier did not hold the archive read lock")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- n.Shutdown() }()
	select {
	case <-shutdownReady:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not reach archive lock acquisition")
	}
	close(release)
	select {
	case err := <-verifyDone:
		if err == nil {
			t.Fatal("verifier accepted an unavailable runtime")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("verifier did not exit after release")
	}
	select {
	case <-lockedArrived:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not reach post-archive-lock inspection")
	}
	close(verifyDrained)
	select {
	case err := <-lockedCheck:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not report archive write lock acquisition")
	}
	close(allowShutdownContinue)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not drain the verifier")
	}
}
