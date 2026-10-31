package node

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

type managedPeerReadinessChecker struct {
	deny atomic.Bool
}

func (c *managedPeerReadinessChecker) ValidateManagedStartup(context.Context, ManagedStartupIdentity) error {
	return nil
}

func (c *managedPeerReadinessChecker) ValidateManagedAdmission(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error {
	return errors.New("application admission intentionally unavailable")
}

func (c *managedPeerReadinessChecker) ValidateManagedPeerLearn(context.Context, ManagedStartupIdentity) error {
	if c.deny.Load() {
		return errors.New("live peer authority unavailable")
	}
	return nil
}

func TestManagedPeerLearnReadinessIndependentOfClientCatchUp(t *testing.T) {
	checker := &managedPeerReadinessChecker{}
	n := New(&types.ExecutionConfig{NodeID: "managed", ClusterID: "cluster", DataDir: t.TempDir(), PeerAddr: "127.0.0.1:0"})
	n.managedStartup = &ManagedStartupConfig{Checker: checker}
	if err := n.open(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	if n.core == nil || n.wal == nil || n.material == nil || n.peerControl == nil {
		t.Fatal("test did not open a real managed peer runtime")
	}

	// Model the verified post-startup state, then clear only client/catch-up
	// readiness as a compacted peer does. No quorum or SQL admission is granted.
	n.managedPeerVerified.Store(true)
	n.ready.Store(false)
	if !n.managedLearnReady() {
		t.Fatal("verified peer with live authority could not learn while client catch-up is pending")
	}
	if n.Ready() {
		t.Fatal("peer-learning readiness opened client/SQL readiness")
	}

	checker.deny.Store(true)
	if n.managedLearnReady() {
		t.Fatal("peer learning bypassed unavailable live authority")
	}
	checker.deny.Store(false)
	n.managedPeerVerified.Store(false)
	if n.managedLearnReady() {
		t.Fatal("peer learning bypassed managed runtime verification")
	}
	n.managedPeerVerified.Store(true)
	n.opened.Store(false)
	if n.managedLearnReady() {
		t.Fatal("peer learning bypassed opened-runtime requirement")
	}
}
