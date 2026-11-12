package node

import (
	"context"
	"fmt"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const managedMembershipTransitionWait = 10 * time.Second

// waitForManagedVoterAdmission keeps control-barrier and application
// admission behind the certified terminal. A stable non-voter fails without
// allocating a wait; only an actual retained transition uses the bounded poll.
func (n *Node) waitForManagedVoterAdmission(ctx context.Context) error {
	if n.core.CanParticipateReadIndex() {
		return nil
	}
	if err := n.WaitForManagedMembershipTransition(ctx); err != nil {
		return err
	}
	if !n.core.CanParticipateReadIndex() {
		// A certified terminal can be installed before its local fsync marker.
		// ReadIndex performs that exact bounded activation before proving quorum;
		// discard its view here so the ordinary admission still owns the barrier.
		if _, _, err := n.core.ReadIndex(ctx); err != nil {
			return err
		}
		if !n.core.CanParticipateReadIndex() {
			return fmt.Errorf("internal application has no active voter admission")
		}
	}
	return nil
}

// WaitForManagedMembershipTransition keeps a retained voter's client request
// outside runtime and mutation admission until the Core has installed the
// already-certified terminal. It never proposes, replays application values,
// or grants admission authority.
func (n *Node) WaitForManagedMembershipTransition(ctx context.Context) error {
	if n == nil || n.core == nil {
		return nil
	}
	pending, ok := n.core.PendingReconfiguration()
	if !ok {
		return nil
	}
	retained := false
	for _, member := range pending.Members {
		if string(member.ID) == string(n.core.NodeID()) {
			retained = true
			break
		}
	}
	if !retained {
		return network.ErrNotReady
	}

	recoveryCtx, cancel := context.WithTimeout(ctx, managedMembershipTransitionWait)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, pending := n.core.PendingReconfiguration(); !pending {
			return nil
		}
		if n.core.RecoverFrozenMembership(recoveryCtx) {
			return nil
		}
		select {
		case <-recoveryCtx.Done():
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("managed membership transition did not finish: %w", quepaxa.ErrQuorumUnavailable)
		case <-ticker.C:
		}
	}
}
