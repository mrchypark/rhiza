package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const managedMembershipTransitionWait = 10 * time.Second

func isRetainedClusterMember(cluster quepaxa.Cluster, id quepaxa.NodeID) bool {
	for _, member := range cluster.Members {
		if string(member.ID) == string(id) {
			return true
		}
	}
	return false
}

// A retained voter can observe the certified target before its terminal is locally durable.
func (n *Node) waitForManagedLocalReadVoter(ctx context.Context) error {
	if n.core.IsVoter() {
		return nil
	}
	if err := n.waitForManagedVoterAdmission(ctx); err != nil {
		return err
	}
	if !n.core.IsVoter() {
		return network.ErrNotReady
	}
	return nil
}

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
			admissionErr := fmt.Errorf("internal application has no active voter admission")
			if pending, ok := n.core.PendingReconfiguration(); ok {
				if isRetainedClusterMember(pending, n.core.NodeID()) {
					return errors.Join(network.ErrNotReady, quepaxa.ErrReconfigurationDrain, admissionErr)
				}
				return network.ErrNotReady
			}
			return admissionErr
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
	if !isRetainedClusterMember(pending, n.core.NodeID()) {
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
