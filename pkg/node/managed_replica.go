package node

import (
	"context"
	"fmt"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ProbeManagedReplicaVoters proves every voter in the supplied certified
// configuration answered an authenticated ReadTip. The exact configuration,
// abort revision, and absence of a pending transition are rechecked after all
// probes so a concurrent membership change cannot inherit the result.
func (n *Node) ProbeManagedReplicaVoters(ctx context.Context, expected quepaxa.Cluster, abortSlot quepaxa.Slot) error {
	return n.ProbeManagedReplicaVotersExcept(ctx, expected, abortSlot, "")
}

// ProbeManagedReplicaVotersExcept proves every survivor after one exact voter
// has actually been removed from the live Pod set. The excluded voter must
// still be in the certified source configuration.
func (n *Node) ProbeManagedReplicaVotersExcept(ctx context.Context, expected quepaxa.Cluster, abortSlot quepaxa.Slot, excluded quepaxa.NodeID) error {
	before, err := n.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before.Current, expected) || before.AbortSlot != abortSlot || before.PendingOK {
		return fmt.Errorf("replica membership changed before voter probes")
	}
	if !n.ready.Load() || !n.core.CanParticipateReadIndex() {
		return network.ErrNotReady
	}
	if n.transport == nil {
		return network.ErrNotReady
	}
	if err := probeManagedReplicaVoters(ctx, n.transport, expected, n.config.NodeID, excluded); err != nil {
		return err
	}
	after, err := n.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(after.Current, expected) || after.AbortSlot != abortSlot || after.PendingOK {
		return fmt.Errorf("replica membership changed during voter probes")
	}
	return ctx.Err()
}

func probeManagedReplicaVoters(ctx context.Context, transport quepaxa.Transport, cluster quepaxa.Cluster, local, excluded quepaxa.NodeID) error {
	if transport == nil || len(cluster.Members) == 0 {
		return network.ErrInvalidRequest
	}
	seen := make(map[quepaxa.NodeID]struct{}, len(cluster.Members))
	localFound := false
	excludedFound := excluded == ""
	for _, member := range cluster.Members {
		if member.ID == "" {
			return network.ErrInvalidRequest
		}
		if _, duplicate := seen[member.ID]; duplicate {
			return network.ErrInvalidRequest
		}
		seen[member.ID] = struct{}{}
		localFound = localFound || member.ID == local
		excludedFound = excludedFound || member.ID == excluded
	}
	if !localFound || !excludedFound {
		return network.ErrInvalidRequest
	}
	type probeResult struct {
		id  quepaxa.NodeID
		err error
	}
	results := make(chan probeResult, len(cluster.Members))
	probes := 0
	for _, member := range cluster.Members {
		if member.ID == local || member.ID == excluded {
			continue
		}
		probes++
		go func(id quepaxa.NodeID) {
			_, err := transport.ReadTip(ctx, id)
			results <- probeResult{id: id, err: err}
		}(member.ID)
	}
	probeErrors := make(map[quepaxa.NodeID]error, probes)
	for range probes {
		result := <-results
		if result.err != nil {
			probeErrors[result.id] = result.err
		}
	}
	for _, member := range cluster.Members {
		if err := probeErrors[member.ID]; err != nil {
			return fmt.Errorf("voter %q authenticated tip probe failed: %w", member.ID, err)
		}
	}
	return ctx.Err()
}
