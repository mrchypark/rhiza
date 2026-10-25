package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type archiveStartupCheck struct {
	pending   bool
	attempted bool
	cluster   quepaxa.Cluster
}

func (n *Node) queueStartupTerminalGap() {
	if n == nil || n.core == nil || n.archive == nil {
		return
	}
	if key, ok := n.core.StartupTerminalGapSnapshot(); ok {
		n.queueArchiveCatchUpHint(key)
	}
}

func (n *Node) queueInitialStartupArchiveCheck(err error) {
	if !errors.Is(err, quepaxa.ErrQuorumUnavailable) || n == nil || n.config == nil ||
		!n.config.EnableReconfiguration || n.core == nil || n.archive == nil || !n.core.IsVoter() {
		return
	}
	if key, ok := n.core.StartupTerminalGapSnapshot(); ok {
		n.queueArchiveCatchUpHint(key)
		return
	}
	current := n.core.CurrentCluster()
	if !n.core.StartupCatchUpRoundEligible(current) {
		return
	}
	n.queueStartupArchiveCheck(current)
}

func (n *Node) queueStartupArchiveCheck(current quepaxa.Cluster) {
	n.archiveHintMu.Lock()
	if n.archiveHintWake == nil || n.archiveStartupCheck.attempted {
		n.archiveHintMu.Unlock()
		return
	}
	n.archiveStartupCheck = archiveStartupCheck{
		pending:   true,
		attempted: true,
		cluster:   cloneStartupCluster(current),
	}
	wake := n.archiveHintWake
	n.archiveHintMu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func cloneStartupCluster(cluster quepaxa.Cluster) quepaxa.Cluster {
	cluster.Members = append([]quepaxa.Member(nil), cluster.Members...)
	return cluster
}

func (n *Node) takeStartupArchiveCheck() (quepaxa.Cluster, bool) {
	n.archiveHintMu.Lock()
	defer n.archiveHintMu.Unlock()
	if !n.archiveStartupCheck.pending {
		return quepaxa.Cluster{}, false
	}
	cluster := cloneStartupCluster(n.archiveStartupCheck.cluster)
	n.archiveStartupCheck.pending = false
	return cluster, true
}

func (n *Node) catchUpStartupArchive(ctx context.Context, captured quepaxa.Cluster) error {
	if n.archive == nil || n.core == nil || n.material == nil || !n.core.StartupCatchUpRoundEligible(captured) {
		n.queueStartupTerminalGap()
		return nil
	}
	if err := n.archive.LoadOnce(ctx); err != nil {
		return err
	}
	if !n.core.StartupCatchUpRoundEligible(captured) {
		n.queueStartupTerminalGap()
		return nil
	}
	target := n.archive.Tip()
	if target <= n.core.Tip() {
		return nil
	}
	if seal, _, ok := n.archive.RecoveryBase(); ok && archiveRecoveryRequired(n.core.Tip(), seal.Index) {
		return fmt.Errorf("fixed startup archive range requires checkpoint recovery")
	}
	for from := n.core.Tip() + 1; from <= target; {
		remaining := target - from + 1
		limit := 256
		if remaining < quepaxa.Slot(limit) {
			limit = int(remaining)
		}
		values, readTip, err := n.archive.DecisionsFrom(ctx, from, limit)
		if err != nil {
			return err
		}
		end := from + quepaxa.Slot(limit) - 1
		if readTip < end || len(values) != limit {
			return fmt.Errorf("shared archive omitted fixed startup range %d..%d", from, end)
		}
		for i, value := range values {
			if value.Slot != from+quepaxa.Slot(i) {
				return fmt.Errorf("shared archive returned a non-contiguous startup range")
			}
			if err := types.ValidateExecutionPolicy(value.Value); err != nil {
				return err
			}
		}
		if !n.core.StartupCatchUpRoundEligible(captured) {
			n.queueStartupTerminalGap()
			return nil
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
		if err := n.material.ApplyBatch(ctx, values); err != nil {
			return err
		}
		if !n.core.StartupCatchUpRoundEligible(captured) {
			n.queueStartupTerminalGap()
			return nil
		}
		from = end + 1
	}
	return n.replayLocalDecisions(ctx)
}

func (n *Node) catchUpStartupVoterQuorum(ctx context.Context, transport *network.Transport) error {
	if n.core == nil || !n.core.IsVoter() {
		return quepaxa.ErrQuorumUnavailable
	}
	captured := n.core.CurrentCluster()
	err := n.catchUpQuorum(ctx, transport, &captured)
	if err != nil {
		if errors.Is(err, quepaxa.ErrQuorumUnavailable) {
			n.queueStartupTerminalGap()
		}
		return err
	}
	if !n.core.StartupCatchUpRoundEligible(captured) {
		n.queueStartupTerminalGap()
		return quepaxa.ErrQuorumUnavailable
	}
	return nil
}
