package node

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A single worker per node proposes at most one barrier per interval. Multiple
// live voters may propose concurrently; this is not a cluster-wide rate limit.
const sqlRetentionBarrierInterval = time.Second
const sqlRetentionBarrierTimeout = 5 * time.Second

// SQLRetentionBarrierStatus is a local, eventually updated worker snapshot.
type SQLRetentionBarrierStatus struct {
	LastTargetSlot     uint64
	Attempts           uint64
	Completed          uint64
	Errors             uint64
	QuorumCheckPending bool
}

// SQLRetentionBarriers reports maintenance attempts, not quorum availability.
func (n *Node) SQLRetentionBarriers() SQLRetentionBarrierStatus {
	return SQLRetentionBarrierStatus{
		LastTargetSlot:     n.sqlRetentionBarrierTarget.Load(),
		Attempts:           n.sqlRetentionBarrierAttempts.Load(),
		Completed:          n.sqlRetentionBarrierCompleted.Load(),
		Errors:             n.sqlRetentionBarrierErrors.Load(),
		QuorumCheckPending: n.sqlRetentionQuorumBlocked.Load(),
	}
}

func (n *Node) runSQLRetentionBarriers(ctx context.Context) {
	runSQLRetentionBarrierLoop(ctx, localtesthooks.RetentionInterval(sqlRetentionBarrierInterval), n.advanceSQLRetention)
}

func runSQLRetentionBarrierLoop(ctx context.Context, interval time.Duration, advance func(context.Context)) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		advance(ctx)
		timer.Reset(interval)
	}
}

func (n *Node) advanceSQLRetention(ctx context.Context) {
	if localtesthooks.RetentionPaused() {
		return
	}
	localtesthooks.RecordRetentionCycle()
	if !n.ready.Load() || n.material == nil || n.core == nil || n.server == nil || !n.core.IsVoter() {
		return
	}
	operation, cancel := context.WithTimeout(ctx, sqlRetentionBarrierTimeout)
	defer cancel()
	applied := n.material.Tip()
	if n.sqlRetentionScanned.Load() && n.sqlRetentionScannedTip.Load() == applied && n.sqlRetentionBarrierTarget.Load() == 0 {
		return
	}
	target, err := n.material.SQLRetentionBarrierTarget(operation)
	if err != nil {
		n.sqlRetentionBarrierErrors.Add(1)
		return
	}
	n.sqlRetentionBarrierTarget.Store(target)
	n.sqlRetentionScannedTip.Store(applied)
	n.sqlRetentionScanned.Store(true)
	if target == 0 || !n.Ready() {
		return
	}
	if n.sqlRetentionQuorumBlocked.Load() {
		if _, _, err := n.core.ReadIndex(operation); err != nil {
			return
		}
		n.sqlRetentionQuorumBlocked.Store(false)
	}
	n.proposeSQLRetentionBarrier(operation, ctx)
}

func (n *Node) proposeSQLRetentionBarrier(operation, lifetime context.Context) {
	// Preferred proposers get a head start, but no lease or exclusive ownership.
	if !waitRetentionProposer(operation, n.core.NodeID(), n.core.ProposerOrder(), n.core.Tip) {
		return
	}
	var nonce [types.ReadBarrierNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		n.sqlRetentionBarrierErrors.Add(1)
		return
	}
	if operation.Err() != nil || lifetime.Err() != nil {
		return
	}
	n.sqlRetentionBarrierAttempts.Add(1)
	value := types.EncodeReadBarrier(nonce)
	var err error
	if n.core.LocalMode() {
		_, err = n.server.ProposeLocalRetentionControl(lifetime, value)
	} else {
		_, err = n.server.ProposeRetentionControl(operation, value)
	}
	if err == nil {
		n.sqlRetentionBarrierCompleted.Add(1)
	} else {
		n.sqlRetentionBarrierErrors.Add(1)
		n.sqlRetentionQuorumBlocked.Store(true)
	}
}

func waitRetentionProposer(ctx context.Context, local quepaxa.NodeID, order []quepaxa.NodeID, tip func() quepaxa.Slot) bool {
	start := tip()
	for rank, id := range order {
		if id != local {
			continue
		}
		if rank == 0 {
			return ctx.Err() == nil
		}
		wait := time.NewTimer(time.Duration(min(rank, 20)) * 100 * time.Millisecond)
		defer wait.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-wait.C:
			return ctx.Err() == nil && tip() == start
		}
	}
	return false
}
