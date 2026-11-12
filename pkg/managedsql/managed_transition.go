package managedsql

import (
	"context"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

// waitForMembershipTransition snapshots the Node without holding Host.mu while
// the transition runs. Replacement code needs that mutex to make progress.
func (h *Host) waitForMembershipTransition(ctx context.Context) error {
	h.mu.Lock()
	n, closed := h.node, h.closed
	h.mu.Unlock()
	if closed || n == nil {
		return nil
	}
	localtesthooks.Hit("managedsql:membership-transition:before-node-wait")
	return n.WaitForManagedMembershipTransition(ctx)
}
