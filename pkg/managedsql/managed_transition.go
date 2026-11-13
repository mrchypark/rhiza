package managedsql

import (
	"context"
	"errors"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const maxMembershipTransitionRetries = 8

func shouldRetryMembershipTransition(err error, attempt int) bool {
	return err != nil &&
		attempt < maxMembershipTransitionRetries &&
		errors.Is(err, quepaxa.ErrReconfigurationDrain) &&
		!errors.Is(err, network.ErrCommitUnknown)
}

func exhaustedMembershipTransitionError(err error) error {
	if err == nil || !errors.Is(err, quepaxa.ErrReconfigurationDrain) || errors.Is(err, network.ErrNotReady) || errors.Is(err, network.ErrCommitUnknown) {
		return err
	}
	return errors.Join(network.ErrNotReady, err)
}

func (h *Host) waitForLinearizableTransition(ctx context.Context) error {
	operationCtx, release := h.internalRequestContext(ctx)
	defer release()
	return h.waitForMembershipTransition(operationCtx)
}

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
