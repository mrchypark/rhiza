//go:build rhiza_latency_diag

package quepaxa

import "context"

// DecisionsFromSyncDiagnostic is available only in diagnostic builds.
func (c *Core) DecisionsFromSyncDiagnostic(ctx context.Context, from Slot, limit int) ([]DecidedValue, Slot, error) {
	if from == 0 {
		from = 1
	}
	if limit <= 0 {
		limit = 256
	}
	return c.decisionsFromBounded(ctx, from, limit, int(^uint(0)>>1))
}
