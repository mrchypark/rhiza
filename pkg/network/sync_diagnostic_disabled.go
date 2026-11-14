//go:build !rhiza_latency_diag

package network

import (
	"context"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func syncDecisions(_ context.Context, core *quepaxa.Core, from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	return core.DecisionsFrom(from, limit)
}
