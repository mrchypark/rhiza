//go:build !rhiza_local_testhooks

package node

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func verifyArchiveWhileLaterTerminalPending(*testing.T, context.Context, *quepaxa.Core, quepaxa.Cluster, *managedAppendOnlyTestTransport, *recovery.Manager, quepaxa.CheckpointSeal, []quepaxa.Member) {
}
