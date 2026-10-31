package main

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/managedsql"
)

func TestManagedReplacementLoopCanceledBeforeHostAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// No Host access is permitted once serve owns cancellation/shutdown.
	runManagedReplacementLoop(ctx, nil, managedsql.Config{})
}
