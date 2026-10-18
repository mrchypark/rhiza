package rhiza

import (
	"context"
	"net/http"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
)

// TerminalDrainResult is a cooperative engine drain result, not a complete fence.
type TerminalDrainResult = node.TerminalDrainResult

type TerminalDrainRequest = network.TerminalDrainRequest

// TerminalDrain stops the engine permanently and verifies storage mutation
// outcomes. A fencing supervisor must separately prevent restart and confirm exit.
func (db *DB) TerminalDrain(ctx context.Context, expected TerminalDrainRequest) (TerminalDrainResult, error) {
	return db.node.TerminalDrain(ctx, expected)
}

// OperatorHandler exposes recovery status and authenticated archive and
// membership management endpoints. Serve it on a private HTTP listener reachable by the
// operator; it neither opens a listener nor exposes the database HTTP API.
// Kubernetes should name this listener's container port "recovery".
func (db *DB) OperatorHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/recovery/status", db.api)
	mux.Handle("/recovery/archive", db.api)
	mux.Handle("/recovery/drain", db.api)
	mux.Handle("/recovery/probe", db.api)
	mux.Handle("/membership/status", db.api)
	mux.Handle("/membership/change", db.api)
	mux.Handle("/membership/abort", db.api)
	return mux
}
