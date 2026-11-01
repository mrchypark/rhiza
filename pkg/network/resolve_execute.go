package network

import (
	"context"

	"github.com/mrchypark/rhiza/internal/types"
)

type ResolveExecuteState string
type ResolveExecuteDurability string

const (
	ResolveExecuteRetained                ResolveExecuteState      = "retained"
	ResolveExecuteUnknownOrExpired        ResolveExecuteState      = "unknown_or_expired"
	ResolveExecuteDurabilityConfirmed     ResolveExecuteDurability = "confirmed"
	ResolveExecuteDurabilityNotConfigured ResolveExecuteDurability = "not_configured"
	ResolveExecuteDurabilityUnknown       ResolveExecuteDurability = "unknown"
)

// ResolveExecuteResult is an observation, not application-state evidence or
// permission to write. An unknown or expired request is not proof of no effect.
type ResolveExecuteResult struct {
	State      ResolveExecuteState
	Receipt    *types.MutationReceipt
	Statements []types.SQLStatementResult
	// Confirmed means the configured barrier completed, not a universal
	// object-store durability or canonical application-state proof.
	Durability ResolveExecuteDurability
}

// ResolveExecute checks the retained result for the exact original request.
// It never submits an application mutation. The durability barrier may publish
// already-certified data; a matching receipt does not authorize another write.
func (s *Server) ResolveExecute(ctx context.Context, req ExecuteRequest) (ResolveExecuteResult, error) {
	if s.localFailure() != nil {
		return ResolveExecuteResult{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return ResolveExecuteResult{}, err
	}
	command := types.SQLCommand{RequestID: req.RequestID, SQL: req.SQL, Args: req.Args, WantRows: req.WantRows, RequireOne: req.RequireOne, Statements: req.Statements}
	if _, err := mutationCharge(command); err != nil {
		return ResolveExecuteResult{}, err
	}
	command, err := cloneSQLCommand(command)
	if err != nil {
		return ResolveExecuteResult{}, err
	}
	if err := validateSQLCommand(command); err != nil {
		return ResolveExecuteResult{}, err
	}
	fingerprint, err := types.SQLFingerprint(command)
	if err != nil {
		return ResolveExecuteResult{}, err
	}
	receipt, sqlResult, found, matches, err := s.material.SQLRequestResultFingerprint(ctx, command.RequestID, fingerprint, true)
	if err != nil {
		return ResolveExecuteResult{}, err
	}
	if !matches {
		return ResolveExecuteResult{}, ErrRequestConflict
	}
	if !found {
		return ResolveExecuteResult{State: ResolveExecuteUnknownOrExpired, Durability: ResolveExecuteDurabilityUnknown}, nil
	}
	result := ResolveExecuteResult{State: ResolveExecuteRetained, Receipt: &receipt, Statements: sqlResult.Statements, Durability: ResolveExecuteDurabilityUnknown}
	if s.durability == nil {
		result.Durability = ResolveExecuteDurabilityNotConfigured
		return result, nil
	}
	if err := s.waitReceiptDurable(ctx, receipt, command.RequestID); err != nil {
		return result, err
	}
	result.Durability = ResolveExecuteDurabilityConfirmed
	return result, nil
}
