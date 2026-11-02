package network

import (
	"context"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

type ResolveKVState string
type ResolveKVDurability string

const (
	ResolveKVRetained                ResolveKVState      = "retained"
	ResolveKVUnknownOrExpired        ResolveKVState      = "unknown_or_expired"
	ResolveKVDurabilityConfirmed     ResolveKVDurability = "confirmed"
	ResolveKVDurabilityNotConfigured ResolveKVDurability = "not_configured"
	ResolveKVDurabilityUnknown       ResolveKVDurability = "unknown"
)

// ResolveKVResult observes one exact retained request, not authority to write.
// Unknown/expired is never proof of no effect. Confirmed refers only to the
// configured barrier, not canonical evidence or fresh generation authority.
type ResolveKVResult struct {
	State      ResolveKVState
	Receipt    *types.MutationReceipt
	Durability ResolveKVDurability
}

// ResolveKV never submits a mutation, refreshes its TTL or acquires its writer
// stripe. The configured barrier can publish already-certified original data.
func (s *Server) ResolveKV(ctx context.Context, operation string, req KVMutationRequest) (ResolveKVResult, error) {
	if s.localFailure() != nil {
		return ResolveKVResult{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return ResolveKVResult{}, err
	}
	if s.expectedPolicy != sqlpolicy.ManagedSQLKVMaterializationPolicy {
		return ResolveKVResult{}, sqlpolicy.ErrIncompatible
	}
	if s.material == nil {
		return ResolveKVResult{}, ErrNotReady
	}
	if s.material.MaterializationPolicy() != s.expectedPolicy {
		return ResolveKVResult{}, sqlpolicy.ErrIncompatible
	}
	intent := types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS}
	// Reuse the strict original codec's pure bounds and detached ownership.
	// No execution timestamps are assigned during resolution.
	_, command, err := types.EncodeManagedKVOriginal(intent)
	if err != nil {
		return ResolveKVResult{}, err
	}
	fingerprint, err := types.ManagedKVFingerprint(command)
	if err != nil {
		return ResolveKVResult{}, err
	}
	receipt, found, matches, err := s.material.KVRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
	if err != nil {
		return ResolveKVResult{}, err
	}
	if !matches {
		return ResolveKVResult{}, ErrRequestConflict
	}
	if !found {
		return ResolveKVResult{State: ResolveKVUnknownOrExpired, Durability: ResolveKVDurabilityUnknown}, nil
	}
	result := ResolveKVResult{State: ResolveKVRetained, Receipt: &receipt, Durability: ResolveKVDurabilityUnknown}
	if s.durability == nil {
		result.Durability = ResolveKVDurabilityNotConfigured
		return result, nil
	}
	if err := s.waitReceiptDurable(ctx, receipt, command.RequestID); err != nil {
		return result, err
	}
	result.Durability = ResolveKVDurabilityConfirmed
	return result, nil
}
