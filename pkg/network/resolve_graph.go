package network

import (
	"context"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

type ResolveGraphResult struct {
	State      ResolveKVState
	Receipt    *types.MutationReceipt
	Durability ResolveKVDurability
}

// ResolveGraph observes the exact retained original and its configured ACK
// barrier. It never proposes, obtains a writer stripe or refreshes retention.
func (s *Server) ResolveGraph(ctx context.Context, original types.GraphCommand) (ResolveGraphResult, error) {
	if s.localFailure() != nil || s.material == nil {
		return ResolveGraphResult{}, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return ResolveGraphResult{}, err
	}
	if s.expectedPolicy != sqlpolicy.ManagedGraphRequestMaterializationPolicy || s.material.MaterializationPolicy() != s.expectedPolicy {
		return ResolveGraphResult{}, sqlpolicy.ErrIncompatible
	}
	fingerprint, err := types.ManagedGraphIntentFingerprint(original)
	if err != nil {
		return ResolveGraphResult{}, err
	}
	receipt, found, matches, err := s.material.GraphRequestStatusFingerprint(ctx, original.RequestID, fingerprint)
	if err != nil {
		return ResolveGraphResult{}, err
	}
	if !found {
		return ResolveGraphResult{State: ResolveKVUnknownOrExpired, Durability: ResolveKVDurabilityUnknown}, nil
	}
	if !matches {
		return ResolveGraphResult{}, ErrRequestConflict
	}
	result := ResolveGraphResult{State: ResolveKVRetained, Receipt: &receipt, Durability: ResolveKVDurabilityUnknown}
	if s.durability == nil {
		result.Durability = ResolveKVDurabilityNotConfigured
		return result, nil
	}
	if err := s.waitReceiptDurable(ctx, receipt, original.RequestID); err != nil {
		return result, err
	}
	result.Durability = ResolveKVDurabilityConfirmed
	return result, nil
}
