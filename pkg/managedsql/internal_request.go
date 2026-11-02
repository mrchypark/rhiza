package managedsql

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func (h *Host) internalRequests() bool {
	return h.cfg.Plan.ApplicationProtocol == sqlpolicy.ManagedRequestApplicationProtocol
}

// The first public operation drives real initial quorum/live startup admission.
// Once admitted, Node returns without repeating quorum or Kubernetes checks;
// each operation then uses its own actual runtime/read/write gate.
func (h *Host) ensureInitialRequestAdmission(ctx context.Context) error {
	if h.internalRequests() {
		return h.node.EnsureInitialManagedAdmission(ctx)
	}
	return nil
}

// Called with h.mu held. A hint selects an original; it never supplies it.
func (h *Host) rememberRequestOutcome(kind, id string, err error) {
	selector := network.RequestStatusRequest{Kind: kind, RequestID: id}
	var unknown *network.CommitUnknownError
	if errors.As(err, &unknown) {
		if h.recoveryTargets == nil {
			h.recoveryTargets = make(map[network.RequestStatusRequest]struct{})
		}
		if _, known := h.recoveryTargets[selector]; !known && len(h.recoveryTargets) == 2 {
			// Two already prove ambiguity. Do not grow a RAM history with
			// every uncertain request; overflow permanently requires a selector.
			h.recoveryTargetOverflow = true
			return
		}
		h.recoveryTargets[selector] = struct{}{}
	} else if err == nil {
		delete(h.recoveryTargets, selector)
	}
}

// RecoverRequest resolves the engine's retained original for one exact selector.
// It never accepts a replacement payload or submits an application proposal.
func (h *Host) RecoverRequest(ctx context.Context, selector network.RequestStatusRequest) (RecoveryResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || !h.internalRequests() {
		return RecoveryResult{}, fmt.Errorf("internal-request managed Host required")
	}
	result, err := h.recoverRequestLocked(ctx, selector)
	h.rememberRequestOutcome(selector.Kind, selector.RequestID, err)
	return result, err
}

func (h *Host) recoverRequestLocked(ctx context.Context, selector network.RequestStatusRequest) (RecoveryResult, error) {
	kind := types.MutationSQL
	if selector.Kind == "kv" {
		kind = types.MutationKV
	} else if selector.Kind != "sql" {
		return RecoveryResult{}, network.ErrInvalidRequest
	}
	if selector.RequestID == "" || len(selector.RequestID) > types.MaxRequestIDBytes {
		return RecoveryResult{}, network.ErrInvalidRequest
	}
	p := applicationOriginal{kind: selector.Kind, sql: ExecuteRequest{RequestID: selector.RequestID}, kv: types.KVCommand{RequestID: selector.RequestID}}
	if err := h.ensureInitialRequestAdmission(ctx); err != nil {
		return RecoveryResult{Kind: selector.Kind, RequestID: []byte(selector.RequestID)}, applicationOutcomeUnknown(p, types.MutationReceipt{}, err)
	}
	// The actual Node resolver owns quorum/replay, canonical lookup, barrier
	// and post-barrier binding/original checks in one fixed-target invocation.
	resolved, err := h.node.ResolveRequest(ctx, selector.Kind, selector.RequestID)
	result := RecoveryResult{Kind: selector.Kind, RequestID: []byte(selector.RequestID)}
	if resolved.Receipt.Slot != 0 {
		if kind == types.MutationSQL {
			result.SQL = &ExecuteResponse{MutationReceipt: resolved.Receipt, Statements: resolved.Statements}
		} else {
			result.KV = &network.KVMutationResponse{MutationReceipt: resolved.Receipt}
		}
	}
	if err != nil {
		return result, applicationOutcomeUnknown(p, resolved.Original.Receipt, err)
	}
	if resolved.Original.Kind != kind || resolved.Original.RequestID != selector.RequestID || resolved.Receipt.Slot == 0 || resolved.Receipt.Slot != resolved.Original.Slot {
		return result, applicationOutcomeUnknown(p, resolved.Original.Receipt, fmt.Errorf("retained original selector or result slot changed"))
	}
	delete(h.recoveryTargets, selector)
	return result, nil
}
