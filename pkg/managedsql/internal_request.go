package managedsql

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
)

func (h *Host) internalRequests() bool {
	return sqlpolicy.IsManagedRequestApplicationProtocol(h.cfg.Plan.ApplicationProtocol) || sqlpolicy.IsManagedGraphRequestApplicationProtocol(h.cfg.Plan.ApplicationProtocol)
}

// internalRequestContext scopes the complete lock-held operation to both its
// caller and this Host's close lifetime. The initial-admission scope alone is
// insufficient: Node may re-observe certified progress later in the request.
func (h *Host) internalRequestContext(ctx context.Context) (context.Context, func()) {
	if !h.internalRequests() {
		return ctx, func() {}
	}
	if h.node != nil && sqlpolicy.IsManagedAppendOnlyApplicationProtocol(h.cfg.Plan.ApplicationProtocol) {
		ctx = node.WithManagedControlObservationBudget(ctx)
	}
	if h.admissionLifetime == nil {
		return ctx, func() {}
	}
	scope, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(h.admissionLifetime, cancel)
	if h.admissionLifetime.Err() != nil {
		cancel()
	}
	return scope, func() {
		stop()
		cancel()
	}
}

// The first public operation drives real initial quorum/live startup admission.
// Once admitted, Node returns without repeating quorum or Kubernetes checks;
// each operation then uses its own actual runtime/read/write gate.
func (h *Host) ensureInitialRequestAdmission(ctx context.Context) error {
	if h.internalRequests() {
		if h.admissionLifetime == nil {
			return h.node.EnsureInitialManagedAdmission(ctx)
		}
		scope, release := h.internalRequestContext(ctx)
		defer release()
		if h.admissionLifetime.Err() != nil {
			return network.ErrNotReady
		}
		err := h.node.EnsureInitialManagedAdmission(scope)
		return errors.Join(err, scope.Err(), h.admissionLifetime.Err())
	}
	return nil
}

type requestRecoveryHint struct {
	fingerprint [32]byte
	ambiguous   bool
	// Earliest known nonzero submission slot bounds the original's first
	// canonical position; duplicates may submit after that original position.
	knownAttemptSlot uint64
}

func (h requestRecoveryHint) matches(fingerprint [32]byte, canonicalSlot uint64) bool {
	return !h.ambiguous && h.fingerprint == fingerprint && canonicalSlot != 0 && (h.knownAttemptSlot == 0 || canonicalSlot <= h.knownAttemptSlot)
}

func originalFingerprint(p applicationOriginal) ([32]byte, error) {
	if p.kind == "sql" {
		r := p.sql
		return types.SQLFingerprint(types.SQLCommand{RequestID: r.RequestID, SQL: r.SQL, Args: r.Args, WantRows: r.WantRows, RequireOne: r.RequireOne, Statements: r.Statements})
	}
	if p.kind == "kv" {
		return types.ManagedKVFingerprint(p.kv)
	}
	if p.kind == "graph" {
		return types.ManagedGraphIntentFingerprint(p.graph)
	}
	return [32]byte{}, network.ErrInvalidRequest
}

func internalRequestOutcome(p applicationOriginal, receipt types.MutationReceipt, err error) error {
	if errors.Is(err, network.ErrCommitUnknown) {
		return applicationOutcomeUnknown(p, receipt, err)
	}
	return err
}

// Called with h.mu held. The owned fingerprint identifies attempted intent;
// it is not original storage or execution authority.
func (h *Host) rememberRequestOutcome(p applicationOriginal, err error, actualReceipt types.MutationReceipt) {
	var unknown *network.CommitUnknownError
	isUnknown := errors.As(err, &unknown)
	if err != nil && !isUnknown {
		return // Validation/admission/conflict must not mutate recovery hints.
	}
	selector := network.RequestStatusRequest{Kind: p.kind, RequestID: p.rawID()}
	fingerprint, fingerprintErr := originalFingerprint(p)
	if fingerprintErr != nil {
		h.recoveryTargetOverflow = true // never guess an unrepresentable hint
		return
	}
	if isUnknown {
		if h.recoveryTargets == nil {
			h.recoveryTargets = make(map[network.RequestStatusRequest]requestRecoveryHint)
		}
		if _, known := h.recoveryTargets[selector]; !known && len(h.recoveryTargets) == 2 {
			// Two already prove ambiguity. Do not grow a RAM history with
			// every uncertain request; overflow permanently requires a selector.
			h.recoveryTargetOverflow = true
			return
		}
		hint, known := h.recoveryTargets[selector]
		if known && hint.fingerprint != fingerprint {
			hint.ambiguous = true
		}
		if !known {
			hint.fingerprint = fingerprint
		}
		if !hint.ambiguous && unknown.Slot != 0 && (hint.knownAttemptSlot == 0 || uint64(unknown.Slot) < hint.knownAttemptSlot) {
			hint.knownAttemptSlot = uint64(unknown.Slot)
		}
		h.recoveryTargets[selector] = hint
	} else if err == nil {
		h.clearRequestHint(selector, fingerprint, actualReceipt.Slot)
	}
}

func (h *Host) clearRequestHint(selector network.RequestStatusRequest, fingerprint [32]byte, canonicalSlot uint64) {
	if hint, ok := h.recoveryTargets[selector]; ok && hint.matches(fingerprint, canonicalSlot) {
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
	scope, release := h.internalRequestContext(ctx)
	defer release()
	return h.recoverRequestLocked(scope, selector, nil)
}

func (h *Host) recoverRequestLocked(ctx context.Context, selector network.RequestStatusRequest, expected *[32]byte) (RecoveryResult, error) {
	kind := types.MutationSQL
	if selector.Kind == "kv" {
		kind = types.MutationKV
	} else if selector.Kind == "graph" && sqlpolicy.IsManagedGraphRequestApplicationProtocol(h.cfg.Plan.ApplicationProtocol) {
		kind = types.MutationGraph
	} else if selector.Kind != "sql" {
		return RecoveryResult{}, network.ErrInvalidRequest
	}
	if selector.RequestID == "" || len(selector.RequestID) > types.MaxRequestIDBytes {
		return RecoveryResult{}, network.ErrInvalidRequest
	}
	p := applicationOriginal{kind: selector.Kind, sql: ExecuteRequest{RequestID: selector.RequestID}, kv: types.KVCommand{RequestID: selector.RequestID}, graph: types.GraphCommand{RequestID: selector.RequestID}}
	if err := h.ensureInitialRequestAdmission(ctx); err != nil {
		return RecoveryResult{Kind: selector.Kind, RequestID: []byte(selector.RequestID)}, applicationOutcomeUnknown(p, types.MutationReceipt{}, err)
	}
	// The actual Node resolver owns quorum/replay, canonical lookup, barrier
	// and post-barrier binding/original checks in one fixed-target invocation.
	resolved, err := h.node.ResolveRequest(ctx, selector.Kind, selector.RequestID)
	result := RecoveryResult{Kind: selector.Kind, RequestID: []byte(selector.RequestID)}
	if expected != nil && resolved.Original.Slot != 0 && (resolved.Original.Fingerprint != *expected || !h.recoveryTargets[selector].matches(resolved.Original.Fingerprint, resolved.Original.Slot)) {
		return result, applicationOutcomeUnknown(p, types.MutationReceipt{}, errors.Join(fmt.Errorf("retained original differs from attempted recovery intent"), err))
	}
	if resolved.Receipt.Slot != 0 {
		if kind == types.MutationSQL {
			result.SQL = &ExecuteResponse{MutationReceipt: resolved.Receipt, Statements: resolved.Statements}
		} else if kind == types.MutationKV {
			result.KV = &network.KVMutationResponse{MutationReceipt: resolved.Receipt}
		} else {
			result.Graph = &network.GraphExecuteResponse{MutationReceipt: resolved.Receipt}
		}
	}
	if err != nil {
		return result, applicationOutcomeUnknown(p, resolved.Original.Receipt, err)
	}
	if resolved.Original.Kind != kind || resolved.Original.RequestID != selector.RequestID || resolved.Receipt.Slot == 0 || resolved.Receipt.Slot != resolved.Original.Slot {
		return result, applicationOutcomeUnknown(p, resolved.Original.Receipt, fmt.Errorf("retained original selector or result slot changed"))
	}
	h.clearRequestHint(selector, resolved.Original.Fingerprint, resolved.Original.Slot)
	return result, nil
}
