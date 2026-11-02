package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type ManagedKVWriteChecker interface {
	ValidateManagedKVWrite(context.Context, ManagedStartupIdentity, ManagedRuntimeState, string, network.KVMutationRequest) error
}

type ManagedKVRecoveryChecker interface {
	ValidateManagedKVRecovery(context.Context, ManagedStartupIdentity, string, network.KVMutationRequest) error
}

func (n *Node) managedKVState(ctx context.Context) (ManagedRuntimeState, error) {
	if err := ctx.Err(); err != nil {
		return ManagedRuntimeState{}, err
	}
	state, err := n.ManagedState()
	if err != nil {
		return state, err
	}
	if n.materializationPolicy() != sqlpolicy.ManagedSQLKVMaterializationPolicy || n.server == nil {
		return state, sqlpolicy.ErrIncompatible
	}
	return state, nil
}

func ownedManagedKVRequest(operation string, req network.KVMutationRequest) (network.KVMutationRequest, error) {
	_, command, err := types.EncodeManagedKVOriginal(types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS})
	if err != nil {
		return network.KVMutationRequest{}, fmt.Errorf("%w: %w", network.ErrInvalidRequest, err)
	}
	return network.KVMutationRequest{RequestID: command.RequestID, Key: command.Key, Value: command.Value, Expected: command.Expected, ExpectedExists: command.ExpectedExists, TTLMS: command.TTLMS}, nil
}

// ExecuteManagedKV is an exact reserved-write boundary, not a raw Server API.
func (n *Node) ExecuteManagedKV(ctx context.Context, operation string, req network.KVMutationRequest) (network.KVMutationResponse, error) {
	state, err := n.managedKVState(ctx)
	if err != nil {
		return network.KVMutationResponse{}, err
	}
	req, err = ownedManagedKVRequest(operation, req)
	if err != nil {
		return network.KVMutationResponse{}, err
	}
	checker, ok := n.managedStartup.Checker.(ManagedKVWriteChecker)
	if !ok {
		return network.KVMutationResponse{}, errors.New("exact managed KV reservation checker unavailable")
	}
	if err := checker.ValidateManagedKVWrite(ctx, n.managedStartup.Identity, state, operation, req); err != nil {
		return network.KVMutationResponse{}, err
	}
	if err := n.recordManagedApplicationStart(ctx); err != nil {
		return network.KVMutationResponse{}, err
	}
	return n.server.KVMutate(ctx, operation, req)
}

// ResolveManagedKV never submits or changes the original execution timestamps.
// Exact authority is checked before any configured durability barrier, including
// a retained receipt; public status deliberately uses a different path below.
func (n *Node) ResolveManagedKV(ctx context.Context, operation string, req network.KVMutationRequest) (network.ResolveKVResult, error) {
	if _, err := n.managedKVState(ctx); err != nil {
		return network.ResolveKVResult{}, err
	}
	req, err := ownedManagedKVRequest(operation, req)
	if err != nil {
		return network.ResolveKVResult{}, err
	}
	checker, ok := n.managedStartup.Checker.(ManagedKVRecoveryChecker)
	if !ok {
		return network.ResolveKVResult{}, errors.New("exact managed KV recovery checker unavailable")
	}
	check := func() error { return checker.ValidateManagedKVRecovery(ctx, n.managedStartup.Identity, operation, req) }
	if err := check(); err != nil {
		return network.ResolveKVResult{}, err
	}
	// A retained receipt does not excuse an unresolved newer recorder entry.
	// Try the same certified catch-up path, never a second application proposal.
	state := n.managedRuntimeState()
	if state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
		target, _, err := n.core.ReadIndex(ctx)
		if err != nil {
			return network.ResolveKVResult{}, err
		}
		if err := n.replayLocalDecisions(ctx); err != nil {
			return network.ResolveKVResult{}, err
		}
		state = n.managedRuntimeState()
		if state.CertifiedTip < uint64(target) || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
			return network.ResolveKVResult{State: network.ResolveKVUnknownOrExpired, Durability: network.ResolveKVDurabilityUnknown}, &network.CommitUnknownError{RequestID: req.RequestID, Cause: errors.New("managed KV recovery has unresolved certified history")}
		}
		if err := check(); err != nil {
			return network.ResolveKVResult{}, err
		}
	}
	result, err := n.server.ResolveKV(ctx, operation, req)
	if err == nil && result.State == network.ResolveKVUnknownOrExpired {
		target, _, readErr := n.core.ReadIndex(ctx)
		if readErr != nil {
			return result, readErr
		}
		if err := n.replayLocalDecisions(ctx); err != nil {
			return result, err
		}
		state := n.managedRuntimeState()
		if state.CertifiedTip < uint64(target) || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
			return result, &network.CommitUnknownError{RequestID: req.RequestID, Cause: errors.New("managed KV recovery has unresolved certified history")}
		}
		if err := check(); err != nil {
			return result, err
		}
		result, err = n.server.ResolveKV(ctx, operation, req)
	}
	if authorityErr := check(); authorityErr != nil {
		if result.Receipt != nil {
			result.Durability = network.ResolveKVDurabilityUnknown
			return result, &network.CommitUnknownError{Slot: quepaxa.Slot(result.Receipt.Slot), RequestID: req.RequestID, RetryThroughSlot: result.Receipt.RetryThroughSlot, Cause: errors.Join(err, authorityErr)}
		}
		return result, errors.Join(err, authorityErr)
	}
	return result, err
}

func (n *Node) KVGetManaged(ctx context.Context, req network.KVGetRequest) (network.KVGetResponse, error) {
	if _, err := n.managedKVState(ctx); err != nil {
		return network.KVGetResponse{}, err
	}
	if req.Consistency != "" && req.Consistency != "local" && req.Consistency != "linearizable" {
		return network.KVGetResponse{}, network.ErrInvalidRequest
	}
	if err := n.CheckManagedLocalRead(ctx); err != nil {
		return network.KVGetResponse{}, err
	}
	if req.Consistency == "linearizable" {
		if err := n.managedStartup.Checker.ValidateManagedAdmission(ctx, n.managedStartup.Identity, n.managedRuntimeState()); err != nil {
			return network.KVGetResponse{}, err
		}
	}
	return n.server.KVGet(ctx, req)
}

// RequestStatusManaged observes a local retained receipt. It cannot publish a
// durability barrier, authorize recovery, finalize, or resubmit a mutation.
func (n *Node) RequestStatusManaged(ctx context.Context, req network.RequestStatusRequest) (network.RequestStatusResponse, error) {
	if _, err := n.managedKVState(ctx); err != nil {
		return network.RequestStatusResponse{}, err
	}
	if req.Kind != "sql" && req.Kind != "kv" {
		return network.RequestStatusResponse{}, network.ErrInvalidRequest
	}
	if err := n.CheckManagedLocalRead(ctx); err != nil {
		return network.RequestStatusResponse{}, err
	}
	return n.server.RequestStatus(ctx, req)
}
