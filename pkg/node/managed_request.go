package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ManagedRequestResolution contains one exact stored request's terminal result.
// It does not authorize submission or choose a different retained request.
type ManagedRequestResolution struct {
	Original   materializer.RequestOriginal
	Receipt    types.MutationReceipt
	Statements []types.SQLStatementResult
}

func retainedRequestKind(kind string) (types.MutationKind, error) {
	switch kind {
	case "sql":
		return types.MutationSQL, nil
	case "kv":
		return types.MutationKV, nil
	case "graph":
		return types.MutationGraph, nil
	default:
		return 0, network.ErrInvalidRequest
	}
}

// RetainedRequest is a local observation. Absence is unknown/expired, not
// evidence of no effect, and it neither acquires quorum nor submits a command.
func (n *Node) RetainedRequest(ctx context.Context, kind, rawID string) (materializer.RequestOriginal, bool, error) {
	k, err := retainedRequestKind(kind)
	if err != nil {
		return materializer.RequestOriginal{}, false, err
	}
	if !n.internalApplicationProfile() {
		return materializer.RequestOriginal{}, false, fmt.Errorf("retained original requires the managed request profile")
	}
	_, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		return materializer.RequestOriginal{}, false, err
	}
	defer release()
	if err := n.CheckManagedLocalRead(ctx); err != nil {
		return materializer.RequestOriginal{}, false, err
	}
	return n.material.RetainedRequestOriginal(ctx, k, rawID)
}

// ResolveRequest learns/replays an existing position and confirms its configured
// durability. It never calls an application proposal or accepts caller payload.
func (n *Node) ResolveRequest(ctx context.Context, kind, rawID string) (ManagedRequestResolution, error) {
	unknown := func(cause error) (ManagedRequestResolution, error) {
		return ManagedRequestResolution{}, &network.CommitUnknownError{RequestID: rawID, Cause: cause}
	}
	k, err := retainedRequestKind(kind)
	if err != nil || rawID == "" || len(rawID) > types.MaxRequestIDBytes {
		return ManagedRequestResolution{}, network.ErrInvalidRequest
	}
	if !n.internalApplicationProfile() {
		return ManagedRequestResolution{}, fmt.Errorf("request resolution requires the managed request profile")
	}
	_, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		return unknown(managedRequestResolutionStageFailure("resolve_borrow", err))
	}
	defer release()
	if err := n.admitInternalApplication(ctx); err != nil {
		return unknown(managedRequestResolutionStageFailure("resolve_admit", err))
	}
	bound := n.internalApplication.Load()
	original, found, err := n.material.RetainedRequestOriginal(ctx, k, rawID)
	if err != nil {
		logManagedRequestOriginalLookupError(err)
		return unknown(errors.Join(fmt.Errorf("request original is unknown or expired"), err))
	}
	if !found {
		logManagedRequestOriginalNotFound()
		return unknown(errors.Join(fmt.Errorf("request original is unknown or expired"), err))
	}
	result := ManagedRequestResolution{Original: original}
	fail := func(cause error) (ManagedRequestResolution, error) {
		logManagedRequestOriginalFound(original, k, rawID)
		return result, &network.CommitUnknownError{RequestID: rawID, Slot: quepaxa.Slot(original.Slot), RetryThroughSlot: original.Receipt.RetryThroughSlot, Cause: cause}
	}
	if k == types.MutationSQL {
		if original.SQL == nil || original.KV != nil {
			return fail(fmt.Errorf("stored SQL original is unavailable"))
		}
		c := original.SQL
		resolved, err := n.server.ResolveExecute(ctx, network.ExecuteRequest{RequestID: c.RequestID, SQL: c.SQL, Args: c.Args, WantRows: c.WantRows, RequireOne: c.RequireOne, Statements: c.Statements})
		if err != nil || resolved.State != network.ResolveExecuteRetained || resolved.Durability != network.ResolveExecuteDurabilityConfirmed || resolved.Receipt == nil || *resolved.Receipt != original.Receipt {
			return fail(errors.Join(fmt.Errorf("original SQL result is not durably confirmed"), err))
		}
		result.Receipt, result.Statements = *resolved.Receipt, resolved.Statements
	} else if k == types.MutationKV {
		if original.KV == nil || original.SQL != nil {
			return fail(fmt.Errorf("stored KV original is unavailable"))
		}
		c := original.KV
		resolved, err := n.server.ResolveKV(ctx, c.Operation, network.KVMutationRequest{RequestID: c.RequestID, Key: c.Key, Value: c.Value, Expected: c.Expected, ExpectedExists: c.ExpectedExists, TTLMS: c.TTLMS})
		if err != nil || resolved.State != network.ResolveKVRetained || resolved.Durability != network.ResolveKVDurabilityConfirmed || resolved.Receipt == nil || *resolved.Receipt != original.Receipt {
			return fail(errors.Join(fmt.Errorf("original KV result is not durably confirmed"), err))
		}
		result.Receipt = *resolved.Receipt
	} else {
		if original.Graph == nil || original.SQL != nil || original.KV != nil {
			return fail(fmt.Errorf("stored Graph original is unavailable"))
		}
		resolved, err := n.server.ResolveGraph(ctx, *original.Graph)
		if err != nil || resolved.State != network.ResolveKVRetained || resolved.Durability != network.ResolveKVDurabilityConfirmed || resolved.Receipt == nil || *resolved.Receipt != original.Receipt {
			return fail(errors.Join(fmt.Errorf("original Graph result is not durably confirmed"), err))
		}
		result.Receipt = *resolved.Receipt
	}
	state, err := n.checkInternalApplicationRuntime(ctx)
	if err != nil || bound == nil || bound != n.internalApplication.Load() || !n.core.IsVoter() || original.Slot > state.CertifiedTip || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
		return fail(errors.Join(fmt.Errorf("request runtime changed during resolution"), err))
	}
	after, found, err := n.material.RetainedRequestOriginal(ctx, k, rawID)
	if err != nil || !found || after.Kind != original.Kind || after.RequestID != original.RequestID || after.Slot != original.Slot || after.BatchIndex != original.BatchIndex || after.Fingerprint != original.Fingerprint || after.Receipt != original.Receipt || after.DecisionHash != original.DecisionHash || !bytes.Equal(after.ExecutionItem, original.ExecutionItem) || !bytes.Equal(after.CanonicalOriginal, original.CanonicalOriginal) {
		return fail(errors.Join(fmt.Errorf("exact retained original changed during resolution"), err))
	}
	return result, nil
}

func managedRequestResolutionStageFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	log.Printf("MANAGED_REQUEST_RESOLUTION_FAILURE stage=%s cause=%s", stage, managedMembershipCauseClass(err))
	return err
}

func logManagedControlWaitFailure(leaf string, err error) error {
	if err == nil {
		return nil
	}
	switch leaf {
	case "binding", "budget", "context", "observe", "prefix_recheck_error", "prefix_recheck_false",
		"worker_propose", "worker_wait_tip", "worker_certificate", "worker_replay", "waiter_error", "waiter_slot":
		log.Printf("MANAGED_CONTROL_WAIT_FAILURE leaf=%s cause=%s", leaf, managedMembershipCauseClass(err))
	}
	return err
}

func logManagedRequestOriginalLookupError(err error) {
	log.Printf("MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=error cause=%s", managedMembershipCauseClass(err))
}

func logManagedRequestOriginalNotFound() {
	log.Printf("MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=not_found")
}

func logManagedRequestOriginalFound(original materializer.RequestOriginal, kind types.MutationKind, requestID string) {
	if original.Slot == 0 || original.Receipt.Slot != original.Slot || original.Kind != kind || original.RequestID != requestID {
		log.Printf("MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=false")
		return
	}
	log.Printf("MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=true slot=%d", original.Slot)
}
