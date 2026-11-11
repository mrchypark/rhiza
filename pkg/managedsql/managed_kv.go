package managedsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// RecoveryResult contains the exact retained result read during finalization.
// Empty Kind means there was no pending request. It is not no-effect proof.
type RecoveryResult struct {
	Kind      string
	RequestID []byte
	SQL       *ExecuteResponse
	KV        *network.KVMutationResponse
	Graph     *network.GraphExecuteResponse
}

// MutationContext identifies the original processed by the failed invocation.
// It is observation metadata, never execution or future write authority.
type MutationContext struct {
	Kind             string
	RequestID        []byte
	Slot             uint64
	RetryThroughSlot uint64
}

type mutationContextError struct {
	context MutationContext
	cause   error
}

func (e *mutationContextError) Error() string { return e.cause.Error() }
func (e *mutationContextError) Unwrap() error { return e.cause }

// MutationErrorContext returns owned bytes without reloading mutable pending.
func MutationErrorContext(err error) (MutationContext, bool) {
	var original *mutationContextError
	if !errors.As(err, &original) {
		return MutationContext{}, false
	}
	result := original.context
	result.RequestID = bytes.Clone(result.RequestID)
	return result, true
}

type applicationOriginal struct {
	kind  string
	sql   ExecuteRequest
	kv    types.KVCommand
	graph types.GraphCommand
}

func (p applicationOriginal) rawID() string {
	if p.kind == "graph" {
		return p.graph.RequestID
	}
	if p.kind == "kv" {
		return p.kv.RequestID
	}
	return p.sql.RequestID
}

func kvRequest(command types.KVCommand) network.KVMutationRequest {
	return network.KVMutationRequest{RequestID: command.RequestID, Key: command.Key, Value: bytes.Clone(command.Value), Expected: bytes.Clone(command.Expected), ExpectedExists: command.ExpectedExists, TTLMS: command.TTLMS}
}

func (h *Host) mixed() bool {
	return h.internalRequests()
}

func (h *Host) inspectEvidence(ctx context.Context, fresh, localRead bool) (json.RawMessage, error) {
	evidence, _, err := h.inspectEvidenceSnapshot(ctx, fresh, localRead)
	return evidence, err
}

// Keep the evidence and the exact runtime validated by its inspector together.
func (h *Host) inspectEvidenceSnapshot(ctx context.Context, fresh, localRead bool) (json.RawMessage, node.ManagedRuntimeState, error) {
	if sqlpolicy.IsManagedGraphRequestApplicationProtocol(h.cfg.Plan.ApplicationProtocol) {
		state, err := h.node.InspectManagedGraph(ctx, fresh, localRead)
		if err != nil {
			return nil, node.ManagedRuntimeState{}, err
		}
		evidence, err := json.Marshal(struct {
			Format        string `json:"format"`
			SQLDigest     string `json:"sql_digest"`
			LastGraphSlot uint64 `json:"last_graph_slot"`
			GraphHistory  string `json:"graph_history"`
		}{h.cfg.Plan.EvidenceFormat, fmt.Sprintf("%x", state.Application.Digest), state.History.LastGraphSlot, fmt.Sprintf("%x", state.History.Hash)})
		return evidence, state.Runtime, err
	}
	var state node.ManagedKVSnapshot
	var err error
	if localRead {
		state, err = h.node.InspectManagedKVRead(ctx)
	} else {
		state, err = h.node.InspectManagedKV(ctx, fresh)
	}
	if err != nil {
		return nil, node.ManagedRuntimeState{}, err
	}
	evidence, err := json.Marshal(sqlEvidence{h.cfg.Plan.EvidenceFormat, fmt.Sprintf("%x", state.Application.Digest)})
	return evidence, state.Runtime, err
}

func (h *Host) ValidateManagedKVWrite(ctx context.Context, i node.ManagedStartupIdentity, _ node.ManagedRuntimeState, operation string, req network.KVMutationRequest) error {
	return h.validateKVOriginal(ctx, i, operation, req, true)
}

func (h *Host) ValidateManagedKVRecovery(ctx context.Context, i node.ManagedStartupIdentity, operation string, req network.KVMutationRequest) error {
	return h.validateKVOriginal(ctx, i, operation, req, false)
}

func (h *Host) validateKVOriginal(ctx context.Context, i node.ManagedStartupIdentity, operation string, req network.KVMutationRequest, _ bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !h.mixed() || i != h.identity {
		return fmt.Errorf("execution identity or application profile changed")
	}
	_, _, err := types.EncodeManagedKVOriginal(types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS})
	return err
}

func (h *Host) KVMutate(ctx context.Context, operation string, req network.KVMutationRequest) (network.KVMutationResponse, error) {
	notSubmitted := func(stage SQLSubmissionStage, recovery bool, cause error) (network.KVMutationResponse, error) {
		return network.KVMutationResponse{}, &KVNotSubmittedError{Stage: stage, RecoveryRequired: recovery, Cause: cause}
	}
	if !h.mixed() {
		return notSubmitted(SQLStageAuthority, false, fmt.Errorf("mixed managed Host required"))
	}
	_, original, err := types.EncodeManagedKVOriginal(types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS})
	if err != nil {
		return notSubmitted(SQLStageValidation, false, err)
	}
	// Only NEW reservations get the worst-case execution-size admission check.
	if _, err := types.ManagedKVExecutionSizeBound(original); err != nil {
		return notSubmitted(SQLStageValidation, false, err)
	}
	operationCtx, release := h.internalRequestContext(ctx)
	defer release()
	if err := h.waitForMembershipTransition(operationCtx); err != nil {
		return notSubmitted(SQLStageAdmission, false, err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return notSubmitted(SQLStageAuthority, false, fmt.Errorf("mixed managed Host required"))
	}
	if err := h.ensureInitialRequestAdmission(operationCtx); err != nil {
		return notSubmitted(SQLStageAdmission, false, err)
	}
	response, err := h.node.ExecuteManagedKV(operationCtx, original.Operation, kvRequest(original))
	p := applicationOriginal{kind: "kv", kv: original}
	err = internalRequestOutcome(p, response.MutationReceipt, err)
	h.rememberRequestOutcome(p, err, response.MutationReceipt)
	return response, err
}

func (h *Host) KVGet(ctx context.Context, req network.KVGetRequest) (network.KVGetResponse, error) {
	if !h.mixed() {
		return network.KVGetResponse{}, fmt.Errorf("mixed managed Host required")
	}
	operationCtx, n, release, err := h.localReadNode(ctx)
	if err != nil {
		return network.KVGetResponse{}, err
	}
	defer release()
	if req.Consistency == "linearizable" {
		if err := n.WaitForManagedMembershipTransition(operationCtx); err != nil {
			return network.KVGetResponse{}, err
		}
	}
	return n.KVGetManaged(operationCtx, req)
}

// RequestStatus observes local retained history, not a durability ACK or write
// permission. It does not resolve or finalize pending requests.
func (h *Host) RequestStatus(ctx context.Context, req network.RequestStatusRequest) (network.RequestStatusResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || !h.mixed() {
		return network.RequestStatusResponse{}, fmt.Errorf("mixed managed Host required")
	}
	operationCtx, release := h.internalRequestContext(ctx)
	defer release()
	if err := h.ensureInitialRequestAdmission(operationCtx); err != nil {
		return network.RequestStatusResponse{}, err
	}
	if req.Kind == "graph" {
		return h.node.GraphRequestStatusManaged(operationCtx, req)
	}
	return h.node.RequestStatusManaged(operationCtx, req)
}

func applicationOutcomeUnknown(p applicationOriginal, receipt types.MutationReceipt, cause error) error {
	slot, retry := quepaxa.Slot(receipt.Slot), receipt.RetryThroughSlot
	var previous *network.CommitUnknownError
	if errors.As(cause, &previous) && previous.RequestID == p.rawID() {
		if slot == 0 {
			slot = previous.Slot
		}
		if retry == 0 {
			retry = previous.RetryThroughSlot
		}
	}
	unknown := &network.CommitUnknownError{RequestID: p.rawID(), Slot: slot, RetryThroughSlot: retry, Cause: cause}
	return &mutationContextError{context: MutationContext{Kind: p.kind, RequestID: []byte(p.rawID()), Slot: uint64(slot), RetryThroughSlot: retry}, cause: unknown}
}

// RecoverPendingDetailed never submits. The original kind/raw ID and result
// are captured once for this invocation, including when finalization fails.
func (h *Host) RecoverPendingDetailed(ctx context.Context) (RecoveryResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return RecoveryResult{}, fmt.Errorf("SQL host closed")
	}
	operationCtx, release := h.internalRequestContext(ctx)
	defer release()
	if h.recoveryTargetOverflow || len(h.recoveryTargets) != 1 {
		return RecoveryResult{}, &network.CommitUnknownError{Cause: fmt.Errorf("exact recovery target required or ambiguous")}
	}
	for selector, hint := range h.recoveryTargets {
		if hint.ambiguous {
			return RecoveryResult{}, &network.CommitUnknownError{Cause: fmt.Errorf("recovery target intent is ambiguous")}
		}
		return h.recoverRequestLocked(operationCtx, selector, &hint.fingerprint)
	}
	return RecoveryResult{}, &network.CommitUnknownError{Cause: fmt.Errorf("exact recovery target required")}
}
