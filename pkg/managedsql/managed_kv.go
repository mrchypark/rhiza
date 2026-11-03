package managedsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
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

// Exists only while proving an already-completed remote finalization. It does
// not permit executing a mutation or recovering an unrelated missing pending.
type remoteRecoveryProof struct {
	issuer          *Host
	request         operator.V1Alpha2WriteRequest
	resourceVersion string
	original        string
	scopeDone       <-chan struct{}
}

type remoteRecoveryContextKey struct{}

var errPendingAbsent = errors.New("exact pending KV recovery required: pending is absent")

func proofScopeOpen(p remoteRecoveryProof) bool {
	if p.scopeDone == nil {
		return false
	}
	select {
	case <-p.scopeDone:
		return false
	default:
		return true
	}
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

func (p applicationOriginal) identity() (operator.V1Alpha2WriteRequest, error) {
	if p.kind == "sql" {
		return requestIdentity(p.sql)
	}
	if p.kind != "kv" {
		return operator.V1Alpha2WriteRequest{}, fmt.Errorf("unsupported pending kind")
	}
	id, err := types.ManagedKVPendingWireID(p.kv.RequestID)
	if err != nil {
		return operator.V1Alpha2WriteRequest{}, err
	}
	fingerprint, err := types.ManagedKVFingerprint(p.kv)
	return operator.V1Alpha2WriteRequest{Kind: "kv", ID: id, Fingerprint: fmt.Sprintf("%x", fingerprint)}, err
}

func pendingOriginal(a operator.V1Alpha2AdmissionSnapshot) (applicationOriginal, error) {
	w, ok := a.Pending()
	if !ok {
		return applicationOriginal{}, fmt.Errorf("original pending request required")
	}
	switch w.Kind {
	case "sql":
		req, ok, err := a.PendingSQL()
		if err != nil || !ok {
			return applicationOriginal{}, errors.Join(fmt.Errorf("original pending SQL required"), err)
		}
		return applicationOriginal{kind: "sql", sql: req}, nil
	case "kv":
		req, ok, err := a.PendingKV()
		if err != nil || !ok {
			return applicationOriginal{}, errors.Join(fmt.Errorf("original pending KV required"), err)
		}
		return applicationOriginal{kind: "kv", kv: req}, nil
	default:
		return applicationOriginal{}, fmt.Errorf("unsupported pending kind")
	}
}

func kvRequest(command types.KVCommand) network.KVMutationRequest {
	return network.KVMutationRequest{RequestID: command.RequestID, Key: command.Key, Value: bytes.Clone(command.Value), Expected: bytes.Clone(command.Expected), ExpectedExists: command.ExpectedExists, TTLMS: command.TTLMS}
}

func (h *Host) mixed() bool {
	return h.cfg.Plan.ApplicationProtocol == types.ManagedApplicationProtocolSQLKV || h.internalRequests()
}

func (h *Host) inspectEvidence(ctx context.Context, fresh, localRead bool) (json.RawMessage, error) {
	evidence, _, err := h.inspectEvidenceSnapshot(ctx, fresh, localRead)
	return evidence, err
}

// Keep the evidence and the exact runtime validated by its inspector together.
func (h *Host) inspectEvidenceSnapshot(ctx context.Context, fresh, localRead bool) (json.RawMessage, node.ManagedRuntimeState, error) {
	if h.cfg.Plan.ApplicationProtocol == sqlpolicy.ManagedGraphRequestApplicationProtocol {
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
	if h.mixed() {
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
	var state node.ManagedSQLSnapshot
	var err error
	if localRead {
		state, err = h.node.InspectManagedSQLRead(ctx)
	} else {
		state, err = h.node.InspectManagedSQL(ctx, fresh)
	}
	if err != nil {
		return nil, node.ManagedRuntimeState{}, err
	}
	return encodeEvidence(state), state.Runtime, nil
}

func (h *Host) ValidateManagedKVWrite(ctx context.Context, i node.ManagedStartupIdentity, _ node.ManagedRuntimeState, operation string, req network.KVMutationRequest) error {
	return h.validateKVOriginal(ctx, i, operation, req, true)
}

func (h *Host) ValidateManagedKVRecovery(ctx context.Context, i node.ManagedStartupIdentity, operation string, req network.KVMutationRequest) error {
	return h.validateKVOriginal(ctx, i, operation, req, false)
}

func (h *Host) validateKVOriginal(ctx context.Context, i node.ManagedStartupIdentity, operation string, req network.KVMutationRequest, activeRequired bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !h.mixed() || i != h.identity {
		return fmt.Errorf("execution identity or application profile changed")
	}
	raw, command, err := types.EncodeManagedKVOriginal(types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS})
	if err != nil {
		return err
	}
	w, err := (applicationOriginal{kind: "kv", kv: command}).identity()
	if err != nil {
		return err
	}
	if h.internalRequests() {
		return nil // Actual Node runtime gate and canonical resolver own profile 9.
	}
	if !activeRequired {
		if value := ctx.Value(remoteRecoveryContextKey{}); value != nil {
			proof, ok := value.(remoteRecoveryProof)
			if !ok || proof.issuer != h || proof.request.Kind != "kv" || proof.request != w || proof.resourceVersion == "" || proof.original != string(raw) || !proofScopeOpen(proof) {
				return fmt.Errorf("invalid or expired remote recovery scope")
			}
			a, err := h.live(ctx)
			if err != nil {
				return err
			}
			if _, pending := a.Pending(); pending || a.ResourceVersion() != proof.resourceVersion || !proofScopeOpen(proof) {
				return fmt.Errorf("remote recovery authority or scope changed")
			}
			return nil
		}
	}
	a, err := h.live(ctx)
	if err != nil {
		return err
	}
	pending, ok := a.Pending()
	if !ok && !activeRequired {
		return errPendingAbsent
	}
	if !ok || pending != w {
		return fmt.Errorf("exact pending KV reservation required")
	}
	original, ok, err := a.PendingKV()
	if err != nil || !ok {
		return errors.Join(fmt.Errorf("original pending KV missing"), err)
	}
	stored, _, err := types.EncodeManagedKVOriginal(original)
	// Fingerprint equivalence alone does not authorize original replacement.
	if err != nil || !bytes.Equal(stored, raw) {
		return fmt.Errorf("original pending KV changed")
	}
	if activeRequired {
		h.gateMu.RLock()
		active := h.active
		h.gateMu.RUnlock()
		if active == nil || *active != w {
			return fmt.Errorf("exact locally owned pending KV reservation required")
		}
	}
	return nil
}

func (h *Host) KVMutate(ctx context.Context, operation string, req network.KVMutationRequest) (network.KVMutationResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	notSubmitted := func(stage SQLSubmissionStage, recovery bool, cause error) (network.KVMutationResponse, error) {
		return network.KVMutationResponse{}, &KVNotSubmittedError{Stage: stage, RecoveryRequired: recovery, Cause: cause}
	}
	if h.closed || !h.mixed() {
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
	if h.internalRequests() {
		if err := h.ensureInitialRequestAdmission(ctx); err != nil {
			return notSubmitted(SQLStageAdmission, false, err)
		}
		response, err := h.node.ExecuteManagedKV(ctx, original.Operation, kvRequest(original))
		p := applicationOriginal{kind: "kv", kv: original}
		err = internalRequestOutcome(p, response.MutationReceipt, err)
		h.rememberRequestOutcome(p, err, response.MutationReceipt)
		return response, err
	}
	a, err := h.live(ctx)
	if err != nil {
		return notSubmitted(SQLStageAuthority, false, err)
	}
	if a.SQLReservationsClosed() {
		return notSubmitted(SQLStageRepair, false, fmt.Errorf("application reservations closed during repair"))
	}
	if _, pending := a.Pending(); pending {
		return notSubmitted(SQLStagePending, true, ErrSQLPending)
	}
	if err := h.node.AdmitManaged(ctx); err != nil {
		return notSubmitted(SQLStageAdmission, errors.Is(err, ErrSQLPending) || errors.Is(err, ErrSQLRecoveryRequired), err)
	}
	if !h.node.Ready() {
		return notSubmitted(SQLStageAdmission, false, network.ErrNotReady)
	}
	evidence, err := h.inspectEvidence(ctx, false, false)
	if err != nil {
		return notSubmitted(SQLStageAdmission, false, err)
	}
	reserved, observed, err := operator.ReserveV1Alpha2KVWrite(ctx, h.cfg.Store, a, h.authority, evidence, original)
	if err != nil || observed != operator.V1Alpha2CASApplied {
		ambiguous := observed == operator.V1Alpha2CASUnknown || observed == operator.V1Alpha2CASMatchingReadback || errors.Is(err, operator.ErrV1Alpha2StatusOutcomeUnknown)
		if ambiguous {
			h.uncertain.Store(true)
		}
		return notSubmitted(SQLStageReservation, ambiguous, errors.Join(fmt.Errorf("KV reservation not owned"), err))
	}
	p, err := pendingOriginal(reserved)
	if err != nil || p.kind != "kv" {
		h.uncertain.Store(true)
		return notSubmitted(SQLStageReservation, true, errors.Join(fmt.Errorf("reserved original KV unavailable"), err))
	}
	w, _ := reserved.Pending()
	h.gateMu.Lock()
	h.active = &w
	h.gateMu.Unlock()
	defer func() { h.gateMu.Lock(); h.active = nil; h.gateMu.Unlock() }()
	response, executeErr := h.node.ExecuteManagedKV(ctx, p.kv.Operation, kvRequest(p.kv))
	_, finalizeErr := h.finalizeApplication(ctx, p, reserved)
	if finalizeErr != nil {
		h.uncertain.Store(true)
		return response, applicationOutcomeUnknown(p, response.MutationReceipt, errors.Join(executeErr, finalizeErr))
	}
	if executeErr != nil {
		return response, applicationOutcomeUnknown(p, response.MutationReceipt, executeErr)
	}
	return response, nil
}

func (h *Host) KVGet(ctx context.Context, req network.KVGetRequest) (network.KVGetResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || !h.mixed() {
		return network.KVGetResponse{}, fmt.Errorf("mixed managed Host required")
	}
	if err := h.ensureInitialRequestAdmission(ctx); err != nil {
		return network.KVGetResponse{}, err
	}
	return h.node.KVGetManaged(ctx, req)
}

// RequestStatus observes local retained history, not a durability ACK or write
// permission. It does not resolve or finalize pending requests.
func (h *Host) RequestStatus(ctx context.Context, req network.RequestStatusRequest) (network.RequestStatusResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || !h.mixed() {
		return network.RequestStatusResponse{}, fmt.Errorf("mixed managed Host required")
	}
	if err := h.ensureInitialRequestAdmission(ctx); err != nil {
		return network.RequestStatusResponse{}, err
	}
	if req.Kind == "graph" {
		return h.node.GraphRequestStatusManaged(ctx, req)
	}
	return h.node.RequestStatusManaged(ctx, req)
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

func (h *Host) validateOriginalRecovery(ctx context.Context, p applicationOriginal) error {
	if p.kind == "sql" {
		return h.ValidateManagedSQLRecovery(ctx, h.identity, p.sql)
	}
	if p.kind == "kv" {
		return h.ValidateManagedKVRecovery(ctx, h.identity, p.kv.Operation, kvRequest(p.kv))
	}
	return fmt.Errorf("unsupported pending kind")
}

func (h *Host) resolveOriginal(ctx context.Context, p applicationOriginal) (RecoveryResult, error) {
	result := RecoveryResult{Kind: p.kind, RequestID: []byte(p.rawID())}
	var receipt *types.MutationReceipt
	var confirmed bool
	switch p.kind {
	case "sql":
		r, err := h.node.ResolveManagedSQL(ctx, p.sql)
		if r.Receipt != nil {
			result.SQL = &ExecuteResponse{MutationReceipt: *r.Receipt, Statements: r.Statements}
		}
		if err != nil {
			return result, err
		}
		receipt, confirmed = r.Receipt, r.State == network.ResolveExecuteRetained && r.Durability == network.ResolveExecuteDurabilityConfirmed
	case "kv":
		r, err := h.node.ResolveManagedKV(ctx, p.kv.Operation, kvRequest(p.kv))
		if r.Receipt != nil {
			result.KV = &network.KVMutationResponse{MutationReceipt: *r.Receipt}
		}
		if err != nil {
			return result, err
		}
		receipt, confirmed = r.Receipt, r.State == network.ResolveKVRetained && r.Durability == network.ResolveKVDurabilityConfirmed
	default:
		return result, fmt.Errorf("unsupported pending kind")
	}
	if receipt == nil || !confirmed {
		return result, fmt.Errorf("application outcome or configured durability remains unknown")
	}
	return result, nil
}

func validateResolvedHistory(p applicationOriginal, result RecoveryResult, state node.ManagedRuntimeState) error {
	receipt := result.receipt()
	terminal := receipt.Status == types.MutationCommitted || p.kind == "sql" && receipt.Status == types.MutationRejected
	if receipt.Slot == 0 || receipt.Slot > state.CertifiedTip || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip || !terminal {
		return fmt.Errorf("receipt is outside terminal certified history: slot=%d certified=%d applied=%d recorder=%d terminal=%t sql=%t", receipt.Slot, state.CertifiedTip, state.AppliedTip, state.RecorderTip, terminal, p.kind == "sql")
	}
	return nil
}

func (r RecoveryResult) receipt() types.MutationReceipt {
	if r.SQL != nil {
		return r.SQL.MutationReceipt
	}
	if r.KV != nil {
		return r.KV.MutationReceipt
	}
	return types.MutationReceipt{}
}

func (h *Host) finalizeApplication(ctx context.Context, p applicationOriginal, a operator.V1Alpha2AdmissionSnapshot) (RecoveryResult, error) {
	remote := func(cause error) (RecoveryResult, error) {
		result, err := h.verifyRemoteApplication(ctx, p, a)
		if err != nil {
			return result, errors.Join(cause, err)
		}
		return result, nil
	}
	if err := h.validateOriginalRecovery(ctx, p); err != nil {
		return remote(err)
	}
	result, err := h.resolveOriginal(ctx, p)
	if err != nil {
		if p.kind == "kv" && onlyPendingAbsent(err) {
			return remote(err)
		}
		return result, err
	}
	evidence, runtime, err := h.inspectEvidenceSnapshot(ctx, false, false)
	if err != nil {
		return result, err
	}
	if err := validateResolvedHistory(p, result, runtime); err != nil {
		return result, err
	}
	if err := h.validateOriginalRecovery(ctx, p); err != nil {
		return remote(err)
	}
	w, _ := a.Pending()
	_, observed, err := operator.FinalizeV1Alpha2Write(ctx, h.cfg.Store, a, h.authority, w, operator.V1Alpha2WriteCommitted, evidence)
	if errors.Is(err, operator.ErrV1Alpha2StatusConflict) {
		return remote(err)
	}
	if err != nil || observed != operator.V1Alpha2CASApplied {
		return result, errors.Join(fmt.Errorf("application finalization not directly confirmed"), err)
	}
	return result, nil
}

// Only this exact fresh-check reason can route once to remote verification.
// A joined barrier/poison/mismatch error must remain UNKNOWN.
func onlyPendingAbsent(err error) bool {
	if err == errPendingAbsent {
		return true
	}
	if unknown, ok := err.(*network.CommitUnknownError); ok {
		return onlyPendingAbsent(unknown.Cause)
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !onlyPendingAbsent(cause) {
				return false
			}
		}
		return true
	}
	return false
}

func (h *Host) resolveRemoteOriginal(ctx context.Context, p applicationOriginal, w operator.V1Alpha2WriteRequest, rv, canonical string) (RecoveryResult, error) {
	scope, cancel := context.WithCancel(ctx)
	defer cancel()
	proof := remoteRecoveryProof{issuer: h, request: w, resourceVersion: rv, original: canonical, scopeDone: scope.Done()}
	return h.resolveOriginal(context.WithValue(scope, remoteRecoveryContextKey{}, proof), p)
}

func (h *Host) verifyRemoteApplication(ctx context.Context, p applicationOriginal, reserved operator.V1Alpha2AdmissionSnapshot) (RecoveryResult, error) {
	want, err := p.identity()
	pending, ok := reserved.Pending()
	if err != nil || !ok || pending != want || reserved.Authority() != h.authority {
		return RecoveryResult{}, fmt.Errorf("exact original reservation required for remote finalization")
	}
	before, err := h.live(ctx)
	if err != nil {
		return RecoveryResult{}, err
	}
	oldRepair, oldOK := reserved.Repair()
	repair, repairOK := before.Repair()
	if _, pending := before.Pending(); pending || oldOK != repairOK || !reflect.DeepEqual(oldRepair, repair) {
		return RecoveryResult{}, fmt.Errorf("pending SQL or repair changed during remote finalization")
	}
	if err := h.node.AdmitManaged(ctx); err != nil {
		return RecoveryResult{}, err
	}
	var result RecoveryResult
	if p.kind == "kv" {
		stored, ok, err := reserved.PendingKV()
		if err != nil || !ok {
			return RecoveryResult{}, errors.Join(fmt.Errorf("original remote KV reservation missing"), err)
		}
		raw, _, err := types.EncodeManagedKVOriginal(stored)
		if err != nil {
			return RecoveryResult{}, err
		}
		requested, _, err := types.EncodeManagedKVOriginal(p.kv)
		if err != nil || !bytes.Equal(raw, requested) {
			return RecoveryResult{}, fmt.Errorf("original remote KV reservation changed")
		}
		result, err = h.resolveRemoteOriginal(ctx, p, want, before.ResourceVersion(), string(raw))
	} else {
		result, err = h.resolveOriginal(ctx, p)
	}
	if err != nil {
		return result, errors.Join(fmt.Errorf("remote finalization lacks exact durable receipt"), err)
	}
	evidence, runtime, err := h.inspectEvidenceSnapshot(ctx, false, false)
	if err != nil {
		return result, errors.Join(fmt.Errorf("remote finalization differs from canonical SQL evidence"), err)
	}
	if err := validateResolvedHistory(p, result, runtime); err != nil {
		return result, err
	}
	if evidenceHash(before.Evidence()) != evidenceHash(evidence) {
		return result, errors.Join(fmt.Errorf("remote finalization differs from canonical SQL evidence"), err)
	}
	after, err := h.live(ctx)
	if err != nil || after.ResourceVersion() != before.ResourceVersion() {
		return result, errors.Join(fmt.Errorf("remote finalization authority changed during proof"), err)
	}
	return result, nil
}

// RecoverPendingDetailed never submits. The original kind/raw ID and result
// are captured once for this invocation, including when finalization fails.
func (h *Host) RecoverPendingDetailed(ctx context.Context) (RecoveryResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return RecoveryResult{}, fmt.Errorf("SQL host closed")
	}
	if h.internalRequests() {
		if h.recoveryTargetOverflow || len(h.recoveryTargets) != 1 {
			return RecoveryResult{}, &network.CommitUnknownError{Cause: fmt.Errorf("exact recovery target required or ambiguous")}
		}
		for selector, hint := range h.recoveryTargets {
			if hint.ambiguous {
				return RecoveryResult{}, &network.CommitUnknownError{Cause: fmt.Errorf("recovery target intent is ambiguous")}
			}
			return h.recoverRequestLocked(ctx, selector, &hint.fingerprint)
		}
		return RecoveryResult{}, &network.CommitUnknownError{Cause: fmt.Errorf("exact recovery target required")}
	}
	a, err := h.live(ctx)
	if err != nil {
		return RecoveryResult{}, err
	}
	if _, ok := a.Pending(); !ok {
		h.uncertain.Store(false)
		err := h.node.AdmitManaged(ctx)
		if err != nil {
			h.uncertain.Store(true)
		}
		return RecoveryResult{}, err
	}
	p, err := pendingOriginal(a)
	if err != nil {
		return RecoveryResult{}, err
	}
	result, err := h.finalizeApplication(ctx, p, a)
	if err != nil {
		h.uncertain.Store(true)
		return result, applicationOutcomeUnknown(p, result.receipt(), err)
	}
	h.uncertain.Store(false)
	return result, nil
}
