package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
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

// The cut is anchored in the actual admitted Core, not a later recorder tip.
// A compacted original is covered by the already-verified local recovery root.
type managedRequestResolutionCut struct {
	through quepaxa.Slot
	prefix  [32]byte
	floor   quepaxa.Slot
	root    [32]byte
}

func (n *Node) captureManagedRequestResolutionCut(ctx context.Context, original materializer.RequestOriginal) (managedRequestResolutionCut, error) {
	var cut managedRequestResolutionCut
	if original.Slot == 0 || original.Receipt.Slot != original.Slot {
		return cut, fmt.Errorf("request original receipt position is invalid")
	}
	cut.floor, cut.root, _ = n.core.RecoveryRoot()
	cut.through = quepaxa.Slot(original.Slot)
	if cut.through <= cut.floor {
		if cut.floor == 0 || cut.root == ([32]byte{}) {
			return cut, fmt.Errorf("request original recovery root is unavailable")
		}
		cut.through = cut.floor
	} else {
		decision, found := n.core.CertifiedValue(cut.through)
		if !found || !managedRequestCertifiedOriginalMatches(decision, original) {
			return cut, fmt.Errorf("request original differs from its exact certified slot")
		}
		if err := n.core.ValidateCertifiedValue(ctx, decision); err != nil {
			return cut, err
		}
	}
	var exists bool
	cut.prefix, exists = n.core.PrefixHash(cut.through)
	if !exists || cut.prefix == ([32]byte{}) {
		return cut, fmt.Errorf("request original certified prefix is unavailable")
	}
	return cut, nil
}

// SQL/KV originals identify one canonical item within a certified batch;
// only Graph retains the whole decision hash and execution frame.
func managedRequestCertifiedOriginalMatches(decision quepaxa.DecidedValue, original materializer.RequestOriginal) bool {
	if uint64(decision.Slot) != original.Slot || len(decision.Certificate) == 0 {
		return false
	}
	switch original.Kind {
	case types.MutationGraph:
		return decision.Hash == original.DecisionHash && bytes.Equal(decision.Value, original.ExecutionItem)
	case types.MutationSQL:
		commands, ok, err := types.DecodeSQLBatch(decision.Value)
		if err != nil || !ok || uint64(original.BatchIndex) >= uint64(len(commands)) {
			return false
		}
		command := commands[original.BatchIndex]
		item, err := types.EncodeSQLBatchItem(command)
		if err != nil || !bytes.Equal(item, original.ExecutionItem) || !bytes.Equal(item, original.CanonicalOriginal) {
			return false
		}
		fingerprint, err := types.SQLFingerprint(command)
		return err == nil && fingerprint == original.Fingerprint && command.RequestID == original.RequestID
	case types.MutationKV:
		commands, ok, err := types.DecodeManagedKVBatch(decision.Value)
		if err != nil || !ok || uint64(original.BatchIndex) >= uint64(len(commands)) {
			return false
		}
		command := commands[original.BatchIndex]
		item, err := types.EncodeManagedKVBatchItem(command)
		if err != nil || !bytes.Equal(item, original.ExecutionItem) {
			return false
		}
		canonical, _, err := types.EncodeManagedKVOriginal(command)
		if err != nil || !bytes.Equal(canonical, original.CanonicalOriginal) {
			return false
		}
		fingerprint, err := types.ManagedKVFingerprint(command)
		return err == nil && fingerprint == original.Fingerprint && command.RequestID == original.RequestID
	default:
		return false
	}
}

func (n *Node) managedRequestResolutionCutCovered(cut managedRequestResolutionCut, state ManagedRuntimeState) bool {
	if cut.through == 0 || cut.prefix == ([32]byte{}) {
		return false
	}
	floor, root, _ := n.core.RecoveryRoot()
	prefix, exists := n.core.PrefixHash(cut.through)
	return floor == cut.floor && root == cut.root && exists && prefix == cut.prefix &&
		state.CertifiedTip >= uint64(cut.through) && state.AppliedTip >= uint64(cut.through)
}

type managedGraphResolutionDiagnosticState struct {
	originalSlot, cut, floor, recorded, certified, applied                    uint64
	certificatePresent, certificateMatches, bindingCurrent, voter, cutCovered bool
}

func managedGraphResolutionFailureDiagnostic(leaf string, state managedGraphResolutionDiagnosticState, err error) string {
	if err == nil {
		return ""
	}
	switch leaf {
	case "original_certificate", "durability_result", "runtime_post", "original_recheck":
	default:
		return ""
	}
	cause := managedMembershipCauseClass(err)
	if cause == "other" && errors.Is(err, network.ErrDurabilityUnavailable) {
		cause = "durability_unavailable"
	}
	return fmt.Sprintf("MANAGED_GRAPH_RESOLUTION_FAILURE leaf=%s original_slot=%d cut=%d floor=%d recorded=%d certified=%d applied=%d certificate_present=%t certificate_matches=%t binding_current=%t voter=%t cut_covered=%t cause=%s", leaf, state.originalSlot, state.cut, state.floor, state.recorded, state.certified, state.applied, state.certificatePresent, state.certificateMatches, state.bindingCurrent, state.voter, state.cutCovered, cause)
}

func logManagedGraphResolutionFailure(leaf string, state managedGraphResolutionDiagnosticState, err error) error {
	if line := managedGraphResolutionFailureDiagnostic(leaf, state, err); line != "" {
		log.Print(line)
	}
	return err
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
	var cut managedRequestResolutionCut
	leaf := "original_certificate"
	fail := func(cause error) (ManagedRequestResolution, error) {
		if k == types.MutationGraph {
			progress := n.core.RuntimeProgressSnapshot()
			decision, present := n.core.CertifiedValue(quepaxa.Slot(original.Slot))
			state := ManagedRuntimeState{CertifiedTip: uint64(progress.CertifiedTip), AppliedTip: n.material.Tip()}
			cause = logManagedGraphResolutionFailure(leaf, managedGraphResolutionDiagnosticState{
				originalSlot: original.Slot, cut: uint64(cut.through), floor: uint64(n.core.CompactionFloor()), recorded: uint64(progress.RecorderTip), certified: state.CertifiedTip, applied: state.AppliedTip,
				certificatePresent: present, certificateMatches: present && managedRequestCertifiedOriginalMatches(decision, original),
				bindingCurrent: bound != nil && bound == n.internalApplication.Load() && bound.core == n.core && bound.lifetime != nil && bound.lifetime.Err() == nil, voter: n.core.IsVoter(), cutCovered: cut.through != 0 && n.managedRequestResolutionCutCovered(cut, state),
			}, cause)
		}
		if k != types.MutationGraph {
			cause = managedRequestResolutionStageFailure("resolve_"+leaf, cause)
		}
		logManagedRequestOriginalFound(original, k, rawID)
		return result, &network.CommitUnknownError{RequestID: rawID, Slot: quepaxa.Slot(original.Slot), RetryThroughSlot: original.Receipt.RetryThroughSlot, Cause: cause}
	}
	cut, err = n.captureManagedRequestResolutionCut(ctx, original)
	if err != nil {
		return fail(err)
	}
	leaf = "durability_result"
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
	localtesthooks.Hit("node:managed-request:resolved-before-runtime-recheck")
	leaf = "runtime_post"
	state, err := n.checkInternalApplicationRuntime(ctx)
	// The exact retained receipt has passed its configured durability barrier.
	// Later ISR or certified suffix work cannot invalidate this fixed cut.
	historyCovered := n.managedRequestResolutionCutCovered(cut, state)
	if err != nil || bound == nil || bound != n.internalApplication.Load() || !n.core.IsVoter() || !historyCovered {
		return fail(errors.Join(fmt.Errorf("request runtime changed during resolution"), err))
	}
	leaf = "original_recheck"
	after, found, err := n.material.RetainedRequestOriginal(ctx, k, rawID)
	if err != nil || !found || after.Kind != original.Kind || after.RequestID != original.RequestID || after.Slot != original.Slot || after.BatchIndex != original.BatchIndex || after.Fingerprint != original.Fingerprint || after.Receipt != original.Receipt || after.DecisionHash != original.DecisionHash || !bytes.Equal(after.ExecutionItem, original.ExecutionItem) || !bytes.Equal(after.CanonicalOriginal, original.CanonicalOriginal) {
		return fail(errors.Join(fmt.Errorf("exact retained original changed during resolution"), err))
	}
	localtesthooks.Hit("node:managed-request:original-rechecked")
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if bound.lifetime == nil || bound.lifetime.Err() != nil || bound != n.internalApplication.Load() || !n.core.IsVoter() || !n.managedRequestResolutionCutCovered(cut, state) {
		return fail(fmt.Errorf("request runtime changed after original recheck"))
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
