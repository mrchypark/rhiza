package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

var errLogicalMutationExecutionUnavailable = errors.New("fencing: logical mutation provider is unavailable")

func (e *FencingExecutor) resolveLogicalWALContinuationGrant(ctx context.Context, uid string, request fencing.Identity, walIdentity string) (fencing.StartupGrant, error) {
	if e == nil || e.AuthorityProfile != fencing.LogicalWriterProfileV1 || !fencing.MutationProfileSupported(e.sourceMutationProfile()) ||
		uid == "" || request.WALIdentity == "" || request.WALIdentity != walIdentity || !validEvidenceDigest(walIdentity) {
		return fencing.StartupGrant{}, fencing.ErrClosed
	}
	root, err := e.read(ctx, uid, request.SourceClusterID)
	if err != nil {
		return fencing.StartupGrant{}, err
	}
	if err := e.validateMutationJournalProfile(root); err != nil {
		return fencing.StartupGrant{}, err
	}
	run, ok := root.Ledger.Runs[request.RunID]
	if !ok || root.Ledger.Profile != fencing.LogicalWriterProfileV1 || root.Ledger.ClosedBy != "" || root.Ledger.ClosedHash != "" ||
		run.LogicalBinding == nil || run.LogicalBinding.ValidateBound() != nil || run.LogicalBinding.Role != fencing.StartupRoleVoterActive ||
		run.Stop != nil || run.LogicalClosure != nil || fencing.RunRole(run) != fencing.StartupRoleVoterActive {
		return fencing.StartupGrant{}, fencing.ErrClosed
	}
	registered := run.Identity
	registered.WALIdentity = ""
	requested := request
	requested.WALIdentity = ""
	if registered != requested || run.Identity.WALIdentity != walIdentity || run.LogicalBinding.WALIdentity != walIdentity ||
		run.LogicalBinding.AssignmentID != "" || run.LogicalBinding.Recovery != (fencing.LogicalRecoveryBindingV1{}) {
		return fencing.StartupGrant{}, fencing.ErrClosed
	}
	grant, static, err := e.resolveStaticVoterStartupGrant(ctx, uid, requested)
	if err != nil || !static || grant.AuthorityProfile != fencing.LogicalWriterProfileV1 || grant.LogicalBinding == nil {
		return fencing.StartupGrant{}, errors.Join(err, fencing.ErrClosed)
	}
	canonicalBinding := *run.LogicalBinding
	canonicalBinding.WALIdentity = ""
	if *grant.LogicalBinding != canonicalBinding {
		return fencing.StartupGrant{}, fencing.ErrClosed
	}
	grant.Identity = requested
	grant.Identity.WALIdentity = ""
	grant.LogicalBinding = &canonicalBinding
	if err := grant.Validate(requested); err != nil {
		return fencing.StartupGrant{}, errors.Join(err, fencing.ErrClosed)
	}
	return grant, nil
}

func (e *FencingExecutor) executeLogicalSourceMutationRequest(ctx context.Context, uid string, id fencing.Identity, request sourceMutationRequest) error {
	if e == nil || e.AuthorityState == nil {
		return errLogicalMutationExecutionUnavailable
	}
	root, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return err
	}
	if err := e.validateMutationJournalProfile(root); err != nil || root.Ledger.Profile != fencing.LogicalWriterProfileV1 {
		return errors.Join(err, fmt.Errorf("fencing: logical mutation execution is outside the selected profile"))
	}
	run, ok := root.Ledger.Runs[id.RunID]
	if !ok || run.Identity != id || run.Runtime != nil || run.LogicalBinding == nil || run.LogicalBinding.ValidateBound() != nil ||
		run.LogicalBinding.Role != fencing.StartupRoleVoterActive || run.MutationProducerUID != request.ProducerUID ||
		run.MutationScopeHash == "" || run.MutationScopeHash != request.Intent.StoreScopeHash || run.MutationProfile == "" ||
		request.Sequence == 0 || request.Intent.OperationID != fencing.MutationRequestID(request.ProducerUID, request.Sequence) {
		return fmt.Errorf("fencing: logical mutation is outside the exact enrolled producer scope")
	}
	binding := *run.LogicalBinding
	if binding.WALIdentity != id.WALIdentity || binding.ProducerUID != request.ProducerUID || binding.StoreScopeHash != request.Intent.StoreScopeHash {
		return fmt.Errorf("fencing: logical mutation binding differs from the canonical Run")
	}
	scope, err := e.sourceMutationStorageScopeForRoot(ctx, uid, id.SourceClusterID)
	if err != nil || scope.Provider != "gcs" || hashJSON(scope) != run.MutationScopeHash {
		return errors.Join(err, fmt.Errorf("fencing: logical mutation storage scope differs"))
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, run.MutationProfile)
	if err != nil {
		return err
	}
	receipts, err := e.receiptsForLogicalBinding(uid, binding)
	if err != nil {
		return err
	}
	return e.ExecuteLogicalSourceMutation(ctx, uid, binding, request.ProducerUID, request.Sequence, request.Intent, scope, provider, receipts)
}

func (e *FencingExecutor) reserveLogicalMutationProducer(ctx context.Context, statefulSetUID string, binding fencing.LogicalWriterBindingV1) error {
	if e == nil || e.AuthorityState == nil || binding.Validate() != nil || binding.Role != fencing.StartupRoleVoterActive || binding.ProducerUID == "" || binding.StoreScopeHash == "" {
		return fmt.Errorf("fencing: logical producer grant is incomplete")
	}
	producer, err := fencing.NewLogicalMutationProducer(binding)
	if err != nil {
		return err
	}
	if _, _, _, err := e.AuthorityState.ReadLogicalProducer(ctx, statefulSetUID, binding); err == nil {
		return nil
	} else if !errors.Is(err, errAuthorityStateNotFound) {
		return err
	}
	if err := e.AuthorityState.WriteLogicalProducer(ctx, statefulSetUID, binding, *producer, nil); err != nil {
		if errors.Is(err, errAuthorityStateConflict) {
			stored, _, _, readErr := e.AuthorityState.ReadLogicalProducer(ctx, statefulSetUID, binding)
			if readErr == nil && stored.Profile == fencing.LogicalWriterProfileV1 && sameLogicalProducerBinding(stored.LogicalBinding, &binding) {
				return nil
			}
			return errors.Join(err, readErr)
		}
		return err
	}
	return nil
}

func (e *FencingExecutor) bindLogicalSupervisorWAL(ctx context.Context, uid string, id fencing.Identity) (fencing.BindReceipt, error) {
	state, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	if err := e.validateMutationJournalProfile(state); err != nil || state.Ledger.Profile != fencing.LogicalWriterProfileV1 || id.WALIdentity == "" {
		return fencing.BindReceipt{}, errors.Join(err, fmt.Errorf("fencing: logical WAL bind is outside its selected profile"))
	}
	run, ok := state.Ledger.Runs[id.RunID]
	if !ok || run.Identity.SourceClusterID != id.SourceClusterID || run.Identity.NodeID != id.NodeID || run.Identity.RunID != id.RunID ||
		run.LogicalBinding == nil || run.Runtime != nil || run.Stop != nil {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: logical WAL bind does not match a live logical Run")
	}
	binding := *run.LogicalBinding
	if binding.Validate() != nil {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: logical WAL bind lacks a valid authority binding")
	}
	if _, err := e.change(ctx, uid, id.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.BindLogicalWAL(id)
	}); err != nil {
		return fencing.BindReceipt{}, err
	}
	root, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	boundRun, ok := root.Ledger.Runs[id.RunID]
	if !ok || boundRun.Identity != id || boundRun.LogicalBinding == nil || boundRun.LogicalBinding.ValidateBound() != nil {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: logical WAL binding was not durably pinned")
	}
	logicalBinding := *boundRun.LogicalBinding
	if logicalBinding.Role == fencing.StartupRoleLearnerPendingRemove {
		receipt := fencing.BindReceipt{Identity: id, Role: boundRun.Role, AuthorityProfile: fencing.LogicalWriterProfileV1, LogicalBinding: &logicalBinding}
		return receipt, receipt.Validate(id)
	}
	if logicalBinding.Role != fencing.StartupRoleVoterActive || logicalBinding.ProducerUID == "" || logicalBinding.StoreScopeHash == "" {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: logical WAL bind lacks active producer authority")
	}
	producer, producerUID, version, err := e.AuthorityState.readLogicalProducerForWALBind(ctx, uid, logicalBinding)
	if err != nil {
		return fencing.BindReceipt{}, err
	}
	if producerUID != boundRun.MutationProducerUID || (producer.Phase != fencing.MutationReserved && producer.Phase != fencing.MutationOpen) {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: logical producer is not reserved for WAL activation")
	}
	if producer.Phase == fencing.MutationReserved {
		if err := producer.Activate(id); err != nil {
			return fencing.BindReceipt{}, err
		}
		if err := e.AuthorityState.WriteLogicalProducer(ctx, uid, *boundRun.LogicalBinding, producer, version); err != nil {
			return fencing.BindReceipt{}, err
		}
	} else if producer.WALIdentity != id.WALIdentity {
		return fencing.BindReceipt{}, fmt.Errorf("fencing: logical producer WAL differs from canonical Run")
	}
	receipt := fencing.BindReceipt{Identity: id, Role: boundRun.Role, AuthorityProfile: fencing.LogicalWriterProfileV1, LogicalBinding: &logicalBinding,
		Producer: &fencing.ProducerBinding{ContractVersion: fencing.MutationIntentVersion, Profile: fencing.LogicalWriterProfileV1,
			LogicalBinding: &logicalBinding, ProducerUID: producerUID, StoreScopeHash: logicalBinding.StoreScopeHash,
			ExecutionProfile: boundRun.MutationProfile, StageIncarnation: boundRun.MutationStageIncarnation}}
	return receipt, receipt.Validate(id)
}

// recordLogicalRunClosure is the only operator path that turns a logical Run
// stop into reusable-slot storage closure. It first seals producer admission,
// then requires a complete journal barrier and a trusted storage verifier.
// Stop/exit state alone can never produce a LogicalRunClosureReference.
func (e *FencingExecutor) recordLogicalRunClosure(ctx context.Context, uid string, id fencing.Identity) error {
	if e == nil || e.AuthorityState == nil {
		return fmt.Errorf("fencing: logical storage closure verifier is unavailable")
	}
	root, err := e.read(ctx, uid, id.SourceClusterID)
	if err != nil {
		return err
	}
	if err := e.validateMutationJournalProfile(root); err != nil || root.Ledger.Profile != fencing.LogicalWriterProfileV1 {
		return errors.Join(err, fmt.Errorf("fencing: logical storage closure is outside its selected profile"))
	}
	run, ok := root.Ledger.Runs[id.RunID]
	if !ok || run.Identity != id || run.LogicalBinding == nil || run.LogicalBinding.Validate() != nil ||
		run.LogicalBinding.Role != fencing.StartupRoleVoterActive || run.Runtime != nil || run.Stop == nil || run.Stop.Identity != id ||
		run.MutationProducerUID == "" || run.MutationScopeHash == "" || run.MutationProfile == "" {
		return fmt.Errorf("fencing: logical storage closure lacks an exact stopped active Run")
	}
	binding := *run.LogicalBinding
	if id.WALIdentity == "" || binding.WALIdentity == "" {
		return e.recordLogicalUnboundNoWorkClosure(ctx, uid, id, run, binding)
	}
	if binding.ValidateBound() != nil {
		return fmt.Errorf("fencing: logical storage closure lacks its exact bound WAL")
	}
	for range 8 {
		producer, producerUID, version, readErr := e.AuthorityState.ReadLogicalProducer(ctx, uid, binding)
		if readErr != nil {
			return readErr
		}
		if producerUID != run.MutationProducerUID || producer.WALIdentity != id.WALIdentity || producer.Profile != fencing.LogicalWriterProfileV1 {
			return fmt.Errorf("fencing: logical storage closure producer differs from canonical Run")
		}
		if producer.Phase != fencing.MutationClosed {
			if err := producer.Close(run.Stop.OperationID, run.Stop.RequestHash); err != nil {
				return err
			}
			if err := e.AuthorityState.WriteLogicalProducer(ctx, uid, binding, producer, version); err != nil {
				if errors.Is(err, errAuthorityStateConflict) {
					continue
				}
				return err
			}
			continue
		}
		if producer.CloseOperationID != run.Stop.OperationID || producer.CloseRequestHash != run.Stop.RequestHash || producer.ClosedThrough != producer.LastAdmitted {
			return fencing.ErrPending
		}
		evidenceName, evidenceUID, evidenceDigest, verifyErr := e.verifyLogicalStorageClosure(ctx, uid, id, binding, run, producerUID, producer)
		if verifyErr != nil {
			return verifyErr
		}
		ref := fencing.LogicalRunClosureReference{Version: fencing.LogicalRunClosureReferenceVersion, Identity: id,
			Kind:        fencing.LogicalRunClosureBoundWAL,
			ProducerUID: producerUID, StoreScopeHash: run.MutationScopeHash, CloseOperationID: run.Stop.OperationID,
			CloseRequestHash: run.Stop.RequestHash, ClosedThrough: producer.ClosedThrough,
			ProducerStateDigest: hashJSON(producer), EvidenceName: evidenceName, EvidenceUID: evidenceUID, EvidenceDigest: evidenceDigest}
		if _, err := e.change(ctx, uid, id.SourceClusterID, func(ledger *fencing.Ledger) error {
			current, exists := ledger.Runs[id.RunID]
			if !exists || current.Identity != id || current.LogicalBinding == nil || *current.LogicalBinding != binding || current.Stop == nil || *current.Stop != *run.Stop {
				return fencing.ErrClosed
			}
			return ledger.RecordLogicalRunClosure(ref)
		}); err != nil {
			return err
		}
		return nil
	}
	return fencing.ErrUnavailable
}

// An unbound Run is reusable only when its canonical Stop predates WAL
// activation and its exact reserved producer is closed with no journal work.
// A missing or unreadable producer is not evidence of zero work.
func (e *FencingExecutor) recordLogicalUnboundNoWorkClosure(ctx context.Context, uid string, id fencing.Identity, run fencing.Run,
	binding fencing.LogicalWriterBindingV1) error {
	if id.WALIdentity != "" || binding.WALIdentity != "" || run.Stop == nil || run.Stop.Identity != id ||
		run.Stop.OperationID == "" || !validEvidenceDigest(run.Stop.RequestHash) || run.MutationProducerUID != binding.ProducerUID ||
		run.MutationScopeHash != binding.StoreScopeHash || binding.ProducerUID == "" || !validEvidenceDigest(binding.StoreScopeHash) ||
		run.MutationProfile == "" || run.MutationStageIncarnation == "" {
		return fmt.Errorf("fencing: unbound logical closure lacks the exact stopped zero-work scope")
	}
	for range 8 {
		producer, producerUID, version, err := e.AuthorityState.ReadLogicalProducer(ctx, uid, binding)
		if err != nil {
			return errors.Join(ErrFencePending, err)
		}
		if producerUID != run.MutationProducerUID || producer.Profile != fencing.LogicalWriterProfileV1 ||
			!sameLogicalProducerBinding(producer.LogicalBinding, &binding) || producer.WALIdentity != "" ||
			producer.LastAdmitted != 0 || producer.ClosedThrough != 0 || producer.CompactedThrough != 0 ||
			producer.RetiredStageThrough != 0 || producer.ArchiveHead != nil || producer.ArchiveScan != nil ||
			producer.EvidenceFault != "" || len(producer.Entries) != 0 {
			return fmt.Errorf("fencing: unbound logical producer has activation or mutation evidence")
		}
		if producer.Phase == fencing.MutationClosed {
			if producer.CloseOperationID != run.Stop.OperationID || producer.CloseRequestHash != run.Stop.RequestHash || producer.ClosedThrough != 0 {
				return fencing.ErrClosed
			}
		} else if producer.Phase == fencing.MutationReserved {
			if err := producer.Close(run.Stop.OperationID, run.Stop.RequestHash); err != nil {
				return err
			}
			if err := e.AuthorityState.WriteLogicalProducer(ctx, uid, binding, producer, version); err != nil {
				if errors.Is(err, errAuthorityStateConflict) {
					continue
				}
				return err
			}
			continue
		} else {
			return fmt.Errorf("fencing: unbound logical producer is not reserved or exactly closed")
		}
		if err := producer.Validate(); err != nil {
			return errors.Join(ErrFencePending, err)
		}
		evidenceStore, err := e.authorityEvidenceForRoot(uid, id.SourceClusterID)
		if err != nil || evidenceStore.Identity() == "" {
			return errors.Join(ErrFencePending, err)
		}
		proof := logicalStorageClosureEvidence{Version: 1, ClosureKind: fencing.LogicalRunClosureUnboundNoWork,
			RootIdentity: evidenceStore.Identity(), Identity: id, Binding: binding, ProducerUID: producerUID,
			StoreScopeHash: run.MutationScopeHash, CloseOperationID: run.Stop.OperationID,
			CloseRequestHash: run.Stop.RequestHash, ProducerStateDigest: hashJSON(producer)}
		body, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		name := "logical-run-closures/" + hashJSON([]any{"logical-run-closure-unbound-no-work-v1", uid, id}) + ".json"
		evidenceVersion, err := evidenceStore.CreateImmutable(ctx, name, body)
		if err != nil {
			return err
		}
		stored, storedVersion, err := evidenceStore.Read(ctx, name)
		if err != nil {
			return err
		}
		readback, readErr := io.ReadAll(io.LimitReader(stored, int64(len(body))+1))
		closeErr := stored.Close()
		if err := errors.Join(readErr, closeErr); err != nil || !bytes.Equal(readback, body) || hashJSON(storedVersion) != hashJSON(evidenceVersion) {
			return errors.Join(err, fmt.Errorf("fencing: unbound logical closure evidence readback differs"))
		}
		ref := fencing.LogicalRunClosureReference{Version: fencing.LogicalRunClosureReferenceVersion,
			Kind: fencing.LogicalRunClosureUnboundNoWork, Identity: id, ProducerUID: producerUID,
			StoreScopeHash: run.MutationScopeHash, CloseOperationID: run.Stop.OperationID,
			CloseRequestHash: run.Stop.RequestHash, ProducerStateDigest: hashJSON(producer),
			EvidenceName: name, EvidenceUID: hashJSON(evidenceVersion), EvidenceDigest: authorityStateDigest(body)}
		if _, err := e.change(ctx, uid, id.SourceClusterID, func(ledger *fencing.Ledger) error {
			current, exists := ledger.Runs[id.RunID]
			if !exists || current.Identity != id || current.LogicalBinding == nil || *current.LogicalBinding != binding ||
				current.Runtime != nil || current.Stop == nil || *current.Stop != *run.Stop {
				return fencing.ErrClosed
			}
			return ledger.RecordLogicalRunClosure(ref)
		}); err != nil {
			return err
		}
		return nil
	}
	return fencing.ErrUnavailable
}

type logicalStorageClosureEvidence struct {
	Version             int                               `json:"version"`
	ClosureKind         string                            `json:"closureKind"`
	RootIdentity        string                            `json:"rootIdentity"`
	Identity            fencing.Identity                  `json:"identity"`
	Binding             fencing.LogicalWriterBindingV1    `json:"binding"`
	ProducerUID         string                            `json:"producerUID"`
	StoreScopeHash      string                            `json:"storeScopeHash"`
	CloseOperationID    string                            `json:"closeOperationID"`
	CloseRequestHash    string                            `json:"closeRequestHash"`
	ClosedThrough       uint64                            `json:"closedThrough"`
	CompactedThrough    uint64                            `json:"compactedThrough,omitempty"`
	ArchiveHead         *fencing.MutationArchiveReference `json:"archiveHead,omitempty"`
	ClosedPrefixDigest  string                            `json:"closedPrefixDigest,omitempty"`
	EntryCount          uint64                            `json:"entryCount,omitempty"`
	ProducerStateDigest string                            `json:"producerStateDigest"`
	Entries             []logicalStorageClosureEntry      `json:"entries"`
}

type logicalStorageClosureEntry struct {
	Sequence           uint64 `json:"sequence"`
	OperationID        string `json:"operationID"`
	Outcome            string `json:"outcome,omitempty"`
	ReceiptDigest      string `json:"receiptDigest,omitempty"`
	CancellationDigest string `json:"cancellationDigest,omitempty"`
	GuardDigest        string `json:"guardDigest,omitempty"`
	EffectsExcluded    bool   `json:"effectsExcluded"`
}

func (e *FencingExecutor) verifyLogicalStorageClosureEntry(ctx context.Context, uid string, id fencing.Identity,
	binding fencing.LogicalWriterBindingV1, run fencing.Run, producerUID string, sequence uint64, entry fencing.MutationEntry,
	provider *gcsMutationProvider) (logicalStorageClosureEntry, error) {
	result := logicalStorageClosureEntry{Sequence: sequence, OperationID: entry.Intent.OperationID}
	if sequence == 0 || entry.Intent.OperationID != fencing.MutationRequestID(producerUID, sequence) ||
		entry.Intent.StoreScopeHash != run.MutationScopeHash || !entry.DispatchClosed {
		return result, ErrFencePending
	}
	if entry.GuardedLogicalResolution != nil {
		authority := logicalMutationAuthority(uid, binding)
		if err := e.verifyGuardedLogicalResolutionForAuthority(ctx, authority, run, producerUID, sequence, entry, provider); err != nil {
			return result, errors.Join(ErrFencePending, err)
		}
		result.Outcome = string(fencing.MutationCommitted)
		result.GuardDigest = entry.GuardedLogicalResolution.GuardDigest
		result.EffectsExcluded = true
		return result, nil
	}
	if entry.Resolution == nil || entry.CopyCompletion != nil || entry.GenerationZeroGuard != nil {
		return result, ErrFencePending
	}
	resolution := entry.Resolution
	if entry.Preparation == nil {
		if resolution.Outcome != fencing.MutationRejected || resolution.ConditionFailed || resolution.PayloadDigest != "" ||
			!resolution.AllCopiesTerminal || len(entry.Dispatches) != 0 {
			return result, ErrFencePending
		}
		if err := e.verifyLogicalArchiveGenesisCancellation(ctx, uid, id, binding, producerUID, sequence, entry.Intent, resolution.ReceiptDigest); err != nil {
			return result, err
		}
		result.Outcome = string(resolution.Outcome)
		result.CancellationDigest = resolution.ReceiptDigest
		result.EffectsExcluded = true
		return result, nil
	}
	if len(entry.Dispatches) != 1 || resolution.Outcome != fencing.MutationCommitted && resolution.Outcome != fencing.MutationExcluded && resolution.Outcome != fencing.MutationRejected {
		return result, ErrFencePending
	}
	authority := logicalMutationAuthority(uid, binding)
	owner := authority.mutationOwner(run, producerUID, sequence, entry.Intent)
	receipts, err := e.receiptsForLogicalBinding(uid, binding)
	if err != nil {
		return result, err
	}
	receipt, err := receipts.get("receipt-" + owner)
	if err != nil || !authority.matchesReceipt(receipt, run, producerUID, sequence, entry.Intent) || receipt.Preparation != *entry.Preparation ||
		receipt.ReceiptDigest != receipt.digest() || receipt.ReceiptDigest != resolution.ReceiptDigest || receipt.Outcome != resolution.Outcome ||
		receipt.ConditionFailed != resolution.ConditionFailed || receipt.PayloadDigest != resolution.PayloadDigest ||
		receipt.AllCopiesTerminal != resolution.AllCopiesTerminal || !mutationReceiptMatchesDispatch(entry, receipt) {
		return result, errors.Join(ErrFencePending, err, fmt.Errorf("fencing: logical mutation receipt differs from its closed journal tuple"))
	}
	result.Outcome = string(resolution.Outcome)
	result.ReceiptDigest = receipt.ReceiptDigest
	switch resolution.Outcome {
	case fencing.MutationCommitted:
		if !receipt.AllCopiesTerminal {
			return result, ErrFencePending
		}
		if entry.Intent.Kind != "upload" && entry.Intent.Kind != "delete" {
			return result, ErrFencePending
		}
	case fencing.MutationRejected:
		if !receipt.ConditionFailed || !receipt.AllCopiesTerminal || receipt.PayloadDigest != "" || receipt.Metageneration != 0 ||
			receipt.Generation != receipt.Preparation.Generation || entry.Intent.Condition.Mode != fencing.MutationMatchGeneration ||
			entry.Intent.Condition.Generation != receipt.Preparation.Generation {
			return result, ErrFencePending
		}
	case fencing.MutationExcluded:
		if receipt.ConditionFailed || receipt.PayloadDigest != "" || entry.Intent.Kind != "delete" || receipt.Generation != receipt.Preparation.Generation ||
			entry.Preparation.Generation <= 0 {
			return result, ErrFencePending
		}
		if err := verifyLogicalDeletedGeneration(ctx, provider, entry.Intent.Key, receipt.Generation); err != nil {
			return result, errors.Join(ErrFencePending, err)
		}
	}
	result.EffectsExcluded = true
	return result, nil
}

func verifyLogicalDeletedGeneration(ctx context.Context, provider *gcsMutationProvider, key string, generation int64) error {
	if provider == nil || generation <= 0 {
		return ErrFencePending
	}
	object := provider.object(key)
	if object == nil {
		return ErrFencePending
	}
	_, err := object.Generation(generation).Retryer(storage.WithPolicy(storage.RetryNever)).Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	return errors.Join(err, ErrFencePending)
}

func (e *FencingExecutor) verifyLogicalArchiveGenesisCancellation(ctx context.Context, uid string, id fencing.Identity,
	binding fencing.LogicalWriterBindingV1, producerUID string, sequence uint64, intent fencing.MutationIntent, digest string) error {
	if !validEvidenceDigest(digest) || isArchiveGenesisMutation(intent) && intent.OperationID == "" {
		return ErrFencePending
	}
	evidence, err := e.authorityEvidenceForRoot(uid, id.SourceClusterID)
	if err != nil {
		return err
	}
	reader, _, err := evidence.Read(ctx, "archive-genesis-cancellations/"+digest)
	if err != nil {
		return errors.Join(ErrFencePending, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil || len(data) > 1<<20 || authorityStateDigest(data) != digest {
		return errors.Join(ErrFencePending, err)
	}
	var proof logicalArchiveGenesisCancellationProof
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&proof); err != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.Join(ErrFencePending, err)
	}
	canonical, err := json.Marshal(proof)
	if err != nil || !bytes.Equal(canonical, data) || proof.Version != 1 || proof.StatefulSetUID != uid || proof.SourceClusterID != id.SourceClusterID ||
		proof.Binding != binding || proof.WALIdentity != id.WALIdentity || proof.ProducerUID != producerUID || proof.Sequence != sequence || proof.Intent != intent ||
		proof.WinningGenesis.Validate() != nil || proof.WinningGenesis.Mutation == nil ||
		(proof.WinningGenesis.Mutation.ProducerUID == producerUID && proof.WinningGenesis.Mutation.Sequence == sequence && proof.WinningGenesis.Mutation.Intent == intent) {
		return errors.Join(ErrFencePending, err)
	}
	scope, scopeHash, err := e.logicalArchiveScope(ctx, uid, binding)
	if err != nil || proof.WinningGenesis.ScopeHash != scopeHash {
		return errors.Join(ErrFencePending, err)
	}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	root, rootErr := e.read(ctx, uid, id.SourceClusterID)
	if err != nil || rootErr != nil || claim.ConfigID != recovery.ArchiveConfigID || claim.ScopeHash != scopeHash ||
		!sameArchiveGenesisRef(claim.GenesisRef, &proof.WinningGenesis) || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, &proof.WinningGenesis) {
		return errors.Join(ErrFencePending, err, rootErr)
	}
	return nil
}

func (e *FencingExecutor) verifyLogicalStorageClosure(ctx context.Context, uid string, id fencing.Identity, binding fencing.LogicalWriterBindingV1,
	run fencing.Run, producerUID string, producer fencing.MutationProducer) (string, string, string, error) {
	if e == nil || e.SourceMutationStorage == nil || run.LogicalBinding == nil || *run.LogicalBinding != binding || run.Runtime != nil ||
		producer.Profile != fencing.LogicalWriterProfileV1 || !sameLogicalProducerBinding(producer.LogicalBinding, &binding) ||
		producer.Phase != fencing.MutationClosed || producer.Validate() != nil || producerUID != run.MutationProducerUID ||
		producer.WALIdentity != id.WALIdentity || producer.ClosedThrough != producer.LastAdmitted {
		return "", "", "", ErrFencePending
	}
	scope, err := e.sourceMutationStorageScopeForRoot(ctx, uid, id.SourceClusterID)
	if err != nil || scope.Provider != "gcs" || hashJSON(scope) != run.MutationScopeHash || hashJSON(scope) != binding.StoreScopeHash {
		return "", "", "", errors.Join(ErrFencePending, err, fmt.Errorf("fencing: logical closure storage scope differs"))
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, run.MutationProfile)
	if err != nil {
		return "", "", "", err
	}
	evidenceStore, err := e.authorityEvidenceForRoot(uid, id.SourceClusterID)
	if err != nil || evidenceStore.Identity() == "" {
		return "", "", "", errors.Join(ErrFencePending, err)
	}
	proof := logicalStorageClosureEvidence{Version: 1, ClosureKind: fencing.LogicalRunClosureBoundWAL, RootIdentity: evidenceStore.Identity(), Identity: id, Binding: binding,
		ProducerUID: producerUID, StoreScopeHash: run.MutationScopeHash, CloseOperationID: producer.CloseOperationID,
		CloseRequestHash: producer.CloseRequestHash, ClosedThrough: producer.ClosedThrough, CompactedThrough: producer.CompactedThrough,
		ArchiveHead: producer.ArchiveHead, ProducerStateDigest: hashJSON(producer), EntryCount: producer.ClosedThrough}
	authority := logicalMutationAuthority(uid, binding)
	prefixDigest := hashJSON([]any{"logical-storage-closed-prefix-v1", uid, id.SourceClusterID, id, binding, producerUID, producer.ClosedThrough})
	if producer.CompactedThrough > 0 {
		if producer.ArchiveHead == nil {
			return "", "", "", ErrFencePending
		}
		refs := make([]fencing.MutationArchiveReference, 0, 8)
		next := *producer.ArchiveHead
		expectedThrough := producer.CompactedThrough
		for links := 0; ; links++ {
			if links >= 8192 || !next.Valid() {
				return "", "", "", ErrFencePending
			}
			plan, readErr := e.readMutationArchiveForAuthority(ctx, authority, next, producerUID, producer.WALIdentity)
			if readErr != nil {
				return "", "", "", errors.Join(ErrFencePending, e.logicalMutationArchiveFailure(ctx, authority, readErr))
			}
			if plan.Through != expectedThrough || plan.PreviousThrough >= plan.Through || uint64(len(plan.Entries)) != plan.Through-plan.PreviousThrough {
				return "", "", "", e.logicalMutationArchiveFailure(ctx, authority, errMutationArchiveEvidence)
			}
			refs = append(refs, next)
			expectedThrough = plan.PreviousThrough
			if plan.Previous == nil {
				if expectedThrough != 0 {
					return "", "", "", ErrFencePending
				}
				break
			}
			next = *plan.Previous
		}
		for i := len(refs) - 1; i >= 0; i-- {
			plan, readErr := e.readMutationArchiveForAuthority(ctx, authority, refs[i], producerUID, producer.WALIdentity)
			if readErr != nil {
				return "", "", "", errors.Join(ErrFencePending, e.logicalMutationArchiveFailure(ctx, authority, readErr))
			}
			for sequence := plan.PreviousThrough + 1; sequence <= plan.Through; sequence++ {
				entry, exists := plan.Entries[sequence]
				if !exists {
					return "", "", "", e.logicalMutationArchiveFailure(ctx, authority, errMutationArchiveEvidence)
				}
				closureEntry, verifyErr := e.verifyLogicalStorageClosureEntry(ctx, uid, id, binding, run, producerUID, sequence, entry, provider)
				if verifyErr != nil {
					return "", "", "", verifyErr
				}
				prefixDigest = hashJSON([]any{"logical-storage-closed-prefix-entry-v1", sequence, closureEntry, prefixDigest})
			}
		}
	}
	for sequence := producer.CompactedThrough; sequence < producer.ClosedThrough; {
		sequence++
		entry, exists := producer.Entries[sequence]
		if !exists {
			return "", "", "", ErrFencePending
		}
		closureEntry, verifyErr := e.verifyLogicalStorageClosureEntry(ctx, uid, id, binding, run, producerUID, sequence, entry, provider)
		if verifyErr != nil {
			return "", "", "", verifyErr
		}
		proof.Entries = append(proof.Entries, closureEntry)
		prefixDigest = hashJSON([]any{"logical-storage-closed-prefix-entry-v1", sequence, closureEntry, prefixDigest})
	}
	proof.ClosedPrefixDigest = prefixDigest
	body, err := json.Marshal(proof)
	if err != nil {
		return "", "", "", err
	}
	name := "logical-run-closures/" + hashJSON([]any{"logical-run-closure-v1", uid, id}) + ".json"
	version, err := evidenceStore.CreateImmutable(ctx, name, body)
	if err != nil {
		return "", "", "", err
	}
	stored, storedVersion, err := evidenceStore.Read(ctx, name)
	if err != nil {
		return "", "", "", err
	}
	defer stored.Close()
	readback, err := io.ReadAll(io.LimitReader(stored, int64(len(body))+1))
	if err != nil || !bytes.Equal(readback, body) || hashJSON(storedVersion) != hashJSON(version) {
		return "", "", "", errors.Join(err, fmt.Errorf("fencing: logical closure evidence readback differs"))
	}
	return name, hashJSON(version), authorityStateDigest(body), nil
}

func (e *FencingExecutor) lookupMutationForAuthority(ctx context.Context, authority mutationExecutionAuthority, run fencing.Run, request fencing.MutationLookupRequest) (fencing.MutationLookupResult, error) {
	if authority.runtime != nil {
		return e.lookupMutation(ctx, authority.statefulSetUID, run.Identity, request)
	}
	if authority.logicalBinding == nil || run.LogicalBinding == nil || *run.LogicalBinding != *authority.logicalBinding ||
		request.ProducerUID != run.MutationProducerUID || request.Intent.StoreScopeHash != run.MutationScopeHash ||
		request.Sequence == 0 || request.Intent.OperationID != fencing.MutationRequestID(request.ProducerUID, request.Sequence) {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: logical mutation lookup is outside its enrolled producer scope")
	}
	producer, actualUID, _, err := e.AuthorityState.ReadLogicalProducer(ctx, authority.statefulSetUID, *authority.logicalBinding)
	if err != nil {
		return fencing.MutationLookupResult{}, err
	}
	if actualUID != run.MutationProducerUID || producer.WALIdentity != run.Identity.WALIdentity || producer.Profile != fencing.LogicalWriterProfileV1 || producer.Validate() != nil {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: logical mutation producer differs from canonical Run")
	}
	if request.Sequence > producer.LastAdmitted {
		return fencing.MutationLookupResult{}, nil
	}
	if request.Sequence <= producer.CompactedThrough {
		entry, err := e.lookupArchivedMutationForAuthority(ctx, authority, request.ProducerUID, request.Sequence, request.Intent)
		if err != nil {
			return fencing.MutationLookupResult{}, err
		}
		return fencing.MutationLookupResult{Found: true, Archived: true, CompactedThrough: producer.CompactedThrough,
			ArchiveHead: producer.ArchiveHead, Entry: &entry}, nil
	}
	entry, ok := producer.Entries[request.Sequence]
	if !ok || entry.Intent != request.Intent {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: logical mutation lookup differs from original tuple")
	}
	return fencing.MutationLookupResult{Found: true, CompactedThrough: producer.CompactedThrough, ArchiveHead: producer.ArchiveHead, Entry: &entry}, nil
}

func (e *FencingExecutor) readMutationProducerForAuthority(ctx context.Context, authority mutationExecutionAuthority) (fencing.MutationProducer, string, *objstore.ObjectVersion, error) {
	if authority.runtime != nil {
		return e.AuthorityState.ReadProducer(ctx, *authority.runtime)
	}
	if authority.logicalBinding == nil {
		return fencing.MutationProducer{}, "", nil, fmt.Errorf("fencing: logical mutation authority is missing")
	}
	return e.AuthorityState.ReadLogicalProducer(ctx, authority.statefulSetUID, *authority.logicalBinding)
}

func (e *FencingExecutor) replayGuardedLogicalResolutionForAuthority(ctx context.Context, authority mutationExecutionAuthority, run fencing.Run,
	producerUID string, sequence uint64, entry fencing.MutationEntry, stages *SourceStageStore, provider mutationExecutionProvider) error {
	guardProvider, ok := provider.(mutationGuardedReplayProvider)
	if !ok || e == nil || e.AuthorityState == nil || run.MutationProfile != fencing.MutationHeldExecutionProfile ||
		entry.Preparation == nil || !fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) || !entry.DispatchClosed || len(entry.Dispatches) != 1 {
		return ErrFencePending
	}
	producer, actualUID, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil || actualUID != producerUID || producer.WALIdentity != run.Identity.WALIdentity || producer.Phase != fencing.MutationClosed || producer.Validate() != nil {
		return errors.Join(ErrFencePending, err)
	}
	current, exists := producer.Entries[sequence]
	if !exists || hashJSON(current) != hashJSON(entry) {
		return ErrFencePending
	}
	preparationHash, err := producer.PreparationHash(sequence)
	if err != nil {
		return errors.Join(ErrFencePending, err)
	}
	dispatchID := ""
	for id, dispatch := range entry.Dispatches {
		if dispatch.ReceiptDigest != "" {
			return ErrFencePending
		}
		dispatchID = id
	}
	var evidenceStore *AuthorityEvidenceStore
	if authority.runtime != nil {
		evidenceStore, err = e.authorityEvidenceFor(*authority.runtime)
	} else {
		evidenceStore, err = e.authorityEvidenceForRoot(authority.statefulSetUID, authority.sourceClusterID)
	}
	if err != nil {
		return err
	}
	request := mutationGuardedReplayRequest{WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence,
		Intent: entry.Intent, Preparation: *entry.Preparation, PreparationHash: preparationHash,
		DispatchID: dispatchID, GuardRootIdentity: evidenceStore.Identity()}
	if authority.runtime != nil {
		request.Runtime = *authority.runtime
	} else {
		request.AuthorityProfile = fencing.LogicalWriterProfileV1
		binding := *authority.logicalBinding
		request.LogicalBinding = &binding
	}
	if entry.GuardedLogicalResolution != nil {
		if err := guardProvider.verifyGuardedCreate(ctx, evidenceStore, request, *entry.GuardedLogicalResolution); err != nil {
			return errors.Join(ErrFencePending, err)
		}
		return nil
	}
	payload, err := stages.openMutationPayloadContext(ctx, producerUID, entry.Intent)
	if err != nil {
		return errors.Join(ErrFencePending, err)
	}
	evidence, replayErr := guardProvider.replayGuardedCreate(ctx, evidenceStore, request, payload)
	closeErr := payload.Close()
	if err := errors.Join(replayErr, closeErr); err != nil {
		return errors.Join(ErrFencePending, err)
	}
	if err := guardProvider.verifyGuardedCreate(ctx, evidenceStore, request, evidence); err != nil {
		return errors.Join(ErrFencePending, err)
	}
	if evidence.GuardRootIdentity != evidenceStore.Identity() {
		return ErrFencePending
	}
	_, err = e.changeMutationProducerForAuthority(ctx, authority, producerUID, func(p *fencing.MutationProducer) error {
		if p.WALIdentity != run.Identity.WALIdentity {
			return fmt.Errorf("fencing: guarded mutation producer WAL differs")
		}
		return p.ResolveGuardedLogical(producerUID, sequence, evidence)
	})
	return err
}

func (e *FencingExecutor) verifyGuardedLogicalResolutionForAuthority(ctx context.Context, authority mutationExecutionAuthority, run fencing.Run,
	producerUID string, sequence uint64, entry fencing.MutationEntry, provider mutationExecutionProvider) error {
	guardProvider, ok := provider.(mutationGuardedReplayProvider)
	if !ok || entry.GuardedLogicalResolution == nil || entry.Preparation == nil {
		return ErrFencePending
	}
	producer, actualUID, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil || actualUID != producerUID || producer.Phase != fencing.MutationClosed || producer.WALIdentity != run.Identity.WALIdentity || producer.ValidateGuardedLogicalResolution(producerUID, sequence, entry) != nil {
		return errors.Join(ErrFencePending, err)
	}
	var evidenceStore *AuthorityEvidenceStore
	if authority.runtime != nil {
		evidenceStore, err = e.authorityEvidenceFor(*authority.runtime)
	} else {
		evidenceStore, err = e.authorityEvidenceForRoot(authority.statefulSetUID, authority.sourceClusterID)
	}
	if err != nil {
		return err
	}
	evidence := entry.GuardedLogicalResolution
	request := mutationGuardedReplayRequest{WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence,
		Intent: entry.Intent, Preparation: *entry.Preparation, PreparationHash: evidence.PreparationHash,
		DispatchID: evidence.DispatchID, GuardRootIdentity: evidenceStore.Identity()}
	if authority.runtime != nil {
		request.Runtime = *authority.runtime
	} else {
		request.AuthorityProfile = fencing.LogicalWriterProfileV1
		binding := *authority.logicalBinding
		request.LogicalBinding = &binding
	}
	if evidence.GuardRootIdentity != evidenceStore.Identity() {
		return ErrFencePending
	}
	if err := guardProvider.verifyGuardedCreate(ctx, evidenceStore, request, *evidence); err != nil {
		return errors.Join(ErrFencePending, err)
	}
	return nil
}

func (e *FencingExecutor) changeMutationProducerForAuthority(ctx context.Context, authority mutationExecutionAuthority, producerUID string, transition func(*fencing.MutationProducer) error) (fencing.MutationProducer, error) {
	if authority.runtime != nil {
		return e.changeMutationProducer(ctx, *authority.runtime, transition)
	}
	if e == nil || e.AuthorityState == nil || authority.logicalBinding == nil || producerUID == "" || producerUID != authority.logicalBinding.ProducerUID {
		return fencing.MutationProducer{}, fmt.Errorf("fencing: logical mutation producer change lacks its exact binding")
	}
	for range 8 {
		root, err := e.read(ctx, authority.statefulSetUID, authority.sourceClusterID)
		if err != nil {
			return fencing.MutationProducer{}, err
		}
		if root.Ledger.Profile != fencing.LogicalWriterProfileV1 || e.validateMutationJournalProfile(root) != nil {
			return fencing.MutationProducer{}, fmt.Errorf("fencing: logical producer change is outside the selected profile")
		}
		run, ok := root.Ledger.Runs[authority.runID]
		if !ok || !authority.matchesRun(run) || run.MutationProducerUID != producerUID {
			return fencing.MutationProducer{}, fencing.ErrClosed
		}
		producer, actualUID, version, err := e.AuthorityState.ReadLogicalProducer(ctx, authority.statefulSetUID, *authority.logicalBinding)
		if err != nil {
			return fencing.MutationProducer{}, err
		}
		if actualUID != producerUID || producer.WALIdentity != run.Identity.WALIdentity {
			return fencing.MutationProducer{}, fmt.Errorf("fencing: logical producer differs from canonical Run")
		}
		if err := transition(&producer); err != nil {
			return fencing.MutationProducer{}, err
		}
		if err := e.AuthorityState.WriteLogicalProducer(ctx, authority.statefulSetUID, *authority.logicalBinding, producer, version); err != nil {
			if errors.Is(err, errAuthorityStateConflict) {
				continue
			}
			return fencing.MutationProducer{}, err
		}
		return producer, nil
	}
	return fencing.MutationProducer{}, fencing.ErrUnavailable
}

// logicalProducerKey keeps logical producer state disjoint from legacy
// RuntimeBinding envelopes while remaining stable across the one-time WAL bind.
func (s *AuthorityStateStore) logicalProducerKey(statefulSetUID string, binding fencing.LogicalWriterBindingV1) (string, string, error) {
	if s == nil || s.bucket == nil || statefulSetUID == "" || binding.Validate() != nil || binding.Role != fencing.StartupRoleVoterActive || binding.ProducerUID == "" || binding.StoreScopeHash == "" {
		return "", "", fmt.Errorf("fencing: logical producer identity is incomplete")
	}
	identity := hashJSON([]any{"rhiza-logical-mutation-producer-v1", statefulSetUID, binding.SourceClusterID, binding.NodeID, binding.RunID, binding.ProducerUID, binding.StoreScopeHash})
	return s.key("logical-producers", identity), identity, nil
}

func sameLogicalProducerBinding(left, right *fencing.LogicalWriterBindingV1) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// readLogicalProducerForWALBind is deliberately narrower than the normal
// reader: the canonical Run has committed this one-time WAL bind, while the
// separately CASed producer may still be at its pre-bind value after a crash.
// Every other binding field must match, and the producer WAL may only be empty
// or exactly the canonical Run WAL.
func (s *AuthorityStateStore) readLogicalProducerForWALBind(ctx context.Context, statefulSetUID string, binding fencing.LogicalWriterBindingV1) (fencing.MutationProducer, string, *objstore.ObjectVersion, error) {
	key, identity, err := s.logicalProducerKey(statefulSetUID, binding)
	if err != nil {
		return fencing.MutationProducer{}, "", nil, err
	}
	data, version, err := s.read(ctx, key, identity)
	if err != nil {
		return fencing.MutationProducer{}, "", nil, err
	}
	var stored struct {
		ProducerUID string                   `json:"producerUID"`
		Producer    fencing.MutationProducer `json:"producer"`
	}
	if err := decodeAuthorityValue(data, &stored); err != nil {
		return fencing.MutationProducer{}, "", nil, err
	}
	actual := stored.Producer.LogicalBinding
	if actual == nil {
		return fencing.MutationProducer{}, "", nil, fmt.Errorf("fencing: logical WAL transition producer binding is absent")
	}
	actualWithoutWAL, expectedWithoutWAL := *actual, binding
	actualWithoutWAL.WALIdentity, expectedWithoutWAL.WALIdentity = "", ""
	if stored.ProducerUID != binding.ProducerUID || actualWithoutWAL != expectedWithoutWAL ||
		(actual.WALIdentity != "" && actual.WALIdentity != binding.WALIdentity) || stored.Producer.Profile != fencing.LogicalWriterProfileV1 || stored.Producer.Validate() != nil {
		return fencing.MutationProducer{}, "", nil, fmt.Errorf("fencing: logical WAL transition producer identity differs")
	}
	return stored.Producer, stored.ProducerUID, version, nil
}

// ReadLogicalProducer authenticates the profile-specific envelope and journal
// against the exact canonical Run binding before exposing CAS state.
func (s *AuthorityStateStore) ReadLogicalProducer(ctx context.Context, statefulSetUID string, binding fencing.LogicalWriterBindingV1) (fencing.MutationProducer, string, *objstore.ObjectVersion, error) {
	key, identity, err := s.logicalProducerKey(statefulSetUID, binding)
	if err != nil {
		return fencing.MutationProducer{}, "", nil, err
	}
	data, version, err := s.read(ctx, key, identity)
	if err != nil {
		return fencing.MutationProducer{}, "", nil, err
	}
	var stored struct {
		ProducerUID string                   `json:"producerUID"`
		Producer    fencing.MutationProducer `json:"producer"`
	}
	if err := decodeAuthorityValue(data, &stored); err != nil || stored.ProducerUID != binding.ProducerUID || stored.Producer.Profile != fencing.LogicalWriterProfileV1 ||
		!sameLogicalProducerBinding(stored.Producer.LogicalBinding, &binding) || stored.Producer.Validate() != nil {
		return fencing.MutationProducer{}, "", nil, errors.Join(err, fmt.Errorf("fencing: canonical logical mutation producer identity differs"))
	}
	return stored.Producer, stored.ProducerUID, version, nil
}

func (s *AuthorityStateStore) WriteLogicalProducer(ctx context.Context, statefulSetUID string, binding fencing.LogicalWriterBindingV1, producer fencing.MutationProducer, expected *objstore.ObjectVersion) error {
	key, identity, err := s.logicalProducerKey(statefulSetUID, binding)
	if err != nil {
		return err
	}
	if producer.Profile != fencing.LogicalWriterProfileV1 || !sameLogicalProducerBinding(producer.LogicalBinding, &binding) || producer.Validate() != nil {
		return fmt.Errorf("fencing: canonical logical mutation producer state is invalid")
	}
	value := struct {
		ProducerUID string                   `json:"producerUID"`
		Producer    fencing.MutationProducer `json:"producer"`
	}{ProducerUID: binding.ProducerUID, Producer: producer}
	return s.write(ctx, key, identity, value, expected)
}
