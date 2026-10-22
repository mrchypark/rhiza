package operator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type mutationProviderResult struct {
	Outcome           fencing.MutationOutcome
	ConditionFailed   bool
	Generation        int64
	Metageneration    int64
	PayloadDigest     string
	AllCopiesTerminal bool
}

type mutationExecutionProvider interface {
	observe(context.Context, string) (int64, bool, error)
	upload(context.Context, string, int64, fencing.MutationIntent, io.Reader) (mutationProviderResult, error)
	delete(context.Context, string, int64) (mutationProviderResult, error)
	committed(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation) (mutationProviderResult, bool, error)
}

type mutationKeyState struct {
	ScopeHash string `json:"scopeHash"`
	Key       string `json:"key"`
	Owner     string `json:"owner,omitempty"`
}

// compactSourceMutationJournal keeps the canonical producer below its fixed
// live-entry bound. Archives are committed only for the contiguous terminal
// prefix and commitMutationArchive verifies all prepared-entry receipts.
func (e *FencingExecutor) compactSourceMutationJournal(ctx context.Context, runtime fencing.RuntimeBinding, nextSequence uint64) error {
	if err := e.ReconcileArchivedStages(ctx, runtime); err != nil {
		return err
	}
	producer, err := e.reserveMutationProducer(ctx, runtime)
	if err != nil {
		return err
	}
	if producer.LastAdmitted == ^uint64(0) || nextSequence != producer.LastAdmitted+1 || producer.LastAdmitted-producer.CompactedThrough < fencing.MaxMutationJournalEntries/2 {
		return nil
	}
	through := producer.CompactedThrough
	for sequence := producer.CompactedThrough; sequence < producer.LastAdmitted; {
		sequence++
		entry := producer.Entries[sequence]
		if entry.Resolution == nil || (entry.Preparation != nil && entry.Preparation.Generation == 0 && !entry.Resolution.AllCopiesTerminal && entry.CopyCompletion == nil) {
			break
		}
		through = sequence
	}
	if through == producer.CompactedThrough {
		return nil
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return err
	}
	plan, err := producer.PlanMutationCompaction(run.MutationProducerUID, through)
	if err != nil {
		return err
	}
	committed, err := e.commitMutationArchive(ctx, runtime, plan)
	if err != nil {
		return err
	}
	if committed.CompactedThrough != plan.Through || committed.ArchiveHead == nil {
		return nil
	}
	return e.ReconcileArchivedStages(ctx, runtime)
}

const archivedStageRetirementBatch = 16

// ReconcileArchivedStages resumes stage retirement from the durable producer
// cursor. It verifies each archived entry and exact archive generation before
// retiring its payload; each cursor step is CAS-persisted only after the
// tombstone/quota update succeeds. Work is bounded per call so admission can
// retry maintenance without an unbounded request.
func (e *FencingExecutor) ReconcileArchivedStages(ctx context.Context, runtime fencing.RuntimeBinding) error {
	if ctx == nil || e == nil || e.AuthorityState == nil {
		return fmt.Errorf("fencing: archived stage reconciliation is not configured")
	}
	producer, err := e.reserveMutationProducer(ctx, runtime)
	if err != nil {
		return err
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return err
	}
	if run.MutationProducerUID == "" || run.MutationProducerUID != stableMutationProducerUID(runtime) {
		return fmt.Errorf("fencing: archived stage reconciliation producer differs from root")
	}
	stop := producer.CompactedThrough
	if remaining := stop - producer.RetiredStageThrough; remaining > archivedStageRetirementBatch {
		stop = producer.RetiredStageThrough + archivedStageRetirementBatch
	}
	for retired := producer.RetiredStageThrough; retired < stop; retired++ {
		sequence := retired + 1
		entry, archiveRef, err := e.archivedMutationAt(ctx, runtime, run.MutationProducerUID, sequence)
		if err != nil {
			return err
		}
		if entry.Intent.Kind == "upload" && strings.HasPrefix(entry.Intent.PayloadRef, "payload-") {
			if err := e.retireArchivedSourceStage(ctx, runtime, run.MutationProducerUID, sequence, entry.Intent, archiveRef); err != nil {
				return err
			}
		}
		if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
			return p.AdvanceArchivedStageRetirement(sequence)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *FencingExecutor) archivedMutationAt(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64) (fencing.MutationEntry, fencing.MutationArchiveReference, error) {
	verificationID := hashJSON([]any{"lookup-archived-sequence", producerUID, sequence})
	done, found, err := e.scanMutationArchiveHistory(ctx, runtime, verificationID, 32, nil, sequence)
	if err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, err
	}
	if !done || !found {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, ErrFencePending
	}
	producer, storedUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, err
	}
	if storedUID != producerUID || producer.ArchiveScan == nil || producer.ArchiveScan.VerificationID != verificationID || producer.ArchiveScan.Sequence != sequence || producer.ArchiveScan.ResultArchive == nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, ErrFencePending
	}
	ref := *producer.ArchiveScan.ResultArchive
	plan, err := e.readMutationArchive(ctx, ref, runtime, producerUID, producer.WALIdentity)
	if err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, e.mutationArchiveFailure(ctx, runtime, err)
	}
	entry, ok := plan.Entries[sequence]
	if !ok {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, e.mutationArchiveFailure(ctx, runtime, errMutationArchiveEvidence)
	}
	if err := e.verifyMutationArchiveReceipts(ctx, plan); err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, e.mutationArchiveFailure(ctx, runtime, err)
	}
	return entry, ref, nil
}

func (e *FencingExecutor) mutationKeyIdentity(scopeHash, key string) (string, string) {
	identity := hashJSON([]string{"rhiza-mutation-key-v1", scopeHash, key})
	return e.AuthorityState.key("mutation-keys", identity), identity
}

// acquireMutationKey is a durable, non-expiring cross-producer key lock. An
// uncertain provider call intentionally holds it across authority restarts.
func (e *FencingExecutor) acquireMutationKey(ctx context.Context, scopeHash, key, owner string, allowCreate bool) error {
	if e == nil || e.AuthorityState == nil || scopeHash == "" || key == "" || owner == "" {
		return fmt.Errorf("fencing: canonical mutation key lock is not configured")
	}
	endpoint, identity := e.mutationKeyIdentity(scopeHash, key)
	for range 8 {
		data, version, err := e.AuthorityState.read(ctx, endpoint, identity)
		creating := errors.Is(err, errAuthorityStateNotFound)
		if err != nil && !creating {
			return err
		}
		if creating && !allowCreate {
			return fmt.Errorf("fencing: unresolved provider dispatch lost its durable key lock")
		}
		state := mutationKeyState{ScopeHash: scopeHash, Key: key, Owner: owner}
		if !creating {
			if err := decodeAuthorityValue(data, &state); err != nil || state.ScopeHash != scopeHash || state.Key != key {
				return fmt.Errorf("fencing: mutation key lock identity differs")
			}
			if state.Owner == owner {
				return nil
			}
			if state.Owner != "" {
				return ErrFencePending
			}
			state.Owner = owner
		}
		err = e.AuthorityState.write(ctx, endpoint, identity, state, version)
		if err == nil {
			return nil
		}
		if errors.Is(err, errAuthorityStateConflict) {
			continue
		}
		// A create/CAS response may be lost after it commits. Confirm only the
		// exact owner tuple; never infer ownership from a matching key alone.
		stored, _, readErr := e.AuthorityState.read(ctx, endpoint, identity)
		var actual mutationKeyState
		if readErr == nil && decodeAuthorityValue(stored, &actual) == nil && actual == state {
			return nil
		}
		if readErr != nil {
			return errors.Join(err, readErr)
		}
		if !creating && actual.ScopeHash == scopeHash && actual.Key == key && actual.Owner != "" && actual.Owner != owner {
			return ErrFencePending
		}
		if !creating && actual.ScopeHash == scopeHash && actual.Key == key && actual.Owner == "" {
			continue
		}
		if creating {
			return err
		}
	}
	return ErrFencePending
}

func (e *FencingExecutor) releaseMutationKey(ctx context.Context, scopeHash, key, owner string) error {
	if e == nil || e.AuthorityState == nil || scopeHash == "" || key == "" || owner == "" {
		return fmt.Errorf("fencing: canonical mutation key lock is not configured")
	}
	endpoint, identity := e.mutationKeyIdentity(scopeHash, key)
	for range 8 {
		data, version, err := e.AuthorityState.read(ctx, endpoint, identity)
		if err != nil {
			return err
		}
		var state mutationKeyState
		if err := decodeAuthorityValue(data, &state); err != nil || state.ScopeHash != scopeHash || state.Key != key {
			return fmt.Errorf("fencing: mutation key lock identity differs")
		}
		if state.Owner == "" {
			return nil
		}
		if state.Owner != owner {
			return fmt.Errorf("fencing: mutation key lock owner changed")
		}
		state.Owner = ""
		if err := e.AuthorityState.write(ctx, endpoint, identity, state, version); err == nil {
			return nil
		} else {
			if !errors.Is(err, errAuthorityStateConflict) {
				stored, _, readErr := e.AuthorityState.read(ctx, endpoint, identity)
				var actual mutationKeyState
				if readErr == nil && decodeAuthorityValue(stored, &actual) == nil && actual.ScopeHash == scopeHash && actual.Key == key && actual.Owner == "" {
					return nil
				}
				if readErr != nil {
					return errors.Join(err, readErr)
				}
				return err
			}
		}
	}
	return ErrFencePending
}

// ExecuteSourceMutation consumes the exact journaled payload and sends at most
// one provider request under a durable cross-producer key lock. Only a fsynced
// provider receipt followed by a committed journal CAS can return success.
func (e *FencingExecutor) ExecuteSourceMutation(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent, scope AttestationStoreScope, provider mutationExecutionProvider, receipts *MutationExecutionReceiptStore) error {
	if e == nil || e.AuthorityState == nil {
		return fmt.Errorf("fencing: canonical authority state is required for source mutation execution")
	}
	if ctx == nil || e == nil || provider == nil || receipts == nil || sequence == 0 || producerUID == "" || scope.Provider != "gcs" || hashJSON(scope) != intent.StoreScopeHash {
		return fmt.Errorf("fencing: source mutation execution is not configured for this scope")
	}
	if intent.Kind != "upload" && intent.Kind != "delete" {
		return fmt.Errorf("fencing: unsupported source mutation kind")
	}
	stages, err := e.sourceStagesFor(ctx, runtime)
	if err != nil {
		return err
	}
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return err
	}
	run, ok := state.Ledger.Runs[runtime.RunID]
	if !ok || run.Runtime == nil || *run.Runtime != runtime || run.MutationProducerUID != producerUID || run.MutationScopeHash != intent.StoreScopeHash || run.MutationProfile != fencing.MutationExecutionProfile || run.Identity.WALIdentity == "" {
		return fmt.Errorf("fencing: source mutation is outside enrolled producer scope")
	}
	lookup, err := e.lookupMutation(ctx, runtime.StatefulSetUID, run.Identity, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: sequence, Intent: intent})
	if err != nil {
		return err
	}
	if !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != intent {
		return ErrFencePending
	}
	entry := *lookup.Entry
	owner := hashJSON([]any{runtime, run.Identity.WALIdentity, producerUID, sequence, intent.OperationID})
	if err := e.acquireMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner, len(lookup.Entry.Dispatches) == 0); err != nil {
		return err
	}
	finish := func(receipt mutationExecutionReceipt) error {
		if receipt.Runtime != runtime || receipt.WALIdentity != run.Identity.WALIdentity || receipt.ProducerUID != producerUID || receipt.Sequence != sequence || receipt.Intent != intent {
			return fmt.Errorf("fencing: mutation receipt tuple differs")
		}
		digest, err := receipts.put(receipt)
		if err != nil {
			return err
		}
		if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
			if p.WALIdentity != run.Identity.WALIdentity {
				return fmt.Errorf("fencing: mutation producer WAL differs")
			}
			if receipt.AllCopiesTerminal {
				if err := p.RecordDispatchTerminal(sequence, receipt.DispatchID, digest); err != nil {
					return err
				}
			}
			preparationHash, err := p.PreparationHash(sequence)
			if err != nil {
				return err
			}
			return p.Resolve(sequence, fencing.MutationResolution{Outcome: receipt.Outcome, PreparationHash: preparationHash, ReceiptDigest: digest, ConditionFailed: receipt.ConditionFailed,
				PayloadDigest: receipt.PayloadDigest, AllCopiesTerminal: receipt.AllCopiesTerminal})
		}); err != nil {
			return err
		}
		// A create-only write with unresolved copies retains the cross-producer
		// key guard after its caller-visible commit. A later zero-generation
		// delete must not race a delayed copy that could recreate the name.
		if receipt.Preparation.Generation != 0 || receipt.AllCopiesTerminal {
			if err := e.releaseMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner); err != nil {
				return err
			}
		}
		if receipt.Outcome != fencing.MutationCommitted {
			if receipt.ConditionFailed {
				return fencing.ErrMutationConditionFailed
			}
			return fmt.Errorf("fencing: original source mutation was rejected")
		}
		return nil
	}
	if entry.Resolution != nil {
		if entry.Resolution.Outcome == fencing.MutationCommitted {
			if entry.Preparation.Generation != 0 || entry.Resolution.AllCopiesTerminal || entry.CopyCompletion != nil {
				return e.releaseMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner)
			}
			return nil
		}
		if entry.Preparation != nil && (entry.Preparation.Generation != 0 || entry.Resolution.AllCopiesTerminal || entry.CopyCompletion != nil) {
			if err := e.releaseMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner); err != nil {
				return err
			}
		}
		if entry.Resolution.ConditionFailed {
			return fencing.ErrMutationConditionFailed
		}
		return fmt.Errorf("fencing: original source mutation is terminally rejected")
	}
	receipt, receiptErr := receipts.get("receipt-" + owner)
	if receiptErr == nil {
		return finish(receipt)
	}
	if !errors.Is(receiptErr, os.ErrNotExist) {
		return receiptErr
	}
	if entry.Preparation == nil {
		generation := int64(0)
		if intent.Condition.Mode == fencing.MutationMatchGeneration {
			generation = intent.Condition.Generation
		} else if intent.Condition.Mode == fencing.MutationUnconditional {
			observed, exists, err := provider.observe(ctx, intent.Key)
			if err != nil {
				return err
			}
			if exists {
				generation = observed
			}
		}
		prepared := fencing.MutationPreparation{Profile: fencing.MutationExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: generation}
		if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error { return p.Prepare(sequence, prepared) }); err != nil {
			return err
		}
		entry.Preparation = &prepared
	}
	dispatchID := ""
	if len(entry.Dispatches) != 0 {
		if len(entry.Dispatches) != 1 {
			return ErrFencePending
		}
		for id := range entry.Dispatches {
			dispatchID = id
		}
		// A granted but unresolved right is never reused for another physical call.
		result, found, err := provider.committed(ctx, intent.Key, intent, *entry.Preparation)
		if err != nil || !found {
			return ErrFencePending
		}
		receipt := mutationExecutionReceipt{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence,
			Intent: intent, Preparation: *entry.Preparation, DispatchID: dispatchID, Outcome: result.Outcome, Generation: result.Generation,
			Metageneration: result.Metageneration, PayloadDigest: result.PayloadDigest, AllCopiesTerminal: result.AllCopiesTerminal}
		receipt.ReceiptDigest = receipt.digest()
		return finish(receipt)
	}
	dispatchID, err = newMutationDispatchID()
	if err != nil {
		return err
	}
	if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		return grantMutationDispatch(p, sequence, dispatchID)
	}); err != nil {
		return err
	}
	// Persist the permission before the one provider dispatch; any failure after
	// this point remains an unresolved lock and is not retried by another worker.
	var result mutationProviderResult
	if intent.Kind == "upload" {
		payload, err := stages.openMutationPayloadContext(ctx, producerUID, intent)
		if err != nil {
			return err
		}
		defer payload.Close()
		result, err = provider.upload(ctx, intent.Key, entry.Preparation.Generation, intent, payload)
		if err != nil {
			return ErrFencePending
		}
	} else {
		result, err = provider.delete(ctx, intent.Key, entry.Preparation.Generation)
		if err != nil {
			return ErrFencePending
		}
	}
	conditionRejected := result.Outcome == fencing.MutationRejected && result.ConditionFailed && result.AllCopiesTerminal && result.PayloadDigest == "" && result.Generation == entry.Preparation.Generation
	if result.ConditionFailed && !conditionRejected {
		return ErrFencePending
	}
	if (result.Outcome != fencing.MutationCommitted && result.Outcome != fencing.MutationExcluded && !conditionRejected) ||
		(intent.Kind == "upload" && (result.Outcome != fencing.MutationCommitted || result.PayloadDigest != intent.PayloadDigest || result.Generation <= 0)) ||
		(intent.Kind == "delete" && result.Generation != entry.Preparation.Generation) {
		if !conditionRejected {
			return ErrFencePending
		}
	}
	receipt = mutationExecutionReceipt{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence,
		Intent: intent, Preparation: *entry.Preparation, DispatchID: dispatchID, Outcome: result.Outcome, ConditionFailed: result.ConditionFailed, Generation: result.Generation,
		Metageneration: result.Metageneration, PayloadDigest: result.PayloadDigest, AllCopiesTerminal: result.AllCopiesTerminal}
	receipt.ReceiptDigest = receipt.digest()
	return finish(receipt)
}

func grantMutationDispatch(producer *fencing.MutationProducer, sequence uint64, dispatchID string) error {
	current, ok := producer.Entries[sequence]
	if !ok || current.Resolution != nil || current.Preparation == nil {
		return ErrFencePending
	}
	if len(current.Dispatches) != 0 {
		if dispatch, exists := current.Dispatches[dispatchID]; len(current.Dispatches) == 1 && exists && dispatch.ReceiptDigest == "" && current.DispatchClosed {
			return nil // only this exact unresolved grant proves a lost CAS response
		}
		return ErrFencePending
	}
	if current.DispatchClosed {
		return ErrFencePending
	}
	if err := producer.GrantDispatch(sequence, dispatchID); err != nil {
		return err
	}
	return producer.SealDispatch(sequence)
}

func newMutationDispatchID() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("fencing: create unique mutation dispatch identity: %w", err)
	}
	return hex.EncodeToString(nonce[:]), nil
}
