package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

// compactLogicalSourceMutationJournal shares the physical producer state
// machine and archive format while resolving the producer through its exact
// logical root binding.
func (e *FencingExecutor) compactLogicalSourceMutationJournal(ctx context.Context, statefulSetUID string, binding fencing.LogicalWriterBindingV1, nextSequence uint64) error {
	authority := logicalMutationAuthority(statefulSetUID, binding)
	if !authority.valid() {
		return fmt.Errorf("fencing: logical compaction binding is incomplete")
	}
	return e.compactMutationJournalForAuthority(ctx, authority, nextSequence)
}

func (e *FencingExecutor) compactMutationJournalForAuthority(ctx context.Context, authority mutationExecutionAuthority, nextSequence uint64) error {
	if ctx == nil || !authority.valid() || e == nil || e.AuthorityState == nil {
		return fmt.Errorf("fencing: mutation compaction authority is incomplete")
	}
	if err := e.reconcileArchivedStagesForAuthority(ctx, authority); err != nil {
		return err
	}
	producer, err := e.reserveMutationProducerForAuthority(ctx, authority)
	if err != nil {
		return err
	}
	run, err := e.enrolledMutationRunForAuthority(ctx, authority)
	if err != nil {
		return err
	}
	producerUID := run.MutationProducerUID
	actualUID := mutationProducerUIDForAuthority(authority)
	if producerUID != actualUID || producerUID == "" || producer.Validate() != nil || producer.WALIdentity != run.Identity.WALIdentity || !mutationProducerMatchesAuthority(producer, authority) {
		return fmt.Errorf("fencing: compaction producer differs from its canonical Run")
	}
	if producer.LastAdmitted == ^uint64(0) || nextSequence != producer.LastAdmitted+1 ||
		producer.LastAdmitted-producer.CompactedThrough < fencing.MaxMutationJournalEntries/2 {
		return nil
	}
	through := producer.CompactedThrough
	for sequence := producer.CompactedThrough; sequence < producer.LastAdmitted; {
		sequence++
		entry := producer.Entries[sequence]
		if entry.Resolution == nil || (entry.Preparation != nil && entry.Preparation.Generation == 0 &&
			!entry.Resolution.AllCopiesTerminal && entry.CopyCompletion == nil && entry.GuardedLogicalResolution == nil) {
			break
		}
		through = sequence
	}
	if through == producer.CompactedThrough {
		return nil
	}
	plan, err := producer.PlanMutationCompaction(producerUID, through)
	if err != nil {
		return err
	}
	committed, err := e.commitMutationArchiveForAuthority(ctx, authority, plan)
	if err != nil {
		return err
	}
	if committed.CompactedThrough != plan.Through || committed.ArchiveHead == nil {
		return nil
	}
	return e.reconcileArchivedStagesForAuthority(ctx, authority)
}

func (e *FencingExecutor) reserveMutationProducerForAuthority(ctx context.Context, authority mutationExecutionAuthority) (fencing.MutationProducer, error) {
	if authority.runtime != nil {
		return e.reserveMutationProducer(ctx, *authority.runtime)
	}
	producer, uid, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err == nil && (uid != mutationProducerUIDForAuthority(authority) || !mutationProducerMatchesAuthority(producer, authority)) {
		err = fmt.Errorf("fencing: logical compaction producer differs from canonical binding")
	}
	return producer, err
}

// lookupArchivedMutationForAuthority returns only a previously compacted
// exact tuple. It never grants permission to dispatch or admit the request.
func (e *FencingExecutor) lookupArchivedMutationForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	producerUID string, sequence uint64, intent fencing.MutationIntent) (fencing.MutationEntry, error) {
	if ctx == nil || !authority.valid() || producerUID != mutationProducerUIDForAuthority(authority) || sequence == 0 ||
		intent.OperationID != fencing.MutationRequestID(producerUID, sequence) {
		return fencing.MutationEntry{}, fmt.Errorf("fencing: archived mutation lookup identity is incomplete")
	}
	producer, actualUID, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil {
		return fencing.MutationEntry{}, err
	}
	if actualUID != producerUID || producer.Validate() != nil || producer.EvidenceFault != "" ||
		!mutationProducerMatchesAuthority(producer, authority) {
		return fencing.MutationEntry{}, fmt.Errorf("fencing: archived mutation lookup producer differs")
	}
	if sequence > producer.CompactedThrough {
		return fencing.MutationEntry{}, ErrFencePending
	}
	verificationID := hashJSON([]any{"lookup-archived-mutation", producerUID, sequence, intent})
	done, found, err := e.scanMutationArchiveHistoryForAuthority(ctx, authority, verificationID, 32, nil, sequence)
	if err != nil {
		return fencing.MutationEntry{}, err
	}
	if !done || !found {
		return fencing.MutationEntry{}, ErrFencePending
	}
	producer, actualUID, _, err = e.readMutationProducerForAuthority(ctx, authority)
	if err != nil {
		return fencing.MutationEntry{}, err
	}
	if actualUID != producerUID || producer.ArchiveScan == nil || producer.ArchiveScan.VerificationID != verificationID ||
		producer.ArchiveScan.Sequence != sequence || producer.ArchiveScan.ResultArchive == nil {
		return fencing.MutationEntry{}, ErrFencePending
	}
	plan, err := e.readMutationArchiveForAuthority(ctx, authority, *producer.ArchiveScan.ResultArchive, producerUID, producer.WALIdentity)
	if err != nil {
		return fencing.MutationEntry{}, e.mutationArchiveFailureForAuthority(ctx, authority, err)
	}
	entry, ok := plan.Entries[sequence]
	if !ok {
		return fencing.MutationEntry{}, ErrFencePending
	}
	if entry.Intent != intent {
		return fencing.MutationEntry{}, fmt.Errorf("fencing: archived mutation retry changed the original intent")
	}
	if err := e.verifyMutationArchiveEntryForAuthority(ctx, authority, producerUID, sequence, entry); err != nil {
		return fencing.MutationEntry{}, e.mutationArchiveFailureForAuthority(ctx, authority, err)
	}
	return entry, nil
}

func logicalMutationAuthority(statefulSetUID string, binding fencing.LogicalWriterBindingV1) mutationExecutionAuthority {
	copy := binding
	return mutationExecutionAuthority{statefulSetUID: statefulSetUID, sourceClusterID: binding.SourceClusterID, runID: binding.RunID, logicalBinding: &copy}
}

func runtimeMutationAuthority(runtime fencing.RuntimeBinding) mutationExecutionAuthority {
	return mutationExecutionAuthority{statefulSetUID: runtime.StatefulSetUID, sourceClusterID: runtime.SourceClusterID,
		runID: runtime.RunID, runtime: &runtime}
}

func (e *FencingExecutor) enrolledLogicalMutationRun(ctx context.Context, authority mutationExecutionAuthority) (fencing.Run, error) {
	if ctx == nil || !authority.valid() || authority.logicalBinding == nil || e == nil || e.AuthorityState == nil {
		return fencing.Run{}, fmt.Errorf("fencing: logical mutation authority is incomplete")
	}
	state, err := e.read(ctx, authority.statefulSetUID, authority.sourceClusterID)
	if err != nil {
		return fencing.Run{}, err
	}
	if err := e.validateMutationJournalProfile(state); err != nil || state.Ledger.Profile != fencing.LogicalWriterProfileV1 {
		return fencing.Run{}, errors.Join(err, fmt.Errorf("fencing: logical mutation profile is unavailable"))
	}
	run, ok := state.Ledger.Runs[authority.runID]
	if !ok || !authority.matchesRun(run) || run.LogicalBinding == nil || !sameLogicalProducerBinding(run.LogicalBinding, authority.logicalBinding) ||
		run.MutationProducerUID == "" || run.MutationProducerUID != authority.producerUID() || run.MutationScopeHash != authority.logicalBinding.StoreScopeHash {
		return fencing.Run{}, fencing.ErrClosed
	}
	return run, nil
}

func (e *FencingExecutor) enrolledMutationRunForAuthority(ctx context.Context, authority mutationExecutionAuthority) (fencing.Run, error) {
	if authority.runtime != nil {
		return e.enrolledMutationRun(ctx, *authority.runtime)
	}
	return e.enrolledLogicalMutationRun(ctx, authority)
}

func mutationProducerMatchesAuthority(producer fencing.MutationProducer, authority mutationExecutionAuthority) bool {
	if authority.runtime != nil {
		return producer.Profile != fencing.LogicalWriterProfileV1 && producer.LogicalBinding == nil && producer.Runtime == *authority.runtime
	}
	return authority.logicalBinding != nil && producer.Profile == fencing.LogicalWriterProfileV1 && producer.Runtime == (fencing.RuntimeBinding{}) &&
		producer.LogicalBinding != nil && *producer.LogicalBinding == *authority.logicalBinding
}

func mutationProducerUIDForAuthority(authority mutationExecutionAuthority) string {
	if authority.logicalBinding != nil {
		return authority.logicalBinding.ProducerUID
	}
	if authority.runtime != nil {
		return stableMutationProducerUID(*authority.runtime)
	}
	return ""
}

func (e *FencingExecutor) evidenceStoreForMutationAuthority(authority mutationExecutionAuthority) (*AuthorityEvidenceStore, error) {
	if !authority.valid() {
		return nil, fmt.Errorf("fencing: mutation archive authority is incomplete")
	}
	if authority.runtime != nil {
		return e.authorityEvidenceFor(*authority.runtime)
	}
	return e.authorityEvidenceForRoot(authority.statefulSetUID, authority.sourceClusterID)
}

func mutationArchiveMatchesAuthority(plan fencing.MutationCompactionPlan, authority mutationExecutionAuthority,
	producerUID, wal string) bool {
	if plan.ProducerUID != producerUID || plan.WALIdentity != wal {
		return false
	}
	if authority.runtime != nil {
		return plan.Runtime == *authority.runtime && plan.Profile != fencing.LogicalWriterProfileV1 && plan.LogicalBinding == nil
	}
	return authority.logicalBinding != nil && plan.Runtime == (fencing.RuntimeBinding{}) &&
		plan.Profile == fencing.LogicalWriterProfileV1 && plan.LogicalBinding != nil && *plan.LogicalBinding == *authority.logicalBinding
}

func (e *FencingExecutor) readMutationArchiveForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	reference fencing.MutationArchiveReference, producerUID, wal string) (fencing.MutationCompactionPlan, error) {
	var empty fencing.MutationCompactionPlan
	if !reference.Valid() {
		return empty, errMutationArchiveEvidence
	}
	evidence, err := e.evidenceStoreForMutationAuthority(authority)
	if err != nil {
		return empty, err
	}
	r, version, err := evidence.Read(ctx, reference.Name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || evidence.IsNotFound(err) {
			return empty, errMutationArchiveEvidence
		}
		return empty, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, 700001))
	if err := errors.Join(readErr, r.Close()); err != nil {
		return empty, err
	}
	return e.decodeMutationArchiveForAuthority(data, version, reference, authority, producerUID, wal)
}

func (e *FencingExecutor) verifyLogicalMutationArchiveEntry(ctx context.Context, authority mutationExecutionAuthority,
	producerUID string, sequence uint64, entry fencing.MutationEntry) error {
	if entry.Preparation == nil {
		return nil
	}
	if entry.GuardedLogicalResolution != nil {
		producer := fencing.MutationProducer{Profile: fencing.LogicalWriterProfileV1, LogicalBinding: authority.logicalBinding,
			WALIdentity: authority.logicalBinding.WALIdentity, Phase: fencing.MutationClosed, ClosedThrough: sequence,
			LastAdmitted: sequence, CompactedThrough: sequence - 1, Entries: map[uint64]fencing.MutationEntry{sequence: entry}}
		return producer.ValidateGuardedLogicalResolution(producerUID, sequence, entry)
	}
	if entry.Resolution == nil {
		return errMutationArchiveEvidence
	}
	receipts, err := e.receiptsForLogicalBinding(authority.statefulSetUID, *authority.logicalBinding)
	if err != nil {
		return err
	}
	owner := hashJSON([]any{"rhiza-logical-mutation-receipt-v1", *authority.logicalBinding,
		authority.logicalBinding.WALIdentity, producerUID, sequence, entry.Intent.OperationID})
	receipt, err := receipts.get("receipt-" + owner)
	if err != nil {
		return err
	}
	dispatch, dispatched := entry.Dispatches[receipt.DispatchID]
	resolution := entry.Resolution
	if receipt.Runtime != (fencing.RuntimeBinding{}) || receipt.AuthorityProfile != fencing.LogicalWriterProfileV1 ||
		receipt.LogicalBinding == nil || *receipt.LogicalBinding != *authority.logicalBinding || receipt.WALIdentity != authority.logicalBinding.WALIdentity ||
		receipt.ProducerUID != producerUID || receipt.Sequence != sequence || receipt.Intent != entry.Intent || receipt.Preparation != *entry.Preparation ||
		!mutationReceiptMatchesDispatch(entry, receipt) || !dispatched || receipt.ReceiptDigest != resolution.ReceiptDigest ||
		receipt.Outcome != resolution.Outcome || receipt.ConditionFailed != resolution.ConditionFailed || receipt.PayloadDigest != resolution.PayloadDigest ||
		receipt.AllCopiesTerminal != resolution.AllCopiesTerminal || (resolution.AllCopiesTerminal && dispatch.ReceiptDigest != receipt.ReceiptDigest) {
		return errMutationArchiveEvidence
	}
	return nil
}

func (e *FencingExecutor) verifyMutationArchiveEntryForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	producerUID string, sequence uint64, entry fencing.MutationEntry) error {
	if authority.runtime == nil {
		return e.verifyLogicalMutationArchiveEntry(ctx, authority, producerUID, sequence, entry)
	}
	if entry.Preparation == nil {
		return nil
	}
	verify := e.VerifyMutationEvidence
	if verify == nil {
		verify = e.VerifySourceMutationEvidence
	}
	return verify(ctx, *authority.runtime, sequence, entry)
}

func (e *FencingExecutor) persistMutationArchiveForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	plan fencing.MutationCompactionPlan) (fencing.MutationArchiveReference, error) {
	var zero fencing.MutationArchiveReference
	if err := plan.Validate(); err != nil || !mutationArchiveMatchesAuthority(plan, authority, plan.ProducerUID, plan.WALIdentity) {
		return zero, errors.Join(err, fmt.Errorf("fencing: compaction plan authority differs"))
	}
	if authority.runtime != nil {
		if err := e.verifyMutationArchiveReceipts(ctx, plan); err != nil {
			return zero, err
		}
	} else {
		for sequence, entry := range plan.Entries {
			if err := e.verifyLogicalMutationArchiveEntry(ctx, authority, plan.ProducerUID, sequence, entry); err != nil {
				return zero, err
			}
		}
	}
	data, err := json.Marshal(plan)
	if err != nil || len(data) > 700000 {
		return zero, errors.Join(err, fmt.Errorf("fencing: archive capacity exhausted"))
	}
	reference := fencing.MutationArchiveReference{Digest: plan.Digest()}
	reference.Name = fencing.MutationArchiveName(reference.Digest)
	evidence, err := e.evidenceStoreForMutationAuthority(authority)
	if err != nil {
		return zero, err
	}
	version, err := evidence.CreateImmutable(ctx, reference.Name, data)
	if err != nil {
		return reference, err
	}
	if version == nil || version.Type != objstore.Generation || version.Value == "" {
		return reference, errMutationArchiveEvidence
	}
	reference.UID = version.Value
	if _, err := e.readMutationArchiveForAuthority(ctx, authority, reference, plan.ProducerUID, plan.WALIdentity); err != nil {
		return reference, err
	}
	return reference, nil
}

func (e *FencingExecutor) commitMutationArchiveForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	plan fencing.MutationCompactionPlan) (fencing.MutationProducer, error) {
	var empty fencing.MutationProducer
	run, err := e.enrolledMutationRunForAuthority(ctx, authority)
	if err != nil {
		return empty, err
	}
	if plan.ProducerUID != run.MutationProducerUID || plan.WALIdentity != run.Identity.WALIdentity ||
		!mutationArchiveMatchesAuthority(plan, authority, run.MutationProducerUID, run.Identity.WALIdentity) {
		return empty, fmt.Errorf("fencing: compaction identity differs from logical enrollment")
	}
	ref, err := e.persistMutationArchiveForAuthority(ctx, authority, plan)
	if err != nil {
		return empty, err
	}
	committed, commitErr := e.changeMutationProducerForAuthority(ctx, authority, run.MutationProducerUID, func(producer *fencing.MutationProducer) error {
		return producer.CommitMutationCompaction(run.MutationProducerUID, plan, ref)
	})
	if commitErr == nil {
		return committed, nil
	}
	done, found, scanErr := e.scanMutationArchiveHistoryForAuthority(ctx, authority, hashJSON([]any{"resolve-compaction", ref}), 32, &ref, 0)
	if scanErr != nil {
		return committed, errors.Join(commitErr, scanErr)
	}
	if !done {
		return committed, ErrFencePending
	}
	if !found {
		return committed, commitErr
	}
	committed, _, _, readErr := e.readMutationProducerForAuthority(ctx, authority)
	return committed, readErr
}

func (e *FencingExecutor) scanMutationArchiveHistoryForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	verificationID string, budget int, sought *fencing.MutationArchiveReference, sequence uint64) (bool, bool, error) {
	if ctx == nil || !authority.valid() || budget < 1 || budget > 64 ||
		len(verificationID) != 64 || (sought != nil && (!sought.Valid() || sequence != 0)) {
		return false, false, fmt.Errorf("fencing: invalid logical archive scan request")
	}
	producer, producerUID, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil {
		return false, false, err
	}
	if producer.EvidenceFault != "" || producer.Validate() != nil || !mutationProducerMatchesAuthority(producer, authority) {
		return false, false, fencing.ErrUnavailable
	}
	if producer.ArchiveHead == nil {
		return true, false, nil
	}
	if sequence > producer.CompactedThrough {
		return false, false, fmt.Errorf("fencing: result sequence is not archived")
	}
	run, err := e.enrolledMutationRunForAuthority(ctx, authority)
	if err != nil {
		return false, false, err
	}
	head := *producer.ArchiveHead
	scan := fencing.MutationArchiveScan{VerificationID: verificationID, Head: head, Next: &head, Through: producer.CompactedThrough, Sought: sought, Sequence: sequence}
	if old := producer.ArchiveScan; old != nil && old.VerificationID == verificationID && old.Head == head &&
		hashJSON(old.Sought) == hashJSON(sought) && old.Sequence == sequence {
		scan = *old
	}
	for i := 0; i < budget && scan.Next != nil; i++ {
		ref := *scan.Next
		plan, readErr := e.readMutationArchiveForAuthority(ctx, authority, ref, run.MutationProducerUID, producer.WALIdentity)
		if readErr == nil && plan.Through != scan.Through {
			readErr = errMutationArchiveEvidence
		}
		if readErr == nil {
			if authority.runtime != nil {
				readErr = e.verifyMutationArchiveReceipts(ctx, plan)
			} else {
				for entrySequence, entry := range plan.Entries {
					if readErr = e.verifyLogicalMutationArchiveEntry(ctx, authority, run.MutationProducerUID, entrySequence, entry); readErr != nil {
						break
					}
				}
			}
		}
		if readErr != nil {
			return false, false, e.mutationArchiveFailureForAuthority(ctx, authority, readErr)
		}
		if sought != nil && ref == *sought {
			scan.Found = true
		}
		if sequence != 0 && plan.PreviousThrough < sequence && sequence <= plan.Through {
			scan.Found = true
			copy := ref
			scan.ResultArchive = &copy
		}
		scan.Through, scan.Next = plan.PreviousThrough, plan.Previous
		scan.Links++
	}
	if (scan.Next == nil) != (scan.Through == 0) {
		return false, false, e.mutationArchiveFailureForAuthority(ctx, authority, errMutationArchiveEvidence)
	}
	_, err = e.changeMutationProducerForAuthority(ctx, authority, producerUID, func(current *fencing.MutationProducer) error {
		if current.ArchiveHead == nil || *current.ArchiveHead != head || current.CompactedThrough != producer.CompactedThrough {
			return ErrFencePending
		}
		current.ArchiveScan = &scan
		return nil
	})
	if err != nil {
		return false, false, err
	}
	return scan.Next == nil, scan.Found, nil
}

func (e *FencingExecutor) mutationArchiveFailureForAuthority(ctx context.Context, authority mutationExecutionAuthority, err error) error {
	if authority.runtime != nil {
		return e.mutationArchiveFailure(ctx, *authority.runtime, err)
	}
	if !errors.Is(err, errMutationArchiveEvidence) || authority.logicalBinding == nil {
		return err
	}
	_, latchErr := e.changeMutationProducerForAuthority(ctx, authority, authority.producerUID(), func(producer *fencing.MutationProducer) error {
		if producer.EvidenceFault == "" {
			producer.EvidenceFault = hashJSON([]any{"logical-mutation-archive-evidence-fault", authority.statefulSetUID,
				authority.sourceClusterID, authority.runID, producer.ArchiveHead})
		}
		return nil
	})
	return errors.Join(err, latchErr)
}

// logicalMutationArchiveFailure remains the closure verifier's profile-specific
// entry point while both profiles share the archive lifecycle implementation.
func (e *FencingExecutor) logicalMutationArchiveFailure(ctx context.Context, authority mutationExecutionAuthority, err error) error {
	if authority.runtime != nil || authority.logicalBinding == nil {
		return err
	}
	return e.mutationArchiveFailureForAuthority(ctx, authority, err)
}

func (e *FencingExecutor) reconcileArchivedStagesForAuthority(ctx context.Context, authority mutationExecutionAuthority) error {
	if ctx == nil || !authority.valid() || e == nil || e.AuthorityState == nil {
		return fmt.Errorf("fencing: archived-stage reconciliation is not configured")
	}
	producer, err := e.reserveMutationProducerForAuthority(ctx, authority)
	if err != nil {
		return err
	}
	run, err := e.enrolledMutationRunForAuthority(ctx, authority)
	producerUID := mutationProducerUIDForAuthority(authority)
	if err != nil || producerUID != run.MutationProducerUID || producer.Validate() != nil || !mutationProducerMatchesAuthority(producer, authority) {
		return errors.Join(err, fmt.Errorf("fencing: archived-stage producer differs from canonical Run"))
	}
	stop := producer.CompactedThrough
	if remaining := stop - producer.RetiredStageThrough; remaining > archivedStageRetirementBatch {
		stop = producer.RetiredStageThrough + archivedStageRetirementBatch
	}
	for retired := producer.RetiredStageThrough; retired < stop; retired++ {
		sequence := retired + 1
		entry, archiveRef, err := e.archivedMutationAtForAuthority(ctx, authority, producerUID, sequence)
		if err != nil {
			return err
		}
		if entry.Intent.Kind == "upload" && strings.HasPrefix(entry.Intent.PayloadRef, "payload-") {
			if err := e.retireArchivedSourceStageForAuthority(ctx, authority, producerUID, sequence, entry.Intent, archiveRef); err != nil {
				return err
			}
		}
		if _, err := e.changeMutationProducerForAuthority(ctx, authority, producerUID, func(p *fencing.MutationProducer) error {
			return p.AdvanceArchivedStageRetirement(sequence)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *FencingExecutor) archivedMutationAtForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	producerUID string, sequence uint64) (fencing.MutationEntry, fencing.MutationArchiveReference, error) {
	verificationID := hashJSON([]any{"lookup-archived-sequence", producerUID, sequence})
	done, found, err := e.scanMutationArchiveHistoryForAuthority(ctx, authority, verificationID, 32, nil, sequence)
	if err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, err
	}
	if !done || !found {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, ErrFencePending
	}
	producer, actualUID, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, err
	}
	if actualUID != producerUID || producer.ArchiveScan == nil || producer.ArchiveScan.VerificationID != verificationID ||
		producer.ArchiveScan.Sequence != sequence || producer.ArchiveScan.ResultArchive == nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, ErrFencePending
	}
	ref := *producer.ArchiveScan.ResultArchive
	plan, err := e.readMutationArchiveForAuthority(ctx, authority, ref, producerUID, producer.WALIdentity)
	if err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, e.mutationArchiveFailureForAuthority(ctx, authority, err)
	}
	entry, ok := plan.Entries[sequence]
	if !ok {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, e.mutationArchiveFailureForAuthority(ctx, authority, errMutationArchiveEvidence)
	}
	if err := e.verifyMutationArchiveEntryForAuthority(ctx, authority, producerUID, sequence, entry); err != nil {
		return fencing.MutationEntry{}, fencing.MutationArchiveReference{}, e.mutationArchiveFailureForAuthority(ctx, authority, err)
	}
	return entry, ref, nil
}

func (e *FencingExecutor) retireArchivedSourceStageForAuthority(ctx context.Context, authority mutationExecutionAuthority,
	producerUID string, sequence uint64, intent fencing.MutationIntent, archiveRef fencing.MutationArchiveReference) error {
	if authority.runtime != nil {
		return e.retireArchivedSourceStage(ctx, *authority.runtime, producerUID, sequence, intent, archiveRef)
	}
	if authority.logicalBinding == nil || !archiveRef.Valid() || intent.Kind != "upload" || !strings.HasPrefix(intent.PayloadRef, "payload-") {
		return fmt.Errorf("fencing: logical archived source-stage retirement proof is incomplete")
	}
	entry, err := e.lookupArchivedMutationForAuthority(ctx, authority, producerUID, sequence, intent)
	if err != nil {
		return err
	}
	if entry.Intent != intent {
		return fmt.Errorf("fencing: logical archived source-stage intent differs")
	}
	verificationID := hashJSON([]any{"lookup-archived-mutation", producerUID, sequence, intent})
	producer, actualUID, _, err := e.readMutationProducerForAuthority(ctx, authority)
	if err != nil {
		return err
	}
	if actualUID != producerUID || producer.ArchiveScan == nil || producer.ArchiveScan.VerificationID != verificationID ||
		producer.ArchiveScan.Sequence != sequence || producer.ArchiveScan.ResultArchive == nil || *producer.ArchiveScan.ResultArchive != archiveRef {
		return ErrFencePending
	}
	stages, err := e.sourceStagesForRoot(ctx, authority.statefulSetUID, authority.sourceClusterID)
	if err != nil {
		return err
	}
	ticket := strings.TrimPrefix(intent.PayloadRef, "payload-")
	if !validStageTicket(ticket) {
		return fmt.Errorf("fencing: logical archived source-stage ticket is invalid")
	}
	draft := intent
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	want := sourceStageDescriptor{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}
	record, generation, err := stages.readRemoteStage(ctx, producerUID, ticket)
	if err != nil {
		return err
	}
	if record.Draft != draft || record.Payload == nil || *record.Payload != want || record.Cancelled {
		return fmt.Errorf("fencing: logical archived source stage differs from its reservation")
	}
	if !record.Retired {
		payloadKey := sourceStagePayloadKey(producerUID, ticket)
		payload, payloadVersion, err := stages.evidence.Read(ctx, payloadKey)
		if err != nil && !stages.evidence.IsNotFound(err) {
			return err
		}
		if err == nil {
			verified, verifyErr := verifiedStageTemp(payload, want.Digest, want.Size)
			closeErr := payload.Close()
			if verifyErr != nil || closeErr != nil || payloadVersion == nil || payloadVersion.Type != objstore.Generation {
				if verified != nil {
					_ = verified.Close()
				}
				return errors.Join(verifyErr, closeErr, fmt.Errorf("fencing: logical archived payload failed exact-version verification"))
			}
			_ = verified.Close()
			if err := stages.evidence.DeleteVersion(ctx, payloadKey, *payloadVersion); err != nil {
				return err
			}
		}
		record.Retired = true
		if _, err := stages.writeRemoteStage(ctx, producerUID, ticket, generation, record); err != nil {
			return err
		}
	}
	return stages.retireRemoteStageQuota(ctx, producerUID, ticket, want.Size)
}
