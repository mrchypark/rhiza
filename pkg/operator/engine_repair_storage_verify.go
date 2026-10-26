package operator

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// verifyEngineRemovalStorageEffects is the read-only terminal gate used by a
// survivor's admission path. It reads canonical root state, immutable proof,
// producer journals and generation-zero guards; it never settles or closes a
// producer and never uses the removed or candidate runtime as its caller.
func (e *FencingExecutor) verifyEngineRemovalStorageEffects(
	ctx context.Context,
	statefulSetUID string,
	caller fencing.Identity,
	operationID string,
	terminal quepaxa.EngineRemoval,
) error {
	if e == nil || ctx == nil || e.AuthorityState == nil || statefulSetUID == "" || operationID == "" ||
		terminal.OperationID != operationID || terminal.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) {
		return fmt.Errorf("fencing: engine removal storage-effects verification is incomplete")
	}
	state, err := e.read(ctx, statefulSetUID, caller.SourceClusterID)
	if err != nil {
		return err
	}
	if state.StatefulSetUID != statefulSetUID || state.Ledger.SourceClusterID != caller.SourceClusterID ||
		state.Ledger.ClosedBy != "" || state.Ledger.ClosedHash != "" {
		return fmt.Errorf("fencing: engine removal storage-effects source is not the exact open authority root")
	}
	if err := engineRepairCallerIsActiveSurvivor(state, caller); err != nil {
		return err
	}
	run := state.Ledger.Runs[caller.RunID]
	if run.Runtime == nil || run.Runtime.StatefulSetUID != statefulSetUID || run.Runtime.Validate(caller) != nil {
		return fmt.Errorf("fencing: engine removal storage-effects caller has no exact survivor runtime")
	}
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return err
	}
	repair, err := readEngineRepairStorageRecordByOperation(ctx, store, state, caller.SourceClusterID, operationID)
	if err != nil {
		return err
	}
	if repair.Validate() != nil || validateEngineRepairStorageInventory(repair, state.Ledger) != nil || repair.RemovalOperation == nil ||
		repair.Binding.StatefulSetUID != statefulSetUID || repair.Binding.SourceClusterID != caller.SourceClusterID ||
		repair.Binding.OperationID != operationID || repair.Binding.OldMemberID == caller.NodeID ||
		repair.StorageProofDigest == "" || !repairHasStorageProofPhase(repair) {
		return fmt.Errorf("fencing: engine removal storage proof lacks its canonical frozen repair record")
	}
	survivor, exists := repair.SourceConfig.MemberSet()[quepaxa.NodeID(caller.NodeID)]
	if !exists || survivor.WALIdentity != caller.WALIdentity || caller.NodeID == repair.CurrentCandidateIdentity().NodeID {
		return fmt.Errorf("fencing: engine removal proof verifier is not an original surviving voter")
	}
	// The canonical record pins the original freeze marker with a zero proof
	// hash. The terminal marker may fill only that field after proof settlement.
	canonicalOperation := *repair.RemovalOperation
	if !fencing.SameEngineRemovalOperation(canonicalOperation, clearStorageEffectsHash(terminal)) {
		return fmt.Errorf("fencing: engine removal terminal differs from the canonical marker")
	}
	if terminal.StorageEffectsExcludedHash != valueHashFromDigest(repair.StorageProofDigest) {
		return fmt.Errorf("fencing: engine removal terminal names another storage-effects proof")
	}
	proof, err := e.readCanonicalEngineRepairStorageProof(ctx, repair, run.Runtime)
	if err != nil {
		return err
	}
	if proof.ProofDigest != repair.StorageProofDigest || proof.Validate(repair) != nil {
		return fmt.Errorf("fencing: immutable engine removal storage proof differs from the canonical repair")
	}
	return e.verifyEngineRepairStorageSettlements(ctx, repair, state.Ledger, proof)
}

func repairHasStorageProofPhase(repair fencing.EngineRepairRecord) bool {
	switch repair.Phase {
	case fencing.RepairStorageClosing, fencing.RepairRemoved, fencing.RepairAdding, fencing.RepairActive:
		return true
	case fencing.RepairGenerationClosing:
		return repair.ClosingFrom == fencing.RepairStorageClosing || repair.ClosingFrom == fencing.RepairRemoved ||
			repair.ClosingFrom == fencing.RepairAdding || repair.ClosingFrom == fencing.RepairActive
	default:
		return false
	}
}

func clearStorageEffectsHash(operation quepaxa.EngineRemoval) quepaxa.EngineRemoval {
	operation.StorageEffectsExcludedHash = quepaxa.ValueHash{}
	return operation
}

func valueHashFromDigest(digest string) quepaxa.ValueHash {
	var out quepaxa.ValueHash
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != len(out) {
		return out
	}
	copy(out[:], decoded)
	return out
}

func (e *FencingExecutor) readCanonicalEngineRepairStorageProof(ctx context.Context, repair fencing.EngineRepairRecord, runtime *fencing.RuntimeBinding) (fencing.StorageEffectsExcluded, error) {
	var proof fencing.StorageEffectsExcluded
	if e == nil || e.AuthorityState == nil || repair.Validate() != nil || runtime != nil &&
		(runtime.StatefulSetUID != repair.Binding.StatefulSetUID || runtime.SourceClusterID != repair.Binding.SourceClusterID) {
		return proof, fmt.Errorf("fencing: immutable storage proof has no matching authority root")
	}
	evidence, err := NewAuthorityEvidenceStore(e.AuthorityState.bucket, e.AuthorityState.prefix, repair.Binding.StatefulSetUID, repair.Binding.SourceClusterID)
	if err != nil {
		return proof, err
	}
	r, _, err := evidence.Read(ctx, engineRepairStorageEffectsKey(repair.IdentityDigest(), repair.StorageProofDigest))
	if err != nil {
		return proof, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxAuthorityStateBytes+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) > maxAuthorityStateBytes {
		return proof, errors.Join(readErr, closeErr, fmt.Errorf("fencing: immutable engine repair storage proof is unreadable"))
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&proof); err != nil || decoder.Decode(new(any)) != io.EOF {
		return fencing.StorageEffectsExcluded{}, errors.Join(err, fmt.Errorf("fencing: immutable engine repair storage proof is malformed"))
	}
	canonical, err := json.Marshal(proof)
	if err != nil || !bytes.Equal(canonical, data) {
		return fencing.StorageEffectsExcluded{}, errors.Join(err, fmt.Errorf("fencing: immutable engine repair storage proof is not canonical JSON"))
	}
	return proof, nil
}

func (e *FencingExecutor) verifyEngineRepairStorageSettlements(ctx context.Context, repair fencing.EngineRepairRecord, ledger fencing.Ledger, proof fencing.StorageEffectsExcluded) error {
	settlements := make(map[string]fencing.StorageRunSettlement, len(proof.RunSettlements))
	for _, settlement := range proof.RunSettlements {
		settlements[settlement.RunID] = settlement
	}
	for _, incarnation := range repair.Incarnations {
		if incarnation.Identity.NodeID != repair.Binding.OldMemberID {
			continue
		}
		settlement, ok := settlements[incarnation.Identity.RunID]
		run, exists := ledger.Runs[incarnation.Identity.RunID]
		if !ok || !exists || run.Identity != incarnation.Identity || run.MutationProducerUID != incarnation.ProducerUID {
			return fmt.Errorf("fencing: storage proof does not match the canonical removed-run inventory")
		}
		if incarnation.ProducerUID == "" {
			if run.Runtime != nil {
				if _, uid, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime); err == nil || uid != "" {
					return fmt.Errorf("fencing: removed run without a producer has canonical producer state")
				} else if !errors.Is(err, errAuthorityStateNotFound) {
					return err
				}
			}
			want, err := engineRepairNoProducerSettlement(repair, ledger, incarnation, run)
			if err != nil || settlement != want {
				return errors.Join(err, fmt.Errorf("fencing: no-producer proof differs from the exact closed old run"))
			}
			continue
		}
		if run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil || run.Runtime.StatefulSetUID != repair.Binding.StatefulSetUID ||
			run.MutationProducerUID != settlement.ProducerUID || run.Identity.WALIdentity != settlement.WALIdentity {
			return fmt.Errorf("fencing: removed producer runtime differs from its canonical settlement")
		}
		producer, uid, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
		if err != nil || uid != incarnation.ProducerUID || producer.Validate() != nil || producer.Runtime != *run.Runtime ||
			producer.WALIdentity != run.Identity.WALIdentity || producer.Phase != fencing.MutationClosed {
			return errors.Join(err, fmt.Errorf("fencing: removed producer is not at the proof's exact closed settlement"))
		}
		if err := e.verifyEngineRepairProducerStorageClosure(ctx, repair, run, uid, producer); err != nil {
			return err
		}
		want, err := e.engineRepairProducerRunSettlement(ctx, repair, run, producer, uid)
		if err != nil || want != settlement {
			return errors.Join(err, fmt.Errorf("fencing: removed producer differs from the proof's exact closed settlement"))
		}
		if err := e.validateMutationArchiveHead(ctx, *run.Runtime, uid, producer); err != nil {
			return err
		}
	}
	return nil
}

// engineRepairProducerRunSettlement hashes the immutable closed journal prefix,
// not the mutable archive cursor/head used to store that prefix. It walks the
// committed archive chain newest-to-oldest, verifies every page and entry, then
// folds entries in descending sequence order so page boundaries do not affect
// the digest. The same projection is used when creating and revalidating proof.
func (e *FencingExecutor) engineRepairProducerRunSettlement(
	ctx context.Context,
	repair fencing.EngineRepairRecord,
	run fencing.Run,
	producer fencing.MutationProducer,
	producerUID string,
) (fencing.StorageRunSettlement, error) {
	var empty fencing.StorageRunSettlement
	if ctx == nil || producer.Validate() != nil || producer.Phase != fencing.MutationClosed ||
		producerUID == "" || producer.EvidenceFault != "" || producer.ClosedThrough != producer.LastAdmitted ||
		run.Runtime == nil || producer.Runtime != *run.Runtime || producer.WALIdentity != run.Identity.WALIdentity {
		return empty, ErrFencePending
	}
	state := engineRepairStorageClosureState{
		Runtime: producer.Runtime, WALIdentity: producer.WALIdentity, ProducerUID: producerUID,
		Phase: producer.Phase, LastAdmitted: producer.LastAdmitted, ClosedThrough: producer.ClosedThrough,
		CloseOperation: producer.CloseOperationID, CloseRequest: producer.CloseRequestHash,
	}
	// A reverse fold makes the semantic digest independent of how the immutable
	// prefix was split among successive compaction pages.
	prefix := hashJSON([]any{"engine-repair-closed-prefix-v1", repair.IdentityDigest(), producer.Runtime, producer.WALIdentity, producerUID, producer.ClosedThrough})
	nextSequence := producer.ClosedThrough
	consume := func(sequence uint64, entry fencing.MutationEntry) error {
		if nextSequence == 0 || sequence != nextSequence {
			return errMutationArchiveEvidence
		}
		if err := e.verifyEngineRepairStorageClosureEntry(ctx, repair, run, producerUID, sequence, entry); err != nil {
			return err
		}
		prefix = hashJSON([]any{"engine-repair-entry-v1", sequence, engineRepairStorageClosureEntry(entry), prefix})
		nextSequence--
		return nil
	}

	for sequence := producer.ClosedThrough; sequence > producer.CompactedThrough; sequence-- {
		entry, ok := producer.Entries[sequence]
		if !ok {
			return empty, errMutationArchiveEvidence
		}
		if err := consume(sequence, entry); err != nil {
			return empty, err
		}
	}
	if producer.CompactedThrough == 0 {
		if producer.ArchiveHead != nil {
			return empty, errMutationArchiveEvidence
		}
	} else {
		if producer.ArchiveHead == nil {
			return empty, errMutationArchiveEvidence
		}
		through := producer.CompactedThrough
		ref := producer.ArchiveHead
		for ref != nil {
			plan, err := e.readMutationArchive(ctx, *ref, producer.Runtime, producerUID, producer.WALIdentity)
			if err != nil || plan.Through != through || plan.PreviousThrough >= plan.Through ||
				uint64(len(plan.Entries)) != plan.Through-plan.PreviousThrough {
				return empty, errors.Join(err, errMutationArchiveEvidence)
			}
			if err := e.verifyMutationArchiveReceipts(ctx, plan); err != nil {
				return empty, err
			}
			for sequence := plan.Through; sequence > plan.PreviousThrough; sequence-- {
				entry, ok := plan.Entries[sequence]
				if !ok {
					return empty, errMutationArchiveEvidence
				}
				if err := consume(sequence, entry); err != nil {
					return empty, err
				}
			}
			through = plan.PreviousThrough
			ref = plan.Previous
		}
		if through != 0 {
			return empty, errMutationArchiveEvidence
		}
	}
	if nextSequence != 0 {
		return empty, errMutationArchiveEvidence
	}
	state.PrefixDigest = prefix
	stateHash := hashJSON(state)
	return fencing.StorageRunSettlement{
		RunID: producer.Runtime.RunID, PodUID: producer.Runtime.PodUID, WALIdentity: producer.WALIdentity, ProducerUID: producerUID,
		ClosedThrough: producer.ClosedThrough, SettledThrough: producer.ClosedThrough, ProducerStateHash: stateHash,
		SettlementDigest: hashJSON([]any{"engine-repair-producer-settlement-v2", repair.IdentityDigest(), state}),
	}, nil
}

func engineRepairStorageClosureEntry(entry fencing.MutationEntry) fencing.MutationEntry {
	if entry.GenerationZeroGuard == nil {
		return entry
	}
	entry.CopyCompletion = nil
	dispatches := make(map[string]fencing.MutationDispatch, len(entry.Dispatches))
	for id, dispatch := range entry.Dispatches {
		if id == entry.GenerationZeroGuard.DispatchID {
			dispatch.ReceiptDigest = ""
		}
		dispatches[id] = dispatch
	}
	entry.Dispatches = dispatches
	return entry
}

func (e *FencingExecutor) verifyEngineRepairStorageClosureEntry(
	ctx context.Context,
	repair fencing.EngineRepairRecord,
	run fencing.Run,
	producerUID string,
	sequence uint64,
	entry fencing.MutationEntry,
) error {
	if (entry.GenerationZeroGuard == nil || entry.CopyCompletion != nil) && !storageClosureEntryTerminal(entry) {
		return ErrFencePending
	}
	if entry.GenerationZeroGuard != nil && entry.CopyCompletion == nil {
		rights := entry.GenerationZeroRights
		dispatch, ok := entry.Dispatches[entry.GenerationZeroGuard.DispatchID]
		if entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.AllCopiesTerminal || !ok || dispatch.ReceiptDigest != "" ||
			rights == nil || !rights.Sealed || !rights.HelperRightsRetired || len(rights.Rights) == 0 {
			return ErrFencePending
		}
		for _, right := range rights.Rights {
			if right.TerminalReceiptDigest == "" || !mutationDigestValidOperator(right.TerminalReceiptDigest) {
				return ErrFencePending
			}
		}
	}
	if run.Runtime == nil || run.MutationProducerUID != producerUID {
		return ErrFencePending
	}
	if err := e.VerifySourceMutationEvidence(ctx, *run.Runtime, sequence, entry); err != nil {
		return fmt.Errorf("fencing: removed producer journal evidence failed verification: %w", err)
	}
	return nil
}

func (e *FencingExecutor) verifyEngineRepairProducerStorageClosure(ctx context.Context, repair fencing.EngineRepairRecord, run fencing.Run, producerUID string, producer fencing.MutationProducer) error {
	if producer.Validate() != nil || producer.Phase != fencing.MutationClosed || run.Runtime == nil || producer.Runtime != *run.Runtime ||
		producerUID == "" || producer.WALIdentity != run.Identity.WALIdentity || producer.ClosedThrough != producer.LastAdmitted {
		return ErrFencePending
	}
	for sequence := producer.CompactedThrough; sequence < producer.ClosedThrough; {
		sequence++
		entry, exists := producer.Entries[sequence]
		if !exists {
			return ErrFencePending
		}
		if err := e.verifyEngineRepairStorageClosureEntry(ctx, repair, run, producerUID, sequence, entry); err != nil {
			return err
		}
	}
	return nil
}

func storageClosureEntryTerminal(entry fencing.MutationEntry) bool {
	if entry.Resolution == nil {
		return false
	}
	if entry.GenerationZeroGuard != nil {
		return entry.CopyCompletion != nil && entry.GenerationZeroRights != nil
	}
	if entry.Preparation == nil || entry.Preparation.Generation != 0 || entry.Resolution.AllCopiesTerminal || entry.CopyCompletion != nil ||
		entry.GuardedLogicalResolution != nil {
		return true
	}
	return false
}
