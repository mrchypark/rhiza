package operator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
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

// mutationGuardedReplayRequest carries the complete canonical mutation tuple
// plus the exact root identity that must own the immutable guard record.
type mutationGuardedReplayRequest struct {
	Runtime           fencing.RuntimeBinding
	AuthorityProfile  string
	LogicalBinding    *fencing.LogicalWriterBindingV1
	WALIdentity       string
	ProducerUID       string
	Sequence          uint64
	Intent            fencing.MutationIntent
	Preparation       fencing.MutationPreparation
	PreparationHash   string
	DispatchID        string
	GuardRootIdentity string
}

// mutationGuardedReplayProvider is implemented only by the held GCS profile.
// replay creates/read-backs the immutable guard before replaying exact staged
// bytes; verify rereads that guard and the exact held object generation.
type mutationGuardedReplayProvider interface {
	replayGuardedCreate(context.Context, *AuthorityEvidenceStore, mutationGuardedReplayRequest, io.Reader) (fencing.MutationGuardedLogicalResolution, error)
	verifyGuardedCreate(context.Context, *AuthorityEvidenceStore, mutationGuardedReplayRequest, fencing.MutationGuardedLogicalResolution) error
}

// mutationHeldCreateFinalizer releases a successful original create's hold
// only after its exact receipt has been committed to the canonical producer.
type mutationHeldCreateFinalizer interface {
	finalizeHeldCreate(context.Context, string, fencing.MutationIntent, mutationExecutionReceipt) error
}

type engineRepairMutationSettlement struct {
	Version               int                         `json:"version"`
	RepairRecordHash      string                      `json:"repairRecordHash"`
	Runtime               fencing.RuntimeBinding      `json:"runtime"`
	WALIdentity           string                      `json:"walIdentity"`
	ProducerUID           string                      `json:"producerUID"`
	Sequence              uint64                      `json:"sequence"`
	Intent                fencing.MutationIntent      `json:"intent"`
	Preparation           fencing.MutationPreparation `json:"preparation"`
	DispatchID            string                      `json:"dispatchID"`
	Outcome               fencing.MutationOutcome     `json:"outcome"`
	ConditionFailed       bool                        `json:"conditionFailed,omitempty"`
	AllCopiesTerminal     bool                        `json:"allCopiesTerminal,omitempty"`
	ActualCommitProven    bool                        `json:"actualCommitProven"`
	FutureEffectsExcluded bool                        `json:"futureEffectsExcluded"`
	Generation            int64                       `json:"generation"`
	Metageneration        int64                       `json:"metageneration,omitempty"`
	PayloadDigest         string                      `json:"payloadDigest,omitempty"`
	ReceiptDigest         string                      `json:"receiptDigest"`
}

type mutationKeyState struct {
	ScopeHash             string                                     `json:"scopeHash"`
	Key                   string                                     `json:"key"`
	Owner                 string                                     `json:"owner,omitempty"`
	PendingGenerationZero *mutationGenerationZeroRight               `json:"pendingGenerationZeroRight,omitempty"`
	Barrier               *mutationGenerationZeroBarrier             `json:"generationZeroBarrier,omitempty"`
	Retirement            *mutationGenerationZeroRetirementReference `json:"generationZeroRetirement,omitempty"`
}

// mutationGenerationZeroRight records the exact already-sealed journal
// dispatch before repair settlement may send its create-only replay. It is a
// reference to the original permission, not a second permission or a terminal
// claim. The object-level hold is applied atomically by GCS when that request
// creates the name.
type mutationGenerationZeroRight struct {
	RepairRecordHash  string                           `json:"repairRecordHash"`
	RepairBinding     fencing.EngineRemovalBinding     `json:"repairBinding"`
	Runtime           fencing.RuntimeBinding           `json:"runtime"`
	WALIdentity       string                           `json:"walIdentity"`
	ProducerUID       string                           `json:"producerUID"`
	Sequence          uint64                           `json:"sequence"`
	DispatchID        string                           `json:"dispatchID"`
	Intent            fencing.MutationIntent           `json:"intent"`
	Preparation       fencing.MutationPreparation      `json:"preparation"`
	PreparationHash   string                           `json:"preparationHash"`
	BucketEnvironment recovery.FrozenBucketEnvironment `json:"bucketEnvironment"`
	Attempt           uint32                           `json:"attempt"`
	RequestDigest     string                           `json:"requestDigest"`
}

// mutationGenerationZeroBarrier turns the original held generation into a
// durable per-name nonabsence barrier. Owner is deliberately independent: a
// completed mutation may release serialization without retiring this barrier.
type mutationGenerationZeroBarrier struct {
	GuardRootIdentity     string                           `json:"guardRootIdentity"`
	GuardRef              string                           `json:"guardRef"`
	GuardDigest           string                           `json:"guardDigest"`
	Runtime               fencing.RuntimeBinding           `json:"runtime"`
	WALIdentity           string                           `json:"walIdentity"`
	ProducerUID           string                           `json:"producerUID"`
	Sequence              uint64                           `json:"sequence"`
	ScopeHash             string                           `json:"scopeHash"`
	Key                   string                           `json:"key"`
	OriginGeneration      int64                            `json:"originGeneration"`
	OriginMetageneration  int64                            `json:"originMetageneration"`
	OriginOperationID     string                           `json:"originOperationID"`
	OriginPayloadDigest   string                           `json:"originPayloadDigest"`
	OriginPayloadSize     int64                            `json:"originPayloadSize"`
	InitialHoldReleased   bool                             `json:"initialHoldReleased,omitempty"`
	CurrentGeneration     int64                            `json:"currentGeneration"`
	CurrentMetageneration int64                            `json:"currentMetageneration"`
	CurrentOperationID    string                           `json:"currentOperationID"`
	CurrentPayloadDigest  string                           `json:"currentPayloadDigest"`
	CurrentPayloadSize    int64                            `json:"currentPayloadSize"`
	BucketEnvironment     recovery.FrozenBucketEnvironment `json:"bucketEnvironment"`
	LineageRef            string                           `json:"lineageRef,omitempty"`
	LineageDigest         string                           `json:"lineageDigest,omitempty"`
}

// mutationGenerationZeroRetirementProof records only the immutable terminal
// permission set. The canonical journal/archive remains the source for the
// original entry; EntryDigest binds this proof to that exact retained entry.
type mutationGenerationZeroRetirementProof struct {
	Version         int                                 `json:"version"`
	Guard           fencing.MutationGenerationZeroGuard `json:"guard"`
	Runtime         fencing.RuntimeBinding              `json:"runtime"`
	WALIdentity     string                              `json:"walIdentity"`
	ProducerUID     string                              `json:"producerUID"`
	Sequence        uint64                              `json:"sequence"`
	Intent          fencing.MutationIntent              `json:"intent"`
	Preparation     fencing.MutationPreparation         `json:"preparation"`
	EntryDigest     string                              `json:"entryDigest"`
	Completion      fencing.MutationCopyCompletion      `json:"completion"`
	RightsDigest    string                              `json:"rightsDigest"`
	DispatchID      string                              `json:"dispatchID"`
	DispatchReceipt string                              `json:"dispatchReceipt"`
	Digest          string                              `json:"digest"`
}

func (p mutationGenerationZeroRetirementProof) digest() string {
	p.Digest = ""
	return hashJSON(p)
}

func (p mutationGenerationZeroRetirementProof) ref() string {
	return "generation-zero-retirements/" + hashJSON([]any{"g0-retirement-v1", p.Guard.GuardRootIdentity, p.Guard.GuardUID, p.Guard.GuardDigest}) + ".json"
}

type mutationGenerationZeroRetirementReference struct {
	ProofRef     string                        `json:"proofRef"`
	ProofDigest  string                        `json:"proofDigest"`
	ProofVersion objstore.ObjectVersion        `json:"proofVersion"`
	Barrier      mutationGenerationZeroBarrier `json:"barrier"`
}

// mutationGenerationZeroLineageRecord is immutable evidence for one positive
// generation transition. The authority key state points at the current head;
// each record points to its exact predecessor and receipt.
type mutationGenerationZeroLineageRecord struct {
	Version              int                    `json:"version"`
	GuardRef             string                 `json:"guardRef"`
	GuardDigest          string                 `json:"guardDigest"`
	PreviousRef          string                 `json:"previousRef,omitempty"`
	PreviousDigest       string                 `json:"previousDigest"`
	ReceiptRef           string                 `json:"receiptRef"`
	ReceiptDigest        string                 `json:"receiptDigest"`
	Runtime              fencing.RuntimeBinding `json:"runtime"`
	WALIdentity          string                 `json:"walIdentity"`
	ProducerUID          string                 `json:"producerUID"`
	Sequence             uint64                 `json:"sequence"`
	DispatchID           string                 `json:"dispatchID"`
	ParentGeneration     int64                  `json:"parentGeneration"`
	ParentMetageneration int64                  `json:"parentMetageneration"`
	Generation           int64                  `json:"generation"`
	Metageneration       int64                  `json:"metageneration"`
	OperationID          string                 `json:"operationID"`
	PayloadDigest        string                 `json:"payloadDigest"`
	PayloadSize          int64                  `json:"payloadSize"`
	Digest               string                 `json:"digest"`
}

func (r mutationGenerationZeroLineageRecord) digest() string {
	r.Digest = ""
	return hashJSON(r)
}

func (r mutationGenerationZeroLineageRecord) ref() string {
	return "mutation-effect-lineages/" + r.Digest + ".json"
}

// compactSourceMutationJournal keeps the canonical producer below its fixed
// live-entry bound. Archives are committed only for the contiguous terminal
// prefix and commitMutationArchive verifies all prepared-entry receipts.
func (e *FencingExecutor) compactSourceMutationJournal(ctx context.Context, runtime fencing.RuntimeBinding, nextSequence uint64) error {
	return e.compactMutationJournalForAuthority(ctx, runtimeMutationAuthority(runtime), nextSequence)
}

const archivedStageRetirementBatch = 16

// ReconcileArchivedStages resumes stage retirement from the durable producer
// cursor. It verifies each archived entry and exact archive generation before
// retiring its payload; each cursor step is CAS-persisted only after the
// tombstone/quota update succeeds. Work is bounded per call so admission can
// retry maintenance without an unbounded request.
func (e *FencingExecutor) ReconcileArchivedStages(ctx context.Context, runtime fencing.RuntimeBinding) error {
	return e.reconcileArchivedStagesForAuthority(ctx, runtimeMutationAuthority(runtime))
}

func (e *FencingExecutor) archivedMutationAt(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64) (fencing.MutationEntry, fencing.MutationArchiveReference, error) {
	return e.archivedMutationAtForAuthority(ctx, runtimeMutationAuthority(runtime), producerUID, sequence)
}

func (e *FencingExecutor) mutationKeyIdentity(scopeHash, key string) (string, string) {
	identity := hashJSON([]string{"rhiza-mutation-key-v1", scopeHash, key})
	return e.AuthorityState.key("mutation-keys", identity), identity
}

func mutationBarrierFromGuard(guard fencing.MutationGenerationZeroGuard) mutationGenerationZeroBarrier {
	return mutationGenerationZeroBarrier{
		GuardRootIdentity: guard.GuardRootIdentity, GuardRef: guard.GuardRef, GuardDigest: guard.GuardDigest,
		Runtime: guard.Runtime, WALIdentity: guard.WALIdentity, ProducerUID: guard.ProducerUID, Sequence: guard.Sequence,
		ScopeHash: guard.StorageScopeHash, Key: guard.Key, OriginGeneration: guard.ObjectGeneration,
		OriginMetageneration: guard.ObjectMetageneration, OriginOperationID: guard.Intent.OperationID,
		OriginPayloadDigest: guard.Intent.PayloadDigest, OriginPayloadSize: guard.Intent.PayloadSize, CurrentGeneration: guard.ObjectGeneration,
		CurrentMetageneration: guard.ObjectMetageneration, CurrentOperationID: guard.Intent.OperationID,
		CurrentPayloadDigest: guard.Intent.PayloadDigest, CurrentPayloadSize: guard.Intent.PayloadSize,
	}
}

func (r mutationGenerationZeroRight) matchesGuard(guard fencing.MutationGenerationZeroGuard) bool {
	return r.RepairRecordHash == guard.RepairRecordHash && r.RepairBinding == guard.RepairBinding && r.Runtime == guard.Runtime &&
		r.WALIdentity == guard.WALIdentity && r.ProducerUID == guard.ProducerUID && r.Sequence == guard.Sequence &&
		r.DispatchID == guard.DispatchID && r.Intent == guard.Intent && r.Preparation == guard.Preparation &&
		r.PreparationHash == guard.PreparationHash
}

func (r mutationGenerationZeroRight) valid() bool {
	return mutationDigestValidOperator(r.RepairRecordHash) && r.RepairBinding.Validate() == nil &&
		r.Runtime.SourceClusterID != "" && r.Runtime.NodeID != "" && r.Runtime.PodUID != "" && r.Runtime.RunID != "" &&
		r.Runtime.StatefulSetUID != "" && r.WALIdentity != "" && r.ProducerUID != "" && r.Sequence > 0 &&
		r.DispatchID != "" && r.Intent.Kind == "upload" && r.Intent.Key != "" && r.Intent.OperationID != "" &&
		r.Attempt > 0 && mutationDigestValidOperator(r.RequestDigest) &&
		mutationDigestValidOperator(r.Intent.PayloadDigest) && r.Intent.PayloadSize >= 0 &&
		r.Preparation.Profile == fencing.MutationHeldExecutionProfile && r.Preparation.GenerationSet && r.Preparation.Generation == 0 &&
		r.Preparation.StoreScopeHash == r.Intent.StoreScopeHash && mutationDigestValidOperator(r.PreparationHash) &&
		r.BucketEnvironment.Metageneration != "" && mutationDigestValidOperator(r.BucketEnvironment.LifecycleDigest)
}

// recordMutationGenerationZeroRight durably pins the original sealed G0
// dispatch before a repair helper can issue its create-only replay.
func (e *FencingExecutor) recordMutationGenerationZeroRight(ctx context.Context, scopeHash, key, owner string, right mutationGenerationZeroRight) error {
	if !right.valid() || right.Intent.StoreScopeHash != scopeHash || right.Intent.Key != key {
		return fmt.Errorf("fencing: incomplete generation-zero right")
	}
	endpoint, identity := e.mutationKeyIdentity(scopeHash, key)
	for range 8 {
		data, version, err := e.AuthorityState.read(ctx, endpoint, identity)
		if err != nil {
			return err
		}
		var state mutationKeyState
		if err := decodeAuthorityValue(data, &state); err != nil || state.ScopeHash != scopeHash || state.Key != key || state.Owner != owner {
			return fmt.Errorf("fencing: generation-zero right lost its exact key owner")
		}
		if state.Barrier != nil {
			return fmt.Errorf("fencing: generation-zero right cannot replace an existing lineage")
		}
		if state.PendingGenerationZero != nil {
			if *state.PendingGenerationZero == right {
				return nil
			}
			previous := *state.PendingGenerationZero
			previous.Attempt, previous.RequestDigest = 0, ""
			next := right
			next.Attempt, next.RequestDigest = 0, ""
			if previous != next || right.Attempt != state.PendingGenerationZero.Attempt+1 {
				return fmt.Errorf("fencing: another generation-zero right is pending for this key")
			}
			producer, uid, _, readErr := e.AuthorityState.ReadProducer(ctx, right.Runtime)
			entry, exists := producer.Entries[right.Sequence]
			if readErr != nil || uid != right.ProducerUID || !exists || entry.GenerationZeroRights == nil ||
				int(state.PendingGenerationZero.Attempt) > len(entry.GenerationZeroRights.Rights) {
				return errors.Join(readErr, fmt.Errorf("fencing: prior generation-zero helper right is not durably terminal"))
			}
			prior := entry.GenerationZeroRights.Rights[state.PendingGenerationZero.Attempt-1]
			if prior.Attempt != state.PendingGenerationZero.Attempt || prior.RequestDigest != state.PendingGenerationZero.RequestDigest ||
				!mutationDigestValidOperator(prior.TerminalReceiptDigest) {
				return fmt.Errorf("fencing: prior generation-zero helper right is not durably terminal")
			}
		}
		state.PendingGenerationZero = &right
		writeErr := e.AuthorityState.write(ctx, endpoint, identity, state, version)
		if writeErr == nil || !errors.Is(writeErr, errAuthorityStateConflict) {
			stored, _, readErr := e.AuthorityState.read(ctx, endpoint, identity)
			var actual mutationKeyState
			if readErr == nil && decodeAuthorityValue(stored, &actual) == nil && actual.ScopeHash == scopeHash && actual.Key == key &&
				actual.Owner == owner && actual.PendingGenerationZero != nil && *actual.PendingGenerationZero == right {
				return nil
			}
			return errors.Join(writeErr, readErr, fmt.Errorf("fencing: generation-zero right durable readback differs"))
		}
	}
	return ErrFencePending
}

func (b mutationGenerationZeroBarrier) matchesGuard(guard fencing.MutationGenerationZeroGuard) bool {
	origin := mutationBarrierFromGuard(guard)
	return b.GuardRootIdentity == origin.GuardRootIdentity && b.GuardRef == origin.GuardRef && b.GuardDigest == origin.GuardDigest &&
		b.Runtime == origin.Runtime && b.WALIdentity == origin.WALIdentity && b.ProducerUID == origin.ProducerUID && b.Sequence == origin.Sequence &&
		b.ScopeHash == origin.ScopeHash && b.Key == origin.Key && b.OriginGeneration == origin.OriginGeneration &&
		b.OriginMetageneration == origin.OriginMetageneration && b.OriginOperationID == origin.OriginOperationID &&
		b.OriginPayloadDigest == origin.OriginPayloadDigest && b.OriginPayloadSize == origin.OriginPayloadSize && b.CurrentGeneration > 0 && b.CurrentMetageneration > 0 &&
		b.CurrentOperationID != "" && mutationDigestValidOperator(b.CurrentPayloadDigest) && b.CurrentPayloadSize >= 0 &&
		b.BucketEnvironment.Metageneration != "" && mutationDigestValidOperator(b.BucketEnvironment.LifecycleDigest)
}

func (b mutationGenerationZeroBarrier) valid() bool {
	return mutationDigestValidOperator(b.GuardRootIdentity) && b.GuardRef != "" && mutationDigestValidOperator(b.GuardDigest) &&
		b.Runtime.SourceClusterID != "" && b.Runtime.NodeID != "" && b.Runtime.PodUID != "" && b.Runtime.RunID != "" && b.Runtime.StatefulSetUID != "" &&
		b.WALIdentity != "" && b.ProducerUID != "" && b.Sequence > 0 && b.ScopeHash != "" && b.Key != "" &&
		b.OriginGeneration > 0 && b.OriginMetageneration > 0 && b.OriginOperationID != "" && mutationDigestValidOperator(b.OriginPayloadDigest) && b.OriginPayloadSize >= 0 &&
		b.CurrentGeneration > 0 && b.CurrentMetageneration > 0 && b.CurrentOperationID != "" && mutationDigestValidOperator(b.CurrentPayloadDigest) && b.CurrentPayloadSize >= 0 &&
		b.BucketEnvironment.Metageneration != "" && mutationDigestValidOperator(b.BucketEnvironment.LifecycleDigest)
}

func (e *FencingExecutor) readMutationKeyState(ctx context.Context, scopeHash, key string) (mutationKeyState, error) {
	if e == nil || e.AuthorityState == nil || scopeHash == "" || key == "" {
		return mutationKeyState{}, fmt.Errorf("fencing: canonical mutation key lock is not configured")
	}
	endpoint, identity := e.mutationKeyIdentity(scopeHash, key)
	data, _, err := e.AuthorityState.read(ctx, endpoint, identity)
	if err != nil {
		return mutationKeyState{}, err
	}
	var state mutationKeyState
	if err := decodeAuthorityValue(data, &state); err != nil || state.ScopeHash != scopeHash || state.Key != key {
		return mutationKeyState{}, fmt.Errorf("fencing: mutation key lock identity differs")
	}
	return state, nil
}

func generationZeroRetirementProofFor(runtime fencing.RuntimeBinding, walIdentity, producerUID string, sequence uint64, entry fencing.MutationEntry) (mutationGenerationZeroRetirementProof, error) {
	var zero mutationGenerationZeroRetirementProof
	guard := entry.GenerationZeroGuard
	rights := entry.GenerationZeroRights
	if guard == nil || rights == nil || entry.Preparation == nil || entry.Resolution == nil || entry.CopyCompletion == nil ||
		entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.AllCopiesTerminal || !entry.DispatchClosed ||
		len(entry.Dispatches) != 1 || !rights.Sealed || !rights.HelperRightsRetired || !generationZeroOperatorRightsTerminal(rights) ||
		entry.CopyCompletion.GenerationZeroRightsDigest != rights.Digest() || !mutationDigestValidOperator(entry.CopyCompletion.ReceiptDigest) ||
		guard.Runtime != runtime || guard.WALIdentity != walIdentity || guard.ProducerUID != producerUID || guard.Sequence != sequence ||
		guard.Intent != entry.Intent || guard.Preparation != *entry.Preparation || guard.GuardDigest != guard.Digest() {
		return zero, ErrFencePending
	}
	if entry.CopyCompletion.PreparationHash != guard.PreparationHash {
		return zero, fmt.Errorf("fencing: generation-zero completion differs from its original preparation")
	}
	dispatch, ok := entry.Dispatches[guard.DispatchID]
	if !ok || !mutationDigestValidOperator(dispatch.ReceiptDigest) || dispatch.ReceiptDigest != entry.CopyCompletion.ReceiptDigest {
		return zero, ErrFencePending
	}
	return mutationGenerationZeroRetirementProof{Version: 1, Guard: *guard, Runtime: runtime, WALIdentity: walIdentity,
		ProducerUID: producerUID, Sequence: sequence, Intent: entry.Intent, Preparation: *entry.Preparation,
		EntryDigest: hashJSON(entry), Completion: *entry.CopyCompletion, RightsDigest: rights.Digest(), DispatchID: guard.DispatchID,
		DispatchReceipt: dispatch.ReceiptDigest}, nil
}

func (p mutationGenerationZeroRetirementProof) validate(guard fencing.MutationGenerationZeroGuard, entry fencing.MutationEntry) error {
	want, err := generationZeroRetirementProofFor(p.Runtime, p.WALIdentity, p.ProducerUID, p.Sequence, entry)
	if err != nil || p.Version != 1 || p.Guard != guard || p.Guard.GuardRootIdentity != guard.GuardRootIdentity ||
		p.Runtime != guard.Runtime || p.WALIdentity != guard.WALIdentity || p.ProducerUID != guard.ProducerUID || p.Sequence != guard.Sequence ||
		p.Intent != guard.Intent || p.Preparation != guard.Preparation || p.EntryDigest != hashJSON(entry) ||
		p.Digest == "" || p.Digest != p.digest() ||
		p.Completion != *entry.CopyCompletion || p.RightsDigest != entry.GenerationZeroRights.Digest() ||
		p.DispatchID != guard.DispatchID || p.DispatchReceipt != entry.Dispatches[guard.DispatchID].ReceiptDigest ||
		p.EntryDigest != want.EntryDigest || p.Completion != want.Completion || p.RightsDigest != want.RightsDigest ||
		p.DispatchReceipt != want.DispatchReceipt {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero retirement proof differs from canonical terminal entry"))
	}
	return nil
}

func (e *FencingExecutor) readGenerationZeroRetirementProof(ctx context.Context, guard fencing.MutationGenerationZeroGuard, entry fencing.MutationEntry) (mutationGenerationZeroRetirementReference, error) {
	var zero mutationGenerationZeroRetirementReference
	evidence, err := e.authorityEvidenceFor(guard.Runtime)
	if err != nil || evidence.Identity() != guard.GuardRootIdentity {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero retirement proof root differs"))
	}
	name := (mutationGenerationZeroRetirementProof{Guard: guard}).ref()
	reader, version, err := evidence.Read(ctx, name)
	if err != nil {
		return zero, err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 1<<20+1))
	closeErr := reader.Close()
	var proof mutationGenerationZeroRetirementProof
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&proof)
	if decodeErr == nil {
		decodeErr = decoder.Decode(new(any))
		if decodeErr == io.EOF {
			decodeErr = nil
		}
	}
	canonical, marshalErr := json.Marshal(proof)
	if err := errors.Join(readErr, closeErr, decodeErr, marshalErr); err != nil || len(data) > 1<<20 || version == nil ||
		version.Type != objstore.Generation || version.Value == "" || !bytes.Equal(canonical, data) || proof.ref() != name {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero retirement proof is unreadable"))
	}
	if err := proof.validate(guard, entry); err != nil {
		return zero, err
	}
	return mutationGenerationZeroRetirementReference{ProofRef: name, ProofDigest: proof.Digest, ProofVersion: *version}, nil
}

func (e *FencingExecutor) persistGenerationZeroRetirementProof(ctx context.Context, proof mutationGenerationZeroRetirementProof) (mutationGenerationZeroRetirementReference, error) {
	var zero mutationGenerationZeroRetirementReference
	if proof.Version != 1 || proof.Guard.GuardDigest != proof.Guard.Digest() || proof.Digest != "" {
		return zero, fmt.Errorf("fencing: incomplete generation-zero retirement proof")
	}
	proof.Digest = proof.digest()
	body, err := json.Marshal(proof)
	if err != nil {
		return zero, err
	}
	evidence, err := e.authorityEvidenceFor(proof.Runtime)
	if err != nil || evidence.Identity() != proof.Guard.GuardRootIdentity {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero retirement proof root differs"))
	}
	version, err := evidence.CreateImmutable(ctx, proof.ref(), body)
	if err != nil {
		// Resolve a lost immutable-create response only by exact readback at
		// the content-addressed name. Never replace or adopt different bytes.
		reader, readVersion, readErr := evidence.Read(ctx, proof.ref())
		if readErr != nil {
			return zero, errors.Join(err, readErr)
		}
		readback, bodyErr := io.ReadAll(io.LimitReader(reader, int64(len(body))+1))
		closeErr := reader.Close()
		if bodyErr != nil || closeErr != nil || readVersion == nil || readVersion.Type != objstore.Generation ||
			readVersion.Value == "" || !bytes.Equal(readback, body) {
			return zero, errors.Join(err, bodyErr, closeErr, fmt.Errorf("fencing: generation-zero retirement proof create response is unresolved"))
		}
		version = readVersion
	}
	reader, readVersion, err := evidence.Read(ctx, proof.ref())
	if err != nil {
		return zero, err
	}
	readback, readErr := io.ReadAll(io.LimitReader(reader, int64(len(body))+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || version == nil || readVersion == nil || version.Type != objstore.Generation ||
		version.Value == "" || *version != *readVersion || !bytes.Equal(readback, body) {
		return zero, errors.Join(readErr, closeErr, fmt.Errorf("fencing: generation-zero retirement proof readback differs"))
	}
	return mutationGenerationZeroRetirementReference{ProofRef: proof.ref(), ProofDigest: proof.Digest, ProofVersion: *version}, nil
}

// retireGenerationZeroBarrier adopts exact terminal journal evidence without
// treating helper-right retirement or future-effect exclusion as original
// dispatch terminality. Conflicts leave the active barrier in place.
func (e *FencingExecutor) retireGenerationZeroBarrier(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, expected fencing.MutationEntry) (retired bool, err error) {
	if expected.CopyCompletion == nil || expected.GenerationZeroGuard == nil {
		return false, nil
	}
	guard := *expected.GenerationZeroGuard
	if guard.Runtime != runtime || guard.ProducerUID != producerUID || guard.Sequence != sequence {
		return false, fmt.Errorf("fencing: generation-zero retirement tuple differs")
	}
	producer, uid, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || uid != producerUID || producer.Runtime != runtime || producer.WALIdentity != guard.WALIdentity || producer.Validate() != nil {
		return false, errors.Join(err, fmt.Errorf("fencing: generation-zero retirement producer differs"))
	}
	entry := expected
	if sequence <= producer.CompactedThrough {
		// The caller supplied this entry through lookupMutation's verified
		// archive path. Re-reading it here would recursively validate the same
		// archive when its receipt validation reaches this G0 guard.
	} else if actual, ok := producer.Entries[sequence]; ok {
		if hashJSON(actual) != hashJSON(expected) {
			return false, fmt.Errorf("fencing: generation-zero retirement live journal tuple changed")
		}
		entry = actual
	} else {
		return false, ErrFencePending
	}
	if entry.GenerationZeroGuard == nil || *entry.GenerationZeroGuard != guard {
		return false, fmt.Errorf("fencing: generation-zero retirement journal guard changed")
	}
	if _, err := generationZeroRetirementProofFor(runtime, producer.WALIdentity, producerUID, sequence, entry); err != nil {
		return false, err
	}
	if err := e.VerifySourceMutationEvidence(ctx, runtime, sequence, entry); err != nil {
		return false, err
	}
	proof, err := generationZeroRetirementProofFor(runtime, producer.WALIdentity, producerUID, sequence, entry)
	if err != nil {
		return false, err
	}
	reference, err := e.persistGenerationZeroRetirementProof(ctx, proof)
	if err != nil {
		return false, err
	}
	// Ensure the immutable proof is readable against the canonical entry before
	// taking any step that can expose the name to later ordinary mutations.
	if _, err := e.readGenerationZeroRetirementProof(ctx, guard, entry); err != nil {
		return false, err
	}

	owner := hashJSON([]any{"generation-zero-retirement-owner-v1", guard.GuardRootIdentity, guard.GuardUID, guard.GuardDigest})
	state, err := e.readMutationKeyState(ctx, guard.StorageScopeHash, guard.Key)
	if err != nil {
		return false, err
	}
	if state.Barrier == nil {
		if state.Retirement != nil && state.Retirement.ProofRef == reference.ProofRef && state.Retirement.ProofDigest == reference.ProofDigest {
			return true, nil
		}
		return false, nil
	}
	if !state.Barrier.matchesGuard(guard) || state.Owner != "" || state.PendingGenerationZero != nil {
		return false, nil
	}
	if err := e.acquireMutationKey(ctx, guard.StorageScopeHash, guard.Key, owner, false); err != nil {
		if errors.Is(err, ErrFencePending) {
			return false, nil
		}
		return false, err
	}
	locked := true
	defer func() {
		if locked {
			err = errors.Join(err, e.releaseMutationKey(ctx, guard.StorageScopeHash, guard.Key, owner))
		}
	}()
	state, err = e.readMutationKeyState(ctx, guard.StorageScopeHash, guard.Key)
	if err != nil || state.Owner != owner || state.PendingGenerationZero != nil || state.Barrier == nil || !state.Barrier.matchesGuard(guard) {
		return false, errors.Join(err, fmt.Errorf("fencing: generation-zero retirement lost its exact key owner"))
	}
	barrier := *state.Barrier
	if err := e.verifyMutationGenerationZeroBarrier(ctx, barrier); err != nil {
		return false, err
	}
	scope, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil || hashJSON(scope) != guard.StorageScopeHash {
		return false, errors.Join(err, fmt.Errorf("fencing: generation-zero retirement storage scope differs"))
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		return false, err
	}
	if !barrier.InitialHoldReleased {
		barrier, err = e.releaseGenerationZeroOriginHold(ctx, provider, guard.StorageScopeHash, guard.Key, owner, barrier)
		if err != nil {
			return false, err
		}
	}
	if err := e.releaseMutationKey(ctx, guard.StorageScopeHash, guard.Key, owner); err != nil {
		return false, err
	}
	locked = false

	endpoint, identity := e.mutationKeyIdentity(guard.StorageScopeHash, guard.Key)
	for range 8 {
		data, version, readErr := e.AuthorityState.read(ctx, endpoint, identity)
		if readErr != nil {
			return false, readErr
		}
		var current mutationKeyState
		if err := decodeAuthorityValue(data, &current); err != nil || current.ScopeHash != guard.StorageScopeHash || current.Key != guard.Key {
			return false, fmt.Errorf("fencing: generation-zero retirement key identity differs")
		}
		if current.Barrier == nil {
			if current.Retirement != nil && current.Retirement.ProofRef == reference.ProofRef && current.Retirement.ProofDigest == reference.ProofDigest {
				return true, nil
			}
			return false, nil
		}
		if current.Owner != "" || current.PendingGenerationZero != nil || !current.Barrier.matchesGuard(guard) {
			return false, nil
		}
		if err := e.verifyMutationGenerationZeroBarrier(ctx, *current.Barrier); err != nil {
			return false, err
		}
		adopted := reference
		adopted.Barrier = *current.Barrier // preserve the latest lineage observed by this CAS.
		current.Barrier = nil
		current.Retirement = &adopted
		writeErr := e.AuthorityState.write(ctx, endpoint, identity, current, version)
		if writeErr == nil || !errors.Is(writeErr, errAuthorityStateConflict) {
			stored, _, confirmErr := e.AuthorityState.read(ctx, endpoint, identity)
			var actual mutationKeyState
			if confirmErr == nil && decodeAuthorityValue(stored, &actual) == nil && actual.ScopeHash == guard.StorageScopeHash && actual.Key == guard.Key &&
				actual.Owner == "" && actual.PendingGenerationZero == nil && actual.Barrier == nil && actual.Retirement != nil && *actual.Retirement == adopted {
				return true, nil
			}
			return false, errors.Join(writeErr, confirmErr, fmt.Errorf("fencing: generation-zero retirement CAS readback differs"))
		}
	}
	return false, ErrFencePending
}

// installMutationGenerationZeroBarrier records the durable name-presence
// invariant before releasing the settlement owner. It never replaces an
// existing, differently bound barrier.
func (e *FencingExecutor) installMutationGenerationZeroBarrier(ctx context.Context, scopeHash, key, owner string, guard fencing.MutationGenerationZeroGuard) error {
	if !mutationDigestValidOperator(guard.GuardDigest) || guard.StorageScopeHash != scopeHash || guard.Key != key || guard.ObjectGeneration <= 0 || guard.ObjectMetageneration <= 0 {
		return fmt.Errorf("fencing: incomplete generation-zero barrier source")
	}
	want := mutationBarrierFromGuard(guard)
	endpoint, identity := e.mutationKeyIdentity(scopeHash, key)
	for range 8 {
		data, version, err := e.AuthorityState.read(ctx, endpoint, identity)
		if err != nil {
			return err
		}
		var state mutationKeyState
		if err := decodeAuthorityValue(data, &state); err != nil || state.ScopeHash != scopeHash || state.Key != key || state.Owner != owner {
			return fmt.Errorf("fencing: generation-zero barrier lost its exact key owner")
		}
		if state.Barrier != nil {
			if !state.Barrier.matchesGuard(guard) {
				return fmt.Errorf("fencing: generation-zero barrier is bound to another origin")
			}
			return nil
		}
		if state.PendingGenerationZero != nil {
			if !state.PendingGenerationZero.matchesGuard(guard) {
				return fmt.Errorf("fencing: generation-zero barrier differs from its pre-dispatch right")
			}
			want.BucketEnvironment = state.PendingGenerationZero.BucketEnvironment
			state.PendingGenerationZero = nil
		}
		state.Barrier = &want
		writeErr := e.AuthorityState.write(ctx, endpoint, identity, state, version)
		if writeErr == nil || !errors.Is(writeErr, errAuthorityStateConflict) {
			stored, _, readErr := e.AuthorityState.read(ctx, endpoint, identity)
			var actual mutationKeyState
			if readErr == nil && decodeAuthorityValue(stored, &actual) == nil && actual.ScopeHash == scopeHash && actual.Key == key &&
				actual.Owner == owner && actual.PendingGenerationZero == nil && actual.Barrier != nil && *actual.Barrier == want {
				return nil
			}
			return errors.Join(writeErr, readErr, fmt.Errorf("fencing: generation-zero barrier durable readback differs"))
		}
	}
	return ErrFencePending
}

func (e *FencingExecutor) updateMutationGenerationZeroBarrier(ctx context.Context, scopeHash, key, owner string, expected mutationGenerationZeroBarrier, update func(*mutationGenerationZeroBarrier) error) (mutationGenerationZeroBarrier, error) {
	var zero mutationGenerationZeroBarrier
	endpoint, identity := e.mutationKeyIdentity(scopeHash, key)
	for range 8 {
		data, version, err := e.AuthorityState.read(ctx, endpoint, identity)
		if err != nil {
			return zero, err
		}
		var state mutationKeyState
		if err := decodeAuthorityValue(data, &state); err != nil || state.ScopeHash != scopeHash || state.Key != key || state.Owner != owner || state.Barrier == nil || *state.Barrier != expected {
			return zero, fmt.Errorf("fencing: generation-zero barrier lineage or owner changed")
		}
		next := *state.Barrier
		if err := update(&next); err != nil {
			return zero, err
		}
		state.Barrier = &next
		writeErr := e.AuthorityState.write(ctx, endpoint, identity, state, version)
		if writeErr == nil || !errors.Is(writeErr, errAuthorityStateConflict) {
			stored, _, readErr := e.AuthorityState.read(ctx, endpoint, identity)
			var actual mutationKeyState
			if readErr == nil && decodeAuthorityValue(stored, &actual) == nil && actual.ScopeHash == scopeHash && actual.Key == key &&
				actual.Owner == owner && actual.Barrier != nil && *actual.Barrier == next {
				return next, nil
			}
			return zero, errors.Join(writeErr, readErr, fmt.Errorf("fencing: generation-zero barrier update readback differs"))
		}
	}
	return zero, ErrFencePending
}

func (e *FencingExecutor) verifyMutationGenerationZeroBarrier(ctx context.Context, barrier mutationGenerationZeroBarrier) error {
	if e == nil || e.SourceMutationStorage == nil {
		return fmt.Errorf("fencing: generation-zero storage verifier is not configured")
	}
	if err := e.verifyMutationGenerationZeroBarrierAuthority(ctx, barrier); err != nil {
		return err
	}
	scope, err := e.sourceMutationStorageScope(ctx, barrier.Runtime)
	if err != nil || hashJSON(scope) != barrier.ScopeHash {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero barrier storage scope differs"))
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		return err
	}
	if err := provider.verifyMutationGenerationZeroBarrier(ctx, barrier); err != nil {
		return err
	}
	if !barrier.InitialHoldReleased && barrier.CurrentGeneration == barrier.OriginGeneration {
		return provider.verifyGenerationZeroOrigin(ctx, barrier)
	}
	return nil
}

// verifyMutationGenerationZeroBarrierAuthority validates the durable barrier,
// frozen bucket contract, and immutable lineage without asserting that GCS is
// still at the barrier head. It is used only to resume a journaled terminal
// successor whose storage write committed before the barrier CAS.
func (e *FencingExecutor) verifyMutationGenerationZeroBarrierAuthority(ctx context.Context, barrier mutationGenerationZeroBarrier) error {
	if e == nil || e.AuthorityState == nil || e.SourceMutationStorage == nil {
		return fmt.Errorf("fencing: generation-zero authority verifier is not configured")
	}
	if !barrier.valid() || barrier.Runtime.Validate(fencing.Identity{SourceClusterID: barrier.Runtime.SourceClusterID, NodeID: barrier.Runtime.NodeID,
		PodName: barrier.Runtime.PodName, PodUID: barrier.Runtime.PodUID, RunID: barrier.Runtime.RunID,
		AssignmentID: barrier.Runtime.AssignmentID}) != nil {
		return fmt.Errorf("fencing: generation-zero barrier identity is incomplete")
	}
	state, err := e.readMutationKeyState(ctx, barrier.ScopeHash, barrier.Key)
	if err != nil || state.Barrier == nil || *state.Barrier != barrier {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero barrier is not the canonical key state"))
	}
	scope, err := e.sourceMutationStorageScope(ctx, barrier.Runtime)
	if err != nil || hashJSON(scope) != barrier.ScopeHash {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero barrier storage scope differs"))
	}
	attrs, err := e.SourceMutationStorage.Bucket(scope.Bucket).Attrs(ctx)
	if err != nil {
		return err
	}
	environment, err := frozenBucketEnvironmentFromAttrs(attrs, scope.Bucket, scope.Prefix)
	if err != nil || environment != barrier.BucketEnvironment {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero bucket environment differs from its sealed barrier"))
	}
	if err := e.verifyMutationGenerationZeroLineage(ctx, barrier); err != nil {
		return err
	}
	return nil
}

func (e *FencingExecutor) verifyMutationGenerationZeroLineage(ctx context.Context, barrier mutationGenerationZeroBarrier) error {
	if barrier.LineageRef == "" {
		if barrier.LineageDigest != "" || barrier.CurrentGeneration != barrier.OriginGeneration ||
			(!barrier.InitialHoldReleased && barrier.CurrentMetageneration != barrier.OriginMetageneration) ||
			(barrier.InitialHoldReleased && barrier.CurrentMetageneration != barrier.OriginMetageneration+1) {
			return fmt.Errorf("fencing: generation-zero origin lineage is incomplete")
		}
		return nil
	}
	if !mutationDigestValidOperator(barrier.LineageDigest) || barrier.Runtime.SourceClusterID == "" {
		return fmt.Errorf("fencing: generation-zero lineage head is incomplete")
	}
	evidence, err := e.authorityEvidenceFor(barrier.Runtime)
	if err != nil {
		return err
	}
	receipts, err := e.receiptsForRuntime(barrier.Runtime)
	if err != nil {
		return err
	}
	wantRef, wantDigest := barrier.LineageRef, barrier.LineageDigest
	wantGeneration, wantMetageneration := barrier.CurrentGeneration, barrier.CurrentMetageneration
	for depth := 0; depth < 8192; depth++ {
		if !strings.HasPrefix(wantRef, "mutation-effect-lineages/") || len(wantRef) > 512 || path.Clean(wantRef) != wantRef ||
			!mutationDigestValidOperator(wantDigest) || path.Base(wantRef) != wantDigest+".json" {
			return fmt.Errorf("fencing: generation-zero lineage reference is invalid")
		}
		reader, _, err := evidence.Read(ctx, wantRef)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
		closeErr := reader.Close()
		var record mutationGenerationZeroLineageRecord
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&record)
		if decodeErr == nil {
			decodeErr = decoder.Decode(new(any))
			if decodeErr == io.EOF {
				decodeErr = nil
			}
		}
		if err := errors.Join(readErr, closeErr, decodeErr); err != nil || len(data) > 64<<10 {
			return errors.Join(err, fmt.Errorf("fencing: generation-zero lineage record is unreadable"))
		}
		if record.Version != 1 || record.Digest != wantDigest || record.digest() != wantDigest || record.ref() != wantRef ||
			record.GuardRef != barrier.GuardRef || record.GuardDigest != barrier.GuardDigest || record.Generation != wantGeneration ||
			record.Metageneration != wantMetageneration || record.ParentGeneration <= 0 || record.ParentMetageneration <= 0 ||
			!mutationDigestValidOperator(record.ReceiptDigest) || record.ReceiptRef != "receipt-"+hashJSON([]any{record.Runtime, record.WALIdentity, record.ProducerUID, record.Sequence, record.OperationID}) {
			return fmt.Errorf("fencing: generation-zero lineage record binding differs")
		}
		receipt, err := receipts.get(record.ReceiptRef)
		if err != nil {
			return err
		}
		if receipt.ReceiptDigest != record.ReceiptDigest || receipt.digest() != record.ReceiptDigest ||
			receipt.WALIdentity != record.WALIdentity || receipt.ProducerUID != record.ProducerUID || receipt.Sequence != record.Sequence ||
			receipt.DispatchID != record.DispatchID || receipt.Intent.Key != barrier.Key || receipt.Intent.OperationID != record.OperationID ||
			receipt.Intent.StoreScopeHash != barrier.ScopeHash || receipt.Intent.Kind != "upload" || receipt.Intent.Condition.Mode != fencing.MutationMatchGeneration ||
			receipt.Intent.PayloadDigest != record.PayloadDigest || receipt.Intent.PayloadSize != record.PayloadSize ||
			receipt.Preparation.Profile != fencing.MutationHeldExecutionProfile ||
			receipt.Preparation.Generation != record.ParentGeneration || receipt.Outcome != fencing.MutationCommitted || !receipt.AllCopiesTerminal ||
			receipt.Generation != record.Generation || receipt.Metageneration != record.Metageneration || receipt.PayloadDigest != record.PayloadDigest {
			return fmt.Errorf("fencing: generation-zero lineage receipt differs")
		}
		if err := e.verifyGenerationZeroLineageReceipt(ctx, barrier, record, receipt); err != nil {
			return err
		}
		if record.PreviousRef == "" {
			wantParentMeta := barrier.OriginMetageneration
			if barrier.InitialHoldReleased {
				wantParentMeta++
			}
			if record.PreviousDigest != barrier.GuardDigest || record.ParentGeneration != barrier.OriginGeneration || record.ParentMetageneration != wantParentMeta {
				return fmt.Errorf("fencing: generation-zero lineage does not terminate at its guarded origin")
			}
			return nil
		}
		wantRef, wantDigest = record.PreviousRef, record.PreviousDigest
		wantGeneration, wantMetageneration = record.ParentGeneration, record.ParentMetageneration
	}
	return fmt.Errorf("fencing: generation-zero lineage exceeds verification bound")
}

func (e *FencingExecutor) releaseGenerationZeroOriginHold(ctx context.Context, provider *gcsMutationProvider, scopeHash, key, owner string, barrier mutationGenerationZeroBarrier) (mutationGenerationZeroBarrier, error) {
	if barrier.InitialHoldReleased {
		if err := provider.verifyMutationGenerationZeroBarrier(ctx, barrier); err != nil {
			return mutationGenerationZeroBarrier{}, err
		}
		return barrier, nil
	}
	if barrier.CurrentGeneration != barrier.OriginGeneration || barrier.Key != key || barrier.ScopeHash != scopeHash {
		return mutationGenerationZeroBarrier{}, ErrFencePending
	}
	metageneration, err := provider.releaseGenerationZeroOriginHold(ctx, barrier)
	if err != nil {
		return mutationGenerationZeroBarrier{}, err
	}
	updated, err := e.updateMutationGenerationZeroBarrier(ctx, scopeHash, key, owner, barrier, func(next *mutationGenerationZeroBarrier) error {
		next.InitialHoldReleased = true
		next.CurrentMetageneration = metageneration
		return nil
	})
	if err != nil {
		return mutationGenerationZeroBarrier{}, err
	}
	if err := provider.verifyMutationGenerationZeroBarrier(ctx, updated); err != nil {
		return mutationGenerationZeroBarrier{}, err
	}
	return updated, nil
}

func (e *FencingExecutor) advanceMutationGenerationZeroBarrier(ctx context.Context, scopeHash, key, owner string, barrier mutationGenerationZeroBarrier, receipt mutationExecutionReceipt) error {
	if e.SourceMutationStorage == nil {
		return ErrFencePending
	}
	scope, err := e.sourceMutationStorageScope(ctx, barrier.Runtime)
	if err != nil || hashJSON(scope) != scopeHash {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero successor scope differs"))
	}
	gcsProvider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, fencing.MutationHeldExecutionProfile)
	if err != nil {
		return err
	}
	result := mutationProviderResult{Outcome: receipt.Outcome, Generation: receipt.Generation, Metageneration: receipt.Metageneration, PayloadDigest: receipt.PayloadDigest}
	if err := gcsProvider.verifyRepairCommit(ctx, receipt.Intent.Key, receipt.Intent, receipt.Preparation, result); err != nil {
		return err
	}
	current, err := e.readMutationKeyState(ctx, scopeHash, key)
	if err != nil || current.Barrier == nil {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero successor lost its durable barrier"))
	}
	if current.Barrier.CurrentGeneration == receipt.Generation && current.Barrier.CurrentMetageneration == receipt.Metageneration &&
		current.Barrier.CurrentOperationID == receipt.Intent.OperationID && current.Barrier.CurrentPayloadDigest == receipt.Intent.PayloadDigest &&
		current.Barrier.CurrentPayloadSize == receipt.Intent.PayloadSize {
		return e.verifyMutationGenerationZeroLineage(ctx, *current.Barrier)
	}
	if *current.Barrier != barrier || receipt.Preparation.Generation != barrier.CurrentGeneration || !barrier.InitialHoldReleased {
		return fmt.Errorf("fencing: generation-zero successor does not extend the exact live lineage")
	}
	receipts, err := e.receiptsForRuntime(barrier.Runtime)
	if err != nil {
		return err
	}
	receiptRef := mutationExecutionReceiptName(receipt)
	storedReceipt, err := receipts.get(receiptRef)
	if err != nil || storedReceipt != receipt {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero successor receipt readback differs"))
	}
	lineage := mutationGenerationZeroLineageRecord{Version: 1, GuardRef: barrier.GuardRef, GuardDigest: barrier.GuardDigest,
		PreviousRef: barrier.LineageRef, PreviousDigest: barrier.GuardDigest, ReceiptRef: receiptRef, ReceiptDigest: receipt.ReceiptDigest,
		Runtime: receipt.Runtime, WALIdentity: receipt.WALIdentity, ProducerUID: receipt.ProducerUID, Sequence: receipt.Sequence, DispatchID: receipt.DispatchID,
		ParentGeneration: barrier.CurrentGeneration, ParentMetageneration: barrier.CurrentMetageneration,
		Generation: receipt.Generation, Metageneration: receipt.Metageneration, OperationID: receipt.Intent.OperationID,
		PayloadDigest: receipt.PayloadDigest, PayloadSize: receipt.Intent.PayloadSize}
	if barrier.LineageRef != "" {
		lineage.PreviousDigest = barrier.LineageDigest
	}
	lineage.Digest = lineage.digest()
	lineageBytes, err := json.Marshal(lineage)
	if err != nil {
		return err
	}
	evidence, err := e.authorityEvidenceFor(barrier.Runtime)
	if err != nil {
		return err
	}
	if _, err := evidence.CreateImmutable(ctx, lineage.ref(), lineageBytes); err != nil {
		// The immutable write may have committed while its response was lost.
		// Accept only a byte-identical record at the content-addressed name.
		readback, _, readErr := evidence.Read(ctx, lineage.ref())
		if readErr != nil {
			return errors.Join(err, readErr)
		}
		readBytes, readErr := io.ReadAll(io.LimitReader(readback, int64(len(lineageBytes))+1))
		closeErr := readback.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(readBytes, lineageBytes) {
			return errors.Join(err, readErr, closeErr, fmt.Errorf("fencing: immutable generation-zero lineage response is unresolved"))
		}
	}
	readback, _, err := evidence.Read(ctx, lineage.ref())
	if err != nil {
		return err
	}
	readBytes, readErr := io.ReadAll(io.LimitReader(readback, int64(len(lineageBytes))+1))
	closeErr := readback.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(readBytes, lineageBytes) {
		return errors.Join(readErr, closeErr, fmt.Errorf("fencing: generation-zero lineage readback differs"))
	}
	_, err = e.updateMutationGenerationZeroBarrier(ctx, scopeHash, key, owner, barrier, func(next *mutationGenerationZeroBarrier) error {
		next.CurrentGeneration = receipt.Generation
		next.CurrentMetageneration = receipt.Metageneration
		next.CurrentOperationID = receipt.Intent.OperationID
		next.CurrentPayloadDigest = receipt.Intent.PayloadDigest
		next.CurrentPayloadSize = receipt.Intent.PayloadSize
		next.InitialHoldReleased = true
		next.LineageRef = lineage.ref()
		next.LineageDigest = lineage.Digest
		return nil
	})
	return err
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
		state := mutationKeyState{ScopeHash: scopeHash, Key: key}
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
		}
		state.Owner = owner
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
		if state.PendingGenerationZero != nil {
			return ErrFencePending
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

// releaseMutationKeyAfterVerifiedTerminal is idempotent after an exact
// terminal journal/evidence readback. A missing lock or successor owner means
// this tuple no longer owns the key; neither may be overwritten by a retry.
func (e *FencingExecutor) releaseMutationKeyAfterVerifiedTerminal(ctx context.Context, scopeHash, key, owner string) error {
	if e == nil || e.AuthorityState == nil || scopeHash == "" || key == "" || owner == "" {
		return fmt.Errorf("fencing: canonical mutation key lock is not configured")
	}
	if err := e.releaseMutationKey(ctx, scopeHash, key, owner); err == nil {
		return nil
	} else {
		endpoint, identity := e.mutationKeyIdentity(scopeHash, key)
		data, _, readErr := e.AuthorityState.read(ctx, endpoint, identity)
		if errors.Is(readErr, errAuthorityStateNotFound) {
			return nil
		}
		var current mutationKeyState
		if readErr == nil && decodeAuthorityValue(data, &current) == nil && current.ScopeHash == scopeHash && current.Key == key && current.Owner != owner {
			return nil
		}
		return errors.Join(err, readErr)
	}
}

// ExecuteSourceMutation consumes the exact journaled payload and sends at most
// one provider request under a durable cross-producer key lock. Only a fsynced
// provider receipt followed by a committed journal CAS can return success.
func (e *FencingExecutor) ExecuteSourceMutation(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent, scope AttestationStoreScope, provider mutationExecutionProvider, receipts *MutationExecutionReceiptStore) error {
	return e.executeSourceMutation(ctx, mutationExecutionAuthority{statefulSetUID: runtime.StatefulSetUID,
		sourceClusterID: runtime.SourceClusterID, runID: runtime.RunID, runtime: &runtime},
		producerUID, sequence, intent, scope, provider, receipts)
}

func (e *FencingExecutor) ExecuteLogicalSourceMutation(ctx context.Context, statefulSetUID string, binding fencing.LogicalWriterBindingV1, producerUID string, sequence uint64, intent fencing.MutationIntent, scope AttestationStoreScope, provider mutationExecutionProvider, receipts *MutationExecutionReceiptStore) error {
	return e.executeSourceMutation(ctx, mutationExecutionAuthority{statefulSetUID: statefulSetUID, sourceClusterID: binding.SourceClusterID, runID: binding.RunID, logicalBinding: &binding}, producerUID, sequence, intent, scope, provider, receipts)
}

type mutationExecutionAuthority struct {
	statefulSetUID  string
	sourceClusterID string
	runID           string
	runtime         *fencing.RuntimeBinding
	logicalBinding  *fencing.LogicalWriterBindingV1
}

func (a mutationExecutionAuthority) valid() bool {
	return a.statefulSetUID != "" && a.sourceClusterID != "" && a.runID != "" &&
		((a.runtime != nil && a.logicalBinding == nil && a.runtime.StatefulSetUID == a.statefulSetUID && a.runtime.SourceClusterID == a.sourceClusterID && a.runtime.RunID == a.runID) ||
			(a.logicalBinding != nil && a.runtime == nil && a.logicalBinding.ValidateBound() == nil && a.logicalBinding.Role == fencing.StartupRoleVoterActive && a.logicalBinding.SourceClusterID == a.sourceClusterID && a.logicalBinding.RunID == a.runID))
}

func (a mutationExecutionAuthority) matchesRun(run fencing.Run) bool {
	if !a.valid() || run.Identity.SourceClusterID != a.sourceClusterID || run.Identity.RunID != a.runID {
		return false
	}
	if a.runtime != nil {
		return run.Runtime != nil && *run.Runtime == *a.runtime && run.LogicalBinding == nil
	}
	return run.Runtime == nil && run.LogicalBinding != nil && *run.LogicalBinding == *a.logicalBinding
}

func (a mutationExecutionAuthority) producerUID() string {
	if a.logicalBinding != nil {
		return a.logicalBinding.ProducerUID
	}
	return ""
}

func (a mutationExecutionAuthority) mutationOwner(run fencing.Run, producerUID string, sequence uint64, intent fencing.MutationIntent) string {
	if a.runtime != nil {
		return hashJSON([]any{*a.runtime, run.Identity.WALIdentity, producerUID, sequence, intent.OperationID})
	}
	return hashJSON([]any{"rhiza-logical-mutation-receipt-v1", *a.logicalBinding, run.Identity.WALIdentity, producerUID, sequence, intent.OperationID})
}

func (a mutationExecutionAuthority) makeReceipt(run fencing.Run, producerUID string, sequence uint64, intent fencing.MutationIntent,
	preparation fencing.MutationPreparation, dispatchID string, result mutationProviderResult) mutationExecutionReceipt {
	receipt := mutationExecutionReceipt{WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence,
		Intent: intent, Preparation: preparation, DispatchID: dispatchID, Outcome: result.Outcome, ConditionFailed: result.ConditionFailed,
		Generation: result.Generation, Metageneration: result.Metageneration, PayloadDigest: result.PayloadDigest, AllCopiesTerminal: result.AllCopiesTerminal}
	if a.runtime != nil {
		receipt.Runtime = *a.runtime
	} else {
		receipt.AuthorityProfile = fencing.LogicalWriterProfileV1
		binding := *a.logicalBinding
		receipt.LogicalBinding = &binding
	}
	receipt.ReceiptDigest = receipt.digest()
	return receipt
}

func (a mutationExecutionAuthority) matchesReceipt(receipt mutationExecutionReceipt, run fencing.Run, producerUID string, sequence uint64, intent fencing.MutationIntent) bool {
	if receipt.WALIdentity != run.Identity.WALIdentity || receipt.ProducerUID != producerUID || receipt.Sequence != sequence || receipt.Intent != intent {
		return false
	}
	if a.runtime != nil {
		return receipt.Runtime == *a.runtime && receipt.AuthorityProfile == "" && receipt.LogicalBinding == nil
	}
	return receipt.Runtime == (fencing.RuntimeBinding{}) && receipt.AuthorityProfile == fencing.LogicalWriterProfileV1 &&
		receipt.LogicalBinding != nil && *receipt.LogicalBinding == *a.logicalBinding
}

func (e *FencingExecutor) executeSourceMutation(ctx context.Context, authority mutationExecutionAuthority, producerUID string, sequence uint64, intent fencing.MutationIntent, scope AttestationStoreScope, provider mutationExecutionProvider, receipts *MutationExecutionReceiptStore) (returnErr error) {
	localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationAttemptStarted, localtesthooks.LogicalMutationStarted)
	defer func() {
		localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationAttemptReturned, returnErr)
	}()
	if e == nil || e.AuthorityState == nil {
		return fmt.Errorf("fencing: canonical authority state is required for source mutation execution")
	}
	if ctx == nil || e == nil || !authority.valid() || provider == nil || receipts == nil || sequence == 0 || producerUID == "" || scope.Provider != "gcs" || hashJSON(scope) != intent.StoreScopeHash {
		return fmt.Errorf("fencing: source mutation execution is not configured for this scope")
	}
	if authority.logicalBinding != nil && (authority.logicalBinding.ProducerUID != producerUID || authority.logicalBinding.StoreScopeHash != intent.StoreScopeHash) {
		return fmt.Errorf("fencing: logical source mutation differs from its authority binding")
	}
	if intent.Kind != "upload" && intent.Kind != "delete" {
		return fmt.Errorf("fencing: unsupported source mutation kind")
	}
	stages, err := e.sourceStagesForRoot(ctx, authority.statefulSetUID, authority.sourceClusterID)
	if err != nil {
		return err
	}
	state, err := e.read(ctx, authority.statefulSetUID, authority.sourceClusterID)
	if err != nil {
		return err
	}
	run, ok := state.Ledger.Runs[authority.runID]
	if !ok || !authority.matchesRun(run) || run.MutationProducerUID != producerUID || run.MutationScopeHash != intent.StoreScopeHash || !fencing.MutationProfileSupported(run.MutationProfile) || run.Identity.WALIdentity == "" {
		return fmt.Errorf("fencing: source mutation is outside enrolled producer scope")
	}
	if (authority.logicalBinding != nil && (state.Ledger.Profile != fencing.LogicalWriterProfileV1 || run.LogicalBinding == nil || !sameLogicalProducerBinding(run.LogicalBinding, authority.logicalBinding))) ||
		(authority.runtime != nil && (state.Ledger.Profile == fencing.LogicalWriterProfileV1 || run.Runtime == nil)) {
		return fmt.Errorf("fencing: source mutation authority profile differs from the canonical Run")
	}
	lookup, err := e.lookupMutationForAuthority(ctx, authority, run, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: sequence, Intent: intent})
	if err != nil && authority.logicalBinding != nil && errors.Is(err, ErrFencePending) {
		// The producer retains only the bounded live interval. An exact retry
		// below its compaction watermark is resolved from the root-bound archive
		// chain; this lookup is read-only with respect to dispatch permission.
		archived, archiveErr := e.lookupArchivedMutationForAuthority(ctx, authority, producerUID, sequence, intent)
		if archiveErr == nil {
			lookup = fencing.MutationLookupResult{Found: true, Archived: true, Entry: &archived}
			err = nil
		} else {
			err = archiveErr
		}
	}
	if err != nil {
		return err
	}
	if !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != intent {
		return ErrFencePending
	}
	entry := *lookup.Entry
	if entry.GenerationZeroGuard != nil {
		if authority.runtime == nil {
			return ErrFencePending
		}
		if entry.CopyCompletion != nil {
			if _, err := e.retireGenerationZeroBarrier(ctx, *authority.runtime, producerUID, sequence, entry); err != nil {
				return err
			}
		}
		if err := e.verifyGenerationZeroGuard(ctx, *authority.runtime, run, producerUID, sequence, entry); err != nil {
			return err
		}
		// Settlement already released the original key owner after guard
		// readback. Replaying this tuple must not reacquire or release a key that
		// may now belong to a later producer.
		return nil
	}
	if entry.Resolution != nil && entry.Preparation == nil {
		// Cancellation before preparation granted no provider permission and
		// therefore owns no cross-producer key lock. In particular, replaying an
		// archived no-work tuple must not manufacture a permanent key reservation.
		if !entry.DispatchClosed || len(entry.Dispatches) != 0 || entry.Resolution.Outcome != fencing.MutationRejected {
			return ErrFencePending
		}
		if entry.Resolution.ConditionFailed {
			return fencing.ErrMutationConditionFailed
		}
		return fmt.Errorf("fencing: original source mutation is terminally rejected")
	}
	owner := authority.mutationOwner(run, producerUID, sequence, intent)
	if terminalMutationKeyCleanupCandidateForAuthority(run, entry) {
		return e.resumeTerminalMutationKeyCleanup(ctx, authority, run, producerUID, sequence, entry, owner, receipts)
	}
	allowCreate := len(lookup.Entry.Dispatches) == 0
	if err := e.acquireMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner, allowCreate); err != nil {
		if authority.logicalBinding != nil && errors.Is(err, ErrFencePending) {
			if cleanupErr := e.resumeForeignLogicalTerminalKeyOwner(ctx, authority, intent.StoreScopeHash, intent.Key, owner); cleanupErr == nil {
				err = e.acquireMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner, allowCreate)
			} else if !errors.Is(cleanupErr, ErrFencePending) {
				return errors.Join(err, cleanupErr)
			}
		}
		if err != nil {
			return err
		}
	}
	keyState, err := e.readMutationKeyState(ctx, intent.StoreScopeHash, intent.Key)
	if err != nil {
		return err
	}
	if keyState.PendingGenerationZero != nil {
		return ErrFencePending
	}
	if authority.logicalBinding != nil && keyState.Barrier != nil {
		return ErrFencePending
	}
	if authority.logicalBinding != nil && entry.GenerationZeroGuard != nil {
		return ErrFencePending
	}
	var barrier *mutationGenerationZeroBarrier
	if keyState.Barrier != nil {
		barrierCopy := *keyState.Barrier
		barrier = &barrierCopy
		if barrier.Key != intent.Key || barrier.ScopeHash != intent.StoreScopeHash {
			return ErrFencePending
		}
		currentPinnedDelete := intent.Kind == "delete" && intent.Condition.Mode == fencing.MutationMatchGeneration &&
			intent.Condition.Generation == barrier.CurrentGeneration
		if currentPinnedDelete {
			// A journal cancellation is terminal evidence. Do not write it from
			// the key-state fields alone: first verify the canonical barrier,
			// frozen bucket contract, and exact pinned object lineage.
			if err := e.verifyMutationGenerationZeroBarrier(ctx, barrierCopy); err != nil {
				return errors.Join(ErrFencePending, err)
			}
			if entry.Resolution != nil || len(lookup.Entry.Dispatches) != 0 || !entry.DispatchClosed {
				return ErrFencePending
			}
			if authority.runtime == nil {
				return ErrFencePending
			}
			if err := e.cancelClosedMutationWithoutDispatch(ctx, *authority.runtime, producerUID, sequence, entry,
				fencing.MutationResolutionReasonGenerationZeroPinned); err != nil {
				return errors.Join(ErrFencePending, err)
			}
			if err := e.releaseMutationKey(ctx, intent.StoreScopeHash, intent.Key, owner); err != nil {
				return errors.Join(ErrFencePending, err)
			}
			return fencing.ErrMutationConditionFailed
		}
		if intent.Condition.Mode != fencing.MutationMatchGeneration ||
			(intent.Kind != "upload" && (intent.Kind != "delete" || intent.Condition.Generation <= 0)) {
			return ErrFencePending
		}
		resolvedSuccessor := entry.Resolution != nil && entry.Resolution.Outcome == fencing.MutationCommitted &&
			entry.Preparation != nil && entry.Preparation.Generation > 0 && entry.Resolution.AllCopiesTerminal
		verifyBarrier := e.verifyMutationGenerationZeroBarrier
		if resolvedSuccessor {
			verifyBarrier = e.verifyMutationGenerationZeroBarrierAuthority
		}
		if err := verifyBarrier(ctx, barrierCopy); err != nil {
			return err
		}
	}
	finish := func(receipt mutationExecutionReceipt) error {
		if !authority.matchesReceipt(receipt, run, producerUID, sequence, intent) {
			return fmt.Errorf("fencing: mutation receipt tuple differs")
		}
		digest, err := receipts.put(receipt)
		if err != nil {
			localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationReceiptPersisted, err)
			return err
		}
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationReceiptPersisted, localtesthooks.LogicalMutationSucceeded)
		if _, err := e.changeMutationProducerForAuthority(ctx, authority, producerUID, func(p *fencing.MutationProducer) error {
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
			localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationJournalResolved, err)
			return err
		}
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationJournalResolved, localtesthooks.LogicalMutationSucceeded)
		if barrier != nil && receipt.Outcome == fencing.MutationCommitted && receipt.Intent.Kind == "upload" {
			if receipt.Preparation.Generation <= 0 || receipt.Generation <= 0 || receipt.Generation == receipt.Preparation.Generation || receipt.Metageneration <= 0 {
				return ErrFencePending
			}
			if err := e.advanceMutationGenerationZeroBarrier(ctx, intent.StoreScopeHash, intent.Key, owner, *barrier, receipt); err != nil {
				return err
			}
		}
		if barrier != nil && receipt.Intent.Kind == "delete" {
			if receipt.Preparation.Generation != receipt.Intent.Condition.Generation || receipt.Preparation.Generation == barrier.CurrentGeneration {
				return ErrFencePending
			}
			if err := e.verifyMutationGenerationZeroBarrier(ctx, *barrier); err != nil {
				return errors.Join(ErrFencePending, err)
			}
		}
		if fencing.IsHeldCreateEligible(receipt.Intent, receipt.Preparation) &&
			receipt.Outcome == fencing.MutationCommitted && receipt.AllCopiesTerminal {
			finalizer, ok := provider.(mutationHeldCreateFinalizer)
			if !ok {
				return ErrFencePending
			}
			if err := finalizer.finalizeHeldCreate(ctx, intent.Key, intent, receipt); err != nil {
				return errors.Join(ErrFencePending, err)
			}
		}
		// A zero-generation held upload with unresolved copies retains the
		// cross-producer key guard after its caller-visible commit.
		if receipt.Preparation.Generation != 0 || receipt.AllCopiesTerminal {
			if err := e.releaseMutationKeyAfterVerifiedTerminal(ctx, intent.StoreScopeHash, intent.Key, owner); err != nil {
				localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationKeyReleased, err)
				return err
			}
			localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationKeyReleased, localtesthooks.LogicalMutationSucceeded)
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
		if entry.GuardedLogicalResolution != nil {
			return e.verifyGuardedLogicalResolutionForAuthority(ctx, authority, run, producerUID, sequence, entry, provider)
		}
		if entry.Resolution.Outcome == fencing.MutationCommitted {
			if barrier != nil && intent.Kind == "upload" {
				if entry.Preparation == nil || entry.Preparation.Generation <= 0 || !entry.Resolution.AllCopiesTerminal {
					return ErrFencePending
				}
				if authority.runtime == nil {
					return ErrFencePending
				}
				if err := e.resumeResolvedGenerationZeroBarrier(ctx, *authority.runtime, run, producerUID, sequence, entry, intent, owner, *barrier); err != nil {
					return errors.Join(ErrFencePending, err)
				}
			} else if barrier != nil && intent.Kind == "delete" {
				if entry.Preparation == nil || entry.Preparation.Generation != intent.Condition.Generation ||
					entry.Preparation.Generation == barrier.CurrentGeneration || !entry.Resolution.AllCopiesTerminal {
					return ErrFencePending
				}
				if authority.runtime == nil {
					return ErrFencePending
				}
				if err := e.VerifySourceMutationEvidence(ctx, *authority.runtime, sequence, entry); err != nil {
					return err
				}
				if err := e.verifyMutationGenerationZeroBarrier(ctx, *barrier); err != nil {
					return err
				}
			}
			if fencing.IsHeldCreateEligible(intent, *entry.Preparation) && entry.Resolution.AllCopiesTerminal {
				finalizer, ok := provider.(mutationHeldCreateFinalizer)
				if !ok {
					return ErrFencePending
				}
				stored, receiptErr := receipts.get("receipt-" + owner)
				if receiptErr != nil {
					return errors.Join(ErrFencePending, receiptErr)
				}
				if err := finalizer.finalizeHeldCreate(ctx, intent.Key, intent, stored); err != nil {
					return errors.Join(ErrFencePending, err)
				}
			}
			if entry.Preparation.Generation != 0 || entry.Resolution.AllCopiesTerminal || entry.CopyCompletion != nil {
				return e.releaseMutationKeyAfterVerifiedTerminal(ctx, intent.StoreScopeHash, intent.Key, owner)
			}
			if run.MutationProfile == fencing.MutationHeldExecutionProfile {
				current, actualUID, _, readErr := e.readMutationProducerForAuthority(ctx, authority)
				if readErr != nil || actualUID != producerUID {
					return errors.Join(ErrFencePending, readErr)
				}
				if current.Phase != fencing.MutationClosed {
					return nil // the caller-visible logical commit remains valid while the producer is open
				}
				return e.replayGuardedLogicalResolutionForAuthority(ctx, authority, run, producerUID, sequence, entry, stages, provider)
			}
			return nil
		}
		if entry.Preparation != nil && (entry.Preparation.Generation != 0 || entry.Resolution.AllCopiesTerminal || entry.CopyCompletion != nil) {
			if err := e.releaseMutationKeyAfterVerifiedTerminal(ctx, intent.StoreScopeHash, intent.Key, owner); err != nil {
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
		prepared := fencing.MutationPreparation{Profile: run.MutationProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: generation}
		if barrier != nil && ((intent.Kind == "upload" && generation != barrier.CurrentGeneration) ||
			(intent.Kind == "delete" && generation == barrier.CurrentGeneration)) {
			return ErrFencePending
		}
		if _, err := e.changeMutationProducerForAuthority(ctx, authority, producerUID, func(p *fencing.MutationProducer) error { return p.Prepare(sequence, prepared) }); err != nil {
			return err
		}
		entry.Preparation = &prepared
	}
	if barrier != nil {
		if !entry.Preparation.GenerationSet || entry.Preparation.Generation <= 0 {
			return ErrFencePending
		}
		if intent.Kind == "upload" && entry.Preparation.Generation != barrier.CurrentGeneration {
			return ErrFencePending
		}
		if intent.Kind == "delete" && (entry.Preparation.Generation != intent.Condition.Generation || entry.Preparation.Generation == barrier.CurrentGeneration) {
			return ErrFencePending
		}
	}
	dispatchID := ""
	if len(entry.Dispatches) != 0 {
		if len(entry.Dispatches) != 1 {
			return ErrFencePending
		}
		for id := range entry.Dispatches {
			dispatchID = id
		}
		// A granted but unresolved legacy right is never reused for another
		// physical call. The held profile has a separate root-guarded replay below.
		if fencing.IsHeldCreateEligible(intent, *entry.Preparation) {
			return e.replayGuardedLogicalResolutionForAuthority(ctx, authority, run, producerUID, sequence, entry, stages, provider)
		}
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderStarted, localtesthooks.LogicalMutationStarted)
		result, found, err := provider.committed(ctx, intent.Key, intent, *entry.Preparation)
		if err != nil || !found {
			if err == nil {
				err = errors.New("exact mutation readback not found")
			}
			localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderReturned, err)
			return ErrFencePending
		}
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderReturned, localtesthooks.LogicalMutationSucceeded)
		receipt := authority.makeReceipt(run, producerUID, sequence, intent, *entry.Preparation, dispatchID, result)
		return finish(receipt)
	}
	dispatchID, err = newMutationDispatchID()
	if err != nil {
		return err
	}
	if _, err := e.changeMutationProducerForAuthority(ctx, authority, producerUID, func(p *fencing.MutationProducer) error {
		return grantMutationDispatch(p, sequence, dispatchID)
	}); err != nil {
		localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationDispatchGranted, err)
		return err
	}
	localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationDispatchGranted, localtesthooks.LogicalMutationSucceeded)
	// Persist the permission before the one provider dispatch; any failure after
	// this point remains an unresolved lock and is not retried by another worker.
	var result mutationProviderResult
	if intent.Kind == "upload" {
		payload, err := stages.openMutationPayloadContext(ctx, producerUID, intent)
		if err != nil {
			localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationPayloadOpened, err)
			return err
		}
		defer payload.Close()
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationPayloadOpened, localtesthooks.LogicalMutationSucceeded)
		dispatchContext := ctx
		if e.GCSMutationAckLoss != nil && fencing.IsHeldCreateEligible(intent, *entry.Preparation) {
			if authority.runtime == nil {
				return ErrFencePending
			}
			bootstrapGenesisAuthorized := false
			if e.GCSMutationAckLoss.RequiresBootstrapGenesisAuthorization() {
				bootstrapGenesisAuthorized = e.canonicalBootstrapGenesisDispatch(ctx, *authority.runtime, state, producerUID, sequence, intent, scope)
			}
			dispatchContext = e.GCSMutationAckLoss.arm(ctx, *authority.runtime, run, producerUID, sequence, dispatchID, intent, *entry.Preparation, scope, bootstrapGenesisAuthorized)
		}
		if barrier != nil {
			gcsProvider, ok := provider.(*gcsMutationProvider)
			if !ok {
				return ErrFencePending
			}
			updatedBarrier, releaseErr := e.releaseGenerationZeroOriginHold(ctx, gcsProvider, intent.StoreScopeHash, intent.Key, owner, *barrier)
			err = releaseErr
			if err != nil {
				return err
			}
			barrier = &updatedBarrier
		}
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderStarted, localtesthooks.LogicalMutationStarted)
		result, err = provider.upload(dispatchContext, intent.Key, entry.Preparation.Generation, intent, payload)
		if err != nil {
			localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderReturned, err)
			return ErrFencePending
		}
	} else {
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderStarted, localtesthooks.LogicalMutationStarted)
		result, err = provider.delete(ctx, intent.Key, entry.Preparation.Generation)
		if err != nil {
			localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderReturned, err)
			return ErrFencePending
		}
	}
	conditionRejected := result.Outcome == fencing.MutationRejected && result.ConditionFailed && result.AllCopiesTerminal && result.PayloadDigest == "" && result.Generation == entry.Preparation.Generation
	if result.ConditionFailed && !conditionRejected {
		localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderReturned, errors.New("provider result validation failed"))
		return ErrFencePending
	}
	if (result.Outcome != fencing.MutationCommitted && result.Outcome != fencing.MutationExcluded && !conditionRejected) ||
		(intent.Kind == "upload" && (result.Outcome != fencing.MutationCommitted || result.PayloadDigest != intent.PayloadDigest || result.Generation <= 0)) ||
		(intent.Kind == "delete" && result.Generation != entry.Preparation.Generation) {
		if !conditionRejected {
			localtesthooks.HitLogicalMutationExecutionError(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderReturned, errors.New("provider result validation failed"))
			return ErrFencePending
		}
	}
	localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationProviderReturned, localtesthooks.LogicalMutationSucceeded)
	receipt = authority.makeReceipt(run, producerUID, sequence, intent, *entry.Preparation, dispatchID, result)
	return finish(receipt)
}

func terminalMutationKeyCleanupCandidateForAuthority(run fencing.Run, entry fencing.MutationEntry) bool {
	if run.MutationProfile != fencing.MutationExecutionProfile || entry.Resolution == nil ||
		entry.Resolution.Outcome != fencing.MutationCommitted || entry.Resolution.ConditionFailed || !entry.Resolution.AllCopiesTerminal ||
		entry.Preparation == nil || entry.Preparation.Profile != fencing.MutationExecutionProfile || entry.Preparation.Generation <= 0 ||
		entry.Intent.Kind != "upload" || entry.Intent.Condition.Mode != fencing.MutationMatchGeneration ||
		entry.Intent.Condition.Generation != entry.Preparation.Generation || entry.Resolution.PayloadDigest != entry.Intent.PayloadDigest ||
		entry.CopyCompletion != nil || entry.GuardedLogicalResolution != nil || entry.GenerationZeroGuard != nil || entry.GenerationZeroRights != nil ||
		!entry.DispatchClosed || len(entry.Dispatches) != 1 {
		return false
	}
	for _, dispatch := range entry.Dispatches {
		if dispatch.ReceiptDigest == "" || dispatch.ReceiptDigest != entry.Resolution.ReceiptDigest {
			return false
		}
	}
	return true
}

func (e *FencingExecutor) resumeTerminalMutationKeyCleanup(ctx context.Context, authority mutationExecutionAuthority, run fencing.Run,
	producerUID string, sequence uint64, entry fencing.MutationEntry, owner string, receipts *MutationExecutionReceiptStore) error {
	resolution := entry.Resolution
	if resolution == nil || entry.Preparation == nil || len(entry.Dispatches) != 1 || !entry.DispatchClosed ||
		entry.Intent.Condition.Mode != fencing.MutationMatchGeneration || entry.Intent.Condition.Generation != entry.Preparation.Generation ||
		entry.Preparation.Generation <= 0 || resolution.Outcome != fencing.MutationCommitted || !resolution.AllCopiesTerminal ||
		resolution.ConditionFailed || resolution.PayloadDigest != entry.Intent.PayloadDigest {
		return ErrFencePending
	}
	receipt, err := receipts.getContext(ctx, "receipt-"+owner)
	if err != nil || !authority.matchesReceipt(receipt, run, producerUID, sequence, entry.Intent) || receipt.Preparation != *entry.Preparation ||
		receipt.ReceiptDigest == "" || receipt.ReceiptDigest != receipt.digest() || receipt.ReceiptDigest != resolution.ReceiptDigest ||
		receipt.Outcome != fencing.MutationCommitted || receipt.ConditionFailed || !receipt.AllCopiesTerminal ||
		receipt.Generation <= receipt.Preparation.Generation || receipt.Metageneration <= 0 || receipt.PayloadDigest != entry.Intent.PayloadDigest ||
		!mutationReceiptMatchesDispatch(entry, receipt) {
		return errors.Join(ErrFencePending, err, fmt.Errorf("fencing: terminal mutation cleanup receipt differs from canonical journal"))
	}
	keyState, err := e.readMutationKeyState(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key)
	if errors.Is(err, errAuthorityStateNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if keyState.PendingGenerationZero != nil || keyState.Barrier != nil || keyState.Retirement != nil {
		return ErrFencePending
	}
	if err := e.releaseMutationKeyAfterVerifiedTerminal(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner); err != nil {
		localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationKeyReleased, localtesthooks.LogicalMutationPending)
		return err
	}
	localtesthooks.HitLogicalMutationExecution(ctx, producerUID, sequence, localtesthooks.LogicalMutationKeyReleased, localtesthooks.LogicalMutationSucceeded)
	return nil
}

// resumeForeignLogicalTerminalKeyOwner releases a predecessor only after
// locating its exact terminal tuple in the canonical root's producer journal
// or authenticated archive and validating that tuple's immutable receipt.
// It never reacquires, redispatches, or performs historical provider reads.
func (e *FencingExecutor) resumeForeignLogicalTerminalKeyOwner(ctx context.Context, current mutationExecutionAuthority,
	scopeHash, key, currentOwner string) error {
	if ctx == nil || e == nil || e.AuthorityState == nil || current.logicalBinding == nil || scopeHash == "" || key == "" || currentOwner == "" {
		return ErrFencePending
	}
	keyState, err := e.readMutationKeyState(ctx, scopeHash, key)
	if err != nil || keyState.ScopeHash != scopeHash || keyState.Key != key || keyState.Owner == "" || keyState.Owner == currentOwner ||
		keyState.PendingGenerationZero != nil || keyState.Barrier != nil || keyState.Retirement != nil {
		return ErrFencePending
	}
	evidence, err := e.authorityEvidenceForRoot(current.statefulSetUID, current.sourceClusterID)
	if err != nil {
		return err
	}
	receipts, err := NewRemoteMutationExecutionReceiptStore(evidence)
	if err != nil {
		return err
	}
	prior, err := receipts.getContext(ctx, "receipt-"+keyState.Owner)
	if err != nil || prior.AuthorityProfile != fencing.LogicalWriterProfileV1 || prior.LogicalBinding == nil || prior.Runtime != (fencing.RuntimeBinding{}) ||
		prior.Sequence == 0 || prior.ProducerUID == "" || prior.Intent.StoreScopeHash != scopeHash || prior.Intent.Key != key ||
		prior.Intent.OperationID != fencing.MutationRequestID(prior.ProducerUID, prior.Sequence) {
		return errors.Join(ErrFencePending, err)
	}
	binding := *prior.LogicalBinding
	if binding.SourceClusterID != current.sourceClusterID || binding.ProducerUID != prior.ProducerUID || binding.WALIdentity != prior.WALIdentity ||
		binding.ValidateBound() != nil || binding.Role != fencing.StartupRoleVoterActive {
		return ErrFencePending
	}
	priorAuthority := logicalMutationAuthority(current.statefulSetUID, binding)
	owner := priorAuthority.mutationOwner(fencing.Run{Identity: fencing.Identity{SourceClusterID: priorAuthority.sourceClusterID,
		RunID: priorAuthority.runID, WALIdentity: prior.WALIdentity}}, prior.ProducerUID, prior.Sequence, prior.Intent)
	if owner != keyState.Owner {
		return ErrFencePending
	}
	state, err := e.read(ctx, current.statefulSetUID, current.sourceClusterID)
	if err != nil {
		return err
	}
	run, exists := state.Ledger.Runs[binding.RunID]
	if !exists || !priorAuthority.matchesRun(run) || run.LogicalBinding == nil || *run.LogicalBinding != binding ||
		run.MutationProfile != fencing.MutationExecutionProfile || run.MutationProducerUID != prior.ProducerUID ||
		run.MutationScopeHash != scopeHash || run.Identity.WALIdentity != prior.WALIdentity {
		return ErrFencePending
	}
	lookup, err := e.lookupMutationForAuthority(ctx, priorAuthority, run, fencing.MutationLookupRequest{ProducerUID: prior.ProducerUID,
		Sequence: prior.Sequence, Intent: prior.Intent})
	if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != prior.Intent ||
		!terminalMutationKeyCleanupCandidateForAuthority(run, *lookup.Entry) {
		return errors.Join(ErrFencePending, err)
	}
	exactReceipts, err := e.receiptsForLogicalBinding(priorAuthority.statefulSetUID, binding)
	if err != nil {
		return err
	}
	return e.resumeTerminalMutationKeyCleanup(ctx, priorAuthority, run, prior.ProducerUID, prior.Sequence, *lookup.Entry, keyState.Owner, exactReceipts)
}

// resumeResolvedGenerationZeroBarrier completes the lineage CAS after a crash
// or lost response between the producer's terminal resolution and barrier
// advancement. It reads the immutable receipt and never redispatches the
// provider request.
func (e *FencingExecutor) resumeResolvedGenerationZeroBarrier(ctx context.Context, runtime fencing.RuntimeBinding, run fencing.Run,
	producerUID string, sequence uint64, entry fencing.MutationEntry, intent fencing.MutationIntent, owner string, barrier mutationGenerationZeroBarrier) error {
	if entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted || entry.Preparation == nil ||
		entry.Intent != intent || !entry.Resolution.AllCopiesTerminal || len(entry.Dispatches) != 1 {
		return ErrFencePending
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		return err
	}
	name := "receipt-" + owner
	receipt, err := receipts.get(name)
	if err != nil {
		return err
	}
	if receipt.Runtime != runtime || receipt.WALIdentity != run.Identity.WALIdentity || receipt.ProducerUID != producerUID ||
		receipt.Sequence != sequence || receipt.Intent != intent || receipt.Preparation != *entry.Preparation ||
		receipt.Outcome != fencing.MutationCommitted || !receipt.AllCopiesTerminal || receipt.ReceiptDigest != entry.Resolution.ReceiptDigest ||
		receipt.ReceiptDigest != receipt.digest() || receipt.Generation <= 0 || receipt.Metageneration <= 0 ||
		receipt.Generation == receipt.Preparation.Generation || !mutationReceiptMatchesDispatch(entry, receipt) {
		return fmt.Errorf("fencing: resolved generation-zero successor receipt differs from the exact journal tuple")
	}
	if err := e.VerifySourceMutationEvidence(ctx, runtime, sequence, entry); err != nil {
		return err
	}
	return e.advanceMutationGenerationZeroBarrier(ctx, intent.StoreScopeHash, intent.Key, owner, barrier, receipt)
}

// settleEngineRepairMutation closes one admitted positive-generation mutation
// under its original generation precondition. The repair record is the only
// authority to settle a closed old incarnation; this never grants a new tuple.
func (e *FencingExecutor) settleEngineRepairMutation(ctx context.Context, repair fencing.EngineRepairRecord, runtime fencing.RuntimeBinding,
	producerUID string, sequence uint64, scope AttestationStoreScope, provider mutationExecutionProvider) (fencing.StorageRunSettlement, error) {
	if e == nil || e.AuthorityState == nil || e.SourceMutationStorage == nil || ctx == nil || producerUID == "" || sequence == 0 {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: engine repair settlement is not configured")
	}
	if err := repair.Validate(); err != nil || repair.Binding.FreezeSlot == 0 || (repair.Phase != fencing.RepairStorageClosing && repair.Phase != fencing.RepairGenerationClosing) {
		return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: engine repair storage settlement lacks a frozen closing record"))
	}
	if _, err := e.readExactEngineRepairRecord(ctx, repair); err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	if err := repair.ValidateAgainstLedger(state.Ledger); err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	run, ok := state.Ledger.Runs[runtime.RunID]
	if !ok || run.Runtime == nil || *run.Runtime != runtime || run.Identity.NodeID != repair.Binding.OldMemberID || run.MutationProducerUID != producerUID ||
		run.Identity.WALIdentity == "" || run.MutationScopeHash != hashJSON(scope) || scope.Provider != "gcs" {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair settlement runtime is outside the frozen old incarnation")
	}
	incarnationFound := false
	for _, incarnation := range repair.Incarnations {
		if incarnation.Identity == run.Identity && incarnation.ProducerUID == producerUID && incarnation.MutationScopeHash == run.MutationScopeHash && incarnation.MutationProfile == run.MutationProfile {
			incarnationFound = true
			break
		}
	}
	if !incarnationFound {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair settlement run is absent from the reserved inventory")
	}
	producer, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || producer.Runtime != runtime || producer.WALIdentity != run.Identity.WALIdentity || producer.Phase != fencing.MutationClosed || producer.Validate() != nil {
		return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: repair settlement requires the exact closed producer"))
	}
	entry, live := producer.Entries[sequence]
	if !live || sequence <= producer.CompactedThrough || sequence > producer.ClosedThrough || (entry.Intent.Kind != "upload" && entry.Intent.Kind != "delete") || entry.Preparation == nil ||
		entry.Preparation.Generation <= 0 || !entry.Preparation.GenerationSet || entry.Preparation.StoreScopeHash != hashJSON(scope) ||
		len(entry.Dispatches) != 1 || !entry.DispatchClosed {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair settlement is not an exact closed positive-generation dispatch")
	}
	var dispatchID string
	for id, dispatch := range entry.Dispatches {
		if id == "" || (dispatch.ReceiptDigest != "" && entry.Resolution == nil) {
			return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair settlement dispatch is already terminal or malformed")
		}
		dispatchID = id
	}
	evidenceStore, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	if evidenceStore.Identity() == "" {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair settlement lacks canonical evidence root")
	}
	repairHash := repair.IdentityDigest()
	settlementKey := engineRepairMutationSettlementKey(repairHash, runtime, producerUID, sequence)
	owner := hashJSON([]any{runtime, run.Identity.WALIdentity, producerUID, sequence, entry.Intent.OperationID})
	if entry.Resolution != nil {
		settlement, err := e.readEngineRepairRunSettlement(ctx, evidenceStore, producer, producerUID, sequence, settlementKey, repairHash)
		if err != nil {
			if !evidenceStore.IsNotFound(err) || !isTerminalConditionalRejection(entry, dispatchID) {
				return fencing.StorageRunSettlement{}, err
			}
			receiptStore, storeErr := e.receiptsForRuntime(runtime)
			if storeErr != nil {
				return fencing.StorageRunSettlement{}, storeErr
			}
			receipt, receiptErr := receiptStore.get("receipt-" + owner)
			if receiptErr != nil || !mutationReceiptMatchesTerminalRejection(entry, runtime, run.Identity.WALIdentity, producerUID, sequence, dispatchID, receipt) {
				return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, receiptErr, fmt.Errorf("fencing: resolved rejection lacks its exact immutable provider receipt"))
			}
			if provider == nil {
				provider, storeErr = newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, run.MutationProfile)
				if storeErr != nil {
					return fencing.StorageRunSettlement{}, storeErr
				}
			}
			if err := verifyRejectedMutationExcluded(ctx, provider, entry.Intent, *entry.Preparation, receipt); err != nil {
				return fencing.StorageRunSettlement{}, err
			}
			rejection := engineRepairMutationSettlement{Version: 1, RepairRecordHash: repairHash, Runtime: runtime, WALIdentity: run.Identity.WALIdentity,
				ProducerUID: producerUID, Sequence: sequence, Intent: entry.Intent, Preparation: *entry.Preparation, DispatchID: dispatchID,
				Outcome: fencing.MutationRejected, ConditionFailed: true, AllCopiesTerminal: true, FutureEffectsExcluded: true,
				Generation: receipt.Generation, ReceiptDigest: receipt.ReceiptDigest}
			if err := e.persistEngineRepairMutationSettlement(ctx, evidenceStore, settlementKey, rejection); err != nil {
				return fencing.StorageRunSettlement{}, err
			}
			settlement, err = e.readEngineRepairRunSettlement(ctx, evidenceStore, producer, producerUID, sequence, settlementKey, repairHash)
			if err != nil {
				return fencing.StorageRunSettlement{}, err
			}
		}
		// A prior attempt may have committed the immutable settlement and
		// producer CAS, then lost its response before releasing the key. Verify
		// the complete settlement first, then release only this exact owner; a
		// successor owner is evidence that the original release already occurred.
		if err := e.releaseMutationKeyAfterVerifiedTerminal(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner); err != nil {
			return fencing.StorageRunSettlement{}, err
		}
		return settlement, nil
	}
	if err := e.acquireMutationKey(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner, false); err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	if provider == nil {
		provider, err = newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, run.MutationProfile)
		if err != nil {
			return fencing.StorageRunSettlement{}, err
		}
	}
	if entry.Intent.Kind != "upload" && entry.Intent.Kind != "delete" {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair settlement intent kind is unsupported")
	}
	receiptStore, err := e.receiptsForRuntime(runtime)
	if err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	priorReceipt, priorReceiptErr := receiptStore.get("receipt-" + owner)
	if priorReceiptErr != nil && !errors.Is(priorReceiptErr, os.ErrNotExist) {
		return fencing.StorageRunSettlement{}, priorReceiptErr
	}
	hasPriorReceipt := priorReceiptErr == nil
	if hasPriorReceipt && (priorReceipt.Runtime != runtime || priorReceipt.WALIdentity != run.Identity.WALIdentity || priorReceipt.ProducerUID != producerUID ||
		priorReceipt.Sequence != sequence || priorReceipt.Intent != entry.Intent || priorReceipt.Preparation != *entry.Preparation || priorReceipt.DispatchID != dispatchID ||
		priorReceipt.ReceiptDigest != priorReceipt.digest() || !mutationReceiptMatchesDispatch(entry, priorReceipt)) {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: prior provider receipt differs from the closed dispatch tuple")
	}
	if hasPriorReceipt && priorReceipt.Outcome != fencing.MutationCommitted &&
		!mutationReceiptMatchesTerminalRejection(entry, runtime, run.Identity.WALIdentity, producerUID, sequence, dispatchID, priorReceipt) {
		return fencing.StorageRunSettlement{}, ErrFencePending
	}
	var stages *SourceStageStore
	var payload io.ReadCloser
	if entry.Intent.Kind == "upload" && !hasPriorReceipt {
		stages, err = e.sourceStagesFor(ctx, runtime)
		if err != nil || stages == nil || stages.evidence == nil {
			return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: repair settlement requires the canonical remote sealed payload"))
		}
		payload, err = stages.openMutationPayloadContext(ctx, producerUID, entry.Intent)
		if err != nil {
			return fencing.StorageRunSettlement{}, err
		}
	}
	result, committed, err := provider.committed(ctx, entry.Intent.Key, entry.Intent, *entry.Preparation)
	if err != nil {
		if payload != nil {
			err = errors.Join(err, payload.Close())
		}
		return fencing.StorageRunSettlement{}, err
	}
	if committed && result.Outcome != fencing.MutationCommitted && !(entry.Intent.Kind == "delete" && result.Outcome == fencing.MutationExcluded) {
		var closeErr error
		if payload != nil {
			closeErr = payload.Close()
		}
		return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, closeErr, fmt.Errorf("fencing: provider commit lookup returned an unsupported terminal result"))
	}
	actualCommitProven := false
	if hasPriorReceipt {
		if priorReceipt.Outcome == fencing.MutationRejected {
			if committed && result.Outcome == fencing.MutationCommitted {
				return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, fmt.Errorf("fencing: rejected provider receipt conflicts with a confirmed commit"))
			}
			result = mutationProviderResult{Outcome: priorReceipt.Outcome, ConditionFailed: true, Generation: priorReceipt.Generation, AllCopiesTerminal: true}
			if err := verifyRejectedMutationExcluded(ctx, provider, entry.Intent, *entry.Preparation, priorReceipt); err != nil {
				return fencing.StorageRunSettlement{}, err
			}
		} else if !committed || priorReceipt.Outcome != fencing.MutationCommitted || priorReceipt.Generation != result.Generation ||
			priorReceipt.Metageneration != result.Metageneration || priorReceipt.PayloadDigest != result.PayloadDigest || result.PayloadDigest != entry.Intent.PayloadDigest {
			return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, fmt.Errorf("fencing: provider no longer confirms the prior immutable success receipt"))
		} else {
			result = mutationProviderResult{Outcome: priorReceipt.Outcome, Generation: priorReceipt.Generation, Metageneration: priorReceipt.Metageneration,
				PayloadDigest: priorReceipt.PayloadDigest}
			actualCommitProven = true
		}
	} else if !committed {
		var closeErr error
		if entry.Intent.Kind == "upload" {
			result, err = provider.upload(ctx, entry.Intent.Key, entry.Preparation.Generation, entry.Intent, payload)
			closeErr = payload.Close()
		} else {
			result, err = provider.delete(ctx, entry.Intent.Key, entry.Preparation.Generation)
		}
		if err != nil || closeErr != nil {
			return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, err, closeErr)
		}
		if result.Outcome == fencing.MutationRejected && result.ConditionFailed {
			// A 412 can mean the original request won and only its response was
			// lost. Re-read the exact operation tuple before classifying it as
			// excluded; a generation mismatch alone is not commit evidence.
			confirmed, committed, confirmErr := provider.committed(ctx, entry.Intent.Key, entry.Intent, *entry.Preparation)
			if confirmErr != nil {
				return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, confirmErr)
			}
			if committed && confirmed.Outcome != fencing.MutationCommitted && !(entry.Intent.Kind == "delete" && confirmed.Outcome == fencing.MutationExcluded) {
				return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, fmt.Errorf("fencing: provider rejection lookup returned an unsupported terminal result"))
			}
			if committed && confirmed.Outcome == fencing.MutationCommitted {
				verifier, ok := provider.(interface {
					verifyRepairCommit(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation, mutationProviderResult) error
				})
				if !ok {
					return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair provider cannot verify the exact committed generation")
				}
				if err := verifier.verifyRepairCommit(ctx, entry.Intent.Key, entry.Intent, *entry.Preparation, confirmed); err != nil {
					return fencing.StorageRunSettlement{}, err
				}
				result, actualCommitProven = confirmed, true
			} else {
				if !rejectedReceiptShapeValid(result, entry.Intent, *entry.Preparation) {
					return fencing.StorageRunSettlement{}, errors.Join(ErrFencePending, fmt.Errorf("fencing: provider rejection receipt is not an exact terminal 412"))
				}
				if err := verifyRejectedMutationExcluded(ctx, provider, entry.Intent, *entry.Preparation, mutationExecutionReceipt{
					Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence, Intent: entry.Intent,
					Preparation: *entry.Preparation, DispatchID: dispatchID, Outcome: result.Outcome, ConditionFailed: result.ConditionFailed,
					Generation: result.Generation, Metageneration: result.Metageneration, PayloadDigest: result.PayloadDigest,
					AllCopiesTerminal: result.AllCopiesTerminal,
				}); err != nil {
					return fencing.StorageRunSettlement{}, err
				}
			}
		} else if result.Outcome != fencing.MutationCommitted || result.Generation <= 0 || result.Generation == entry.Preparation.Generation || result.PayloadDigest != entry.Intent.PayloadDigest {
			return fencing.StorageRunSettlement{}, ErrFencePending
		}
	} else if payload != nil {
		if err := payload.Close(); err != nil {
			return fencing.StorageRunSettlement{}, err
		}
	}
	if result.Outcome == fencing.MutationCommitted {
		if entry.Intent.Kind == "upload" {
			verifier, ok := provider.(interface {
				verifyRepairCommit(context.Context, string, fencing.MutationIntent, fencing.MutationPreparation, mutationProviderResult) error
			})
			if !ok {
				return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair provider cannot verify the exact committed generation")
			}
			if err := verifier.verifyRepairCommit(ctx, entry.Intent.Key, entry.Intent, *entry.Preparation, result); err != nil {
				return fencing.StorageRunSettlement{}, err
			}
		} else if result.Generation != entry.Preparation.Generation {
			return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: repair deletion commit differs from its selected generation")
		}
		actualCommitProven = true
	}
	if result.Outcome != fencing.MutationCommitted && result.Outcome != fencing.MutationExcluded && result.Outcome != fencing.MutationRejected {
		return fencing.StorageRunSettlement{}, ErrFencePending
	}
	receipt := priorReceipt
	if !hasPriorReceipt {
		receipt = mutationExecutionReceipt{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence,
			Intent: entry.Intent, Preparation: *entry.Preparation, DispatchID: dispatchID, Outcome: result.Outcome, ConditionFailed: result.ConditionFailed,
			Generation: result.Generation, Metageneration: result.Metageneration, PayloadDigest: result.PayloadDigest, AllCopiesTerminal: result.AllCopiesTerminal}
		receipt.ReceiptDigest = receipt.digest()
		if digest, err := receiptStore.put(receipt); err != nil || digest != receipt.ReceiptDigest {
			return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: repair settlement receipt did not persist"))
		}
	}
	settlement := engineRepairMutationSettlement{Version: 1, RepairRecordHash: repairHash, Runtime: runtime, WALIdentity: run.Identity.WALIdentity,
		ProducerUID: producerUID, Sequence: sequence, Intent: entry.Intent, Preparation: *entry.Preparation, DispatchID: dispatchID,
		Outcome: result.Outcome, ActualCommitProven: actualCommitProven, FutureEffectsExcluded: true, Generation: result.Generation,
		Metageneration: result.Metageneration, PayloadDigest: result.PayloadDigest, ReceiptDigest: receipt.ReceiptDigest}
	if result.Outcome == fencing.MutationRejected {
		settlement.ConditionFailed, settlement.AllCopiesTerminal = receipt.ConditionFailed, receipt.AllCopiesTerminal
	}
	if err := e.persistEngineRepairMutationSettlement(ctx, evidenceStore, settlementKey, settlement); err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		current, ok := p.Entries[sequence]
		if !ok || current.Dispatches[dispatchID].ReceiptDigest != "" || current.Intent != entry.Intent || current.Preparation == nil || *current.Preparation != *entry.Preparation {
			return ErrFencePending
		}
		preparationHash, err := p.PreparationHash(sequence)
		if err != nil {
			return err
		}
		if receipt.AllCopiesTerminal {
			if err := p.RecordDispatchTerminal(sequence, dispatchID, receipt.ReceiptDigest); err != nil {
				return err
			}
		}
		return p.Resolve(sequence, fencing.MutationResolution{Outcome: result.Outcome, PreparationHash: preparationHash, ReceiptDigest: receipt.ReceiptDigest,
			PayloadDigest: receipt.PayloadDigest, ConditionFailed: receipt.ConditionFailed, AllCopiesTerminal: receipt.AllCopiesTerminal})
	}); err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	if err := e.releaseMutationKeyAfterVerifiedTerminal(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner); err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	producer, actualUID, _, err = e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || producer.Validate() != nil {
		return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: settled producer readback failed"))
	}
	if !producer.BarrierComplete() {
		return fencing.StorageRunSettlement{}, ErrFencePending
	}
	settlementDigest := engineRepairRunSettlementDigest(repairHash, producer, settlement)
	return fencing.StorageRunSettlement{RunID: runtime.RunID, PodUID: runtime.PodUID, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID,
		ClosedThrough: producer.ClosedThrough, SettledThrough: producer.ClosedThrough, ProducerStateHash: hashJSON(producer), SettlementDigest: settlementDigest}, nil
}

func isTerminalConditionalRejection(entry fencing.MutationEntry, dispatchID string) bool {
	if entry.Resolution == nil || entry.Preparation == nil || !entry.DispatchClosed || len(entry.Dispatches) != 1 ||
		entry.Intent.Condition.Mode != fencing.MutationMatchGeneration || entry.Intent.Condition.Generation != entry.Preparation.Generation ||
		entry.Resolution.Outcome != fencing.MutationRejected ||
		!entry.Resolution.ConditionFailed || !entry.Resolution.AllCopiesTerminal || entry.Resolution.PayloadDigest != "" {
		return false
	}
	dispatch, ok := entry.Dispatches[dispatchID]
	return ok && dispatchID != "" && dispatch.ReceiptDigest != "" && mutationDigestValidOperator(dispatch.ReceiptDigest) &&
		dispatch.ReceiptDigest == entry.Resolution.ReceiptDigest
}

func mutationReceiptMatchesTerminalRejection(entry fencing.MutationEntry, runtime fencing.RuntimeBinding, walIdentity, producerUID string,
	sequence uint64, dispatchID string, receipt mutationExecutionReceipt) bool {
	if entry.Preparation == nil || !entry.DispatchClosed || len(entry.Dispatches) != 1 || entry.Intent.Condition.Mode != fencing.MutationMatchGeneration ||
		entry.Intent.Condition.Generation != entry.Preparation.Generation ||
		receipt.Runtime != runtime || receipt.WALIdentity != walIdentity ||
		receipt.ProducerUID != producerUID || receipt.Sequence != sequence || receipt.Intent != entry.Intent ||
		receipt.Preparation != *entry.Preparation || receipt.DispatchID != dispatchID || receipt.Outcome != fencing.MutationRejected ||
		!receipt.ConditionFailed || !receipt.AllCopiesTerminal || receipt.Metageneration != 0 || receipt.PayloadDigest != "" ||
		receipt.ReceiptDigest == "" || receipt.ReceiptDigest != receipt.digest() ||
		receipt.Generation != entry.Preparation.Generation {
		return false
	}
	dispatch := entry.Dispatches[dispatchID]
	if receipt.DispatchID == "" {
		return false
	}
	if entry.Resolution == nil {
		return dispatch.ReceiptDigest == ""
	}
	return isTerminalConditionalRejection(entry, dispatchID) && dispatch.ReceiptDigest == receipt.ReceiptDigest &&
		entry.Resolution.ReceiptDigest == receipt.ReceiptDigest
}

func rejectedReceiptShapeValid(result mutationProviderResult, intent fencing.MutationIntent, preparation fencing.MutationPreparation) bool {
	return intent.Condition.Mode == fencing.MutationMatchGeneration && intent.Condition.Generation == preparation.Generation &&
		result.Outcome == fencing.MutationRejected && result.ConditionFailed &&
		result.AllCopiesTerminal && result.Generation > 0 && result.Generation == preparation.Generation && result.Metageneration == 0 && result.PayloadDigest == ""
}

func verifyRejectedMutationExcluded(ctx context.Context, provider mutationExecutionProvider, intent fencing.MutationIntent,
	preparation fencing.MutationPreparation, receipt mutationExecutionReceipt) error {
	if provider == nil || receipt.Outcome != fencing.MutationRejected || !receipt.ConditionFailed || !receipt.AllCopiesTerminal ||
		receipt.Intent != intent || receipt.Preparation != preparation || receipt.Generation != preparation.Generation ||
		receipt.Metageneration != 0 || receipt.PayloadDigest != "" || intent.Condition.Mode != fencing.MutationMatchGeneration ||
		intent.Condition.Generation != preparation.Generation {
		return errors.Join(ErrFencePending, fmt.Errorf("fencing: fixed-generation rejection does not match its immutable conditional receipt"))
	}
	confirmed, committed, err := provider.committed(ctx, intent.Key, intent, preparation)
	if err != nil || (committed && confirmed.Outcome != fencing.MutationCommitted && !(intent.Kind == "delete" && confirmed.Outcome == fencing.MutationExcluded)) ||
		(committed && confirmed.Outcome == fencing.MutationCommitted) {
		return errors.Join(ErrFencePending, err, fmt.Errorf("fencing: rejected receipt conflicts with a confirmed provider commit"))
	}
	current, exists, err := provider.observe(ctx, intent.Key)
	if err != nil || (exists && current == preparation.Generation) {
		return errors.Join(ErrFencePending, err, fmt.Errorf("fencing: fixed-generation rejection does not exclude a late original effect"))
	}
	return nil
}

func (e *FencingExecutor) settleEngineRepairZeroGeneration(ctx context.Context, repair fencing.EngineRepairRecord, runtime fencing.RuntimeBinding,
	producerUID string, sequence uint64, scope AttestationStoreScope) (fencing.MutationGenerationZeroGuard, error) {
	var zero fencing.MutationGenerationZeroGuard
	if e == nil || e.AuthorityState == nil || e.SourceMutationStorage == nil || ctx == nil || producerUID == "" || sequence == 0 || scope.Provider != "gcs" {
		return zero, fmt.Errorf("fencing: generation-zero engine repair settlement is not configured")
	}
	if err := repair.Validate(); err != nil || repair.Binding.FreezeSlot == 0 || (repair.Phase != fencing.RepairStorageClosing && repair.Phase != fencing.RepairGenerationClosing) {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero settlement lacks a frozen closing record"))
	}
	if _, err := e.readExactEngineRepairRecord(ctx, repair); err != nil {
		return zero, err
	}
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return zero, err
	}
	if err := repair.ValidateAgainstLedger(state.Ledger); err != nil {
		return zero, err
	}
	run, ok := state.Ledger.Runs[runtime.RunID]
	if !ok || run.Runtime == nil || *run.Runtime != runtime || run.Identity.NodeID != repair.Binding.OldMemberID || run.Identity.WALIdentity != repair.Binding.OldWALIdentity ||
		run.MutationProducerUID != producerUID || run.MutationProfile != fencing.MutationHeldExecutionProfile || run.MutationScopeHash != hashJSON(scope) {
		return zero, fmt.Errorf("fencing: generation-zero settlement runtime differs from the frozen old incarnation")
	}
	producer, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || producer.Runtime != runtime || producer.WALIdentity != run.Identity.WALIdentity || producer.Phase != fencing.MutationClosed || producer.Validate() != nil {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero settlement requires the exact closed producer"))
	}
	entry, ok := producer.Entries[sequence]
	if !ok || sequence <= producer.CompactedThrough || sequence > producer.ClosedThrough || entry.Preparation == nil || !fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) ||
		entry.Preparation.Generation != 0 || entry.Preparation.StoreScopeHash != hashJSON(scope) || !entry.DispatchClosed || len(entry.Dispatches) != 1 {
		return zero, fmt.Errorf("fencing: generation-zero settlement is not an unresolved closed held-create dispatch")
	}
	var dispatchID string
	for id, dispatch := range entry.Dispatches {
		if id == "" || dispatch.ReceiptDigest != "" {
			return zero, fmt.Errorf("fencing: generation-zero original dispatch is already terminal")
		}
		dispatchID = id
	}
	if entry.GenerationZeroGuard == nil && entry.Resolution != nil {
		return zero, fmt.Errorf("fencing: generation-zero settlement has no guard for its existing resolution")
	}
	if entry.GenerationZeroGuard != nil && (entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationCommitted ||
		entry.Resolution.AllCopiesTerminal || entry.Resolution.PayloadDigest != entry.Intent.PayloadDigest) {
		return zero, fmt.Errorf("fencing: generation-zero guard no longer has the exact unresolved logical commit")
	}
	owner := hashJSON([]any{runtime, run.Identity.WALIdentity, producerUID, sequence, entry.Intent.OperationID})
	if err := e.acquireMutationKey(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner, false); err != nil {
		return zero, err
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, run.MutationProfile)
	if err != nil {
		return zero, err
	}
	if entry.GenerationZeroGuard != nil {
		guard := *entry.GenerationZeroGuard
		if err := producer.ValidateGenerationZeroGuard(producerUID, sequence, repair, entry); err != nil {
			return zero, err
		}
		if generationZeroOperatorRightsTerminal(entry.GenerationZeroRights) {
			if err := e.sealEngineRepairZeroGenerationRights(ctx, runtime, producerUID, sequence, repair, entry); err != nil {
				return zero, err
			}
			if err := e.retireEngineRepairZeroGenerationRights(ctx, runtime, producerUID, sequence, repair); err != nil {
				return zero, err
			}
			if err := e.installMutationGenerationZeroBarrier(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner, guard); err != nil {
				return zero, err
			}
			updated, updatedUID, _, readErr := e.AuthorityState.ReadProducer(ctx, runtime)
			updatedEntry, exists := updated.Entries[sequence]
			if readErr != nil || updatedUID != producerUID || !exists {
				return zero, errors.Join(readErr, fmt.Errorf("fencing: generation-zero sealed-right readback changed"))
			}
			if err := e.verifyGenerationZeroGuard(ctx, runtime, run, producerUID, sequence, updatedEntry); err != nil {
				return zero, err
			}
			if err := e.releaseMutationKeyAfterVerifiedGuard(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner); err != nil {
				return zero, err
			}
			return guard, nil
		}
	}
	stages, err := e.sourceStagesFor(ctx, runtime)
	if err != nil || stages == nil || stages.evidence == nil {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero settlement requires canonical remote sealed payload"))
	}
	preparationHash, err := producer.PreparationHash(sequence)
	if err != nil {
		return zero, err
	}
	payload, err := stages.openMutationPayloadContext(ctx, producerUID, entry.Intent)
	if err != nil {
		return zero, err
	}
	attrs, err := e.SourceMutationStorage.Bucket(scope.Bucket).Attrs(ctx)
	if err != nil {
		_ = payload.Close()
		return zero, err
	}
	environment, err := frozenBucketEnvironmentFromAttrs(attrs, scope.Bucket, scope.Prefix)
	if err != nil {
		_ = payload.Close()
		return zero, err
	}
	right := mutationGenerationZeroRight{RepairRecordHash: repair.IdentityDigest(), RepairBinding: repair.Binding, Runtime: runtime,
		WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence, DispatchID: dispatchID,
		Intent: entry.Intent, Preparation: *entry.Preparation, PreparationHash: preparationHash, BucketEnvironment: environment}
	attempt := uint32(1)
	if entry.GenerationZeroRights != nil {
		if entry.GenerationZeroRights.Sealed || !generationZeroOperatorRightsTerminal(entry.GenerationZeroRights) {
			_ = payload.Close()
			return zero, ErrFencePending
		}
		attempt = uint32(len(entry.GenerationZeroRights.Rights) + 1)
	}
	right.Attempt = attempt
	right.RequestDigest = hashJSON([]any{"generation-zero-helper-request-v1", right, attempt})
	if err := e.recordMutationGenerationZeroRight(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner, right); err != nil {
		_ = payload.Close()
		return zero, err
	}
	created := false
	if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		var recordErr error
		created, recordErr = p.RecordGenerationZeroPhysicalRight(producerUID, sequence, repair, attempt, right.RequestDigest)
		if recordErr != nil {
			return recordErr
		}
		if !created {
			return ErrFencePending
		}
		return nil
	}); err != nil {
		_ = payload.Close()
		return zero, err
	}
	// The request is a replay of the exact pre-existing sealed dispatch. Re-read
	// the canonical journal immediately before the provider call so a changed or
	// terminalized permission cannot be treated as the saved right.
	latest, latestUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	latestEntry, latestExists := latest.Entries[sequence]
	if err != nil || latestUID != producerUID || latest.Runtime != runtime || latest.WALIdentity != run.Identity.WALIdentity ||
		latest.Phase != fencing.MutationClosed || latestEntry.Intent != entry.Intent || latestEntry.Preparation == nil || *latestEntry.Preparation != *entry.Preparation ||
		!latestEntry.DispatchClosed || latestEntry.Resolution != nil || len(latestEntry.Dispatches) != 1 || !latestExists ||
		latestEntry.Dispatches[dispatchID].ReceiptDigest != "" || latestEntry.GenerationZeroRights == nil ||
		int(attempt) > len(latestEntry.GenerationZeroRights.Rights) ||
		latestEntry.GenerationZeroRights.Rights[attempt-1].RequestDigest != right.RequestDigest ||
		latestEntry.GenerationZeroRights.Rights[attempt-1].TerminalReceiptDigest != "" || latestEntry.GenerationZeroRights.Sealed {
		_ = payload.Close()
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero right no longer matches the sealed journal dispatch"))
	}
	result, settleErr := provider.settleHeldRepairCreate(ctx, entry.Intent.Key, entry.Intent, *entry.Preparation, payload)
	closeErr := payload.Close()
	if result.AllCopiesTerminal {
		receiptDigest := hashJSON([]any{"generation-zero-helper-terminal-v1", right.RequestDigest, result})
		if _, terminalErr := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
			return p.RecordGenerationZeroPhysicalTerminal(sequence, attempt, right.RequestDigest, receiptDigest)
		}); terminalErr != nil {
			return zero, errors.Join(ErrFencePending, settleErr, closeErr, terminalErr)
		}
	}
	if err := errors.Join(settleErr, closeErr); err != nil {
		return zero, errors.Join(ErrFencePending, err)
	}
	if !result.AllCopiesTerminal || result.Outcome != fencing.MutationCommitted || result.Generation <= 0 || result.Metageneration <= 0 || result.PayloadDigest != entry.Intent.PayloadDigest {
		return zero, ErrFencePending
	}
	evidenceStore, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return zero, err
	}
	repairHash := repair.IdentityDigest()
	guardRef := "mutation-effect-guards/" + hashJSON([]any{repairHash, runtime, producerUID, sequence, entry.Intent.OperationID}) + ".json"
	guard := fencing.MutationGenerationZeroGuard{Version: fencing.MutationGenerationZeroGuardVersion, RepairBinding: repair.Binding, RepairRecordHash: repairHash,
		StorageScopeHash: entry.Intent.StoreScopeHash, Key: entry.Intent.Key, Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID,
		Sequence: sequence, Intent: entry.Intent, Preparation: *entry.Preparation, PreparationHash: preparationHash, DispatchID: dispatchID,
		GuardRootIdentity: evidenceStore.Identity(), GuardRef: guardRef,
		GuardUID:         hashJSON([]any{"rhiza-mutation-effect-guard-uid-v1", repairHash, runtime, producerUID, sequence, entry.Intent.OperationID}),
		ObjectGeneration: result.Generation, ObjectMetageneration: result.Metageneration, ObjectSize: entry.Intent.PayloadSize, PayloadDigest: entry.Intent.PayloadDigest}
	guard.GuardDigest = guard.Digest()
	guardBytes, err := json.Marshal(guard)
	if err != nil {
		return zero, err
	}
	version, err := evidenceStore.CreateImmutable(ctx, guardRef, guardBytes)
	if err != nil {
		return zero, err
	}
	readback, readVersion, err := evidenceStore.Read(ctx, guardRef)
	if err != nil {
		return zero, err
	}
	readBytes, readErr := io.ReadAll(io.LimitReader(readback, int64(len(guardBytes))+1))
	closeErr = readback.Close()
	if readErr != nil || closeErr != nil || version == nil || readVersion == nil || *version != *readVersion || !bytes.Equal(readBytes, guardBytes) {
		return zero, errors.Join(readErr, closeErr, fmt.Errorf("fencing: generation-zero effect guard readback differs"))
	}
	if entry.GenerationZeroGuard == nil {
		if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
			return p.RecordGenerationZeroGuard(producerUID, sequence, repair, guard)
		}); err != nil {
			return zero, err
		}
	} else if *entry.GenerationZeroGuard != guard {
		return zero, fmt.Errorf("fencing: helper confirmation differs from the immutable generation-zero guard")
	}
	updated, updatedUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	updatedEntry, exists := updated.Entries[sequence]
	if err != nil || updatedUID != producerUID || !exists || updated.ValidateGenerationZeroGuard(producerUID, sequence, repair, updatedEntry) != nil {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero journal guard readback differs"))
	}
	if err := provider.verifyHeldCreateGeneration(ctx, entry.Intent.Key, guard.ObjectGeneration, entry.Intent); err != nil {
		return zero, err
	}
	if err := e.sealEngineRepairZeroGenerationRights(ctx, runtime, producerUID, sequence, repair, updatedEntry); err != nil {
		return zero, err
	}
	if err := e.retireEngineRepairZeroGenerationRights(ctx, runtime, producerUID, sequence, repair); err != nil {
		return zero, err
	}
	updated, updatedUID, _, err = e.AuthorityState.ReadProducer(ctx, runtime)
	updatedEntry, exists = updated.Entries[sequence]
	if err != nil || updatedUID != producerUID || !exists || updated.ValidateGenerationZeroGuard(producerUID, sequence, repair, updatedEntry) != nil ||
		!generationZeroOperatorRightsTerminal(updatedEntry.GenerationZeroRights) || !updatedEntry.GenerationZeroRights.Sealed || !updatedEntry.GenerationZeroRights.HelperRightsRetired {
		return zero, errors.Join(err, fmt.Errorf("fencing: generation-zero terminal-right retirement readback differs"))
	}
	if err := e.installMutationGenerationZeroBarrier(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner, guard); err != nil {
		return zero, err
	}
	if err := e.releaseMutationKeyAfterVerifiedGuard(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner); err != nil {
		return zero, err
	}
	return guard, nil
}

func generationZeroOperatorRightsTerminal(rights *fencing.MutationGenerationZeroRights) bool {
	if rights == nil || len(rights.Rights) == 0 {
		return false
	}
	for _, right := range rights.Rights {
		if !mutationDigestValidOperator(right.TerminalReceiptDigest) {
			return false
		}
	}
	return true
}

func (e *FencingExecutor) sealEngineRepairZeroGenerationRights(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string,
	sequence uint64, repair fencing.EngineRepairRecord, entry fencing.MutationEntry) error {
	if !generationZeroOperatorRightsTerminal(entry.GenerationZeroRights) {
		return ErrFencePending
	}
	if entry.GenerationZeroRights.Sealed {
		return nil
	}
	if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		return p.SealGenerationZeroRights(producerUID, sequence, repair)
	}); err != nil {
		return err
	}
	stored, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	actual, exists := stored.Entries[sequence]
	if err != nil || actualUID != producerUID || !exists || actual.GenerationZeroRights == nil ||
		!actual.GenerationZeroRights.Sealed || !generationZeroOperatorRightsTerminal(actual.GenerationZeroRights) {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero rights seal readback differs"))
	}
	return nil
}

func (e *FencingExecutor) retireEngineRepairZeroGenerationRights(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string,
	sequence uint64, repair fencing.EngineRepairRecord) error {
	if _, err := e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		return p.RetireGenerationZeroRights(producerUID, sequence, repair)
	}); err != nil {
		return err
	}
	stored, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	actual, exists := stored.Entries[sequence]
	if err != nil || actualUID != producerUID || !exists || stored.ValidateGenerationZeroGuard(producerUID, sequence, repair, actual) != nil || actual.GenerationZeroRights == nil ||
		!actual.GenerationZeroRights.Sealed || !actual.GenerationZeroRights.HelperRightsRetired || !generationZeroOperatorRightsTerminal(actual.GenerationZeroRights) {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero rights retirement readback differs"))
	}
	return nil
}

// releaseMutationKeyAfterVerifiedGuard is used only after the immutable guard,
// journal entry, and exact object generation have been verified. A different
// owner can only appear after this owner released the key and a successor
// acquired it, so a concurrent retry must not disturb that successor.
func (e *FencingExecutor) releaseMutationKeyAfterVerifiedGuard(ctx context.Context, scopeHash, key, owner string) error {
	return e.releaseMutationKeyAfterVerifiedTerminal(ctx, scopeHash, key, owner)
}

// validateEngineRepairStorageInventory preserves the complete old-member
// cohort while allowing unrelated, already-active source survivors to append
// their own runs after the repair proof was committed.
func validateEngineRepairStorageInventory(repair fencing.EngineRepairRecord, ledger fencing.Ledger) error {
	if repair.Validate() != nil || ledger.SourceClusterID != repair.Binding.SourceClusterID || ledger.ClosedBy != "" || ledger.ClosedHash != "" ||
		len(ledger.Runs) == 0 {
		return fmt.Errorf("fencing: engine repair storage inventory is not the exact open source root")
	}
	// The fencing record validator owns the bounded inventory and the
	// same-survivor fresh-runtime allowance. Keep this layer specific to the
	// storage proof: validate pinned historical runtime bindings and the exact
	// old-cohort stop, without projecting away legitimate survivor restarts.
	if err := repair.ValidateAgainstLedger(ledger); err != nil {
		return err
	}
	for _, incarnation := range repair.Incarnations {
		run, ok := ledger.Runs[incarnation.Identity.RunID]
		if !ok {
			return fmt.Errorf("fencing: storage proof omits a pinned historical incarnation")
		}
		if run.Runtime != nil && (run.Runtime.Validate(run.Identity) != nil || run.Runtime.StatefulSetUID != repair.Binding.StatefulSetUID ||
			run.Runtime.SourceClusterID != repair.Binding.SourceClusterID) {
			return fmt.Errorf("fencing: storage proof historical runtime differs from its root binding")
		}
	}
	for _, incarnation := range repair.Incarnations {
		if incarnation.Identity.NodeID != repair.Binding.OldMemberID {
			continue
		}
		run := ledger.Runs[incarnation.Identity.RunID]
		stop, closed := ledger.ClosedNodes[repair.Binding.OldMemberID]
		wantStop := engineRepairOldMemberStop(repair)
		wantRunStop := wantStop
		wantRunStop.Identity = incarnation.Identity
		if !closed || stop != wantStop || run.Stop == nil || *run.Stop != wantRunStop {
			return fmt.Errorf("fencing: removed-member incarnation lacks its exact canonical closed stop")
		}
	}
	return nil
}

func engineRepairOldMemberStop(repair fencing.EngineRepairRecord) fencing.StopRequest {
	oldIncarnations := make([]fencing.RepairIncarnation, 0, len(repair.Incarnations))
	for _, incarnation := range repair.Incarnations {
		if incarnation.Identity.NodeID == repair.Binding.OldMemberID {
			oldIncarnations = append(oldIncarnations, incarnation)
		}
	}
	return fencing.StopRequest{
		OperationID: "engine-repair-close-" + hashJSON(repair.IdentityDigest()),
		RequestHash: hashJSON([]any{"engine-repair-old-cohort-close-v1", repair.IdentityDigest(), oldIncarnations}),
	}
}

func engineRepairNoProducerSettlement(repair fencing.EngineRepairRecord, ledger fencing.Ledger, incarnation fencing.RepairIncarnation, run fencing.Run) (fencing.StorageRunSettlement, error) {
	var settlement fencing.StorageRunSettlement
	if incarnation.ProducerUID != "" || run.MutationProducerUID != "" || run.Stop == nil {
		return settlement, fmt.Errorf("fencing: no-producer proof lacks its exact closed run")
	}
	wantStop := engineRepairOldMemberStop(repair)
	wantRunStop := wantStop
	wantRunStop.Identity = incarnation.Identity
	if incarnation.Identity.NodeID != repair.Binding.OldMemberID || run.Identity != incarnation.Identity || *run.Stop != wantRunStop ||
		ledger.ClosedNodes[repair.Binding.OldMemberID] != wantStop {
		return settlement, fmt.Errorf("fencing: no-producer proof is not bound to the canonical old-member stop")
	}
	stateHash := hashJSON([]any{"engine-repair-no-producer-state-v2", repair.IdentityDigest(), incarnation, run.Identity, run.Role,
		run.Runtime, run.MutationProducerUID, run.MutationScopeHash, run.MutationProfile, run.MutationStageIncarnation, wantStop})
	noProducerDigest := hashJSON([]any{"engine-repair-no-producer-settlement-v2", repair.IdentityDigest(), incarnation.Identity.RunID, stateHash})
	return fencing.StorageRunSettlement{
		RunID: incarnation.Identity.RunID, PodUID: incarnation.Identity.PodUID, WALIdentity: incarnation.Identity.WALIdentity,
		ProducerStateHash: stateHash, SettlementDigest: noProducerDigest, NoProducerDigest: noProducerDigest,
	}, nil
}

func readEngineRepairStorageRecord(ctx context.Context, store *EngineRepairStore, state fenceLedger, binding fencing.EngineRemovalBinding) (fencing.EngineRepairRecord, error) {
	var empty fencing.EngineRepairRecord
	if binding.Validate() != nil || state.StatefulSetUID != binding.StatefulSetUID || state.Ledger.SourceClusterID != binding.SourceClusterID {
		return empty, fmt.Errorf("fencing: engine repair storage proof requires the exact open canonical root")
	}
	repair, err := readEngineRepairStorageRecordByOperation(ctx, store, state, binding.SourceClusterID, binding.OperationID)
	if err != nil {
		return empty, err
	}
	if repair.Binding != binding {
		return empty, fmt.Errorf("fencing: engine repair storage proof binding differs from its canonical record")
	}
	return repair, nil
}

func readEngineRepairStorageRecordByOperation(ctx context.Context, store *EngineRepairStore, state fenceLedger, sourceClusterID, operationID string) (fencing.EngineRepairRecord, error) {
	var empty fencing.EngineRepairRecord
	if store == nil || operationID == "" || state.StatefulSetUID != store.uid || state.Ledger.SourceClusterID != sourceClusterID ||
		state.Ledger.ClosedBy != "" || state.Ledger.ClosedHash != "" {
		return empty, fmt.Errorf("fencing: engine repair storage proof requires the exact open canonical root")
	}
	if err := validateEngineRepairRootReferences(state, store.uid, sourceClusterID); err != nil {
		return empty, err
	}
	var reference fencing.EngineRepairReference
	found := false
	for _, candidate := range state.Ledger.EngineRepairs {
		if candidate.OperationID != operationID {
			continue
		}
		if found {
			return empty, fmt.Errorf("fencing: engine repair storage operation is ambiguous")
		}
		reference, found = candidate, true
	}
	if !found || reference.Validate() != nil || reference.Binding.SourceClusterID != sourceClusterID || reference.Binding.StatefulSetUID != store.uid {
		return empty, fmt.Errorf("fencing: engine repair storage proof has no exact canonical reservation")
	}
	repair, err := store.readReferencedRecord(ctx, sourceClusterID, reference)
	if err != nil {
		return empty, err
	}
	if engineRepairRootReference(repair) != reference {
		return empty, fmt.Errorf("fencing: engine repair storage proof reference changed")
	}
	return repair, nil
}

// settleEngineRepairStorageEffects settles every old-member producer in the
// frozen inventory, then binds the complete proof through the engine repair
// store's root-CAS reference. It proves storage closure only, not process
// termination.
func (e *FencingExecutor) settleEngineRepairStorageEffects(ctx context.Context, repair fencing.EngineRepairRecord) (fencing.StorageEffectsExcluded, error) {
	var proof fencing.StorageEffectsExcluded
	if e == nil || e.AuthorityState == nil || e.SourceMutationStorage == nil || e.Kube == nil || ctx == nil || repair.Validate() != nil ||
		repair.Binding.FreezeSlot == 0 || (repair.Phase != fencing.RepairStorageClosing && repair.Phase != fencing.RepairGenerationClosing) {
		return proof, fmt.Errorf("fencing: engine repair storage closure is not configured")
	}
	store, err := NewEngineRepairStore(e.AuthorityState, repair.Binding.StatefulSetUID)
	if err != nil {
		return proof, err
	}
	state, err := e.read(ctx, repair.Binding.StatefulSetUID, repair.Binding.SourceClusterID)
	if err != nil {
		return proof, err
	}
	canonical, err := readEngineRepairStorageRecord(ctx, store, state, repair.Binding)
	if err != nil {
		return proof, err
	}
	if canonical.IdentityDigest() != repair.IdentityDigest() {
		return proof, fmt.Errorf("fencing: storage closure request differs from its canonical reservation")
	}
	repair = canonical
	if err := validateEngineRepairStorageInventory(repair, state.Ledger); err != nil {
		return proof, err
	}
	if repair.StorageProofDigest != "" {
		proof, err = e.readCanonicalEngineRepairStorageProof(ctx, repair, nil)
		if err != nil || proof.ProofDigest != repair.StorageProofDigest || proof.Validate(repair) != nil {
			return fencing.StorageEffectsExcluded{}, errors.Join(err, fmt.Errorf("fencing: canonical storage proof readback differs"))
		}
		if err := e.verifyEngineRepairStorageSettlements(ctx, repair, state.Ledger, proof); err != nil {
			return fencing.StorageEffectsExcluded{}, err
		}
		return proof, nil
	}

	proof = fencing.StorageEffectsExcluded{Version: fencing.StorageEffectsProofVersion, Binding: repair.Binding, RepairRecordHash: repair.IdentityDigest()}
	for _, incarnation := range repair.Incarnations {
		if incarnation.Identity.NodeID != repair.Binding.OldMemberID {
			continue
		}
		run, exists := state.Ledger.Runs[incarnation.Identity.RunID]
		if !exists || run.Identity != incarnation.Identity || run.MutationProducerUID != incarnation.ProducerUID {
			return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: storage closure inventory changed during settlement")
		}
		if incarnation.ProducerUID == "" {
			if run.Runtime != nil {
				_, producerUID, _, readErr := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
				if readErr == nil || producerUID != "" {
					return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: unbound old run has a canonical mutation producer")
				} else if !errors.Is(readErr, errAuthorityStateNotFound) {
					return fencing.StorageEffectsExcluded{}, readErr
				}
			}
			settlement, err := engineRepairNoProducerSettlement(repair, state.Ledger, incarnation, run)
			if err != nil {
				return fencing.StorageEffectsExcluded{}, err
			}
			proof.RunSettlements = append(proof.RunSettlements, settlement)
			continue
		}
		if run.Runtime == nil || run.Runtime.RunID != incarnation.Identity.RunID || run.Runtime.PodUID != incarnation.Identity.PodUID ||
			run.MutationScopeHash != incarnation.MutationScopeHash || run.MutationProfile != incarnation.MutationProfile {
			return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: old mutation producer runtime or storage profile is not pinned")
		}
		if err := e.settleEngineRepairProducer(ctx, repair, run, *run.Runtime); err != nil {
			return fencing.StorageEffectsExcluded{}, err
		}
		producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
		if err != nil || producerUID != incarnation.ProducerUID || producer.Runtime != *run.Runtime || producer.WALIdentity != incarnation.Identity.WALIdentity ||
			producer.Phase != fencing.MutationClosed {
			return fencing.StorageEffectsExcluded{}, errors.Join(err, fmt.Errorf("fencing: old producer lacks the exact closed storage journal"))
		}
		if err := e.verifyEngineRepairProducerStorageClosure(ctx, repair, run, incarnation.ProducerUID, producer); err != nil {
			return fencing.StorageEffectsExcluded{}, err
		}
		settlement, err := e.engineRepairProducerRunSettlement(ctx, repair, run, producer, producerUID)
		if err != nil {
			return fencing.StorageEffectsExcluded{}, err
		}
		proof.RunSettlements = append(proof.RunSettlements, settlement)
	}
	proof.ProofDigest = proof.Digest()
	if err := proof.Validate(repair); err != nil {
		return fencing.StorageEffectsExcluded{}, err
	}
	evidence, err := NewAuthorityEvidenceStore(e.AuthorityState.bucket, e.AuthorityState.prefix, repair.Binding.StatefulSetUID, repair.Binding.SourceClusterID)
	if err != nil {
		return fencing.StorageEffectsExcluded{}, err
	}
	proofKey := engineRepairStorageEffectsKey(repair.IdentityDigest(), proof.ProofDigest)
	proofBytes, err := json.Marshal(proof)
	if err != nil {
		return fencing.StorageEffectsExcluded{}, err
	}
	if _, err := evidence.CreateImmutable(ctx, proofKey, proofBytes); err != nil {
		return fencing.StorageEffectsExcluded{}, err
	}
	readback, _, err := evidence.Read(ctx, proofKey)
	if err != nil {
		return fencing.StorageEffectsExcluded{}, err
	}
	readBytes, readErr := io.ReadAll(io.LimitReader(readback, int64(len(proofBytes))+1))
	closeErr := readback.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(readBytes, proofBytes) {
		return fencing.StorageEffectsExcluded{}, errors.Join(readErr, closeErr, fmt.Errorf("fencing: storage-effects proof readback differs"))
	}
	if repair.StorageProofDigest == "" {
		updated := repair
		updated.StorageProofDigest = proof.ProofDigest
		if _, err := store.CompareAndSwapEngineRepair(ctx, repair.Binding.SourceClusterID, repair.Digest(), updated); err != nil {
			return fencing.StorageEffectsExcluded{}, err
		}
	} else if repair.StorageProofDigest != proof.ProofDigest {
		return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: canonical repair record references another storage proof")
	}
	return proof, nil
}

func (e *FencingExecutor) settleEngineRepairProducer(ctx context.Context, repair fencing.EngineRepairRecord, run fencing.Run, runtime fencing.RuntimeBinding) error {
	producer, uid, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || uid != run.MutationProducerUID || producer.Runtime != runtime || producer.WALIdentity != run.Identity.WALIdentity || producer.Phase != fencing.MutationClosed || producer.Validate() != nil {
		return errors.Join(err, fmt.Errorf("fencing: engine repair requires the exact closed producer"))
	}
	if producer.ClosedThrough > producer.LastAdmitted {
		return ErrFencePending
	}
	for sequence := producer.CompactedThrough; sequence < producer.ClosedThrough; {
		sequence++
		entry, exists := producer.Entries[sequence]
		if !exists {
			return ErrFencePending
		}
		if entry.Resolution == nil {
			scope, scopeErr := e.sourceMutationStorageScope(ctx, runtime)
			if scopeErr != nil || hashJSON(scope) != run.MutationScopeHash {
				return errors.Join(scopeErr, fmt.Errorf("fencing: old producer storage scope changed"))
			}
			if len(entry.Dispatches) == 0 {
				if err := e.cancelClosedMutationWithoutDispatch(ctx, runtime, uid, sequence, entry); err != nil {
					return err
				}
				producer, uid, _, err = e.AuthorityState.ReadProducer(ctx, runtime)
				if err != nil || uid != run.MutationProducerUID {
					return errors.Join(err, fmt.Errorf("fencing: cancelled producer readback changed"))
				}
				entry = producer.Entries[sequence]
			} else if entry.Preparation != nil && fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) && entry.Preparation.Generation == 0 {
				if _, err := e.settleEngineRepairZeroGeneration(ctx, repair, runtime, uid, sequence, scope); err != nil {
					return err
				}
			} else if entry.Preparation != nil && entry.Preparation.Generation > 0 {
				if _, err := e.settleEngineRepairMutation(ctx, repair, runtime, uid, sequence, scope, nil); err != nil {
					return err
				}
			} else {
				return ErrFencePending
			}
			producer, uid, _, err = e.AuthorityState.ReadProducer(ctx, runtime)
			if err != nil || uid != run.MutationProducerUID {
				return errors.Join(err, fmt.Errorf("fencing: producer changed during storage settlement"))
			}
			entry = producer.Entries[sequence]
		}
		if entry.Resolution == nil || (entry.Resolution.Outcome != fencing.MutationCommitted && entry.Resolution.Outcome != fencing.MutationExcluded && entry.Resolution.Outcome != fencing.MutationRejected) {
			return ErrFencePending
		}
		if err := e.VerifySourceMutationEvidence(ctx, runtime, sequence, entry); err != nil {
			return err
		}
		owner := hashJSON([]any{runtime, run.Identity.WALIdentity, uid, sequence, entry.Intent.OperationID})
		if err := e.releaseMutationKeyAfterVerifiedTerminal(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner); err != nil {
			return err
		}
		if entry.GenerationZeroGuard != nil {
			owner := hashJSON([]any{runtime, run.Identity.WALIdentity, uid, sequence, entry.Intent.OperationID})
			if err := e.releaseMutationKeyAfterVerifiedGuard(ctx, entry.Intent.StoreScopeHash, entry.Intent.Key, owner); err != nil {
				return err
			}
			if entry.CopyCompletion != nil {
				if _, err := e.retireGenerationZeroBarrier(ctx, runtime, uid, sequence, entry); err != nil {
					return err
				}
			}
		}
	}
	if producer.CompactedThrough != 0 {
		verificationID := hashJSON([]any{"engine-repair-storage-archive-history-v1", repair.IdentityDigest(), runtime, producer.ArchiveHead})
		done, err := e.verifyMutationArchiveHistory(ctx, runtime, verificationID, 32)
		if err != nil || !done {
			return errors.Join(err, ErrFencePending)
		}
		producer, uid, _, err = e.AuthorityState.ReadProducer(ctx, runtime)
		if err != nil || uid != run.MutationProducerUID {
			return errors.Join(err, fmt.Errorf("fencing: verified archive producer readback changed"))
		}
	}
	if producer.Phase != fencing.MutationClosed {
		return ErrFencePending
	}
	if producer.BarrierComplete() {
		if err := e.verifyMutationArchiveReceipts(ctx, fencing.MutationCompactionPlan{Runtime: runtime, Entries: producer.Entries}); err != nil {
			return err
		}
	} else if err := e.verifyEngineRepairProducerStorageClosure(ctx, repair, run, uid, producer); err != nil {
		return err
	}
	return e.validateMutationArchiveHead(ctx, runtime, uid, producer)
}

func (e *FencingExecutor) cancelClosedMutationWithoutDispatch(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64,
	expected fencing.MutationEntry, reason ...fencing.MutationResolutionReason) error {
	if len(reason) > 1 || (len(reason) == 1 && reason[0] != fencing.MutationResolutionReasonGenerationZeroPinned) {
		return ErrFencePending
	}
	if expected.Resolution != nil || len(expected.Dispatches) != 0 || !expected.DispatchClosed {
		return ErrFencePending
	}
	producer, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || producer.Phase != fencing.MutationClosed || producer.Runtime != runtime {
		return errors.Join(err, fmt.Errorf("fencing: no-dispatch cancellation requires the exact closed producer"))
	}
	receiptDigest := engineRepairNoDispatchCancellationDigest(runtime, producer.WALIdentity, producerUID, sequence, expected)
	_, err = e.changeMutationProducer(ctx, runtime, func(current *fencing.MutationProducer) error {
		entry, ok := current.Entries[sequence]
		if current.Phase != fencing.MutationClosed || current.WALIdentity != producer.WALIdentity || !ok || entry.Intent != expected.Intent ||
			!sameMutationPreparation(entry.Preparation, expected.Preparation) || entry.Resolution != nil || len(entry.Dispatches) != 0 || !entry.DispatchClosed {
			return ErrFencePending
		}
		if len(reason) == 1 {
			preparationHash := ""
			if entry.Preparation != nil {
				var err error
				preparationHash, err = current.PreparationHash(sequence)
				if err != nil {
					return err
				}
			}
			return current.CancelPinnedGenerationDelete(sequence, preparationHash, receiptDigest)
		}
		if entry.Preparation == nil {
			return current.CancelUnprepared(sequence, receiptDigest)
		}
		preparationHash, err := current.PreparationHash(sequence)
		if err != nil {
			return err
		}
		return current.CancelPreparedNoDispatch(sequence, preparationHash, receiptDigest)
	})
	if err != nil {
		return err
	}
	stored, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || stored.Phase != fencing.MutationClosed {
		return errors.Join(err, fmt.Errorf("fencing: no-dispatch cancellation readback failed"))
	}
	entry, ok := stored.Entries[sequence]
	if !ok || !isEngineRepairNoDispatchCancellation(runtime, stored.WALIdentity, producerUID, sequence, entry) ||
		(len(reason) == 1 && entry.Resolution.Reason != reason[0]) ||
		(len(reason) == 0 && entry.Resolution.Reason != "") {
		return fmt.Errorf("fencing: no-dispatch cancellation readback differs")
	}
	return nil
}

func sameMutationPreparation(a, b *fencing.MutationPreparation) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func engineRepairNoDispatchCancellationDigest(runtime fencing.RuntimeBinding, walIdentity, producerUID string, sequence uint64, entry fencing.MutationEntry) string {
	preparationHash := ""
	if entry.Preparation != nil {
		preparationHash = hashJSON(struct {
			Runtime     fencing.RuntimeBinding
			WALIdentity string
			Sequence    uint64
			Intent      fencing.MutationIntent
			Prepared    fencing.MutationPreparation
		}{runtime, walIdentity, sequence, entry.Intent, *entry.Preparation})
	}
	return hashJSON(struct {
		Version         string
		Runtime         fencing.RuntimeBinding
		WALIdentity     string
		ProducerUID     string
		Sequence        uint64
		Intent          fencing.MutationIntent
		Preparation     *fencing.MutationPreparation
		PreparationHash string
	}{"engine-repair-no-dispatch-cancellation-v1", runtime, walIdentity, producerUID, sequence, entry.Intent, entry.Preparation, preparationHash})
}

func isEngineRepairNoDispatchCancellation(runtime fencing.RuntimeBinding, walIdentity, producerUID string, sequence uint64, entry fencing.MutationEntry) bool {
	resolution := entry.Resolution
	if resolution == nil || producerUID == "" || walIdentity == "" || sequence == 0 || !entry.DispatchClosed || len(entry.Dispatches) != 0 ||
		resolution.Outcome != fencing.MutationRejected || resolution.ConditionFailed || resolution.PayloadDigest != "" || !resolution.AllCopiesTerminal {
		return false
	}
	preparationHash := ""
	if entry.Preparation != nil {
		preparationHash = hashJSON(struct {
			Runtime     fencing.RuntimeBinding
			WALIdentity string
			Sequence    uint64
			Intent      fencing.MutationIntent
			Prepared    fencing.MutationPreparation
		}{runtime, walIdentity, sequence, entry.Intent, *entry.Preparation})
	}
	return resolution.PreparationHash == preparationHash && resolution.ReceiptDigest == engineRepairNoDispatchCancellationDigest(runtime, walIdentity, producerUID, sequence, entry)
}

func engineRepairStorageEffectsKey(repairHash, proofDigest string) string {
	return "engine-repair-storage-effects/" + hashJSON([]any{repairHash, proofDigest}) + ".json"
}

// engineRepairStorageClosureState projects only immutable producer origin and
// the complete closed sequence prefix. Archive cursors, compaction watermarks,
// and archive refs are storage bookkeeping; the caller reconstructs and
// validates the exact prefix before hashing it.
type engineRepairStorageClosureState struct {
	Runtime        fencing.RuntimeBinding
	WALIdentity    string
	ProducerUID    string
	Phase          fencing.MutationPhase
	LastAdmitted   uint64
	ClosedThrough  uint64
	CloseOperation string
	CloseRequest   string
	PrefixDigest   string
}

func (e *FencingExecutor) verifyGenerationZeroGuard(ctx context.Context, runtime fencing.RuntimeBinding, run fencing.Run, producerUID string, sequence uint64, entry fencing.MutationEntry) error {
	if e == nil || e.AuthorityState == nil || e.SourceMutationStorage == nil || run.Runtime == nil || *run.Runtime != runtime ||
		run.MutationProducerUID != producerUID || run.MutationProfile != fencing.MutationHeldExecutionProfile || entry.GenerationZeroGuard == nil {
		return ErrFencePending
	}
	producer, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || producer.Runtime != runtime || producer.WALIdentity != run.Identity.WALIdentity {
		return errors.Join(ErrFencePending, err)
	}
	guard := *entry.GenerationZeroGuard
	if sequence > producer.CompactedThrough {
		if current, ok := producer.Entries[sequence]; !ok || hashJSON(current) != hashJSON(entry) {
			return fmt.Errorf("fencing: generation-zero live journal tuple differs")
		}
	}
	store, err := NewEngineRepairStore(e.AuthorityState, guard.RepairBinding.StatefulSetUID)
	if err != nil {
		return err
	}
	repair, err := store.ReadEngineRepair(ctx, guard.RepairBinding.SourceClusterID, guard.RepairBinding.OperationID)
	if err != nil || repair.IdentityDigest() != guard.RepairRecordHash || repair.Binding != guard.RepairBinding {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero guard repair record differs"))
	}
	if err := producer.ValidateGenerationZeroGuard(producerUID, sequence, repair, entry); err != nil {
		return err
	}
	if entry.CopyCompletion != nil {
		if err := e.verifyGenerationZeroOriginalDispatchReceipt(ctx, runtime, run.Identity.WALIdentity, producerUID, sequence, entry); err != nil {
			return errors.Join(ErrFencePending, err)
		}
	}
	evidenceStore, err := e.authorityEvidenceFor(runtime)
	if err != nil || evidenceStore.Identity() != guard.GuardRootIdentity {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero guard belongs to another authority root"))
	}
	r, version, err := evidenceStore.Read(ctx, guard.GuardRef)
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxAuthorityStateBytes+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) > maxAuthorityStateBytes || version == nil {
		return errors.Join(readErr, closeErr, fmt.Errorf("fencing: generation-zero guard evidence is unreadable"))
	}
	var saved fencing.MutationGenerationZeroGuard
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&saved); err != nil || decoder.Decode(new(any)) != io.EOF || saved != guard || saved.GuardDigest != saved.Digest() {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero guard evidence differs from canonical journal"))
	}
	scope, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil || hashJSON(scope) != guard.StorageScopeHash {
		return errors.Join(err, fmt.Errorf("fencing: generation-zero guard storage scope differs"))
	}
	provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, run.MutationProfile)
	if err != nil {
		return err
	}
	keyState, err := e.readMutationKeyState(ctx, guard.StorageScopeHash, guard.Key)
	if err != nil {
		return err
	}
	if keyState.Barrier == nil || !keyState.Barrier.matchesGuard(guard) {
		if _, proofErr := e.readGenerationZeroRetirementProof(ctx, guard, entry); proofErr == nil {
			return nil
		} else {
			return errors.Join(proofErr, fmt.Errorf("fencing: generation-zero live-name barrier is absent or mismatched"))
		}
	}
	if err := provider.verifyMutationGenerationZeroBarrier(ctx, *keyState.Barrier); err != nil {
		return err
	}
	if keyState.Barrier.CurrentGeneration == guard.ObjectGeneration {
		return provider.verifyGenerationZeroOrigin(ctx, *keyState.Barrier)
	}
	return nil
}

func engineRepairMutationSettlementKey(repairHash string, runtime fencing.RuntimeBinding, producerUID string, sequence uint64) string {
	return "engine-repair-settlements/" + hashJSON([]any{"v1", repairHash, runtime, producerUID, sequence}) + ".json"
}

func (e *FencingExecutor) readExactEngineRepairRecord(ctx context.Context, expected fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error) {
	if e == nil || e.AuthorityState == nil || ctx == nil || expected.Validate() != nil {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: canonical engine repair record is unavailable")
	}
	store, err := NewEngineRepairStore(e.AuthorityState, expected.Binding.StatefulSetUID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	actual, err := store.ReadEngineRepair(ctx, expected.Binding.SourceClusterID, expected.Binding.OperationID)
	if err != nil || actual.Binding != expected.Binding || actual.IdentityDigest() != expected.IdentityDigest() || actual.Digest() != expected.Digest() {
		return fencing.EngineRepairRecord{}, errors.Join(err, fmt.Errorf("fencing: canonical engine repair record differs from requested settlement"))
	}
	return actual, nil
}

func (e *FencingExecutor) persistEngineRepairMutationSettlement(ctx context.Context, store *AuthorityEvidenceStore, key string, settlement engineRepairMutationSettlement) error {
	if store == nil || settlement.Version != 1 || settlement.RepairRecordHash == "" || settlement.ReceiptDigest == "" {
		return fmt.Errorf("fencing: incomplete engine repair mutation settlement")
	}
	body, err := json.Marshal(settlement)
	if err != nil {
		return err
	}
	if _, err := store.CreateImmutable(ctx, key, body); err != nil {
		return err
	}
	r, _, err := store.Read(ctx, key)
	if err != nil {
		return err
	}
	actual, readErr := io.ReadAll(io.LimitReader(r, int64(len(body))+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(actual, body) {
		return errors.Join(readErr, closeErr, fmt.Errorf("fencing: immutable engine repair settlement readback differs"))
	}
	return nil
}

func (e *FencingExecutor) readEngineRepairRunSettlement(ctx context.Context, store *AuthorityEvidenceStore, producer fencing.MutationProducer,
	producerUID string, sequence uint64, key, repairHash string) (fencing.StorageRunSettlement, error) {
	r, _, err := store.Read(ctx, key)
	if err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) > 1<<20 {
		return fencing.StorageRunSettlement{}, errors.Join(readErr, closeErr, fmt.Errorf("fencing: saved repair settlement exceeds its bound"))
	}
	var saved engineRepairMutationSettlement
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&saved); err != nil || decoder.Decode(new(any)) != io.EOF {
		return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: saved repair settlement is malformed"))
	}
	canonical, err := json.Marshal(saved)
	if err != nil || !bytes.Equal(canonical, data) {
		return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: saved repair settlement is not canonical"))
	}
	entry, exists := producer.Entries[sequence]
	dispatch, dispatched := entry.Dispatches[saved.DispatchID]
	dispatchTerminalMatches := dispatched && (saved.Outcome != fencing.MutationRejected ||
		(saved.AllCopiesTerminal && dispatch.ReceiptDigest == saved.ReceiptDigest))
	if !exists || !dispatched || entry.Resolution == nil || saved.Version != 1 || saved.RepairRecordHash != repairHash || saved.Runtime != producer.Runtime ||
		saved.WALIdentity != producer.WALIdentity || saved.ProducerUID != producerUID || saved.Sequence != sequence || saved.Intent != entry.Intent ||
		entry.Preparation == nil || saved.Preparation != *entry.Preparation || saved.ReceiptDigest != entry.Resolution.ReceiptDigest ||
		saved.Outcome != entry.Resolution.Outcome || saved.Outcome != fencing.MutationCommitted && saved.Outcome != fencing.MutationExcluded && saved.Outcome != fencing.MutationRejected || !saved.FutureEffectsExcluded ||
		saved.ActualCommitProven != (saved.Outcome == fencing.MutationCommitted) || !mutationDigestValidOperator(saved.ReceiptDigest) ||
		!entry.DispatchClosed || len(entry.Dispatches) != 1 || !dispatchTerminalMatches {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: saved repair settlement differs from its producer entry")
	}
	if saved.Outcome == fencing.MutationRejected && (saved.ConditionFailed != true || !saved.AllCopiesTerminal || saved.PayloadDigest != "" ||
		saved.ConditionFailed != entry.Resolution.ConditionFailed || saved.AllCopiesTerminal != entry.Resolution.AllCopiesTerminal || saved.Metageneration != 0 ||
		saved.Generation != saved.Preparation.Generation || entry.Intent.Condition.Mode != fencing.MutationMatchGeneration ||
		entry.Intent.Condition.Generation != saved.Preparation.Generation) {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: saved rejected settlement lacks its exact terminal conditional result")
	}
	if saved.Outcome != fencing.MutationRejected && (saved.ConditionFailed || saved.AllCopiesTerminal) {
		return fencing.StorageRunSettlement{}, fmt.Errorf("fencing: non-rejection settlement carries rejection-only evidence")
	}
	if err := e.VerifySourceMutationEvidence(ctx, producer.Runtime, sequence, entry); err != nil {
		return fencing.StorageRunSettlement{}, err
	}
	if saved.Outcome == fencing.MutationExcluded || saved.Outcome == fencing.MutationRejected {
		scope, err := e.sourceMutationStorageScope(ctx, producer.Runtime)
		if err != nil {
			return fencing.StorageRunSettlement{}, err
		}
		provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, saved.Preparation.Profile)
		if err != nil {
			return fencing.StorageRunSettlement{}, err
		}
		if saved.Outcome == fencing.MutationRejected {
			receipts, err := e.receiptsForRuntime(producer.Runtime)
			if err != nil {
				return fencing.StorageRunSettlement{}, err
			}
			// The canonical producer tuple and receipt are independently checked by
			// VerifySourceMutationEvidence below; current storage must also still
			// exclude the exact prepared generation.
			receipt, err := receipts.get("receipt-" + hashJSON([]any{producer.Runtime, producer.WALIdentity, producerUID, sequence, saved.Intent.OperationID}))
			if err != nil || receipt.ReceiptDigest != saved.ReceiptDigest || !receipt.ConditionFailed || !receipt.AllCopiesTerminal ||
				receipt.Generation != saved.Preparation.Generation || receipt.Outcome != fencing.MutationRejected {
				return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: saved rejection settlement differs from its provider receipt"))
			}
			if err := verifyRejectedMutationExcluded(ctx, provider, saved.Intent, saved.Preparation, receipt); err != nil {
				return fencing.StorageRunSettlement{}, err
			}
		} else {
			generation, exists, err := provider.observe(ctx, saved.Intent.Key)
			if err != nil || (exists && generation == saved.Preparation.Generation) {
				return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: fixed-generation effect is not excluded by current storage state"))
			}
		}
	} else {
		scope, err := e.sourceMutationStorageScope(ctx, producer.Runtime)
		if err != nil {
			return fencing.StorageRunSettlement{}, err
		}
		provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, saved.Preparation.Profile)
		if err != nil {
			return fencing.StorageRunSettlement{}, err
		}
		result := mutationProviderResult{Outcome: saved.Outcome, Generation: saved.Generation, Metageneration: saved.Metageneration, PayloadDigest: saved.PayloadDigest}
		if saved.Intent.Kind == "upload" {
			if err := provider.verifyRepairCommit(ctx, saved.Intent.Key, saved.Intent, saved.Preparation, result); err != nil {
				return fencing.StorageRunSettlement{}, err
			}
		} else {
			confirmed, committed, err := provider.committed(ctx, saved.Intent.Key, saved.Intent, saved.Preparation)
			if err != nil || !committed || confirmed.Outcome != fencing.MutationExcluded {
				return fencing.StorageRunSettlement{}, errors.Join(err, fmt.Errorf("fencing: settled delete generation is not excluded"))
			}
		}
	}
	if !producer.BarrierComplete() {
		return fencing.StorageRunSettlement{}, ErrFencePending
	}
	settlementDigest := engineRepairRunSettlementDigest(repairHash, producer, saved)
	return fencing.StorageRunSettlement{RunID: producer.Runtime.RunID, PodUID: producer.Runtime.PodUID, WALIdentity: producer.WALIdentity, ProducerUID: producerUID,
		ClosedThrough: producer.ClosedThrough, SettledThrough: producer.ClosedThrough, ProducerStateHash: hashJSON(producer), SettlementDigest: settlementDigest}, nil
}

func engineRepairRunSettlementDigest(repairHash string, producer fencing.MutationProducer, settlement engineRepairMutationSettlement) string {
	return hashJSON([]any{repairHash, producer.Runtime, producer.WALIdentity, settlement.ProducerUID, producer.ClosedThrough, producer.ArchiveHead, producer, settlement})
}

// canonicalBootstrapGenesisDispatch authorizes the initial-HEAD fault selector
// only for the exact tuple linked by both immutable CONTROL references. The
// transport receipt is not used to infer whether storage committed the upload.
func (e *FencingExecutor) canonicalBootstrapGenesisDispatch(ctx context.Context, runtime fencing.RuntimeBinding, state fenceLedger, producerUID string, sequence uint64, intent fencing.MutationIntent, scope AttestationStoreScope) bool {
	if e == nil || e.AuthorityState == nil || state.StatefulSetUID != runtime.StatefulSetUID || state.Ledger.SourceClusterID != runtime.SourceClusterID ||
		state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || state.Ledger.ArchiveScopeHash == "" || state.Ledger.ArchiveGenesisRef == nil {
		return false
	}
	ref := state.Ledger.ArchiveGenesisRef
	if ref.ConfigID != recovery.ArchiveConfigID || ref.ScopeHash != state.Ledger.ArchiveScopeHash || ref.Mutation == nil ||
		ref.Mutation.ProducerUID != producerUID || ref.Mutation.Sequence != sequence || ref.Mutation.Intent != intent || !isCanonicalEmptyArchiveHeadIntent(intent) ||
		intent.StoreScopeHash != hashJSON(scope) {
		return false
	}
	archiveScope := fencing.ArchiveScope{Provider: scope.Provider, Bucket: scope.Bucket, Prefix: scope.Prefix, ClusterID: runtime.SourceClusterID}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, archiveScope)
	return err == nil && claim.ScopeHash == ref.ScopeHash && claim.ConfigID == recovery.ArchiveConfigID &&
		claim.OwnerStatefulSetUID == runtime.StatefulSetUID && claim.OwnerSourceClusterID == runtime.SourceClusterID &&
		claim.GenesisRef != nil && sameArchiveGenesisRef(claim.GenesisRef, ref)
}

func (e *FencingExecutor) replayGuardedLogicalResolution(ctx context.Context, runtime fencing.RuntimeBinding, run fencing.Run, producerUID string, sequence uint64, entry fencing.MutationEntry, stages *SourceStageStore, provider mutationExecutionProvider) error {
	guardProvider, ok := provider.(mutationGuardedReplayProvider)
	if !ok || e == nil || e.AuthorityState == nil || run.MutationProfile != fencing.MutationHeldExecutionProfile ||
		entry.Preparation == nil || !fencing.IsHeldCreateEligible(entry.Intent, *entry.Preparation) || !entry.DispatchClosed || len(entry.Dispatches) != 1 {
		return ErrFencePending
	}
	producer, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || producer.Runtime != runtime || producer.WALIdentity != run.Identity.WALIdentity || producer.Phase != fencing.MutationClosed || producer.Validate() != nil {
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
	evidenceStore, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return err
	}
	request := mutationGuardedReplayRequest{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID,
		Sequence: sequence, Intent: entry.Intent, Preparation: *entry.Preparation, PreparationHash: preparationHash,
		DispatchID: dispatchID, GuardRootIdentity: evidenceStore.Identity()}
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
	_, err = e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		if p.WALIdentity != run.Identity.WALIdentity {
			return fmt.Errorf("fencing: guarded mutation producer WAL differs")
		}
		return p.ResolveGuardedLogical(producerUID, sequence, evidence)
	})
	return err
}

func (e *FencingExecutor) verifyGuardedLogicalResolution(ctx context.Context, runtime fencing.RuntimeBinding, run fencing.Run, producerUID string, sequence uint64, entry fencing.MutationEntry, provider mutationExecutionProvider) error {
	guardProvider, ok := provider.(mutationGuardedReplayProvider)
	if !ok || entry.GuardedLogicalResolution == nil || entry.Preparation == nil {
		return ErrFencePending
	}
	producer := fencing.MutationProducer{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, Phase: fencing.MutationClosed, ClosedThrough: sequence}
	if err := producer.ValidateGuardedLogicalResolution(producerUID, sequence, entry); err != nil {
		return err
	}
	evidenceStore, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return err
	}
	preparationHash := entry.GuardedLogicalResolution.PreparationHash
	request := mutationGuardedReplayRequest{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID,
		Sequence: sequence, Intent: entry.Intent, Preparation: *entry.Preparation, PreparationHash: preparationHash,
		DispatchID: entry.GuardedLogicalResolution.DispatchID, GuardRootIdentity: evidenceStore.Identity()}
	if entry.GuardedLogicalResolution.GuardRootIdentity != evidenceStore.Identity() {
		return ErrFencePending
	}
	if err := guardProvider.verifyGuardedCreate(ctx, evidenceStore, request, *entry.GuardedLogicalResolution); err != nil {
		return errors.Join(ErrFencePending, err)
	}
	return nil
}

func (e *FencingExecutor) finalizeHeldMutationEntry(ctx context.Context, runtime fencing.RuntimeBinding, sequence uint64, producerUID string, entry fencing.MutationEntry, provider mutationExecutionProvider, receipts *MutationExecutionReceiptStore) error {
	if entry.Preparation == nil || entry.Resolution == nil || receipts == nil {
		return ErrFencePending
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil || run.MutationProducerUID != producerUID {
		return errors.Join(ErrFencePending, err)
	}
	owner := hashJSON([]any{runtime, run.Identity.WALIdentity, producerUID, sequence, entry.Intent.OperationID})
	receipt, err := receipts.get("receipt-" + owner)
	if err != nil {
		return err
	}
	return e.finalizeHeldMutationReceipt(ctx, runtime, sequence, producerUID, receipt, provider, receipts)
}

func (e *FencingExecutor) finalizeHeldMutationReceipt(ctx context.Context, runtime fencing.RuntimeBinding, sequence uint64, producerUID string, receipt mutationExecutionReceipt, provider mutationExecutionProvider, receipts *MutationExecutionReceiptStore) error {
	finalizer, ok := provider.(mutationHeldCreateFinalizer)
	if !ok || receipts == nil || !fencing.IsHeldCreateEligible(receipt.Intent, receipt.Preparation) ||
		receipt.Outcome != fencing.MutationCommitted || !receipt.AllCopiesTerminal || receipt.Runtime != runtime || receipt.ProducerUID != producerUID || receipt.Sequence != sequence {
		return ErrFencePending
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil || run.MutationProducerUID != producerUID || run.Identity.WALIdentity != receipt.WALIdentity {
		return errors.Join(ErrFencePending, err)
	}
	owner := hashJSON([]any{runtime, run.Identity.WALIdentity, producerUID, sequence, receipt.Intent.OperationID})
	saved, err := receipts.get("receipt-" + owner)
	if err != nil || saved != receipt {
		return errors.Join(ErrFencePending, err)
	}
	producer, actualUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil || actualUID != producerUID || producer.Runtime != runtime || producer.WALIdentity != run.Identity.WALIdentity {
		return errors.Join(ErrFencePending, err)
	}
	entry, exists := producer.Entries[sequence]
	if !exists || entry.Intent != receipt.Intent || entry.Preparation == nil || *entry.Preparation != receipt.Preparation || entry.Resolution == nil ||
		entry.Resolution.ReceiptDigest != receipt.ReceiptDigest || entry.Resolution.AllCopiesTerminal != receipt.AllCopiesTerminal {
		return ErrFencePending
	}
	if err := e.VerifySourceMutationEvidence(ctx, runtime, sequence, entry); err != nil {
		return err
	}
	return finalizer.finalizeHeldCreate(ctx, receipt.Intent.Key, receipt.Intent, receipt)
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
