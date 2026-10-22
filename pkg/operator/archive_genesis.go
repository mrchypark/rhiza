package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

var errArchiveGenesisClaimed = errors.New("fencing: another archive genesis tuple already won the scope claim")

type archiveGenesisCancellationProof struct {
	Version        int                       `json:"version"`
	Runtime        fencing.RuntimeBinding    `json:"runtime"`
	WALIdentity    string                    `json:"walIdentity"`
	ProducerUID    string                    `json:"producerUID"`
	Sequence       uint64                    `json:"sequence"`
	Intent         fencing.MutationIntent    `json:"intent"`
	WinningGenesis fencing.ArchiveGenesisRef `json:"winningGenesis"`
}

func (e *FencingExecutor) claimArchiveScopeForRegistration(ctx context.Context, uid string, id fencing.Identity, runtime fencing.RuntimeBinding) error {
	if e == nil || e.AuthorityState == nil || !e.RequireMutationJournal || id.SourceClusterID != runtime.SourceClusterID || uid != runtime.StatefulSetUID {
		return fmt.Errorf("fencing: archive genesis registration is not configured")
	}
	scope, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil {
		return err
	}
	archiveScope := fencing.ArchiveScope{Provider: scope.Provider, Bucket: scope.Bucket, Prefix: scope.Prefix, ClusterID: runtime.SourceClusterID}
	registration := hashJSON([]any{"rhiza-authenticated-registration-v1", uid, id, runtime})
	claim, err := e.AuthorityState.ClaimArchiveScope(ctx, archiveScope, recovery.ArchiveConfigID, uid, runtime.SourceClusterID, registration)
	if err != nil {
		return err
	}
	for range 8 {
		state, version, err := e.AuthorityState.ReadRoot(ctx, uid, runtime.SourceClusterID)
		if err != nil {
			return err
		}
		if !state.Ledger.RequireMutationJournal {
			return fmt.Errorf("fencing: archive genesis root profile changed")
		}
		switch state.Ledger.ArchiveGenesisState {
		case fencing.ArchiveGenesisRegistrationPending:
			state.Ledger.ArchiveGenesisState = fencing.ArchiveGenesisAwaitingRef
			state.Ledger.ArchiveScopeHash = claim.ScopeHash
		case fencing.ArchiveGenesisAwaitingRef:
			if state.Ledger.ArchiveScopeHash != claim.ScopeHash || state.Ledger.ArchiveGenesisRef != nil {
				return fmt.Errorf("fencing: archive genesis scope claim changed")
			}
			return nil
		case fencing.ArchiveGenesisCommitted:
			if state.Ledger.ArchiveScopeHash != claim.ScopeHash || state.Ledger.ArchiveGenesisRef == nil ||
				claim.GenesisRef == nil || !sameArchiveGenesisRef(state.Ledger.ArchiveGenesisRef, claim.GenesisRef) {
				return fmt.Errorf("fencing: committed archive genesis claim differs")
			}
			return nil
		default:
			return fmt.Errorf("fencing: required archive genesis state is missing")
		}
		if err := e.AuthorityState.WriteRoot(ctx, uid, runtime.SourceClusterID, state, version); err == nil {
			return nil
		} else if !errors.Is(err, errAuthorityStateConflict) {
			confirmed, _, readErr := e.AuthorityState.ReadRoot(ctx, uid, runtime.SourceClusterID)
			if readErr == nil && confirmed.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisAwaitingRef && confirmed.Ledger.ArchiveScopeHash == claim.ScopeHash {
				return nil
			}
			return errors.Join(err, readErr)
		}
	}
	return fmt.Errorf("fencing: archive genesis root claim changed too often")
}

func sameArchiveGenesisRef(left, right *fencing.ArchiveGenesisRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return hashJSON(left) == hashJSON(right)
}

func (e *FencingExecutor) setRootArchiveGenesisRef(ctx context.Context, uid, source string, ref fencing.ArchiveGenesisRef) error {
	if e == nil || e.AuthorityState == nil || ref.Validate() != nil {
		return fmt.Errorf("fencing: archive genesis reference is unavailable")
	}
	for range 8 {
		state, version, err := e.AuthorityState.ReadRoot(ctx, uid, source)
		if err != nil {
			return err
		}
		if err := e.validateMutationJournalProfile(state); err != nil {
			return err
		}
		if state.Ledger.ArchiveScopeHash != ref.ScopeHash {
			return fmt.Errorf("fencing: archive genesis root scope differs")
		}
		if state.Ledger.ArchiveGenesisRef != nil {
			if !sameArchiveGenesisRef(state.Ledger.ArchiveGenesisRef, &ref) {
				return fmt.Errorf("fencing: archive genesis root reference already committed")
			}
			return nil
		}
		if state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisAwaitingRef {
			return fmt.Errorf("fencing: root is not awaiting an archive genesis reference")
		}
		state.Ledger.ArchiveGenesisRef = &ref
		state.Ledger.ArchiveGenesisState = fencing.ArchiveGenesisCommitted
		if err := e.AuthorityState.WriteRoot(ctx, uid, source, state, version); err == nil {
			return nil
		} else if !errors.Is(err, errAuthorityStateConflict) {
			confirmed, _, readErr := e.AuthorityState.ReadRoot(ctx, uid, source)
			if readErr == nil && confirmed.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisCommitted && sameArchiveGenesisRef(confirmed.Ledger.ArchiveGenesisRef, &ref) {
				return nil
			}
			return errors.Join(err, readErr)
		}
	}
	return fmt.Errorf("fencing: archive genesis root changed too often")
}

func (e *FencingExecutor) commitArchiveGenesisRef(ctx context.Context, uid, source string, scope fencing.ArchiveScope, ref fencing.ArchiveGenesisRef) error {
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if err != nil {
		return err
	}
	if claim.OwnerStatefulSetUID != uid || claim.OwnerSourceClusterID != source || claim.ScopeHash != ref.ScopeHash || claim.ConfigID != ref.ConfigID {
		return fmt.Errorf("fencing: archive scope claim belongs to a different root")
	}
	if err := e.AuthorityState.SetArchiveScopeGenesisRef(ctx, scope, ref); err != nil {
		winner, _, readErr := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
		if readErr != nil {
			return errors.Join(err, readErr)
		}
		if winner.GenesisRef == nil || sameArchiveGenesisRef(winner.GenesisRef, &ref) {
			return errors.Join(err, fencing.ErrPending)
		}
		root, rootErr := e.read(ctx, uid, source)
		if rootErr != nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, winner.GenesisRef) {
			return errors.Join(err, rootErr, fencing.ErrPending)
		}
		return errArchiveGenesisClaimed
	}
	return e.setRootArchiveGenesisRef(ctx, uid, source, ref)
}

func (e *FencingExecutor) archiveGenesisScope(ctx context.Context, runtime fencing.RuntimeBinding) (fencing.ArchiveScope, string, error) {
	storeScope, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil {
		return fencing.ArchiveScope{}, "", err
	}
	scope := fencing.ArchiveScope{Provider: storeScope.Provider, Bucket: storeScope.Bucket, Prefix: storeScope.Prefix, ClusterID: runtime.SourceClusterID}
	hash, err := scope.Hash()
	return scope, hash, err
}

func isArchiveGenesisMutation(intent fencing.MutationIntent) bool {
	return intent.Kind == "upload" && intent.Key == "archive/head.bin" && intent.Condition == (fencing.MutationCondition{Mode: fencing.MutationCreateOnly})
}

func validateArchiveGenesisAdmission(ref *fencing.ArchiveGenesisRef, producerUID string, sequence uint64, intent fencing.MutationIntent) error {
	if ref == nil || !isArchiveGenesisMutation(intent) {
		return nil
	}
	if ref.Mutation == nil || ref.Mutation.ProducerUID != producerUID || ref.Mutation.Sequence != sequence || ref.Mutation.Intent != intent {
		return fmt.Errorf("fencing: source mutation differs from the immutable archive genesis tuple")
	}
	return nil
}

func (e *FencingExecutor) validateArchiveGenesisMutationPayload(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent) error {
	scope, scopeHash, err := e.archiveGenesisScope(ctx, runtime)
	storeScope, storeErr := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil || storeErr != nil || intent.StoreScopeHash != hashJSON(storeScope) {
		return errors.Join(err, storeErr, fmt.Errorf("fencing: archive genesis mutation scope differs"))
	}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if err != nil || claim.OwnerStatefulSetUID != runtime.StatefulSetUID || claim.OwnerSourceClusterID != runtime.SourceClusterID || claim.ConfigID != recovery.ArchiveConfigID {
		return errors.Join(err, fmt.Errorf("fencing: archive scope claim does not bind the required root and config"))
	}
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil || !state.Ledger.RequireMutationJournal || state.Ledger.ArchiveScopeHash != claim.ScopeHash {
		return errors.Join(err, fmt.Errorf("fencing: required archive genesis root binding differs"))
	}
	if claim.GenesisRef != nil || state.Ledger.ArchiveGenesisRef != nil {
		if claim.GenesisRef == nil || state.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(claim.GenesisRef, state.Ledger.ArchiveGenesisRef) {
			return fmt.Errorf("fencing: archive genesis claim and root reference differ")
		}
		if err := validateArchiveGenesisAdmission(claim.GenesisRef, producerUID, sequence, intent); err != nil {
			return errArchiveGenesisClaimed
		}
		if isArchiveGenesisMutation(intent) {
			return nil
		}
		return e.requireArchiveGenesisMutationCommitted(ctx, runtime, producerUID, sequence, intent, claim.GenesisRef)
	}
	if state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisAwaitingRef || !isArchiveGenesisMutation(intent) {
		return fencing.ErrPending
	}
	if intent.PayloadSize <= 0 || intent.PayloadSize > 64<<10 {
		return fmt.Errorf("fencing: archive genesis head exceeds the bounded payload size")
	}
	stages, err := e.sourceStagesFor(ctx, runtime)
	if err != nil {
		return err
	}
	reader, err := stages.openMutationPayloadContext(ctx, producerUID, intent)
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if int64(len(data)) != intent.PayloadSize || len(data) > 64<<10 {
		return fmt.Errorf("fencing: archive genesis staged payload length differs")
	}
	if err := recovery.ValidateEmptyArchiveHead(data, recovery.ArchiveConfigID); err != nil {
		return fmt.Errorf("fencing: staged archive head is not the registered empty genesis: %w", err)
	}
	provider, err := newGCSMutationProvider(e.SourceMutationStorage, AttestationStoreScope{Provider: scope.Provider, Bucket: scope.Bucket, Prefix: scope.Prefix})
	if err != nil {
		return err
	}
	if _, exists, err := provider.observe(ctx, intent.Key); err != nil {
		return err
	} else if exists {
		return fencing.ErrPending
	}
	return nil
}

func (e *FencingExecutor) prepareArchiveGenesisMutation(ctx context.Context, uid string, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent) error {
	if !isArchiveGenesisMutation(intent) {
		return nil
	}
	scope, scopeHash, err := e.archiveGenesisScope(ctx, runtime)
	if err != nil || intent.StoreScopeHash == "" {
		return fmt.Errorf("fencing: archive genesis scope is unavailable")
	}
	claim, _, claimErr := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if claimErr != nil {
		return claimErr
	}
	if claim.GenesisRef != nil {
		ref := claim.GenesisRef
		if ref.Mutation == nil || ref.Mutation.ProducerUID != producerUID || ref.Mutation.Sequence != sequence || ref.Mutation.Intent != intent {
			return errArchiveGenesisClaimed
		}
		return e.setRootArchiveGenesisRef(ctx, uid, runtime.SourceClusterID, *ref)
	}
	if err := e.validateArchiveGenesisMutationPayload(ctx, runtime, producerUID, sequence, intent); err != nil {
		// A claim/root CAS may already have succeeded before a response was lost.
		// In that case only the exact committed tuple is an idempotent retry.
		claim, _, claimErr := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
		if claimErr != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil ||
			claim.GenesisRef.Mutation.ProducerUID != producerUID || claim.GenesisRef.Mutation.Sequence != sequence || claim.GenesisRef.Mutation.Intent != intent {
			return errors.Join(err, claimErr)
		}
		if claim.GenesisRef.ScopeHash != scopeHash {
			return fmt.Errorf("fencing: committed archive genesis scope differs")
		}
		return e.setRootArchiveGenesisRef(ctx, uid, runtime.SourceClusterID, *claim.GenesisRef)
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return err
	}
	lookup, err := e.lookupMutation(ctx, uid, run.Identity, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: sequence, Intent: intent})
	if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != intent || lookup.Entry.Resolution != nil || len(lookup.Entry.Dispatches) != 0 {
		return errors.Join(err, fencing.ErrPending)
	}
	ref := fencing.ArchiveGenesisRef{Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID, ScopeHash: scopeHash,
		Mutation: &fencing.ArchiveGenesisMutation{ProducerUID: producerUID, Sequence: sequence, Intent: intent}}
	return e.commitArchiveGenesisRef(ctx, uid, runtime.SourceClusterID, scope, ref)
}

func (e *FencingExecutor) cancelArchiveGenesisLoser(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent) error {
	scope, _, err := e.archiveGenesisScope(ctx, runtime)
	if err != nil {
		return err
	}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if err != nil || claim.ConfigID != recovery.ArchiveConfigID || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil {
		return errors.Join(err, fencing.ErrPending)
	}
	winner := claim.GenesisRef
	if winner.Mutation.ProducerUID == producerUID && winner.Mutation.Sequence == sequence && winner.Mutation.Intent == intent {
		return fencing.ErrPending
	}
	root, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, winner) {
		return errors.Join(err, fencing.ErrPending)
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return err
	}
	lookup, err := e.lookupMutation(ctx, runtime.StatefulSetUID, run.Identity, fencing.MutationLookupRequest{ProducerUID: producerUID, Sequence: sequence, Intent: intent})
	if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != intent {
		return errors.Join(err, fencing.ErrPending)
	}
	entry := lookup.Entry
	if entry.Preparation != nil || len(entry.Dispatches) != 0 {
		return fencing.ErrPending
	}
	proof := archiveGenesisCancellationProof{Version: 1, Runtime: runtime, WALIdentity: run.Identity.WALIdentity, ProducerUID: producerUID, Sequence: sequence, Intent: intent, WinningGenesis: *winner}
	body, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	receiptDigest := hashBytes(body)
	evidence, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return err
	}
	key := "archive-genesis-cancellations/" + receiptDigest
	if _, err := evidence.CreateImmutable(ctx, key, body); err != nil {
		return err
	}
	_, err = e.changeMutationProducer(ctx, runtime, func(p *fencing.MutationProducer) error {
		entry, exists := p.Entries[sequence]
		if !exists || entry.Intent != intent || entry.Preparation != nil || len(entry.Dispatches) != 0 {
			return fencing.ErrPending
		}
		if entry.Resolution != nil {
			if entry.Resolution.Outcome == fencing.MutationRejected && entry.Resolution.AllCopiesTerminal && entry.Resolution.ReceiptDigest == receiptDigest {
				return nil
			}
			return fencing.ErrPending
		}
		return p.CancelUnprepared(sequence, receiptDigest)
	})
	return err
}

func (e *FencingExecutor) requireArchiveGenesisMutationCommitted(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent, ref *fencing.ArchiveGenesisRef) error {
	if ref == nil || ref.Mutation == nil || ref.Mutation.ProducerUID == producerUID && ref.Mutation.Sequence == sequence && ref.Mutation.Intent == intent {
		return nil
	}
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return err
	}
	_, run, err := e.archiveGenesisProducerRun(ctx, state, ref.Mutation.ProducerUID)
	if err != nil {
		return errors.Join(err, fencing.ErrPending)
	}
	m := ref.Mutation
	lookup, err := e.lookupMutation(ctx, runtime.StatefulSetUID, run.Identity, fencing.MutationLookupRequest{ProducerUID: m.ProducerUID, Sequence: m.Sequence, Intent: m.Intent})
	if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != m.Intent || lookup.Entry.Resolution == nil || lookup.Entry.Resolution.Outcome != fencing.MutationCommitted || !lookup.Entry.Resolution.AllCopiesTerminal {
		return errors.Join(err, fencing.ErrPending)
	}
	return nil
}

func (e *FencingExecutor) requireArchiveGenesisForExecution(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent) error {
	ref, err := e.reconcileRootArchiveGenesisRef(ctx, runtime.StatefulSetUID, runtime.SourceClusterID, runtime)
	if err != nil {
		return err
	}
	if err := e.reconcileArchiveGenesisCandidates(ctx, runtime, ref); err != nil {
		return err
	}
	if intent.Key == "archive/head.bin" && isArchiveGenesisMutation(intent) && ref.Mutation != nil &&
		(ref.Mutation.ProducerUID != producerUID || ref.Mutation.Sequence != sequence || ref.Mutation.Intent != intent) {
		return fmt.Errorf("fencing: source mutation differs from the immutable archive genesis tuple")
	}
	return e.requireArchiveGenesisMutationCommitted(ctx, runtime, producerUID, sequence, intent, &ref)
}

// reconcileArchiveGenesisCandidates runs only after both immutable references
// agree. It terminally excludes every exact, unprepared losing admission so a
// lost loser response cannot leave the producer permanently unquiesced. Any
// prepared/dispatched loser remains pending because its physical effect is
// not known to be terminal.
func (e *FencingExecutor) reconcileArchiveGenesisCandidates(ctx context.Context, runtime fencing.RuntimeBinding, ref fencing.ArchiveGenesisRef) error {
	if ref.Mutation == nil {
		return nil
	}
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return err
	}
	if state.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(state.Ledger.ArchiveGenesisRef, &ref) {
		return fencing.ErrPending
	}
	winnerRuntime, winnerRun, err := e.archiveGenesisProducerRun(ctx, state, ref.Mutation.ProducerUID)
	if err != nil {
		return errors.Join(err, fencing.ErrPending)
	}
	winner, err := e.lookupMutation(ctx, runtime.StatefulSetUID, winnerRun.Identity, fencing.MutationLookupRequest{
		ProducerUID: ref.Mutation.ProducerUID, Sequence: ref.Mutation.Sequence, Intent: ref.Mutation.Intent,
	})
	if err != nil || !winner.Found || winner.Entry == nil || winner.Entry.Intent != ref.Mutation.Intent || winnerRuntime.StatefulSetUID != runtime.StatefulSetUID {
		return errors.Join(err, fencing.ErrPending)
	}
	type candidate struct {
		runtime     fencing.RuntimeBinding
		producerUID string
		sequence    uint64
		intent      fencing.MutationIntent
	}
	var losers []candidate
	for _, run := range state.Ledger.Runs {
		if run.Runtime == nil || run.MutationProducerUID == "" || run.MutationProfile != fencing.MutationExecutionProfile {
			continue
		}
		producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
		if err != nil || producerUID != run.MutationProducerUID || producer.Runtime != *run.Runtime || producer.WALIdentity != run.Identity.WALIdentity || producer.Validate() != nil {
			return errors.Join(err, fencing.ErrPending)
		}
		for sequence, entry := range producer.Entries {
			if !isArchiveGenesisMutation(entry.Intent) ||
				(producerUID == ref.Mutation.ProducerUID && sequence == ref.Mutation.Sequence && entry.Intent == ref.Mutation.Intent) {
				continue
			}
			losers = append(losers, candidate{runtime: *run.Runtime, producerUID: producerUID, sequence: sequence, intent: entry.Intent})
		}
	}
	for _, loser := range losers {
		if err := e.cancelArchiveGenesisLoser(ctx, loser.runtime, loser.producerUID, loser.sequence, loser.intent); err != nil {
			return errors.Join(err, fencing.ErrPending)
		}
	}
	return nil
}

func (e *FencingExecutor) archiveGenesisStatus(ctx context.Context, authorization string, id fencing.Identity, archiveKey string, configID uint) (fencing.ArchiveGenesisStatus, error) {
	if archiveKey != "archive/head.bin" || configID != recovery.ArchiveConfigID {
		return fencing.ArchiveGenesisStatus{}, fmt.Errorf("fencing: archive genesis request config or key differs")
	}
	uid, err := e.authenticate(ctx, authorization, id)
	if err != nil {
		return fencing.ArchiveGenesisStatus{}, sourceStageAuthenticationError{err: err}
	}
	callerRuntime, mutationScopeHash, err := e.sourceMutationProducer(ctx, uid, id, false)
	if err != nil {
		return fencing.ArchiveGenesisStatus{}, err
	}
	scope, scopeHash, err := e.archiveGenesisScope(ctx, callerRuntime)
	if err != nil || mutationScopeHash == "" {
		return fencing.ArchiveGenesisStatus{}, errors.Join(err, fmt.Errorf("fencing: archive genesis scope is unavailable"))
	}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
	if err != nil || claim.ConfigID != configID || claim.ScopeHash != scopeHash || claim.OwnerStatefulSetUID != uid || claim.OwnerSourceClusterID != id.SourceClusterID {
		return fencing.ArchiveGenesisStatus{}, errors.Join(err, fmt.Errorf("fencing: archive scope claim differs from authenticated root"))
	}
	state, _, err := e.AuthorityState.ReadRoot(ctx, uid, id.SourceClusterID)
	if err != nil {
		return fencing.ArchiveGenesisStatus{}, err
	}
	if !state.Ledger.RequireMutationJournal || state.Ledger.ArchiveScopeHash != scopeHash {
		return fencing.ArchiveGenesisStatus{}, fmt.Errorf("fencing: required archive genesis root binding differs")
	}
	if claim.GenesisRef == nil {
		if state.Ledger.ArchiveGenesisRef != nil || state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisAwaitingRef {
			return fencing.ArchiveGenesisStatus{}, fmt.Errorf("fencing: archive genesis claim and root reference differ")
		}
		candidate, found, err := e.findArchiveGenesisCandidate(ctx, state)
		if err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		if !found {
			if err := e.ensureArchiveScopeHasNoGenesisEvidence(ctx, scope); err != nil {
				return fencing.ArchiveGenesisStatus{}, err
			}
			return fencing.ArchiveGenesisStatus{ScopeHash: scopeHash, MutationScopeHash: mutationScopeHash, ConfigID: claim.ConfigID, CanInitializeEmpty: true}, nil
		}
		if err := e.validateArchiveGenesisMutationPayload(ctx, candidate.runtime, candidate.producerUID, candidate.sequence, candidate.intent); err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		if err := e.prepareArchiveGenesisMutation(ctx, uid, candidate.runtime, candidate.producerUID, candidate.sequence, candidate.intent); err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		claim, _, err = e.AuthorityState.ReadArchiveScopeClaim(ctx, scope)
		if err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
	}
	if claim.GenesisRef == nil {
		return fencing.ArchiveGenesisStatus{}, fencing.ErrPending
	}
	if state.Ledger.ArchiveGenesisRef == nil {
		if state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisAwaitingRef {
			return fencing.ArchiveGenesisStatus{}, fmt.Errorf("fencing: archive genesis root state differs")
		}
		if err := e.setRootArchiveGenesisRef(ctx, uid, id.SourceClusterID, *claim.GenesisRef); err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
	} else if !sameArchiveGenesisRef(state.Ledger.ArchiveGenesisRef, claim.GenesisRef) {
		return fencing.ArchiveGenesisStatus{}, fmt.Errorf("fencing: root and scope genesis references differ")
	}
	status := fencing.ArchiveGenesisStatus{ScopeHash: scopeHash, MutationScopeHash: mutationScopeHash, ConfigID: claim.ConfigID, Ref: claim.GenesisRef}
	if claim.GenesisRef.Mutation != nil {
		m := claim.GenesisRef.Mutation
		runtime, run, err := e.archiveGenesisProducerRun(ctx, state, m.ProducerUID)
		if err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		runScope, _, err := e.archiveGenesisScope(ctx, runtime)
		if err != nil || runScope != scope {
			return fencing.ArchiveGenesisStatus{}, errors.Join(err, fmt.Errorf("fencing: original genesis runtime changed archive scope"))
		}
		storeScope, err := e.sourceMutationStorageScope(ctx, runtime)
		if err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		provider, err := newGCSMutationProvider(e.SourceMutationStorage, storeScope)
		if err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		receipts, err := e.receiptsForRuntime(runtime)
		if err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		if err := e.requireArchiveGenesisForExecution(ctx, runtime, m.ProducerUID, m.Sequence, m.Intent); err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		if err := e.ExecuteSourceMutation(ctx, runtime, m.ProducerUID, m.Sequence, m.Intent, storeScope, provider, receipts); err != nil {
			return fencing.ArchiveGenesisStatus{}, err
		}
		lookup, err := e.lookupMutation(ctx, uid, run.Identity, fencing.MutationLookupRequest{ProducerUID: m.ProducerUID, Sequence: m.Sequence, Intent: m.Intent})
		if err != nil || !lookup.Found || lookup.Entry == nil || lookup.Entry.Intent != m.Intent {
			return fencing.ArchiveGenesisStatus{}, errors.Join(err, fencing.ErrPending)
		}
		status.Mutation = &lookup
	}
	return status, nil
}

// ensureArchiveScopeHasNoGenesisEvidence prevents a partially published fork
// or a headless checkpoint from being misclassified as a fresh empty archive.
// These keys are written by the existing recovery protocol before activation.
func (e *FencingExecutor) ensureArchiveScopeHasNoGenesisEvidence(ctx context.Context, scope fencing.ArchiveScope) error {
	provider, err := newGCSMutationProvider(e.SourceMutationStorage, AttestationStoreScope{Provider: scope.Provider, Bucket: scope.Bucket, Prefix: scope.Prefix})
	if err != nil {
		return err
	}
	for _, key := range []string{
		"archive/head.bin",
		"fork/intent.json",
		"fork/result.json",
		"recovery/anchor.json",
		"recovery/generation.json",
		"voters/membership.json",
	} {
		if _, exists, err := provider.observe(ctx, key); err != nil {
			return err
		} else if exists {
			return fencing.ErrPending
		}
	}
	return nil
}

type archiveGenesisCandidate struct {
	runtime     fencing.RuntimeBinding
	run         fencing.Run
	producerUID string
	sequence    uint64
	intent      fencing.MutationIntent
}

func (e *FencingExecutor) findArchiveGenesisCandidate(ctx context.Context, state fenceLedger) (archiveGenesisCandidate, bool, error) {
	var candidate archiveGenesisCandidate
	for _, run := range state.Ledger.Runs {
		if run.Runtime == nil || run.MutationProducerUID == "" || run.MutationProfile != fencing.MutationExecutionProfile {
			continue
		}
		producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
		if err != nil {
			return archiveGenesisCandidate{}, false, err
		}
		if producerUID != run.MutationProducerUID || producer.Runtime != *run.Runtime || producer.WALIdentity != run.Identity.WALIdentity || producer.Validate() != nil || producer.CompactedThrough != 0 {
			return archiveGenesisCandidate{}, false, fencing.ErrPending
		}
		for sequence, entry := range producer.Entries {
			if !isArchiveGenesisMutation(entry.Intent) {
				continue
			}
			if entry.Resolution != nil || len(entry.Dispatches) != 0 {
				return archiveGenesisCandidate{}, false, fencing.ErrPending
			}
			if candidate.producerUID != "" {
				return archiveGenesisCandidate{}, false, fmt.Errorf("fencing: multiple admitted archive genesis tuples are ambiguous")
			}
			candidate = archiveGenesisCandidate{runtime: *run.Runtime, run: run, producerUID: producerUID, sequence: sequence, intent: entry.Intent}
		}
	}
	return candidate, candidate.producerUID != "", nil
}

func (e *FencingExecutor) archiveGenesisProducerRun(ctx context.Context, state fenceLedger, producerUID string) (fencing.RuntimeBinding, fencing.Run, error) {
	var found *fencing.Run
	for _, run := range state.Ledger.Runs {
		if run.Runtime == nil || run.MutationProducerUID != producerUID {
			continue
		}
		if found != nil {
			return fencing.RuntimeBinding{}, fencing.Run{}, fmt.Errorf("fencing: archive genesis producer is ambiguously root-bound")
		}
		copy := run
		found = &copy
	}
	if found == nil {
		return fencing.RuntimeBinding{}, fencing.Run{}, fencing.ErrPending
	}
	producer, storedUID, _, err := e.AuthorityState.ReadProducer(ctx, *found.Runtime)
	if err != nil || storedUID != producerUID || producer.Runtime != *found.Runtime || producer.WALIdentity != found.Identity.WALIdentity || producer.Validate() != nil {
		return fencing.RuntimeBinding{}, fencing.Run{}, errors.Join(err, fencing.ErrPending)
	}
	return *found.Runtime, *found, nil
}

func (e *FencingExecutor) reconcileRootArchiveGenesisRef(ctx context.Context, uid, source string, runtime fencing.RuntimeBinding) (fencing.ArchiveGenesisRef, error) {
	state, _, err := e.AuthorityState.ReadRoot(ctx, uid, source)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	if err := e.validateMutationJournalProfile(state); err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	scope, err := e.sourceMutationStorageScope(ctx, runtime)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	archiveScope := fencing.ArchiveScope{Provider: scope.Provider, Bucket: scope.Bucket, Prefix: scope.Prefix, ClusterID: source}
	claim, _, err := e.AuthorityState.ReadArchiveScopeClaim(ctx, archiveScope)
	if err != nil {
		return fencing.ArchiveGenesisRef{}, err
	}
	if claim.OwnerStatefulSetUID != uid || claim.OwnerSourceClusterID != source || claim.ConfigID != recovery.ArchiveConfigID || state.Ledger.ArchiveScopeHash != claim.ScopeHash {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: archive scope claim does not bind the required root")
	}
	if state.Ledger.ArchiveGenesisRef == nil {
		return fencing.ArchiveGenesisRef{}, fencing.ErrPending
	}
	if claim.GenesisRef == nil || !sameArchiveGenesisRef(state.Ledger.ArchiveGenesisRef, claim.GenesisRef) {
		return fencing.ArchiveGenesisRef{}, fmt.Errorf("fencing: archive genesis claim and root reference differ")
	}
	return *state.Ledger.ArchiveGenesisRef, nil
}
