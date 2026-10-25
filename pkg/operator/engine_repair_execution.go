package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type engineRemovalFreezeVerifier interface {
	VerifyEngineRemovalFreeze(context.Context, fencing.RuntimeBinding, fencing.EngineRepairRecord) error
}

type engineRepairCandidateActivationVerifier interface {
	verifyEngineRepairCandidateActive(context.Context, fencing.RuntimeBinding, fencing.EngineRepairRecord) error
}

// confirmEngineRepairActivation advances only a canonical ADD that the
// operator can independently verify as a promoted, producer-bound candidate.
// The request carries an authenticated surviving-voter identity and an
// operation selector; it never carries candidate runtime or readiness claims.
func (e *FencingExecutor) confirmEngineRepairActivation(ctx context.Context, statefulSetUID string, caller fencing.Identity, operationID string) (fencing.EngineRepairRecord, error) {
	verifier, ok := any(e).(engineRepairCandidateActivationVerifier)
	if !ok {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair candidate activation verifier is unavailable")
	}
	return e.confirmEngineRepairActivationWithVerifier(ctx, statefulSetUID, caller, operationID, verifier)
}

func (e *FencingExecutor) confirmEngineRepairActivationWithVerifier(ctx context.Context, statefulSetUID string, caller fencing.Identity, operationID string, verifier engineRepairCandidateActivationVerifier) (fencing.EngineRepairRecord, error) {
	if e == nil || ctx == nil || e.AuthorityState == nil || statefulSetUID == "" || operationID == "" {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair activation request is incomplete")
	}
	if verifier == nil {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair candidate activation verifier is unavailable")
	}
	state, err := e.read(ctx, statefulSetUID, caller.SourceClusterID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if err := engineRepairCallerIsActiveSurvivor(state, caller); err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	callerRun := state.Ledger.Runs[caller.RunID]
	store, err := NewEngineRepairStore(e.AuthorityState, statefulSetUID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	repair, err := store.ReadEngineRepair(ctx, caller.SourceClusterID, operationID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if repair.Binding.StatefulSetUID != statefulSetUID || repair.Binding.SourceClusterID != caller.SourceClusterID ||
		repair.Binding.OperationID != operationID || (repair.Phase != fencing.RepairAdding && repair.Phase != fencing.RepairActive) {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair is not at its candidate activation boundary")
	}
	sourceMember, sourceMemberExists := repair.SourceConfig.MemberSet()[quepaxa.NodeID(caller.NodeID)]
	if !sourceMemberExists || caller.NodeID == repair.Binding.OldMemberID || caller.WALIdentity == "" || caller.WALIdentity != sourceMember.WALIdentity {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair activation caller is not a surviving original voter")
	}
	if err := validateEngineRepairLedgerForActivation(repair, state.Ledger); err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if callerRun.Runtime == nil || callerRun.Runtime.Validate(caller) != nil || callerRun.Runtime.StatefulSetUID != statefulSetUID {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair activation caller runtime differs")
	}
	candidateIdentity := repair.CurrentCandidateIdentity()
	candidateRuntime := repair.CurrentCandidateRuntime()
	candidateRun, exists := state.Ledger.Runs[candidateIdentity.RunID]
	if candidateRuntime == nil || !exists || candidateRun.Identity != candidateIdentity || candidateRun.Runtime == nil ||
		*candidateRun.Runtime != *candidateRuntime || candidateRuntime.Validate(candidateIdentity) != nil ||
		hashJSON(*candidateRuntime) != repair.CurrentCandidateRuntimeHash() {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair candidate runtime is not the exact current incarnation")
	}
	if fencing.RunRole(candidateRun) != fencing.StartupRoleVoterActive || candidateRun.Promotion == nil {
		return fencing.EngineRepairRecord{}, fencing.ErrPending
	}
	if err := verifier.verifyEngineRepairCandidateActive(ctx, *candidateRuntime, repair); err != nil {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate activation evidence is incomplete: %w", err)
	}
	if repair.Phase == fencing.RepairActive {
		return repair, nil
	}

	// The verifier may perform remote quorum and archive reads. Recheck both
	// the CAS revision and candidate run before persisting its result.
	current, err := store.ReadEngineRepair(ctx, caller.SourceClusterID, operationID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if current.Digest() != repair.Digest() || current.Phase != fencing.RepairAdding {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair changed during candidate activation verification")
	}
	latest, err := e.read(ctx, statefulSetUID, caller.SourceClusterID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if err := engineRepairCallerIsActiveSurvivor(latest, caller); err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	currentIdentity := current.CurrentCandidateIdentity()
	currentRuntime := current.CurrentCandidateRuntime()
	latestRun, exists := latest.Ledger.Runs[currentIdentity.RunID]
	if currentRuntime == nil || !exists || latestRun.Identity != currentIdentity || latestRun.Runtime == nil ||
		*latestRun.Runtime != *currentRuntime || currentRuntime.Validate(currentIdentity) != nil ||
		hashJSON(*currentRuntime) != current.CurrentCandidateRuntimeHash() {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair candidate changed during activation verification")
	}
	if err := validateEngineRepairLedgerForActivation(current, latest.Ledger); err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	active := current
	active.Phase = fencing.RepairActive
	activation := engineRepairActivationAuthorization{
		OperationID:              current.Binding.OperationID,
		Candidate:                latestRun.Identity,
		Runtime:                  *latestRun.Runtime,
		PromotionOperationID:     latestRun.Promotion.OperationID,
		PromotionEvidenceHash:    latestRun.Promotion.EvidenceHash,
		MutationProducerUID:      latestRun.MutationProducerUID,
		MutationScopeHash:        latestRun.MutationScopeHash,
		MutationProfile:          latestRun.MutationProfile,
		MutationStageIncarnation: latestRun.MutationStageIncarnation,
	}
	confirmed, err := store.compareAndSwapVerifiedEngineRepairActivation(ctx, caller.SourceClusterID, current.Digest(), active, activation)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if confirmed.Phase != fencing.RepairActive || confirmed.IdentityDigest() != repair.IdentityDigest() {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair activation CAS readback differs")
	}
	return confirmed, nil
}

// executeEngineRepairStorageEffects is the authority-side operation entrypoint.
// The request supplies the authenticated identity and operation ID, never a
// runtime binding, repair record, or proof. Runtime and repair data are loaded
// from canonical authority state after token authentication.
func (e *FencingExecutor) executeEngineRepairStorageEffects(ctx context.Context, authorization string, identity fencing.Identity, operationID string) (fencing.StorageEffectsExcluded, error) {
	verifier, ok := any(e).(engineRemovalFreezeVerifier)
	if !ok {
		return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: authenticated engine removal freeze verifier is unavailable")
	}
	if e == nil || ctx == nil || operationID == "" {
		return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: engine repair storage closure request is incomplete")
	}
	uid, err := e.authenticate(ctx, authorization, identity)
	if err != nil {
		return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: engine repair storage closure authentication failed: %w", err)
	}
	state, err := e.read(ctx, uid, identity.SourceClusterID)
	if err != nil {
		return fencing.StorageEffectsExcluded{}, err
	}
	run, ok := state.Ledger.Runs[identity.RunID]
	if !ok || run.Identity != identity || run.Runtime == nil {
		return fencing.StorageEffectsExcluded{}, fmt.Errorf("fencing: engine repair storage closure has no exact authenticated runtime")
	}
	return e.executeEngineRepairStorageEffectsWithVerifier(ctx, identity, *run.Runtime, operationID, verifier)
}

func (e *FencingExecutor) executeEngineRepairStorageEffectsWithVerifier(ctx context.Context, caller fencing.Identity, callerRuntime fencing.RuntimeBinding, operationID string, verifier engineRemovalFreezeVerifier) (fencing.StorageEffectsExcluded, error) {
	var empty fencing.StorageEffectsExcluded
	if e == nil || ctx == nil || verifier == nil || e.AuthorityState == nil || operationID == "" ||
		caller.SourceClusterID == "" || caller.RunID == "" || callerRuntime.SourceClusterID == "" || callerRuntime.StatefulSetUID == "" {
		return empty, fmt.Errorf("fencing: engine repair storage closure request is incomplete")
	}
	if callerRuntime.Validate(caller) != nil {
		return empty, fmt.Errorf("fencing: engine repair storage closure caller runtime differs from its authenticated identity")
	}
	store, err := NewEngineRepairStore(e.AuthorityState, callerRuntime.StatefulSetUID)
	if err != nil {
		return empty, err
	}
	repair, err := store.ReadEngineRepair(ctx, callerRuntime.SourceClusterID, operationID)
	if err != nil {
		return empty, err
	}
	if repair.Binding.StatefulSetUID != callerRuntime.StatefulSetUID || repair.Binding.SourceClusterID != callerRuntime.SourceClusterID ||
		repair.Binding.OperationID != operationID || repair.Phase != fencing.RepairStorageClosing {
		return empty, fmt.Errorf("fencing: engine repair storage closure is not at its frozen closing phase")
	}
	state, err := e.read(ctx, callerRuntime.StatefulSetUID, callerRuntime.SourceClusterID)
	if err != nil {
		return empty, err
	}
	candidateRuntime, err := engineRepairStorageClosureRuntimes(state, caller, callerRuntime, repair)
	if err != nil {
		return empty, err
	}
	if err := verifier.VerifyEngineRemovalFreeze(ctx, candidateRuntime, repair); err != nil {
		return empty, fmt.Errorf("fencing: authenticated engine removal freeze verification failed: %w", err)
	}
	if err := e.closeEngineRepairOldCohort(ctx, repair); err != nil {
		return empty, err
	}
	return e.settleEngineRepairStorageEffects(ctx, repair)
}

// closeEngineRepairOldCohort cuts admission only for the removed logical
// member, then closes each already-enrolled producer through its normal CAS
// path. It does not close the generation or claim process termination.
func (e *FencingExecutor) closeEngineRepairOldCohort(ctx context.Context, repair fencing.EngineRepairRecord) error {
	if e == nil || e.AuthorityState == nil || ctx == nil || repair.Validate() != nil ||
		repair.Phase != fencing.RepairStorageClosing || repair.Binding.OldMemberID == "" {
		return fmt.Errorf("fencing: engine repair old-cohort closure is incomplete")
	}
	store, err := NewEngineRepairStore(e.AuthorityState, repair.Binding.StatefulSetUID)
	if err != nil {
		return err
	}
	canonical, err := store.ReadEngineRepair(ctx, repair.Binding.SourceClusterID, repair.Binding.OperationID)
	if err != nil {
		return err
	}
	if canonical.Digest() != repair.Digest() || canonical.Phase != fencing.RepairStorageClosing {
		return fmt.Errorf("fencing: old-cohort closure is not bound to the exact canonical repair revision")
	}
	key, err := repair.Binding.EngineRepairGenerationKey()
	if err != nil {
		return err
	}
	oldIncarnations := make([]fencing.RepairIncarnation, 0, len(repair.Incarnations))
	for _, incarnation := range repair.Incarnations {
		if incarnation.Identity.NodeID == repair.Binding.OldMemberID {
			oldIncarnations = append(oldIncarnations, incarnation)
		}
	}
	if len(oldIncarnations) == 0 {
		return fmt.Errorf("fencing: repair has no exact removed-member incarnation cohort")
	}
	closeOperationID := "engine-repair-close-" + hashJSON(repair.IdentityDigest())
	closeRequestHash := hashJSON([]any{"engine-repair-old-cohort-close-v1", repair.IdentityDigest(), oldIncarnations})
	wantReference := engineRepairRootReference(repair)
	_, err = e.change(ctx, repair.Binding.StatefulSetUID, repair.Binding.SourceClusterID, func(ledger *fencing.Ledger) error {
		if ledger.ClosedBy != "" {
			return fencing.ErrClosed
		}
		if reference, ok := ledger.EngineRepairs[key]; !ok || reference != wantReference {
			return fmt.Errorf("fencing: canonical repair changed before old-cohort admission cut")
		}
		if err := repair.ValidateAgainstLedger(*ledger); err != nil {
			return err
		}
		if prior, ok := ledger.ClosedNodes[repair.Binding.OldMemberID]; ok &&
			(prior.OperationID != closeOperationID || prior.RequestHash != closeRequestHash) {
			return fencing.ErrClosed
		}
		for _, incarnation := range oldIncarnations {
			run := ledger.Runs[incarnation.Identity.RunID]
			wantStop := fencing.StopRequest{OperationID: closeOperationID, RequestHash: closeRequestHash, Identity: incarnation.Identity}
			if run.Stop != nil && *run.Stop != wantStop {
				return fencing.ErrClosed
			}
		}
		return ledger.CloseNodes(closeOperationID, closeRequestHash, []string{repair.Binding.OldMemberID})
	})
	if err != nil {
		return fmt.Errorf("fencing: close removed-member admission cohort: %w", err)
	}

	state, err := e.read(ctx, repair.Binding.StatefulSetUID, repair.Binding.SourceClusterID)
	if err != nil {
		return err
	}
	if state.Ledger.ClosedBy != "" || state.Ledger.EngineRepairs[key] != wantReference {
		return fmt.Errorf("fencing: old-cohort admission cut changed the generation or repair reference")
	}
	if err := repair.ValidateAgainstLedger(state.Ledger); err != nil {
		return err
	}
	for _, incarnation := range oldIncarnations {
		run, ok := state.Ledger.Runs[incarnation.Identity.RunID]
		wantStop := fencing.StopRequest{OperationID: closeOperationID, RequestHash: closeRequestHash, Identity: incarnation.Identity}
		if !ok || run.Stop == nil || *run.Stop != wantStop || run.MutationProducerUID != incarnation.ProducerUID {
			return fmt.Errorf("fencing: removed-member cohort cut does not match its exact run")
		}
		if incarnation.ProducerUID == "" {
			if run.Runtime != nil {
				_, uid, _, readErr := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
				if readErr == nil || uid != "" {
					return fmt.Errorf("fencing: removed run without a producer has canonical producer state")
				}
				if !errors.Is(readErr, errAuthorityStateNotFound) {
					return readErr
				}
			}
			continue
		}
		if run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil {
			return fmt.Errorf("fencing: removed producer has no exact runtime binding")
		}
		_, err := e.mutateProducer(ctx, *run.Runtime, false, false, func(*fencing.MutationProducer) error { return nil })
		if err != nil {
			return fmt.Errorf("fencing: close removed mutation producer: %w", err)
		}
		producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
		if err != nil || producerUID != incarnation.ProducerUID || producer.Runtime != *run.Runtime ||
			producer.WALIdentity != incarnation.Identity.WALIdentity || producer.Phase != fencing.MutationClosed ||
			producer.CloseOperationID != closeOperationID || producer.CloseRequestHash != closeRequestHash ||
			producer.ClosedThrough != producer.LastAdmitted || producer.Validate() != nil {
			return fmt.Errorf("fencing: removed producer did not read back the exact closed admission prefix")
		}
	}
	return nil
}

func engineRepairStorageClosureRuntimes(state fenceLedger, caller fencing.Identity, callerRuntime fencing.RuntimeBinding, repair fencing.EngineRepairRecord) (fencing.RuntimeBinding, error) {
	if repair.Validate() != nil || state.StatefulSetUID != repair.Binding.StatefulSetUID || state.Ledger.SourceClusterID != repair.Binding.SourceClusterID ||
		caller.SourceClusterID != repair.Binding.SourceClusterID || callerRuntime.StatefulSetUID != repair.Binding.StatefulSetUID || callerRuntime.SourceClusterID != repair.Binding.SourceClusterID ||
		callerRuntime.Validate(caller) != nil || repair.Phase != fencing.RepairStorageClosing || state.Ledger.ClosedBy != "" || state.Ledger.ClosedHash != "" {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: storage closure source generation or repair identity is not open and exact")
	}
	if err := repair.ValidateAgainstLedger(state.Ledger); err != nil {
		return fencing.RuntimeBinding{}, err
	}
	if _, closed := state.Ledger.ClosedNodes[caller.NodeID]; closed {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: storage closure caller is already terminally closed")
	}
	sourceMember, exists := repair.SourceConfig.MemberSet()[quepaxa.NodeID(caller.NodeID)]
	if !exists || caller.NodeID == repair.Binding.OldMemberID || caller.WALIdentity == "" || caller.WALIdentity != sourceMember.WALIdentity {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: storage closure caller is not a surviving original voter")
	}
	callerRun, exists := state.Ledger.Runs[caller.RunID]
	if !exists || callerRun.Identity != caller || callerRun.Runtime == nil || *callerRun.Runtime != callerRuntime || callerRun.Stop != nil || callerRun.Drain != nil ||
		callerRun.Exited || callerRun.NeverBoundExit != nil || callerRun.IndependentTerminal != nil || fencing.RunRole(callerRun) != fencing.StartupRoleVoterActive {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: storage closure caller is not the exact active survivor runtime")
	}
	candidateIdentity := repair.CurrentCandidateIdentity()
	candidateRuntime := repair.CurrentCandidateRuntime()
	candidateRun, exists := state.Ledger.Runs[candidateIdentity.RunID]
	if candidateRuntime == nil || !exists || (candidateRun.Identity != candidateIdentity && !isExactPrebindEngineRepairCandidate(repair, candidateRun)) || candidateRun.Runtime == nil || candidateRun.Stop != nil || candidateRun.Drain != nil ||
		candidateRun.Exited || candidateRun.NeverBoundExit != nil || candidateRun.IndependentTerminal != nil || fencing.RunRole(candidateRun) != fencing.StartupRoleLearnerPendingRemove {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: storage closure has no exact live reserved candidate runtime")
	}
	if *candidateRun.Runtime != *candidateRuntime || candidateRuntime.Validate(candidateIdentity) != nil ||
		hashJSON(*candidateRuntime) != repair.CurrentCandidateRuntimeHash() {
		return fencing.RuntimeBinding{}, fmt.Errorf("fencing: storage closure candidate runtime differs from its current trusted incarnation")
	}
	return *candidateRuntime, nil
}
