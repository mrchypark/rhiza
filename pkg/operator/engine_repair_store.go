package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const maxEngineRepairRecordBytes = 1 << 20

// EngineRepairStore binds immutable repair evidence to the canonical root by
// updating Ledger.EngineRepairs with AuthorityStateStore's exact-version CAS.
// Evidence written without a root reference is an inert orphan.
type EngineRepairStore struct {
	authority *AuthorityStateStore
	uid       string
}

func NewEngineRepairStore(authority *AuthorityStateStore, statefulSetUID string) (*EngineRepairStore, error) {
	if authority == nil || authority.bucket == nil || statefulSetUID == "" {
		return nil, fmt.Errorf("fencing: canonical engine repair store requires authority and root identity")
	}
	return &EngineRepairStore{authority: authority, uid: statefulSetUID}, nil
}

// CreateOrReadPendingEngineRepair records an exact physical request in the
// canonical root. The authority assigns operation identity; the request does
// not add a Ledger.Run or grant a startup role.
func (s *EngineRepairStore) CreateOrReadPendingEngineRepair(ctx context.Context, proposal fencing.PendingEngineRepairProposal) (fencing.PendingEngineRepairRequest, error) {
	request, err := fencing.NewPendingEngineRepairRequest(proposal)
	if err != nil || ctx == nil || s == nil || s.authority == nil || proposal.StatefulSetUID != s.uid {
		return fencing.PendingEngineRepairRequest{}, errors.Join(err, fmt.Errorf("fencing: invalid pending engine repair request"))
	}
	key := request.SlotKey()
	for range 8 {
		state, version, err := s.authority.ReadRoot(ctx, s.uid, proposal.SourceClusterID)
		if err != nil {
			return fencing.PendingEngineRepairRequest{}, err
		}
		if state.Ledger.ClosedBy != "" {
			return fencing.PendingEngineRepairRequest{}, fencing.ErrClosed
		}
		if err := validatePendingEngineRepairRequests(state.Ledger, s.uid, proposal.SourceClusterID); err != nil {
			return fencing.PendingEngineRepairRequest{}, err
		}
		if err := validatePendingEngineRepairSourceWAL(state.Ledger, request); err != nil {
			return fencing.PendingEngineRepairRequest{}, err
		}
		if current, ok := state.Ledger.PendingEngineRepairs[key]; ok {
			if current.RequestDigest == request.RequestDigest && current == request {
				return current, nil
			}
			return fencing.PendingEngineRepairRequest{}, fmt.Errorf("fencing: stable ordinal already has a different pending repair request")
		}
		if err := validateNoLiveEngineRepairForSource(state.Ledger, proposal.SourceClusterID, ""); err != nil {
			return fencing.PendingEngineRepairRequest{}, err
		}
		for _, current := range state.Ledger.PendingEngineRepairs {
			if current.BootstrapIdentity.RunID == request.BootstrapIdentity.RunID {
				return fencing.PendingEngineRepairRequest{}, fmt.Errorf("fencing: physical repair run is already pending for another slot")
			}
		}
		if _, exists := state.Ledger.Runs[proposal.BootstrapIdentity.RunID]; exists {
			return fencing.PendingEngineRepairRequest{}, fmt.Errorf("fencing: pending repair physical run is already registered")
		}
		for _, reference := range state.Ledger.EngineRepairs {
			if reference.Binding.SourceClusterID == proposal.SourceClusterID && reference.Binding.OldMemberID == proposal.OldMemberID {
				return fencing.PendingEngineRepairRequest{}, fmt.Errorf("fencing: old-member repair slot is already reserved")
			}
		}
		used, err := state.Ledger.EngineRepairCapacityUse()
		if err != nil {
			return fencing.PendingEngineRepairRequest{}, err
		}
		if used >= fencing.MaxEngineRepairIncarnations {
			return fencing.PendingEngineRepairRequest{}, fmt.Errorf("fencing: engine repair incarnation capacity exceeded")
		}
		if state.Ledger.PendingEngineRepairs == nil {
			state.Ledger.PendingEngineRepairs = make(map[string]fencing.PendingEngineRepairRequest)
		}
		state.Ledger.PendingEngineRepairs[key] = request
		if _, err := state.Ledger.EngineRepairCapacityUse(); err != nil {
			return fencing.PendingEngineRepairRequest{}, err
		}
		if err := s.authority.WriteRoot(ctx, s.uid, proposal.SourceClusterID, state, version); err == nil {
			return request, nil
		} else if errors.Is(err, errAuthorityStateConflict) {
			continue
		} else {
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, proposal.SourceClusterID)
			if readErr == nil && confirmed.Ledger.PendingEngineRepairs[key] == request {
				return request, nil
			}
			return fencing.PendingEngineRepairRequest{}, errors.Join(err, readErr)
		}
	}
	return fencing.PendingEngineRepairRequest{}, fencing.ErrUnavailable
}

// ListPendingEngineRepairs returns only requests referenced by the existing
// canonical root. No request outside the root is discovered or adopted.
func (s *EngineRepairStore) ListPendingEngineRepairs(ctx context.Context, sourceClusterID string) ([]fencing.PendingEngineRepairRequest, error) {
	if ctx == nil || s == nil || s.authority == nil || sourceClusterID == "" {
		return nil, fmt.Errorf("fencing: canonical pending engine repair lookup is incomplete")
	}
	state, _, err := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
	if err != nil {
		return nil, err
	}
	if err := validatePendingEngineRepairRequests(state.Ledger, s.uid, sourceClusterID); err != nil {
		return nil, err
	}
	requests := make([]fencing.PendingEngineRepairRequest, 0, len(state.Ledger.PendingEngineRepairs))
	for _, request := range state.Ledger.PendingEngineRepairs {
		requests = append(requests, request)
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].PendingID < requests[j].PendingID })
	return requests, nil
}

func (s *EngineRepairStore) GetPendingEngineRepair(ctx context.Context, sourceClusterID, pendingID string) (fencing.PendingEngineRepairRequest, error) {
	if pendingID == "" {
		return fencing.PendingEngineRepairRequest{}, fmt.Errorf("fencing: pending engine repair ID is incomplete")
	}
	requests, err := s.ListPendingEngineRepairs(ctx, sourceClusterID)
	if err != nil {
		return fencing.PendingEngineRepairRequest{}, err
	}
	var found *fencing.PendingEngineRepairRequest
	for i := range requests {
		if requests[i].PendingID == pendingID {
			if found != nil {
				return fencing.PendingEngineRepairRequest{}, fmt.Errorf("fencing: pending engine repair ID is ambiguous")
			}
			copy := requests[i]
			found = &copy
		}
	}
	if found == nil {
		return fencing.PendingEngineRepairRequest{}, errAuthorityStateNotFound
	}
	return *found, nil
}

// AdoptPendingEngineRepair atomically replaces a pending physical request with
// its exact canonical repair reference. It does not register or grant the
// candidate; trusted runtime enrollment is pinned separately after the
// runtime agent has observed the assigned candidate.
func (s *EngineRepairStore) AdoptPendingEngineRepair(ctx context.Context, pendingID, expectedRequestDigest string, record fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error) {
	if ctx == nil || s == nil || s.authority == nil || pendingID == "" || expectedRequestDigest == "" ||
		record.ValidateAdoptionProposal() != nil || record.Binding.StatefulSetUID != s.uid || record.BootstrapRequestDigest != expectedRequestDigest {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: invalid pending engine repair adoption")
	}
	generationKey, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	for range 8 {
		state, version, err := s.authority.ReadRoot(ctx, s.uid, record.Binding.SourceClusterID)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if state.Ledger.ClosedBy != "" {
			return fencing.EngineRepairRecord{}, fencing.ErrClosed
		}
		if err := validateEngineRepairRootReferences(state, s.uid, record.Binding.SourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validatePendingEngineRepairRequests(state.Ledger, s.uid, record.Binding.SourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		pending, pendingKey, found := pendingEngineRepairByID(state.Ledger, pendingID)
		if !found {
			if current, ok := state.Ledger.EngineRepairs[generationKey]; ok && current.OperationID == record.Binding.OperationID {
				confirmed, readErr := s.readReferencedRecord(ctx, record.Binding.SourceClusterID, current)
				if readErr == nil && confirmed.AdoptionProposalDigest() == record.AdoptionProposalDigest() && confirmed.ValidateAgainstLedger(state.Ledger) == nil {
					return confirmed, nil
				}
				return fencing.EngineRepairRecord{}, errors.Join(readErr, fmt.Errorf("fencing: adopted engine repair readback differs"))
			}
			return fencing.EngineRepairRecord{}, errAuthorityStateNotFound
		}
		if pending.RequestDigest != expectedRequestDigest || !pending.MatchesRecord(record) {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: pending engine repair differs from adoption candidate")
		}
		if state.Ledger.ClosedBy != "" {
			return fencing.EngineRepairRecord{}, fencing.ErrClosed
		}
		if err := record.ValidateAdoptionProposal(); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if state.Ledger.EngineRepairs != nil {
			if current, ok := state.Ledger.EngineRepairs[generationKey]; ok {
				return fencing.EngineRepairRecord{}, errors.Join(current.Validate(), fmt.Errorf("fencing: repair generation already has a canonical reservation"))
			}
		}
		if err := validateNoLiveEngineRepairForSource(state.Ledger, record.Binding.SourceClusterID, pendingID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		incarnations, err := fencing.SortedRepairIncarnations(state.Ledger)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		adoptedRecord := record
		adoptedRecord.Incarnations = incarnations
		if err := adoptedRecord.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := s.persistImmutableRecord(ctx, adoptedRecord); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		rootReference := engineRepairRootReference(adoptedRecord)
		delete(state.Ledger.PendingEngineRepairs, pendingKey)
		if len(state.Ledger.PendingEngineRepairs) == 0 {
			state.Ledger.PendingEngineRepairs = nil
		}
		if state.Ledger.EngineRepairs == nil {
			state.Ledger.EngineRepairs = make(map[string]fencing.EngineRepairReference)
		}
		state.Ledger.EngineRepairs[generationKey] = rootReference
		if err := adoptedRecord.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := s.authority.WriteRoot(ctx, s.uid, record.Binding.SourceClusterID, state, version); err == nil {
			return s.readReferencedRecord(ctx, record.Binding.SourceClusterID, rootReference)
		} else if errors.Is(err, errAuthorityStateConflict) {
			continue
		} else {
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, record.Binding.SourceClusterID)
			if readErr == nil && confirmed.Ledger.EngineRepairs[generationKey] == rootReference &&
				!containsPendingEngineRepair(confirmed.Ledger, pendingID) {
				adopted, adoptedErr := s.readReferencedRecord(ctx, record.Binding.SourceClusterID, rootReference)
				if adoptedErr == nil && adopted.AdoptionProposalDigest() == record.AdoptionProposalDigest() && adopted.ValidateAgainstLedger(confirmed.Ledger) == nil {
					return adopted, nil
				}
				return fencing.EngineRepairRecord{}, errors.Join(adoptedErr, fmt.Errorf("fencing: uncertain pending repair adoption readback differs"))
			}
			return fencing.EngineRepairRecord{}, errors.Join(err, readErr)
		}
	}
	return fencing.EngineRepairRecord{}, fencing.ErrUnavailable
}

func validatePendingEngineRepairRequests(ledger fencing.Ledger, uid, source string) error {
	if _, err := ledger.EngineRepairCapacityUse(); err != nil {
		return err
	}
	seenSlots := make(map[string]string, len(ledger.PendingEngineRepairs))
	seenRuns := make(map[string]struct{}, len(ledger.PendingEngineRepairs))
	for key, request := range ledger.PendingEngineRepairs {
		if request.Validate() != nil || request.StatefulSetUID != uid || request.SourceClusterID != source || key != request.SlotKey() {
			return fmt.Errorf("fencing: canonical pending engine repair index differs")
		}
		if err := validatePendingEngineRepairSourceWAL(ledger, request); err != nil {
			return err
		}
		if prior, exists := seenSlots[key]; exists && prior != request.RequestDigest {
			return fmt.Errorf("fencing: conflicting pending engine repair slot")
		}
		seenSlots[key] = request.RequestDigest
		if _, exists := seenRuns[request.BootstrapIdentity.RunID]; exists {
			return fmt.Errorf("fencing: duplicate pending engine repair run")
		}
		seenRuns[request.BootstrapIdentity.RunID] = struct{}{}
	}
	return nil
}

func validatePendingEngineRepairSourceWAL(ledger fencing.Ledger, request fencing.PendingEngineRepairRequest) error {
	for _, run := range ledger.Runs {
		if run.Identity.NodeID == request.OldMemberID && run.Identity.WALIdentity == request.SourceOldWALIdentity {
			return nil
		}
	}
	return fmt.Errorf("fencing: pending repair does not match a canonical registered old-member WAL")
}

func pendingEngineRepairByID(ledger fencing.Ledger, pendingID string) (fencing.PendingEngineRepairRequest, string, bool) {
	var found fencing.PendingEngineRepairRequest
	var foundKey string
	for key, request := range ledger.PendingEngineRepairs {
		if request.PendingID != pendingID {
			continue
		}
		if foundKey != "" {
			return fencing.PendingEngineRepairRequest{}, "", false
		}
		found, foundKey = request, key
	}
	return found, foundKey, foundKey != ""
}

func containsPendingEngineRepair(ledger fencing.Ledger, pendingID string) bool {
	_, _, found := pendingEngineRepairByID(ledger, pendingID)
	return found
}

// PinCandidateRuntime records the exact runtime-agent enrollment and creates
// the matching pending learner Run in one canonical-root CAS. Callers must
// obtain runtime only from the trusted RuntimeAgent after the candidate has
// received its authority-assigned identity; this method never accepts a hash
// as a substitute for that enrollment.
func (s *EngineRepairStore) PinCandidateRuntime(ctx context.Context, sourceClusterID, operationID, expectedDigest string, runtime fencing.RuntimeBinding) (fencing.EngineRepairRecord, error) {
	if ctx == nil || s == nil || s.authority == nil || sourceClusterID == "" || operationID == "" || expectedDigest == "" {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate runtime pin request is incomplete")
	}
	for range 8 {
		state, version, err := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairMutationBarrier(state.Ledger, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairRootReferences(state, s.uid, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		var reference fencing.EngineRepairReference
		found := false
		for _, current := range state.Ledger.EngineRepairs {
			if current.OperationID != operationID {
				continue
			}
			if found {
				return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate runtime operation is ambiguous")
			}
			reference, found = current, true
		}
		if !found {
			return fencing.EngineRepairRecord{}, errAuthorityStateNotFound
		}
		prior, err := s.readReferencedRecord(ctx, sourceClusterID, reference)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if len(prior.CandidateContinuations) != 0 {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: initial candidate pin cannot resume after a fresh RunID continuation")
		}
		if prior.CandidateRuntimeBinding != nil {
			if *prior.CandidateRuntimeBinding == runtime && prior.CandidateRuntimeHash == fencing.RuntimeBindingDigest(runtime) {
				if err := prior.ValidateAgainstLedger(state.Ledger); err == nil {
					return prior, nil
				}
			}
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate runtime enrollment is already pinned or differs")
		}
		if prior.Digest() != expectedDigest || prior.Phase != fencing.RepairCandidatePrepared || prior.RemovalOperation != nil ||
			prior.CandidateRuntimeHash != "" || prior.CandidateRuntimeBinding != nil ||
			runtime.StatefulSetUID != prior.Binding.StatefulSetUID || runtime.Validate(prior.CandidateIdentity) != nil {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate runtime enrollment differs from its prepared reservation")
		}
		if _, exists := state.Ledger.Runs[prior.CandidateIdentity.RunID]; exists {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate run exists before its trusted runtime pin")
		}
		next := prior
		runtimeCopy := runtime
		next.CandidateRuntimeBinding = &runtimeCopy
		next.CandidateRuntimeHash = fencing.RuntimeBindingDigest(runtime)
		if err := next.Validate(); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := s.persistImmutableRecord(ctx, next); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		runIdentity := next.CandidateIdentity
		runIdentity.WALIdentity = ""
		if err := state.Ledger.RegisterEngineRepairCandidate(runIdentity, runtime, engineRepairRunProvenance(next)); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		state.Ledger.RequireRuntimeBinding = true
		if err := next.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		updated := engineRepairRootReference(next)
		generationKey, err := next.Binding.EngineRepairGenerationKey()
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		state.Ledger.EngineRepairs[generationKey] = updated
		if err := s.authority.WriteRoot(ctx, s.uid, sourceClusterID, state, version); err == nil {
			return s.readReferencedRecord(ctx, sourceClusterID, updated)
		} else if errors.Is(err, errAuthorityStateConflict) {
			continue
		} else {
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
			if readErr == nil && confirmed.Ledger.EngineRepairs[generationKey] == updated {
				pinned, pinErr := s.readReferencedRecord(ctx, sourceClusterID, updated)
				if pinErr == nil && pinned.Digest() == next.Digest() && pinned.ValidateAgainstLedger(confirmed.Ledger) == nil {
					return pinned, nil
				}
				return fencing.EngineRepairRecord{}, errors.Join(pinErr, fmt.Errorf("fencing: candidate runtime pin readback differs"))
			}
			return fencing.EngineRepairRecord{}, errors.Join(err, readErr)
		}
	}
	return fencing.EngineRepairRecord{}, fencing.ErrUnavailable
}

// ContinueCandidateRuntime registers a fresh supervisor run for the same
// authority-assigned learner candidate. The prior run and runtime remain
// immutable; the canonical root reference advances only after the new run and
// its trusted runtime enrollment are persisted together.
func (s *EngineRepairStore) ContinueCandidateRuntime(ctx context.Context, sourceClusterID, operationID, expectedDigest, priorRunID string, nextIdentity fencing.Identity, runtime fencing.RuntimeBinding) (fencing.EngineRepairRecord, error) {
	if ctx == nil || s == nil || s.authority == nil || sourceClusterID == "" || operationID == "" || expectedDigest == "" || priorRunID == "" {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate continuation request is incomplete")
	}
	for range 8 {
		state, version, err := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairMutationBarrier(state.Ledger, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairRootReferences(state, s.uid, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		var reference fencing.EngineRepairReference
		found := false
		for _, current := range state.Ledger.EngineRepairs {
			if current.OperationID != operationID {
				continue
			}
			if found {
				return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate continuation operation is ambiguous")
			}
			reference, found = current, true
		}
		if !found {
			return fencing.EngineRepairRecord{}, errAuthorityStateNotFound
		}
		prior, err := s.readReferencedRecord(ctx, sourceClusterID, reference)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := prior.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		currentIdentity := prior.CurrentCandidateIdentity()
		currentRuntime := prior.CurrentCandidateRuntime()
		if len(prior.CandidateContinuations) > 0 {
			last := prior.CandidateContinuations[len(prior.CandidateContinuations)-1]
			if last.PriorRunID == priorRunID && last.ExpectedRecordDigest == expectedDigest && last.Identity == nextIdentity && last.RuntimeBinding == runtime {
				if prior.ValidateAgainstLedger(state.Ledger) == nil {
					return prior, nil
				}
				return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate continuation readback differs from canonical ledger")
			}
		}
		if prior.Digest() != expectedDigest || currentIdentity.RunID != priorRunID || currentRuntime == nil ||
			prior.Phase == fencing.RepairActive || prior.Phase == fencing.RepairGenerationClosing ||
			reference.CandidateIdentity != currentIdentity || runtime.Validate(nextIdentity) != nil ||
			runtime.StatefulSetUID != prior.Binding.StatefulSetUID || runtime == *currentRuntime ||
			nextIdentity.SourceClusterID != currentIdentity.SourceClusterID || nextIdentity.NodeID != currentIdentity.NodeID ||
			nextIdentity.PodName != currentIdentity.PodName || nextIdentity.PodUID != currentIdentity.PodUID ||
			nextIdentity.WALIdentity != currentIdentity.WALIdentity || nextIdentity.AssignmentID != currentIdentity.AssignmentID ||
			nextIdentity.RunID == "" || nextIdentity.RunID == priorRunID {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate continuation differs from the current immutable reservation")
		}
		oldRun, exists := state.Ledger.Runs[priorRunID]
		unboundPriorIdentity := currentIdentity
		unboundPriorIdentity.WALIdentity = ""
		exactCandidateIdentity := exists && (oldRun.Identity == currentIdentity || oldRun.Identity == unboundPriorIdentity)
		if !exists || !exactCandidateIdentity || oldRun.Runtime == nil || *oldRun.Runtime != *currentRuntime ||
			fencing.RunRole(oldRun) != fencing.StartupRoleLearnerPendingRemove || oldRun.MutationProducerUID != "" ||
			oldRun.MutationScopeHash != "" || oldRun.MutationProfile != "" || oldRun.MutationStageIncarnation != "" ||
			oldRun.Stop != nil || oldRun.Drain != nil || oldRun.Exited || oldRun.NeverBoundExit != nil || oldRun.Promotion != nil {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: prior candidate run is not the exact unadmitted learner incarnation")
		}
		if _, exists := state.Ledger.Runs[nextIdentity.RunID]; exists {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate continuation RunID already exists")
		}
		next := prior
		next.CandidateContinuations = append(append([]fencing.EngineRepairCandidateContinuation(nil), prior.CandidateContinuations...), fencing.EngineRepairCandidateContinuation{
			PriorRunID: priorRunID, ExpectedRecordDigest: expectedDigest, Identity: nextIdentity,
			RuntimeBinding: runtime, RuntimeHash: fencing.RuntimeBindingDigest(runtime),
		})
		if err := next.Validate(); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := s.persistImmutableRecord(ctx, next); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		runIdentity := nextIdentity
		runIdentity.WALIdentity = ""
		if err := state.Ledger.RegisterEngineRepairCandidate(runIdentity, runtime, engineRepairRunProvenance(next)); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		state.Ledger.RequireRuntimeBinding = true
		if err := next.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		updated := engineRepairRootReference(next)
		generationKey, err := next.Binding.EngineRepairGenerationKey()
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		state.Ledger.EngineRepairs[generationKey] = updated
		if _, err := state.Ledger.EngineRepairCapacityUse(); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := s.authority.WriteRoot(ctx, s.uid, sourceClusterID, state, version); err == nil {
			return s.readReferencedRecord(ctx, sourceClusterID, updated)
		} else if errors.Is(err, errAuthorityStateConflict) {
			continue
		} else {
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
			if readErr == nil && confirmed.Ledger.EngineRepairs[generationKey] == updated {
				pinned, pinErr := s.readReferencedRecord(ctx, sourceClusterID, updated)
				if pinErr == nil && pinned.Digest() == next.Digest() && pinned.ValidateAgainstLedger(confirmed.Ledger) == nil {
					return pinned, nil
				}
				return fencing.EngineRepairRecord{}, errors.Join(pinErr, fmt.Errorf("fencing: candidate continuation readback differs"))
			}
			return fencing.EngineRepairRecord{}, errors.Join(err, readErr)
		}
	}
	return fencing.EngineRepairRecord{}, fencing.ErrUnavailable
}

// RestartActiveCandidateRuntime enrolls a fresh process incarnation of an
// already-active engine-owned candidate and advances only the canonical root's
// current-Run reference. The immutable repair record and its original
// candidate identity are not rewritten.
func (s *EngineRepairStore) RestartActiveCandidateRuntime(ctx context.Context, sourceClusterID, operationID, expectedDigest, priorRunID string, nextIdentity fencing.Identity, runtime fencing.RuntimeBinding) (fencing.EngineRepairRecord, error) {
	if ctx == nil || s == nil || s.authority == nil || sourceClusterID == "" || operationID == "" || expectedDigest == "" || priorRunID == "" {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active candidate restart request is incomplete")
	}
	for range 8 {
		state, version, err := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairMutationBarrier(state.Ledger, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairRootReferences(state, s.uid, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		var key string
		var reference fencing.EngineRepairReference
		found := false
		for _, current := range state.Ledger.EngineRepairs {
			if current.OperationID != operationID {
				continue
			}
			if found {
				return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active candidate restart operation is ambiguous")
			}
			key, reference, found = mustEngineRepairKey(current), current, true
		}
		if !found {
			return fencing.EngineRepairRecord{}, errAuthorityStateNotFound
		}
		prior, err := s.readReferencedRecord(ctx, sourceClusterID, reference)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if prior.Digest() != expectedDigest || prior.Phase != fencing.RepairActive ||
			prior.Binding.OperationID != operationID || prior.Binding.StatefulSetUID != s.uid {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active candidate restart differs from the canonical repair record")
		}
		if err := prior.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if !validEngineRepairRootCandidateReference(prior, state.Ledger, reference) {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active candidate reference is not validated by the canonical root")
		}
		priorIdentity := reference.CandidateIdentity
		if priorIdentity.RunID != priorRunID {
			if reference.CandidateIdentity == nextIdentity {
				if run, ok := state.Ledger.Runs[nextIdentity.RunID]; ok && run.Identity == nextIdentity && run.Runtime != nil && *run.Runtime == runtime &&
					validEngineRepairRootCandidateReference(prior, state.Ledger, reference) {
					return prior, nil
				}
			}
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active restart prior Run is not the canonical current incarnation")
		}
		if nextIdentity.RunID == priorRunID || !sameEngineRepairCandidateRun(prior.CurrentCandidateIdentity(), priorIdentity) ||
			!sameEngineRepairCandidateRun(prior.CurrentCandidateIdentity(), nextIdentity) || nextIdentity.WALIdentity == "" ||
			runtime.Validate(nextIdentity) != nil || runtime.StatefulSetUID != s.uid {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active restart identity differs from the pinned candidate")
		}
		priorRun, ok := state.Ledger.Runs[priorRunID]
		if !ok || priorRun.Identity != priorIdentity || priorRun.Runtime == nil || priorRun.Stop != nil || priorRun.Drain != nil || priorRun.Exited ||
			priorRun.NeverBoundExit != nil || fencing.RunRole(priorRun) != fencing.StartupRoleVoterActive || priorRun.Promotion == nil ||
			priorRun.Promotion.OperationID != engineRepairAddOperationID(operationID) || !validEvidenceDigest(priorRun.Promotion.EvidenceHash) ||
			priorRun.EngineRepairProvenance == nil || priorRun.EngineRepairProvenance.ValidateForRun(priorRun) != nil ||
			priorRun.EngineRepairProvenance.OperationID != operationID || priorRun.EngineRepairProvenance.IdentityDigest != prior.IdentityDigest() || runtime == *priorRun.Runtime {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: prior active Run is not the exact healthy trusted incarnation")
		}
		for runID, run := range state.Ledger.Runs {
			if runID == priorRunID || run.EngineRepairProvenance == nil || run.EngineRepairProvenance.OperationID != operationID ||
				run.EngineRepairProvenance.IdentityDigest != prior.IdentityDigest() {
				continue
			}
			if run.Runtime == nil || *run.Runtime == runtime {
				return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active restart reuses an earlier runtime enrollment")
			}
		}
		if _, exists := state.Ledger.Runs[nextIdentity.RunID]; exists {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active restart RunID is already registered")
		}
		if state.Ledger.RequireMutationJournal {
			if priorRun.MutationProducerUID != stableMutationProducerUID(*priorRun.Runtime) ||
				!validEvidenceDigest(priorRun.MutationScopeHash) || !fencing.MutationProfileSupported(priorRun.MutationProfile) ||
				!validEvidenceDigest(priorRun.MutationStageIncarnation) {
				return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active restart source lacks its exact mutation producer binding")
			}
			if err := s.ensureActiveRestartProducer(ctx, nextIdentity, runtime); err != nil {
				return fencing.EngineRepairRecord{}, err
			}
		}
		if err := state.Ledger.RegisterEngineRepairActiveRestart(prior, priorIdentity, nextIdentity, runtime); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		updated := reference
		updated.CandidateIdentity = nextIdentity
		if err := updated.Validate(); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		state.Ledger.EngineRepairs[key] = updated
		if err := prior.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if used, err := state.Ledger.EngineRepairCapacityUse(); err != nil || used > fencing.MaxEngineRepairIncarnations {
			return fencing.EngineRepairRecord{}, errors.Join(err, fmt.Errorf("fencing: active restart exceeds the bounded incarnation inventory"))
		}
		if err := s.authority.WriteRoot(ctx, s.uid, sourceClusterID, state, version); err == nil {
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
			if readErr != nil || confirmed.Ledger.EngineRepairs[key] != updated || prior.ValidateAgainstLedger(confirmed.Ledger) != nil {
				return fencing.EngineRepairRecord{}, errors.Join(readErr, fmt.Errorf("fencing: active restart root reference readback differs"))
			}
			return prior, nil
		} else if errors.Is(err, errAuthorityStateConflict) {
			continue
		} else {
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
			if readErr == nil && confirmed.Ledger.EngineRepairs[key] == updated {
				if err := prior.ValidateAgainstLedger(confirmed.Ledger); err == nil && validEngineRepairRootCandidateReference(prior, confirmed.Ledger, updated) {
					return prior, nil
				}
			}
			return fencing.EngineRepairRecord{}, errors.Join(err, readErr, fmt.Errorf("fencing: active restart root reference readback differs"))
		}
	}
	return fencing.EngineRepairRecord{}, fencing.ErrUnavailable
}

// ensureActiveRestartProducer persists a deterministic Reserved producer with
// the already-pinned candidate WAL before the root CAS registers the active
// Run. A failed root CAS leaves only inert reserved state; it cannot admit
// mutations or issue a grant.
func (s *EngineRepairStore) ensureActiveRestartProducer(ctx context.Context, identity fencing.Identity, runtime fencing.RuntimeBinding) error {
	wantUID := stableMutationProducerUID(runtime)
	producer, producerUID, _, err := s.authority.ReadProducer(ctx, runtime)
	if err == nil {
		if producerUID != wantUID || producer.Runtime != runtime || producer.WALIdentity != identity.WALIdentity ||
			producer.Phase != fencing.MutationReserved || producer.LastAdmitted != 0 || len(producer.Entries) != 0 ||
			producer.EvidenceFault != "" {
			return fmt.Errorf("fencing: active restart producer reservation differs from the exact candidate")
		}
		return nil
	}
	if !errors.Is(err, errAuthorityStateNotFound) {
		return err
	}
	prepared, err := fencing.NewMutationProducer(identity, runtime)
	if err != nil {
		return err
	}
	if err := prepared.BindWAL(identity); err != nil {
		return err
	}
	writeErr := s.authority.WriteProducer(ctx, runtime, *prepared, nil)
	if writeErr == nil {
		return nil
	}
	confirmed, confirmedUID, _, readErr := s.authority.ReadProducer(ctx, runtime)
	if readErr == nil && confirmedUID == wantUID && confirmed.Runtime == runtime && confirmed.WALIdentity == identity.WALIdentity &&
		confirmed.Phase == fencing.MutationReserved && confirmed.LastAdmitted == 0 && len(confirmed.Entries) == 0 && confirmed.EvidenceFault == "" {
		return nil
	}
	return errors.Join(writeErr, readErr, fmt.Errorf("fencing: active restart producer reservation could not be confirmed"))
}

// ReadEngineRepairCurrentCandidate returns the immutable repair record and
// the latest candidate Run identity pinned by the canonical root reference.
func (s *EngineRepairStore) ReadEngineRepairCurrentCandidate(ctx context.Context, sourceClusterID, operationID string) (fencing.EngineRepairRecord, fencing.Identity, fencing.RuntimeBinding, error) {
	if ctx == nil || s == nil || s.authority == nil || sourceClusterID == "" || operationID == "" {
		return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, fmt.Errorf("fencing: current candidate lookup is incomplete")
	}
	state, _, err := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
	if err != nil {
		return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, err
	}
	if err := validateEngineRepairRootReferences(state, s.uid, sourceClusterID); err != nil {
		return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, err
	}
	var reference *fencing.EngineRepairReference
	for _, candidate := range state.Ledger.EngineRepairs {
		if candidate.OperationID != operationID {
			continue
		}
		if reference != nil {
			return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, fmt.Errorf("fencing: canonical engine repair operation is ambiguous")
		}
		copy := candidate
		reference = &copy
	}
	if reference == nil {
		return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, errAuthorityStateNotFound
	}
	run, ok := state.Ledger.Runs[reference.CandidateIdentity.RunID]
	record, err := s.readReferencedRecord(ctx, sourceClusterID, *reference)
	if err != nil {
		return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, err
	}
	if err := record.ValidateAgainstLedger(state.Ledger); err != nil {
		return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, err
	}
	if !ok || (run.Identity != reference.CandidateIdentity && !isExactPrebindEngineRepairCandidate(record, run)) || run.Runtime == nil ||
		!validEngineRepairRootCandidateReference(record, state.Ledger, *reference) {
		return fencing.EngineRepairRecord{}, fencing.Identity{}, fencing.RuntimeBinding{}, fmt.Errorf("fencing: current candidate root reference lacks its exact trusted Run")
	}
	return record, run.Identity, *run.Runtime, nil
}

// isExactPrebindEngineRepairCandidate permits only the authenticated learner
// Run that has not bound its WAL yet. The canonical repair reference and the
// server-written provenance still pin the expected WAL, assignment, runtime,
// Pod incarnation, and operation. This exception is not valid for active or
// otherwise admitted Runs.
func isExactPrebindEngineRepairCandidate(record fencing.EngineRepairRecord, run fencing.Run) bool {
	expected := record.CurrentCandidateIdentity()
	runtime := record.CurrentCandidateRuntime()
	provenance := run.EngineRepairProvenance
	if runtime == nil || provenance == nil || run.Identity.WALIdentity != "" ||
		fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove || run.Promotion != nil ||
		run.MutationProducerUID != "" || run.MutationScopeHash != "" || run.MutationProfile != "" || run.MutationStageIncarnation != "" ||
		run.Stop != nil || run.Drain != nil || run.Exited || run.NeverBoundExit != nil || run.IndependentTerminal != nil ||
		run.Runtime == nil || *run.Runtime != *runtime || run.Runtime.Validate(run.Identity) != nil {
		return false
	}
	unbound := expected
	unbound.WALIdentity = ""
	return run.Identity == unbound && provenance.ValidateForRun(run) == nil &&
		provenance.OperationID == record.Binding.OperationID && provenance.IdentityDigest == record.IdentityDigest() &&
		provenance.CandidateIdentity == expected && provenance.RuntimeBindingDigest == record.CurrentCandidateRuntimeHash()
}

func sameEngineRepairCandidateRun(candidate, actual fencing.Identity) bool {
	candidate.RunID = actual.RunID
	return candidate == actual
}

func validEngineRepairRootCandidateReference(record fencing.EngineRepairRecord, ledger fencing.Ledger, reference fencing.EngineRepairReference) bool {
	if !sameEngineRepairCandidateRun(record.CurrentCandidateIdentity(), reference.CandidateIdentity) {
		return false
	}
	if reference.CandidateIdentity == record.CurrentCandidateIdentity() {
		if record.Phase != fencing.RepairActive && !(record.Phase == fencing.RepairGenerationClosing && record.ClosingFrom == fencing.RepairActive) {
			return true
		}
		run, ok := ledger.Runs[reference.CandidateIdentity.RunID]
		return ok && run.Identity == reference.CandidateIdentity && run.Runtime != nil && record.CurrentCandidateRuntime() != nil &&
			*run.Runtime == *record.CurrentCandidateRuntime() && run.EngineRepairProvenance != nil &&
			run.EngineRepairProvenance.ValidateForRun(run) == nil && run.EngineRepairProvenance.OperationID == record.Binding.OperationID &&
			run.EngineRepairProvenance.IdentityDigest == record.IdentityDigest() && fencing.RunRole(run) == fencing.StartupRoleVoterActive &&
			run.Promotion != nil && run.Promotion.OperationID == engineRepairAddOperationID(record.Binding.OperationID) &&
			validEvidenceDigest(run.Promotion.EvidenceHash) && run.Stop == nil && run.Drain == nil && !run.Exited && run.NeverBoundExit == nil
	}
	if record.Phase != fencing.RepairActive && !(record.Phase == fencing.RepairGenerationClosing && record.ClosingFrom == fencing.RepairActive) {
		return false
	}
	run, ok := ledger.Runs[reference.CandidateIdentity.RunID]
	if !ok || run.Identity != reference.CandidateIdentity || run.EngineRepairProvenance == nil ||
		run.EngineRepairProvenance.ValidateForRun(run) != nil || run.EngineRepairProvenance.OperationID != record.Binding.OperationID ||
		run.EngineRepairProvenance.IdentityDigest != record.IdentityDigest() || fencing.RunRole(run) != fencing.StartupRoleVoterActive ||
		run.Promotion == nil || run.Promotion.OperationID != engineRepairAddOperationID(record.Binding.OperationID) ||
		run.Stop != nil || run.Drain != nil || run.Exited || run.NeverBoundExit != nil {
		return false
	}
	return true
}

func mustEngineRepairKey(reference fencing.EngineRepairReference) string {
	key, _ := reference.Binding.EngineRepairGenerationKey()
	return key
}

func engineRepairRunProvenance(record fencing.EngineRepairRecord) fencing.EngineRepairRunProvenance {
	return fencing.EngineRepairRunProvenance{
		Version: fencing.EngineRepairRunProvenanceVersion, OperationID: record.Binding.OperationID,
		IdentityDigest: record.IdentityDigest(), CandidateIdentity: record.CurrentCandidateIdentity(),
		RuntimeBindingDigest: record.CurrentCandidateRuntimeHash(),
	}
}

// CreateOrReadEngineRepair stores the immutable record first, then installs its
// authority-bearing reference in the canonical root with a root-version CAS.
// Callers must authenticate and validate the engine freeze/reservation tuple
// before invoking this method; the root ledger does not itself contain a live
// QuePaxa ConfigID or abort-slot observation.
func (s *EngineRepairStore) CreateOrReadEngineRepair(ctx context.Context, record fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error) {
	if ctx == nil || s == nil || s.authority == nil || record.Validate() != nil || record.Binding.StatefulSetUID != s.uid ||
		record.CandidateRuntimeBinding != nil || record.CandidateRuntimeHash != "" || len(record.CandidateContinuations) != 0 {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: invalid canonical engine repair reservation")
	}
	if err := s.persistImmutableRecord(ctx, record); err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	generationKey, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	want := engineRepairRootReference(record)
	for range 8 {
		state, version, err := s.authority.ReadRoot(ctx, s.uid, record.Binding.SourceClusterID)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if state.Ledger.ClosedBy != "" {
			return fencing.EngineRepairRecord{}, fencing.ErrClosed
		}
		if err := validateEngineRepairRootReferences(state, s.uid, record.Binding.SourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if state.Ledger.EngineRepairs != nil {
			if current, ok := state.Ledger.EngineRepairs[generationKey]; ok {
				if err := current.Validate(); err != nil || current.OperationID != record.Binding.OperationID || current.IdentityDigest != record.IdentityDigest() {
					return fencing.EngineRepairRecord{}, errors.Join(err, fmt.Errorf("fencing: canonical root already reserves this repair generation"))
				}
				confirmed, err := s.readReferencedRecord(ctx, record.Binding.SourceClusterID, current)
				if err != nil {
					return fencing.EngineRepairRecord{}, err
				}
				if confirmed.IdentityDigest() != record.IdentityDigest() {
					return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: canonical reservation identity differs from idempotent request")
				}
				if err := confirmed.ValidateAgainstLedger(state.Ledger); err != nil {
					return fencing.EngineRepairRecord{}, err
				}
				if !validEngineRepairRootCandidateReference(confirmed, state.Ledger, current) {
					return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: idempotent repair reference has no exact current candidate Run")
				}
				return confirmed, nil
			}
		}
		if err := record.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateNoLiveEngineRepairForSource(state.Ledger, record.Binding.SourceClusterID, ""); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if state.Ledger.EngineRepairs == nil {
			state.Ledger.EngineRepairs = make(map[string]fencing.EngineRepairReference)
		}
		state.Ledger.EngineRepairs[generationKey] = want
		if _, err := state.Ledger.EngineRepairCapacityUse(); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := s.authority.WriteRoot(ctx, s.uid, record.Binding.SourceClusterID, state, version); err == nil {
			return s.readReferencedRecord(ctx, record.Binding.SourceClusterID, want)
		} else if errors.Is(err, errAuthorityStateConflict) {
			continue
		} else { // A failed response is never interpreted as a successful reservation
			// unless the fresh canonical root references these exact bytes.
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, record.Binding.SourceClusterID)
			if readErr == nil && confirmed.Ledger.EngineRepairs[generationKey] == want {
				return s.readReferencedRecord(ctx, record.Binding.SourceClusterID, want)
			}
			return fencing.EngineRepairRecord{}, errors.Join(err, readErr)
		}
	}
	return fencing.EngineRepairRecord{}, fencing.ErrUnavailable
}

// ReadEngineRepair returns only a record referenced by this exact canonical
// root. It does not discover or adopt an unreferenced evidence object.
func (s *EngineRepairStore) ReadEngineRepair(ctx context.Context, sourceClusterID, operationID string) (fencing.EngineRepairRecord, error) {
	if ctx == nil || s == nil || s.authority == nil || s.uid == "" || sourceClusterID == "" || operationID == "" {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: canonical engine repair lookup is incomplete")
	}
	state, _, err := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if err := validateEngineRepairRootReferences(state, s.uid, sourceClusterID); err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	var match *fencing.EngineRepairReference
	for _, reference := range state.Ledger.EngineRepairs {
		if reference.OperationID != operationID {
			continue
		}
		if match != nil {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: canonical engine repair operation is ambiguous")
		}
		copy := reference
		match = &copy
	}
	if match == nil {
		return fencing.EngineRepairRecord{}, errAuthorityStateNotFound
	}
	record, err := s.readReferencedRecord(ctx, sourceClusterID, *match)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if err := record.ValidateAgainstLedger(state.Ledger); err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	if !validEngineRepairRootCandidateReference(record, state.Ledger, *match) {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: current root Run reference lacks exact authority provenance")
	}
	return record, nil
}

// CompareAndSwapEngineRepair advances a referenced record without changing its
// immutable reservation. New content is made durable and verified before the
// canonical root reference is conditionally replaced.
func (s *EngineRepairStore) CompareAndSwapEngineRepair(ctx context.Context, sourceClusterID string, expectedDigest string, next fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error) {
	if next.Phase == fencing.RepairActive {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active transition requires verified candidate activation")
	}
	return s.compareAndSwapEngineRepair(ctx, sourceClusterID, expectedDigest, next, nil)
}

type engineRepairActivationAuthorization struct {
	OperationID              string
	Candidate                fencing.Identity
	Runtime                  fencing.RuntimeBinding
	PromotionOperationID     string
	PromotionEvidenceHash    string
	MutationProducerUID      string
	MutationScopeHash        string
	MutationProfile          string
	MutationStageIncarnation string
}

func (s *EngineRepairStore) compareAndSwapVerifiedEngineRepairActivation(ctx context.Context, sourceClusterID, expectedDigest string, next fencing.EngineRepairRecord, authorization engineRepairActivationAuthorization) (fencing.EngineRepairRecord, error) {
	currentCandidate := next.CurrentCandidateIdentity()
	currentRuntime := next.CurrentCandidateRuntime()
	if next.Phase != fencing.RepairActive || authorization.OperationID != next.Binding.OperationID || authorization.Candidate != currentCandidate ||
		currentRuntime == nil || authorization.Runtime != *currentRuntime ||
		authorization.PromotionOperationID != engineRepairAddOperationID(next.Binding.OperationID) ||
		!validEvidenceDigest(authorization.PromotionEvidenceHash) || authorization.MutationProducerUID == "" ||
		!validEvidenceDigest(authorization.MutationScopeHash) || authorization.MutationProfile == "" || authorization.MutationStageIncarnation == "" {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate activation authorization is incomplete")
	}
	return s.compareAndSwapEngineRepair(ctx, sourceClusterID, expectedDigest, next, &authorization)
}

func (s *EngineRepairStore) compareAndSwapEngineRepair(ctx context.Context, sourceClusterID string, expectedDigest string, next fencing.EngineRepairRecord, activation *engineRepairActivationAuthorization) (fencing.EngineRepairRecord, error) {
	if ctx == nil || s == nil || s.authority == nil || sourceClusterID == "" || expectedDigest == "" ||
		next.Validate() != nil || next.Binding.SourceClusterID != sourceClusterID || next.Binding.StatefulSetUID != s.uid {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: invalid canonical engine repair update")
	}
	generationKey, err := next.Binding.EngineRepairGenerationKey()
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	for range 8 {
		state, version, err := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairMutationBarrier(state.Ledger, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := validateEngineRepairRootReferences(state, s.uid, sourceClusterID); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		current, ok := state.Ledger.EngineRepairs[generationKey]
		if !ok {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: canonical engine repair reservation is missing")
		}
		if err := current.Validate(); err != nil || current.RecordDigest != expectedDigest || current.IdentityDigest != next.IdentityDigest() || current.OperationID != next.Binding.OperationID {
			return fencing.EngineRepairRecord{}, errors.Join(err, fmt.Errorf("fencing: canonical engine repair revision changed"))
		}
		prior, err := s.readReferencedRecord(ctx, sourceClusterID, current)
		if err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if err := prior.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if !validEngineRepairRootCandidateReference(prior, state.Ledger, current) {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: current root candidate reference is not validated by the canonical ledger")
		}
		if err := next.ValidateAgainstLedger(state.Ledger); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		if next.Phase == fencing.RepairActive {
			currentCandidate := next.CurrentCandidateIdentity()
			currentRuntime := next.CurrentCandidateRuntime()
			run, exists := state.Ledger.Runs[currentCandidate.RunID]
			if prior.Phase != fencing.RepairAdding || activation == nil || !exists ||
				run.Identity != activation.Candidate || run.Runtime == nil || currentRuntime == nil || *run.Runtime != activation.Runtime || *currentRuntime != activation.Runtime ||
				fencing.RunRole(run) != fencing.StartupRoleVoterActive || run.Promotion == nil ||
				run.Promotion.OperationID != activation.PromotionOperationID || run.Promotion.EvidenceHash != activation.PromotionEvidenceHash ||
				run.MutationProducerUID != activation.MutationProducerUID || run.MutationScopeHash != activation.MutationScopeHash ||
				run.MutationProfile != activation.MutationProfile || run.MutationStageIncarnation != activation.MutationStageIncarnation {
				return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: active transition lacks the exact verified candidate promotion")
			}
		} else if activation != nil {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: activation authorization used for a non-active transition")
		}
		freezeChanged := prior.Binding.FreezeSlot != 0 && (prior.Binding.FreezeSlot != next.Binding.FreezeSlot || prior.Binding.FreezeHash != next.Binding.FreezeHash)
		closingFromChanged := false
		if prior.Phase == fencing.RepairGenerationClosing {
			closingFromChanged = next.Phase != fencing.RepairGenerationClosing || next.ClosingFrom != prior.ClosingFrom
		} else if next.Phase == fencing.RepairGenerationClosing {
			closingFromChanged = next.ClosingFrom != prior.Phase
		}
		if prior.IdentityDigest() != next.IdentityDigest() || !engineRepairPhaseCanAdvance(prior.Phase, next.Phase) || freezeChanged ||
			closingFromChanged ||
			!sameCandidateRuntimePin(prior, next) ||
			!sameCandidateContinuationHistory(prior, next) ||
			!engineRepairOperationCanAdvance(prior.RemovalOperation, next.RemovalOperation) ||
			(prior.AddRequestHash != "" && prior.AddRequestHash != next.AddRequestHash) ||
			(prior.StorageProofDigest != "" && prior.StorageProofDigest != next.StorageProofDigest) ||
			(prior.RemoveTerminalDigest != "" && prior.RemoveTerminalDigest != next.RemoveTerminalDigest) {
			return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: engine repair update regresses or replaces immutable evidence")
		}
		updated := engineRepairRootReference(next)
		if prior.Phase == fencing.RepairActive &&
			current.CandidateIdentity != prior.CurrentCandidateIdentity() {
			updated.CandidateIdentity = current.CandidateIdentity
		}
		if err := s.persistImmutableRecord(ctx, next); err != nil {
			return fencing.EngineRepairRecord{}, err
		}
		state.Ledger.EngineRepairs[generationKey] = updated
		if err := s.authority.WriteRoot(ctx, s.uid, sourceClusterID, state, version); err == nil {
			return s.readReferencedRecord(ctx, sourceClusterID, updated)
		} else if errors.Is(err, errAuthorityStateConflict) {
			continue
		} else {
			confirmed, _, readErr := s.authority.ReadRoot(ctx, s.uid, sourceClusterID)
			if readErr == nil && confirmed.Ledger.EngineRepairs[generationKey] == updated {
				return s.readReferencedRecord(ctx, sourceClusterID, updated)
			}
			return fencing.EngineRepairRecord{}, errors.Join(err, readErr)
		}
	}
	return fencing.EngineRepairRecord{}, fencing.ErrUnavailable
}

func validateNoLiveEngineRepairForSource(ledger fencing.Ledger, sourceClusterID, exceptPendingID string) error {
	for _, pending := range ledger.PendingEngineRepairs {
		if pending.SourceClusterID == sourceClusterID && pending.PendingID != exceptPendingID {
			return fmt.Errorf("fencing: source already has a pending engine repair")
		}
	}
	for _, reference := range ledger.EngineRepairs {
		if reference.Binding.SourceClusterID != sourceClusterID {
			continue
		}
		// GenerationClosing is still a live reservation. DR closure is not
		// complete until the canonical root's ClosedBy CAS succeeds; only an
		// evidence-validated Active transition releases this source lock.
		if reference.Phase != fencing.RepairActive {
			return fmt.Errorf("fencing: source already has a live engine repair")
		}
	}
	return nil
}

// validateEngineRepairMutationBarrier is checked against the exact root
// revision that the caller will CAS. A close that wins first must stop every
// repair mutation; a repair CAS that wins first is included in the close CAS.
func validateEngineRepairMutationBarrier(ledger fencing.Ledger, sourceClusterID string) error {
	if ledger.ClosedBy != "" {
		return fencing.ErrClosed
	}
	for _, reference := range ledger.EngineRepairs {
		if reference.Binding.SourceClusterID == sourceClusterID && reference.Phase == fencing.RepairGenerationClosing {
			return fencing.ErrClosed
		}
	}
	return nil
}

func sameCandidateRuntimePin(prior, next fencing.EngineRepairRecord) bool {
	if prior.CandidateRuntimeBinding == nil || next.CandidateRuntimeBinding == nil {
		return prior.CandidateRuntimeBinding == nil && next.CandidateRuntimeBinding == nil &&
			prior.CandidateRuntimeHash == "" && next.CandidateRuntimeHash == ""
	}
	return *prior.CandidateRuntimeBinding == *next.CandidateRuntimeBinding && prior.CandidateRuntimeHash == next.CandidateRuntimeHash
}

func sameCandidateContinuationHistory(prior, next fencing.EngineRepairRecord) bool {
	if len(next.CandidateContinuations) < len(prior.CandidateContinuations) {
		return false
	}
	for i := range prior.CandidateContinuations {
		if prior.CandidateContinuations[i] != next.CandidateContinuations[i] {
			return false
		}
	}
	return true
}

func engineRepairOperationCanAdvance(prior, next *quepaxa.EngineRemoval) bool {
	if prior == nil {
		return true // The validated frozen operation is pinned at its first CAS.
	}
	return next != nil && fencing.SameEngineRemovalOperation(*prior, *next)
}

func engineRepairRootReference(record fencing.EngineRepairRecord) fencing.EngineRepairReference {
	digest := record.Digest()
	generation, _ := record.Binding.EngineRepairGenerationKey()
	return fencing.EngineRepairReference{Version: fencing.EngineRepairRecordVersion, OperationID: record.Binding.OperationID, Binding: record.Binding, CandidateIdentity: record.CurrentCandidateIdentity(),
		Phase: record.Phase, ClosingFrom: record.ClosingFrom,
		IdentityDigest: record.IdentityDigest(), RecordDigest: digest, RecordKey: engineRepairObjectKey(generation, digest)}
}

func engineRepairObjectKey(generation, digest string) string {
	return "engine-repairs/" + generation + "/" + digest + ".json"
}

func (s *EngineRepairStore) persistImmutableRecord(ctx context.Context, record fencing.EngineRepairRecord) error {
	body, err := json.Marshal(record)
	if err != nil || len(body) == 0 || len(body) > maxEngineRepairRecordBytes {
		return errors.Join(err, fmt.Errorf("fencing: engine repair evidence is invalid or over capacity"))
	}
	generation, err := record.Binding.EngineRepairGenerationKey()
	if err != nil {
		return err
	}
	store, err := NewAuthorityEvidenceStore(s.authority.bucket, s.authority.prefix, s.uid, record.Binding.SourceClusterID)
	if err != nil {
		return err
	}
	key := engineRepairObjectKey(generation, record.Digest())
	if _, err := store.CreateImmutable(ctx, key, body); err != nil {
		return err
	}
	// Read back even after a successful create: the canonical root must never
	// reference evidence that merely received an ambiguous object-store ACK.
	r, version, err := store.Read(ctx, key)
	if err != nil {
		return err
	}
	got, readErr := io.ReadAll(io.LimitReader(r, int64(len(body))+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || version == nil || !bytes.Equal(got, body) {
		return errors.Join(readErr, closeErr, fmt.Errorf("fencing: engine repair evidence readback differs"))
	}
	return nil
}

func (s *EngineRepairStore) readReferencedRecord(ctx context.Context, sourceClusterID string, reference fencing.EngineRepairReference) (fencing.EngineRepairRecord, error) {
	if err := reference.Validate(); err != nil || sourceClusterID == "" || s == nil || s.authority == nil {
		return fencing.EngineRepairRecord{}, errors.Join(err, fmt.Errorf("fencing: canonical engine repair reference is invalid"))
	}
	store, err := NewAuthorityEvidenceStore(s.authority.bucket, s.authority.prefix, s.uid, sourceClusterID)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	r, version, err := store.Read(ctx, reference.RecordKey)
	if err != nil {
		return fencing.EngineRepairRecord{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxEngineRepairRecordBytes+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || version == nil || len(data) == 0 || len(data) > maxEngineRepairRecordBytes {
		return fencing.EngineRepairRecord{}, errors.Join(readErr, closeErr, fmt.Errorf("fencing: canonical engine repair evidence is unreadable"))
	}
	var record fencing.EngineRepairRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil || decoder.Decode(new(any)) != io.EOF || record.Validate() != nil ||
		record.Binding.SourceClusterID != sourceClusterID || record.Binding.StatefulSetUID != s.uid ||
		record.Binding != reference.Binding || record.Binding.OperationID != reference.OperationID || record.IdentityDigest() != reference.IdentityDigest || record.Digest() != reference.RecordDigest ||
		engineRepairObjectKey(mustRepairGenerationKey(record), record.Digest()) != reference.RecordKey {
		return fencing.EngineRepairRecord{}, errors.Join(err, fmt.Errorf("fencing: canonical engine repair evidence differs from its root reference"))
	}
	if record.Phase != reference.Phase || record.ClosingFrom != reference.ClosingFrom {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: repair phase differs from its canonical root reference")
	}
	if !sameEngineRepairCandidateRun(record.CurrentCandidateIdentity(), reference.CandidateIdentity) {
		return fencing.EngineRepairRecord{}, fmt.Errorf("fencing: candidate identity differs from canonical repair reference")
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, data) {
		return fencing.EngineRepairRecord{}, errors.Join(err, fmt.Errorf("fencing: canonical engine repair evidence encoding differs"))
	}
	return record, nil
}

func mustRepairGenerationKey(record fencing.EngineRepairRecord) string {
	key, _ := record.Binding.EngineRepairGenerationKey()
	return key
}

func validateEngineRepairRootReferences(state fenceLedger, uid, source string) error {
	for key, reference := range state.Ledger.EngineRepairs {
		if err := reference.Validate(); err != nil {
			return err
		}
		generationKey, err := reference.Binding.EngineRepairGenerationKey()
		if err != nil || key != generationKey || reference.Binding.StatefulSetUID != uid || reference.Binding.SourceClusterID != source {
			return errors.Join(err, fmt.Errorf("fencing: canonical engine repair root index differs"))
		}
	}
	return nil
}

func engineRepairPhaseCanAdvance(old, next fencing.EngineRepairPhase) bool {
	order := map[fencing.EngineRepairPhase]int{
		fencing.RepairCandidatePrepared: 1,
		fencing.RepairRemoveFrozen:      2,
		fencing.RepairStorageClosing:    3,
		fencing.RepairRemoved:           4,
		fencing.RepairAdding:            5,
		fencing.RepairActive:            6,
		fencing.RepairGenerationClosing: 7,
	}
	return order[old] != 0 && order[next] >= order[old]
}
