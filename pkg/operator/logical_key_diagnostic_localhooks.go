//go:build localhooks

package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// LogicalMutationKeySnapshot is a bounded, read-only view for localhooks
// diagnostics. It contains no object payloads, raw key-owner hashes, or writes.
type LogicalMutationKeySnapshot struct {
	Exists                bool
	OwnerProducerUID      string
	OwnerSequence         uint64
	OwnerEntryFound       bool
	OwnerEntryTerminal    bool
	OwnerEntryRetired     bool
	CurrentTupleMatch     bool
	CurrentEntryTerminal  bool
	CurrentEntryRetired   bool
	PendingGenerationZero bool
	GenerationZeroBarrier bool
}

// InspectLogicalMutationKeyLocalhooks reads the exact logical Run, its
// producer, and the canonical key record, then maps a live key owner back to
// the bounded set of enrolled logical producer entries. It is available only
// in localhooks builds and performs no acquisition, reconciliation, or writes.
func (e *FencingExecutor) InspectLogicalMutationKeyLocalhooks(ctx context.Context, statefulSetUID string, identity fencing.Identity, sequence uint64, intent fencing.MutationIntent) (LogicalMutationKeySnapshot, error) {
	var snapshot LogicalMutationKeySnapshot
	if e == nil || e.AuthorityState == nil || ctx == nil || statefulSetUID == "" || identity.SourceClusterID == "" || identity.RunID == "" ||
		identity.WALIdentity == "" || sequence == 0 || intent.Key == "" || intent.StoreScopeHash == "" || intent.OperationID == "" {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic identity is incomplete")
	}
	root, err := e.read(ctx, statefulSetUID, identity.SourceClusterID)
	if err != nil {
		return snapshot, err
	}
	if root.Ledger.Profile != fencing.LogicalWriterProfileV1 || len(root.Ledger.Runs) > fencing.MaxEngineRepairIncarnations {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic profile or Run inventory is invalid")
	}
	currentRun, ok := root.Ledger.Runs[identity.RunID]
	if !ok || currentRun.Identity != identity || currentRun.LogicalBinding == nil || currentRun.Runtime != nil ||
		currentRun.LogicalBinding.ValidateBound() != nil || currentRun.LogicalBinding.Role != fencing.StartupRoleVoterActive ||
		currentRun.MutationProducerUID == "" || currentRun.MutationScopeHash == "" || intent.StoreScopeHash != currentRun.MutationScopeHash ||
		intent.OperationID != fencing.MutationRequestID(currentRun.MutationProducerUID, sequence) {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic Run differs from canonical enrollment")
	}
	currentProducer, currentUID, _, err := e.AuthorityState.ReadLogicalProducer(ctx, statefulSetUID, *currentRun.LogicalBinding)
	if err != nil {
		return snapshot, err
	}
	if currentUID != currentRun.MutationProducerUID || currentProducer.WALIdentity != identity.WALIdentity || currentProducer.Validate() != nil {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic producer differs from canonical Run")
	}
	currentEntry, currentEntryFound := currentProducer.Entries[sequence]
	if currentEntryFound && currentEntry.Intent != intent {
		return snapshot, fmt.Errorf("fencing: logical key diagnostic tuple differs from canonical producer")
	}
	currentOwner := (mutationExecutionAuthority{statefulSetUID: statefulSetUID, sourceClusterID: identity.SourceClusterID,
		runID: identity.RunID, logicalBinding: currentRun.LogicalBinding}).mutationOwner(currentRun, currentUID, sequence, intent)

	keyState, err := e.readMutationKeyState(ctx, intent.StoreScopeHash, intent.Key)
	if err != nil {
		if errors.Is(err, errAuthorityStateNotFound) {
			return snapshot, nil
		}
		return snapshot, err
	}
	snapshot.Exists = true
	snapshot.CurrentTupleMatch = keyState.Owner == currentOwner && keyState.Owner != ""
	snapshot.PendingGenerationZero = keyState.PendingGenerationZero != nil
	snapshot.GenerationZeroBarrier = keyState.Barrier != nil
	if currentEntryFound {
		snapshot.CurrentEntryTerminal = currentEntry.Resolution != nil && currentEntry.DispatchClosed
		snapshot.CurrentEntryRetired = sequence <= currentProducer.RetiredStageThrough || sequence <= currentProducer.CompactedThrough
	}
	if keyState.Owner == "" {
		return snapshot, nil
	}

	for runID, run := range root.Ledger.Runs {
		binding := run.LogicalBinding
		if binding == nil || run.Runtime != nil || run.MutationProducerUID == "" || run.Identity.RunID != runID ||
			binding.SourceClusterID != identity.SourceClusterID {
			continue
		}
		producer, producerUID, _, readErr := e.AuthorityState.ReadLogicalProducer(ctx, statefulSetUID, *binding)
		if readErr != nil {
			return snapshot, readErr
		}
		if producerUID != run.MutationProducerUID || producer.WALIdentity != run.Identity.WALIdentity || producer.Validate() != nil {
			return snapshot, fmt.Errorf("fencing: logical key diagnostic inventory contains a mismatched producer")
		}
		for candidateSequence, entry := range producer.Entries {
			if entry.Intent.Key != intent.Key || entry.Intent.StoreScopeHash != intent.StoreScopeHash {
				continue
			}
			owner := (mutationExecutionAuthority{statefulSetUID: statefulSetUID, sourceClusterID: identity.SourceClusterID,
				runID: runID, logicalBinding: binding}).mutationOwner(run, producerUID, candidateSequence, entry.Intent)
			if owner != keyState.Owner {
				continue
			}
			if snapshot.OwnerEntryFound {
				return snapshot, fmt.Errorf("fencing: logical key owner maps to multiple producer entries")
			}
			snapshot.OwnerProducerUID = producerUID
			snapshot.OwnerSequence = candidateSequence
			snapshot.OwnerEntryFound = true
			snapshot.OwnerEntryTerminal = entry.Resolution != nil && entry.DispatchClosed
			snapshot.OwnerEntryRetired = candidateSequence <= producer.RetiredStageThrough || candidateSequence <= producer.CompactedThrough
		}
	}
	return snapshot, nil
}
