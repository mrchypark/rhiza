package operator

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// This backend is deliberately not an application-facing HTTP endpoint. Runtime
// enrollment and receipt updates must come from trusted runtime/provider workers.
// API persistence alone does not grant dispatch or validate provider evidence.
// The root enrollment ledger freezes the cohort before producer closure begins.
func (e *FencingExecutor) mutationProducerPath(runtime fencing.RuntimeBinding) string {
	return e.controller().corePath("configmaps", "rhiza-mutations-"+hashJSON(runtime)[:40])
}

func (e *FencingExecutor) enrolledMutationRun(ctx context.Context, runtime fencing.RuntimeBinding) (fencing.Run, error) {
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return fencing.Run{}, err
	}
	run, ok := state.Ledger.Runs[runtime.RunID]
	if !ok || (!state.Ledger.RequireRuntimeBinding && !state.Ledger.RequireMutationJournal) || run.Runtime == nil || *run.Runtime != runtime {
		return fencing.Run{}, fmt.Errorf("fencing: mutation producer lacks trusted root enrollment")
	}
	if err := runtime.Validate(run.Identity); err != nil {
		return fencing.Run{}, err
	}
	_, voterClosed := state.Ledger.ClosedNodes[run.Identity.NodeID]
	if (state.Ledger.ClosedBy != "" || voterClosed) && run.Stop == nil {
		return fencing.Run{}, fmt.Errorf("fencing: closed producer cohort lacks its stop binding")
	}
	return run, nil
}

// lookupMutation returns only the original entry for a root-enrolled producer
// tuple. This path never admits or executes work; live reads use GET only.
func (e *FencingExecutor) lookupMutation(ctx context.Context, statefulSetUID string, id fencing.Identity, request fencing.MutationLookupRequest) (fencing.MutationLookupResult, error) {
	if e.AuthorityState == nil {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: canonical ObjectStorage authority state is not configured")
	}
	state, err := e.read(ctx, statefulSetUID, id.SourceClusterID)
	if err != nil {
		return fencing.MutationLookupResult{}, err
	}
	run, ok := state.Ledger.Runs[id.RunID]
	if !ok || run.Identity != id || run.Runtime == nil || run.Runtime.StatefulSetUID != statefulSetUID || run.MutationProducerUID == "" ||
		request.ProducerUID != run.MutationProducerUID || request.Sequence == 0 ||
		request.Intent.OperationID != fencing.MutationRequestID(request.ProducerUID, request.Sequence) ||
		run.MutationScopeHash == "" || request.Intent.StoreScopeHash != run.MutationScopeHash {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: mutation lookup is outside the enrolled producer scope")
	}
	if err := run.Runtime.Validate(id); err != nil {
		return fencing.MutationLookupResult{}, err
	}
	producer, producerUID, _, err := e.AuthorityState.ReadProducer(ctx, *run.Runtime)
	if err != nil {
		return fencing.MutationLookupResult{}, err
	}
	if producerUID != run.MutationProducerUID {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: bound mutation producer identity differs from canonical ObjectStorage")
	}
	if producer.Runtime != *run.Runtime || producer.WALIdentity != id.WALIdentity || producer.EvidenceFault != "" || producer.Validate() != nil {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: stored mutation producer identity or state differs")
	}
	if request.Sequence <= producer.CompactedThrough {
		entry, err := e.lookupArchivedMutation(ctx, *run.Runtime, request.ProducerUID, request.Sequence, request.Intent)
		if err != nil {
			return fencing.MutationLookupResult{}, err
		}
		return fencing.MutationLookupResult{Found: true, Archived: true, CompactedThrough: producer.CompactedThrough, ArchiveHead: producer.ArchiveHead, Entry: &entry}, nil
	}
	if request.Sequence > producer.LastAdmitted {
		return fencing.MutationLookupResult{CompactedThrough: producer.CompactedThrough, ArchiveHead: producer.ArchiveHead}, nil
	}
	entry, exists := producer.Entries[request.Sequence]
	if !exists || entry.Intent != request.Intent {
		return fencing.MutationLookupResult{}, fmt.Errorf("fencing: live mutation lookup differs from original tuple")
	}
	return fencing.MutationLookupResult{Found: true, CompactedThrough: producer.CompactedThrough, ArchiveHead: producer.ArchiveHead, Entry: &entry}, nil
}

// reserveMutationProducer may create only Reserved state. It never creates Open
// state from absence. If root closure already happened, create the deterministic
// missing producer directly Closed so a delayed initializer cannot reopen it.
func (e *FencingExecutor) reserveMutationProducer(ctx context.Context, runtime fencing.RuntimeBinding) (fencing.MutationProducer, error) {
	return e.mutateProducer(ctx, runtime, true, false, func(*fencing.MutationProducer) error { return nil })
}

// changeMutationProducer never recreates missing state. The transition is pure;
// storage/execution side effects cannot be performed inside this CAS callback.
// On uncertain response loss, callers retry the same operation identity.
func (e *FencingExecutor) changeMutationProducer(ctx context.Context, runtime fencing.RuntimeBinding, transition func(*fencing.MutationProducer) error) (fencing.MutationProducer, error) {
	return e.mutateProducer(ctx, runtime, false, false, transition)
}

func (e *FencingExecutor) mutateProducer(ctx context.Context, runtime fencing.RuntimeBinding, allowCreate, latchingEvidenceFault bool, transition func(*fencing.MutationProducer) error) (fencing.MutationProducer, error) {
	if e.AuthorityState == nil {
		return fencing.MutationProducer{}, fmt.Errorf("fencing: canonical ObjectStorage authority state is not configured")
	}
	for range 8 {
		run, err := e.enrolledMutationRun(ctx, runtime)
		if err != nil {
			return fencing.MutationProducer{}, err
		}
		producer, producerUID, version, err := e.AuthorityState.ReadProducer(ctx, runtime)
		creating := errors.Is(err, errAuthorityStateNotFound)
		if err != nil && !creating {
			return fencing.MutationProducer{}, err
		}
		if creating {
			if !allowCreate || run.Identity.WALIdentity != "" || run.MutationProducerUID != "" {
				return producer, fmt.Errorf("fencing: missing bound producer cannot be recreated")
			}
			initial, err := fencing.NewMutationProducer(run.Identity, runtime)
			if err != nil {
				return producer, err
			}
			producer = *initial
			producerUID = stableMutationProducerUID(runtime)
			if err := e.AuthorityState.WriteProducer(ctx, runtime, producer, nil); err != nil {
				if errors.Is(err, errAuthorityStateConflict) {
					continue
				}
				stored, storedUID, _, readErr := e.AuthorityState.ReadProducer(ctx, runtime)
				if readErr == nil && storedUID == producerUID && hashJSON(stored) == hashJSON(producer) {
					continue
				}
				return producer, errors.Join(err, readErr)
			}
			continue
		}
		if producerUID != stableMutationProducerUID(runtime) || (run.MutationProducerUID != "" && run.MutationProducerUID != producerUID) {
			return producer, fmt.Errorf("fencing: canonical mutation producer identity differs from root")
		}
		if run.MutationProducerUID == "" {
			if !allowCreate || run.Identity.WALIdentity != "" || producer.LastAdmitted != 0 || (producer.Phase != fencing.MutationReserved && producer.Phase != fencing.MutationClosed) {
				return producer, fmt.Errorf("fencing: producer identity must be pinned before activation")
			}
			_, err := e.change(ctx, runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
				return l.BindMutationProducer(run.Identity, runtime, producerUID)
			})
			if err != nil {
				return producer, err
			}
			// Reload root and producer together; closure may have raced this bind.
			continue
		}
		// Every bound WAL is immutable in the root; producer transitions cannot
		// silently point at a different incarnation's admission authority.
		if producer.Runtime != runtime || (producer.WALIdentity != "" && producer.WALIdentity != run.Identity.WALIdentity) {
			return producer, fmt.Errorf("fencing: stored mutation producer WAL differs from root")
		}
		previousHead := hashJSON(producer.ArchiveHead)
		if !latchingEvidenceFault {
			if producer.EvidenceFault != "" {
				return producer, fencing.ErrUnavailable
			}
			if err := e.checkMutationArchiveHead(ctx, runtime, run.MutationProducerUID, producer); err != nil {
				return producer, err
			}
		}
		if run.Stop != nil && producer.Phase != fencing.MutationClosed {
			if err := producer.Close(run.Stop.OperationID, run.Stop.RequestHash); err != nil {
				return producer, err
			}
		}
		if err := transition(&producer); err != nil {
			return producer, err
		}
		if producer.Runtime != runtime || (producer.WALIdentity != "" && producer.WALIdentity != run.Identity.WALIdentity) {
			return producer, fmt.Errorf("fencing: transition changed producer WAL identity")
		}
		if err := producer.Validate(); err != nil {
			return producer, err
		}
		if !latchingEvidenceFault && previousHead != hashJSON(producer.ArchiveHead) {
			if err := e.validateMutationArchiveHead(ctx, runtime, run.MutationProducerUID, producer); err != nil {
				return producer, err
			}
		}
		writeErr := e.AuthorityState.WriteProducer(ctx, runtime, producer, version)
		if writeErr == nil {
			return producer, nil
		}
		if errors.Is(writeErr, errAuthorityStateConflict) {
			continue
		}
		stored, storedUID, _, readErr := e.AuthorityState.ReadProducer(ctx, runtime)
		if readErr == nil && storedUID == producerUID && hashJSON(stored) == hashJSON(producer) {
			return stored, nil
		}
		if readErr != nil && !errors.Is(readErr, errAuthorityStateNotFound) {
			return producer, errors.Join(writeErr, readErr)
		}
	}
	return fencing.MutationProducer{}, fencing.ErrUnavailable
}

// closeMutationProducers includes every enrolled run in the closed scope, not
// merely ConfigMaps returned by a list. Incomplete creation is still an obligation.
// The result is a mutation barrier only, never runtime-termination evidence.
func (e *FencingExecutor) closeMutationProducers(ctx context.Context, state fenceLedger, nodes []string) (bool, error) {
	if !state.Ledger.RequireRuntimeBinding || (len(nodes) == 0 && state.Ledger.ClosedBy == "") {
		return false, fmt.Errorf("fencing: producer cohort is not frozen")
	}
	for _, nodeID := range nodes {
		if _, ok := state.Ledger.ClosedNodes[nodeID]; !ok {
			return false, fmt.Errorf("fencing: voter producer cohort is not frozen")
		}
	}
	complete := true
	for _, run := range state.Ledger.Runs {
		if len(nodes) != 0 && !slices.Contains(nodes, run.Identity.NodeID) {
			continue
		}
		if run.Runtime == nil || run.Stop == nil {
			return false, fmt.Errorf("fencing: producer cohort lacks runtime enrollment or closure")
		}
		producer, err := e.reserveMutationProducer(ctx, *run.Runtime)
		if err != nil {
			return false, err
		}
		if producer.Phase != fencing.MutationClosed || !producer.BarrierComplete() {
			complete = false
		} else if err := e.verifyMutationArchiveReceipts(ctx, fencing.MutationCompactionPlan{Runtime: *run.Runtime, Entries: producer.Entries}); err != nil {
			return false, e.mutationArchiveFailure(ctx, *run.Runtime, err)
		}
		if producer.CompactedThrough != 0 {
			verified, err := e.verifyMutationArchiveHistory(ctx, *run.Runtime, hashJSON([]any{state.StatefulSetUID, state.Ledger.ClosedBy, state.Ledger.ClosedHash, state.Ledger.ClosedNodes, nodes}), 32)
			if err != nil {
				return false, err
			}
			if !verified {
				complete = false
			}
		}
	}
	return complete, nil
}
