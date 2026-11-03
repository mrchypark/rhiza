package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

// ErrManagedControlObservationUnavailable means the current local recorder
// state cannot be safely classified as one pending managed control.
var ErrManagedControlObservationUnavailable = errors.New("managed control observation unavailable")

func managedControlObservationRejected(reason string, err error) error {
	if err != nil {
		log.Printf("MANAGED_CONTROL_OBSERVATION_REJECTED reason=%s", reason)
	}
	return err
}

type ManagedAppendOnlyControlProposalRole uint8

const (
	ManagedAppendOnlyFirstCurrent ManagedAppendOnlyControlProposalRole = iota + 1
	ManagedAppendOnlyAggregateCurrent
	ManagedAppendOnlyAggregatePrior
)

type ManagedAppendOnlyControlKind uint8

const (
	ManagedAppendOnlyReadBarrier ManagedAppendOnlyControlKind = iota + 1
	ManagedAppendOnlyCheckpointSeal
)

// ManagedAppendOnlyControlProposal reports one validated F/A/P reference.
// Its hash and proposer identity are descriptive, not authority to decide.
type ManagedAppendOnlyControlProposal struct {
	Role       ManagedAppendOnlyControlProposalRole
	Kind       ManagedAppendOnlyControlKind
	Priority   Priority
	ProposerID NodeID
	Hash       ValueHash
}

// ManagedAppendOnlyControlObservation is a point-in-time, read-only
// classification. It is not a reservation or admission authorization; callers
// must still run the ordinary owned control path and its fresh guards.
type ManagedAppendOnlyControlObservation struct {
	CommittedTip        Slot
	ObservedSlot        Slot
	ConfigID            uint
	Step                Step
	ApplicationProtocol string
	Generation          int64
	Proposals           []ManagedAppendOnlyControlProposal
	core                *Core
}

// BelongsTo prevents accidentally applying an observation to another Core.
func (o ManagedAppendOnlyControlObservation) BelongsTo(core *Core) bool {
	return core != nil && o.core == core
}

type managedControlRefSnapshot struct {
	role     ManagedAppendOnlyControlProposalRole
	proposal Proposal
	value    []byte
}

type managedControlSnapshot struct {
	core        *Core
	committed   Slot
	observed    Slot
	configID    uint
	step        Step
	refs        []managedControlRefSnapshot
	protocol    string
	generation  int64
	binding     ManagedAppendOnlyBinding
	walIdentity string
	anchor      [32]byte
	membership  uint64
	cluster     Cluster
	config      *ManagedAppendOnlyConfig
}

type managedControlPrefixAdvanceError struct {
	snapshot managedControlSnapshot
}

func (e *managedControlPrefixAdvanceError) Error() string {
	return ErrManagedControlObservationUnavailable.Error() + ": Core state changed during validation"
}

func (e *managedControlPrefixAdvanceError) Unwrap() error {
	return ErrManagedControlObservationUnavailable
}

// IsManagedControlPrefixAdvance reports only the observer's validated-prefix
// race; other unavailable observations remain ordinary fail-closed errors.
func IsManagedControlPrefixAdvance(err error) bool {
	var advance *managedControlPrefixAdvanceError
	return errors.As(err, &advance)
}

// RecheckManagedControlPrefixAdvance verifies the captured observer authority
// and current continuous prefix in one read-lock section. It never authorizes
// admission by itself.
func (c *Core) RecheckManagedControlPrefixAdvance(ctx context.Context, observationErr error, expectedCommitted, expectedObserved Slot, applicationProtocol string, generation uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var advance *managedControlPrefixAdvanceError
	if c == nil || !errors.As(observationErr, &advance) {
		return false, nil
	}
	snapshot := advance.snapshot
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if snapshot.core != c || snapshot.committed != expectedCommitted || snapshot.observed != expectedObserved || snapshot.protocol != applicationProtocol || snapshot.generation < 0 || uint64(snapshot.generation) != generation || c.localMode || c.managedAppendOnly == nil || c.managedAppendOnly != snapshot.config || c.managedAppendOnly.Verify == nil || c.observer || c.learner || c.managedAppendOnly.Binding != snapshot.binding || c.managedAppendOnly.Binding.ApplicationProtocol != snapshot.protocol || c.managedAppendOnly.Binding.Generation != snapshot.generation || c.walIdentity != snapshot.walIdentity || c.generationAnchorHash != snapshot.anchor || c.membershipVersion != snapshot.membership || !sameCluster(c.clusterForSlotLocked(snapshot.observed), snapshot.cluster) || !c.voterForClusterLocked(snapshot.cluster) || c.tip < snapshot.observed || c.highestKnownSlotLocked() > c.tip {
		return false, nil
	}
	if _, ok := c.prefixes[snapshot.observed]; !ok {
		return false, nil
	}
	return true, nil
}

// ObserveManagedAppendOnlyControl classifies a single unresolved next-slot ISR
// in a managed Core. It reads the complete Step/F/A/P summary under one lock,
// validates exact original value bytes outside Core locks, then requires the
// same Core state and binding to remain current. It performs no WAL writes.
func (c *Core) ObserveManagedAppendOnlyControl(ctx context.Context) (ManagedAppendOnlyControlObservation, error) {
	if c == nil {
		return ManagedAppendOnlyControlObservation{}, managedControlObservationRejected("runtime_role_or_profile", ErrManagedControlObservationUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return ManagedAppendOnlyControlObservation{}, err
	}
	snapshot, err := c.managedControlSnapshot()
	if err != nil {
		return ManagedAppendOnlyControlObservation{}, err
	}
	validated := make([]ManagedAppendOnlyControlProposal, 0, len(snapshot.refs))
	for _, ref := range snapshot.refs {
		if err := ctx.Err(); err != nil {
			return ManagedAppendOnlyControlObservation{}, err
		}
		kind, err := c.validateManagedControlRef(ctx, snapshot, ref)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ManagedAppendOnlyControlObservation{}, ctxErr
			}
			return ManagedAppendOnlyControlObservation{}, fmt.Errorf("%w: %v", ErrManagedControlObservationUnavailable, managedControlObservationRejected("reference_validation", err))
		}
		validated = append(validated, ManagedAppendOnlyControlProposal{
			Role: ref.role, Kind: kind, Priority: ref.proposal.Priority,
			ProposerID: ref.proposal.ProposerID, Hash: ref.proposal.Hash,
		})
	}
	localtesthooks.Hit("core:managed-control:before-snapshot-recheck")
	if err := ctx.Err(); err != nil {
		return ManagedAppendOnlyControlObservation{}, err
	}
	current, reclassified, prefixAdvanced, reason, currentErr := c.recheckManagedControlSnapshot(snapshot, validated)
	if err := ctx.Err(); err != nil {
		return ManagedAppendOnlyControlObservation{}, err
	}
	if prefixAdvanced {
		return ManagedAppendOnlyControlObservation{}, managedControlObservationRejected("validated_prefix_advance", &managedControlPrefixAdvanceError{snapshot: snapshot})
	}
	if currentErr != nil {
		return ManagedAppendOnlyControlObservation{}, fmt.Errorf("%w: Core state changed during validation", managedControlObservationRejected(reason, ErrManagedControlObservationUnavailable))
	}
	if reclassified {
		return current, nil
	}
	return current, nil
}

func currentObservationFromSnapshot(snapshot managedControlSnapshot) ManagedAppendOnlyControlObservation {
	return ManagedAppendOnlyControlObservation{CommittedTip: snapshot.committed, ObservedSlot: snapshot.observed, ConfigID: snapshot.configID, Step: snapshot.step, ApplicationProtocol: snapshot.protocol, Generation: snapshot.generation, core: snapshot.core}
}

func currentObservationFromSnapshotWithValidated(snapshot managedControlSnapshot, validated []ManagedAppendOnlyControlProposal) ManagedAppendOnlyControlObservation {
	observation := currentObservationFromSnapshot(snapshot)
	observation.Proposals = validated
	return observation
}

func (c *Core) managedControlPrefixAdvancedLocked(snapshot managedControlSnapshot) bool {
	if snapshot.core != c || snapshot.committed >= snapshot.observed || c.tip < snapshot.observed || c.highestKnownSlotLocked() > c.tip || c.localMode || c.managedAppendOnly == nil || c.managedAppendOnly != snapshot.config || c.managedAppendOnly.Verify == nil || c.observer || c.learner || c.managedAppendOnly.Binding != snapshot.binding || c.walIdentity != snapshot.walIdentity || c.generationAnchorHash != snapshot.anchor || c.membershipVersion != snapshot.membership || !sameCluster(c.clusterForSlotLocked(snapshot.observed), snapshot.cluster) || !c.voterForClusterLocked(snapshot.cluster) {
		return false
	}
	_, ok := c.prefixes[snapshot.observed]
	return ok
}

func (c *Core) managedControlSnapshot() (snapshot managedControlSnapshot, resultErr error) {
	rejectionReason := ""
	// Register diagnostics before the unlock defer so logging always runs after
	// the Core read lock is released.
	defer func() {
		if rejectionReason != "" {
			managedControlObservationRejected(rejectionReason, resultErr)
		}
	}()
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.managedControlSnapshotLocked(&rejectionReason)
}

// managedControlSnapshotLocked reads the complete current ISR without acquiring
// Core locks. Callers must hold at least c.mu.RLock.
func (c *Core) managedControlSnapshotLocked(rejectionReason *string) (snapshot managedControlSnapshot, resultErr error) {
	if c.localMode || c.managedAppendOnly == nil || c.managedAppendOnly.Verify == nil || c.observer || c.learner {
		*rejectionReason = "runtime_role_or_profile"
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	binding := c.managedAppendOnly.Binding
	if binding.ApplicationProtocol != managedRequestAppendOnlyApplicationProtocol && binding.ApplicationProtocol != managedGraphRequestAppendOnlyApplicationProtocol {
		*rejectionReason = "runtime_role_or_profile"
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	committed := c.tip
	if committed == Slot(math.MaxUint64) {
		*rejectionReason = "slot_not_next"
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	observed := c.highestKnownSlotLocked()
	_, isDecided := c.decided[observed]
	if observed != committed+1 || isDecided {
		*rejectionReason = "slot_not_next"
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	state, ok := c.recorders[observed]
	if !ok || state.Step < 4 || state.FirstCurrent == nil || state.AggregateCurrent == nil || compareProposal(state.AggregateCurrent, state.FirstCurrent) < 0 || (state.Step == 4 && state.AggregatePrior != nil) {
		*rejectionReason = "isr_shape"
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	cluster := cloneCluster(c.clusterForSlotLocked(observed))
	if cluster.ConfigID == 0 || !c.voterForClusterLocked(cluster) {
		*rejectionReason = "voter_configuration"
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	snapshot = managedControlSnapshot{
		core: c, committed: committed, observed: observed, configID: cluster.ConfigID,
		step: state.Step, protocol: binding.ApplicationProtocol, generation: binding.Generation,
		binding:     binding,
		walIdentity: c.walIdentity, anchor: c.generationAnchorHash,
		membership: c.membershipVersion, cluster: cluster, config: c.managedAppendOnly,
	}
	for _, item := range []struct {
		role     ManagedAppendOnlyControlProposalRole
		proposal *Proposal
	}{{ManagedAppendOnlyFirstCurrent, state.FirstCurrent}, {ManagedAppendOnlyAggregateCurrent, state.AggregateCurrent}, {ManagedAppendOnlyAggregatePrior, state.AggregatePrior}} {
		if item.proposal == nil {
			continue
		}
		value, exists := c.values[item.proposal.Hash]
		if !exists || len(value) == 0 || sha256.Sum256(value) != item.proposal.Hash || (len(item.proposal.Value) != 0 && !bytes.Equal(item.proposal.Value, value)) {
			*rejectionReason = "proposal_value"
			return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
		}
		snapshot.refs = append(snapshot.refs, managedControlRefSnapshot{role: item.role, proposal: *cloneProposal(item.proposal), value: bytes.Clone(value)})
	}
	return snapshot, nil
}

// recheckManagedControlSnapshot performs the final fresh read in one lock
// section. It distinguishes a newly certified prefix from a same-slot ISR
// change without invoking verifiers or performing I/O under the lock.
func (c *Core) recheckManagedControlSnapshot(before managedControlSnapshot, validated []ManagedAppendOnlyControlProposal) (ManagedAppendOnlyControlObservation, bool, bool, string, error) {
	if c == nil {
		return ManagedAppendOnlyControlObservation{}, false, false, "final_snapshot_mismatch", ErrManagedControlObservationUnavailable
	}
	readerReason := ""
	c.mu.RLock()
	defer c.mu.RUnlock()
	after, err := c.managedControlSnapshotLocked(&readerReason)
	if err != nil {
		if c.managedControlPrefixAdvancedLocked(before) {
			return ManagedAppendOnlyControlObservation{}, false, true, "validated_prefix_advance", err
		}
		return ManagedAppendOnlyControlObservation{}, false, false, "final_snapshot_mismatch", err
	}
	if sameManagedControlSnapshotValues(before, after) {
		return currentObservationFromSnapshotWithValidated(after, validated), false, false, "", nil
	}
	if c.managedControlPrefixAdvancedLocked(before) {
		return ManagedAppendOnlyControlObservation{}, false, true, "validated_prefix_advance", nil
	}
	if after.step != before.step {
		if observation, ok := c.reclassifiedManagedControlObservationLocked(before, after, validated); ok {
			return observation, true, false, "", nil
		}
	}
	return ManagedAppendOnlyControlObservation{}, false, false, "final_snapshot_mismatch", ErrManagedControlObservationUnavailable
}

func sameManagedControlSnapshotValues(before, after managedControlSnapshot) bool {
	if before.core != after.core || before.committed != after.committed || before.observed != after.observed || before.configID != after.configID || before.step != after.step || before.protocol != after.protocol || before.generation != after.generation || before.binding != after.binding || before.walIdentity != after.walIdentity || before.anchor != after.anchor || before.membership != after.membership || before.config != after.config || !sameCluster(before.cluster, after.cluster) || len(before.refs) != len(after.refs) {
		return false
	}
	for index := range before.refs {
		left, right := before.refs[index], after.refs[index]
		if left.role != right.role || !sameProposal(&left.proposal, &right.proposal) || !bytes.Equal(left.value, right.value) {
			return false
		}
	}
	return true
}

func (c *Core) reclassifiedManagedControlObservationLocked(before, current managedControlSnapshot, validated []ManagedAppendOnlyControlProposal) (ManagedAppendOnlyControlObservation, bool) {
	if before.core != c || current.core != c || before.committed != current.committed || before.observed != current.observed || before.configID != current.configID || before.protocol != current.protocol || before.generation != current.generation || before.binding != current.binding || before.walIdentity != current.walIdentity || before.anchor != current.anchor || before.membership != current.membership || before.config != current.config || !sameCluster(before.cluster, current.cluster) || current.step < before.step || len(before.refs) != len(validated) {
		return ManagedAppendOnlyControlObservation{}, false
	}
	validatedByValue := make(map[ValueHash][]byte, len(before.refs))
	for index, ref := range before.refs {
		if validated[index].Hash != ref.proposal.Hash || validated[index].Role != ref.role {
			return ManagedAppendOnlyControlObservation{}, false
		}
		validatedByValue[ref.proposal.Hash] = ref.value
	}
	proposals := make([]ManagedAppendOnlyControlProposal, 0, len(current.refs))
	for _, ref := range current.refs {
		validatedValue, ok := validatedByValue[ref.proposal.Hash]
		if !ok || !bytes.Equal(validatedValue, ref.value) || sha256.Sum256(ref.value) != ref.proposal.Hash {
			return ManagedAppendOnlyControlObservation{}, false
		}
		kind := ManagedAppendOnlyControlKind(0)
		if barrier, err := DecodeReadBarrier(ref.value); barrier && err == nil {
			kind = ManagedAppendOnlyReadBarrier
		} else {
			_, checkpoint, err := DecodeCheckpointSeal(ref.value)
			if err != nil || !checkpoint {
				return ManagedAppendOnlyControlObservation{}, false
			}
			kind = ManagedAppendOnlyCheckpointSeal
			seal, _, _ := DecodeCheckpointSeal(ref.value)
			candidate := ManagedCheckpointCandidate{Value: ref.value, Hash: ref.proposal.Hash, Seal: seal, DecisionSlot: current.observed, ConfigID: current.configID}
			if err := c.validateManagedCandidateLocalLocked(candidate); err != nil {
				return ManagedAppendOnlyControlObservation{}, false
			}
		}
		proposals = append(proposals, ManagedAppendOnlyControlProposal{Role: ref.role, Kind: kind, Priority: ref.proposal.Priority, ProposerID: ref.proposal.ProposerID, Hash: ref.proposal.Hash})
	}
	return ManagedAppendOnlyControlObservation{CommittedTip: current.committed, ObservedSlot: current.observed, ConfigID: current.configID, Step: current.step, ApplicationProtocol: current.protocol, Generation: current.generation, Proposals: proposals, core: c}, true
}

func (c *Core) validateManagedControlRef(ctx context.Context, snapshot managedControlSnapshot, ref managedControlRefSnapshot) (ManagedAppendOnlyControlKind, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if sha256.Sum256(ref.value) != ref.proposal.Hash {
		return 0, managedControlObservationRejected("reference_validation", fmt.Errorf("proposal value hash mismatch"))
	}
	if barrier, err := DecodeReadBarrier(ref.value); barrier {
		if err != nil {
			return 0, err
		}
		return ManagedAppendOnlyReadBarrier, nil
	}
	seal, checkpoint, err := DecodeCheckpointSeal(ref.value)
	if err != nil || !checkpoint {
		if err == nil {
			err = fmt.Errorf("proposal is not a recognized managed control")
		}
		return 0, managedControlObservationRejected("reference_validation", err)
	}
	if snapshot.config == nil || snapshot.config != c.managedAppendOnly {
		return 0, managedControlObservationRejected("reference_validation", ErrManagedAppendOnlyUnavailable)
	}
	candidate := ManagedCheckpointCandidate{Value: ref.value, Hash: ref.proposal.Hash, Seal: seal, DecisionSlot: snapshot.observed, ConfigID: snapshot.configID}
	if err := c.ValidateManagedCheckpointCandidate(ctx, candidate); err != nil {
		return 0, managedControlObservationRejected("reference_validation", err)
	}
	return ManagedAppendOnlyCheckpointSeal, nil
}
