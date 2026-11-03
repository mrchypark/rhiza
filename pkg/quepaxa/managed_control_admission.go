package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
)

// ErrManagedControlObservationUnavailable means the current local recorder
// state cannot be safely classified as one pending managed control.
var ErrManagedControlObservationUnavailable = errors.New("managed control observation unavailable")

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

// ObserveManagedAppendOnlyControl classifies a single unresolved next-slot ISR
// in a managed Core. It reads the complete Step/F/A/P summary under one lock,
// validates exact original value bytes outside Core locks, then requires the
// same Core state and binding to remain current. It performs no WAL writes.
func (c *Core) ObserveManagedAppendOnlyControl(ctx context.Context) (ManagedAppendOnlyControlObservation, error) {
	if c == nil {
		return ManagedAppendOnlyControlObservation{}, ErrManagedControlObservationUnavailable
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
			return ManagedAppendOnlyControlObservation{}, fmt.Errorf("%w: %v", ErrManagedControlObservationUnavailable, err)
		}
		validated = append(validated, ManagedAppendOnlyControlProposal{
			Role: ref.role, Kind: kind, Priority: ref.proposal.Priority,
			ProposerID: ref.proposal.ProposerID, Hash: ref.proposal.Hash,
		})
	}
	if err := ctx.Err(); err != nil {
		return ManagedAppendOnlyControlObservation{}, err
	}
	if !c.sameManagedControlSnapshot(snapshot) {
		return ManagedAppendOnlyControlObservation{}, fmt.Errorf("%w: Core state changed during validation", ErrManagedControlObservationUnavailable)
	}
	return ManagedAppendOnlyControlObservation{
		CommittedTip: snapshot.committed, ObservedSlot: snapshot.observed,
		ConfigID: snapshot.configID, Step: snapshot.step,
		ApplicationProtocol: snapshot.protocol, Generation: snapshot.generation,
		Proposals: validated, core: c,
	}, nil
}

func (c *Core) managedControlSnapshot() (managedControlSnapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.localMode || c.managedAppendOnly == nil || c.managedAppendOnly.Verify == nil || c.observer || c.learner {
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	binding := c.managedAppendOnly.Binding
	if binding.ApplicationProtocol != managedRequestAppendOnlyApplicationProtocol && binding.ApplicationProtocol != managedGraphRequestAppendOnlyApplicationProtocol {
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	committed := c.tip
	if committed == Slot(math.MaxUint64) {
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	observed := c.highestKnownSlotLocked()
	_, isDecided := c.decided[observed]
	if observed != committed+1 || isDecided {
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	state, ok := c.recorders[observed]
	if !ok || state.Step < 4 || state.FirstCurrent == nil || state.AggregateCurrent == nil || compareProposal(state.AggregateCurrent, state.FirstCurrent) < 0 || (state.Step == 4 && state.AggregatePrior != nil) {
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	cluster := cloneCluster(c.clusterForSlotLocked(observed))
	if cluster.ConfigID == 0 || !c.voterForClusterLocked(cluster) {
		return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
	}
	snapshot := managedControlSnapshot{
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
			return managedControlSnapshot{}, ErrManagedControlObservationUnavailable
		}
		snapshot.refs = append(snapshot.refs, managedControlRefSnapshot{role: item.role, proposal: *cloneProposal(item.proposal), value: bytes.Clone(value)})
	}
	return snapshot, nil
}

func (c *Core) validateManagedControlRef(ctx context.Context, snapshot managedControlSnapshot, ref managedControlRefSnapshot) (ManagedAppendOnlyControlKind, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if sha256.Sum256(ref.value) != ref.proposal.Hash {
		return 0, fmt.Errorf("proposal value hash mismatch")
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
		return 0, err
	}
	if snapshot.config == nil || snapshot.config != c.managedAppendOnly {
		return 0, ErrManagedAppendOnlyUnavailable
	}
	candidate := ManagedCheckpointCandidate{Value: ref.value, Hash: ref.proposal.Hash, Seal: seal, DecisionSlot: snapshot.observed, ConfigID: snapshot.configID}
	if err := c.ValidateManagedCheckpointCandidate(ctx, candidate); err != nil {
		return 0, err
	}
	return ManagedAppendOnlyCheckpointSeal, nil
}

func (c *Core) sameManagedControlSnapshot(before managedControlSnapshot) bool {
	after, err := c.managedControlSnapshot()
	if err != nil || before.core != after.core || before.committed != after.committed || before.observed != after.observed || before.configID != after.configID || before.step != after.step || before.protocol != after.protocol || before.generation != after.generation || before.binding != after.binding || before.walIdentity != after.walIdentity || before.anchor != after.anchor || before.membership != after.membership || before.config != after.config || !sameCluster(before.cluster, after.cluster) || len(before.refs) != len(after.refs) {
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
