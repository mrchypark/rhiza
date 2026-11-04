package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

var ErrManagedAppendOnlyUnavailable = errors.New("managed append-only checkpoint validation is unavailable")
var ErrManagedAppendOnlyCandidateBytesUnavailable = errors.New("managed append-only checkpoint candidate bytes are unavailable")

// These values mirror the authoritative constants in internal/sqlpolicy.
// ManagedAppendOnlyV1 is deliberately a closed two-profile mode.
const (
	managedRequestAppendOnlyApplicationProtocol      = "rhiza-managed-request-append-only/v1"
	managedGraphRequestAppendOnlyApplicationProtocol = "rhiza-managed-request-graph-append-only/v1"
)

// ManagedAppendOnlyBinding is immutable trusted startup identity for one Core.
type ManagedAppendOnlyBinding struct {
	ApplicationProtocol string
	BootstrapPlanDigest string
	MemberConfigDigest  string
	Generation          int64
	WALIdentity         [32]byte
}

// ManagedCheckpointCandidate carries the exact original QCKP and the slot at
// which this Core is being asked to record it. Value is never re-encoded.
type ManagedCheckpointCandidate struct {
	Value        []byte
	Hash         ValueHash
	Seal         CheckpointSeal
	DecisionSlot Slot
	ConfigID     uint
}

// ManagedCheckpointCandidateVerifier is fixed trusted startup code. A nil
// error attests the exact immutable object/pair, expected policy and role,
// candidate hash and cut, and the certified historical prefix, membership,
// and leader-order proof. When Core lacks the cut history, the verifier must
// establish that proof from the configured archive. It must not treat a
// locator or local prepared marker as proof.
type ManagedCheckpointCandidateVerifier func(context.Context, ManagedCheckpointCandidate) error

type ManagedAppendOnlyConfig struct {
	Binding ManagedAppendOnlyBinding
	Verify  ManagedCheckpointCandidateVerifier
}

func validateManagedAppendOnlyConfig(config *ManagedAppendOnlyConfig, wal *qlog.WAL, nodeID NodeID, localMember Member, localMode, learner bool) (*ManagedAppendOnlyConfig, error) {
	if config == nil {
		return nil, nil
	}
	if localMode {
		return nil, fmt.Errorf("managed append-only policy is not supported in Local mode")
	}
	binding := config.Binding
	if binding.ApplicationProtocol != managedRequestAppendOnlyApplicationProtocol && binding.ApplicationProtocol != managedGraphRequestAppendOnlyApplicationProtocol {
		return nil, fmt.Errorf("application protocol is not an approved managed append-only V1 profile")
	}
	if binding.Generation < 0 || binding.WALIdentity == ([32]byte{}) {
		return nil, fmt.Errorf("application protocol, nonnegative generation, and WAL identity are required")
	}
	if err := validateManagedDigest("bootstrap plan digest", binding.BootstrapPlanDigest); err != nil {
		return nil, err
	}
	if err := validateManagedDigest("member config digest", binding.MemberConfigDigest); err != nil {
		return nil, err
	}
	identity, registered := wal.Identity()
	if !registered || len(identity) != len(binding.WALIdentity) || !bytes.Equal(identity, binding.WALIdentity[:]) {
		return nil, fmt.Errorf("binding WAL identity does not match registered WAL identity")
	}
	if (learner && localMember.ID != "") || (!learner && (localMember.ID != nodeID || localMember.WALIdentity != "" && localMember.WALIdentity != hex.EncodeToString(binding.WALIdentity[:]))) {
		return nil, fmt.Errorf("current member registration does not bind this WAL identity")
	}
	if config.Verify == nil {
		return nil, fmt.Errorf("candidate verifier is required")
	}
	copy := &ManagedAppendOnlyConfig{Binding: binding, Verify: config.Verify}
	return copy, nil
}

// ValidateCertifiedValue verifies a value's QuePaxa certificate against this
// Core's slot configuration and recovery decision rules without importing or
// persisting the value. It is suitable for authenticating externally supplied
// certified evidence during read-only recovery.
func (c *Core) ValidateCertifiedValue(ctx context.Context, value DecidedValue) error {
	if c == nil {
		return fmt.Errorf("QuePaxa Core unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	decision, err := c.certifiedDecision(value)
	if err != nil {
		return err
	}
	if err := c.validateDecisionForRecovery(decision, true); err != nil {
		return err
	}
	return ctx.Err()
}

// ValidateManagedCheckpointRoot authenticates an already-decided checkpoint
// for read-only root selection. Proposal/record eligibility remains stricter.
func (c *Core) ValidateManagedCheckpointRoot(ctx context.Context, candidate ManagedCheckpointCandidate) error {
	if c == nil || c.managedAppendOnly == nil || c.managedAppendOnly.Verify == nil {
		return ErrManagedAppendOnlyUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	candidate.Value = bytes.Clone(candidate.Value)
	seal, checkpoint, err := DecodeCheckpointSeal(candidate.Value)
	if err != nil || !checkpoint {
		if err == nil {
			err = fmt.Errorf("candidate value is not a QCKP")
		}
		return err
	}
	if sha256.Sum256(candidate.Value) != candidate.Hash || !sameManagedCheckpointSeal(seal, candidate.Seal) || candidate.DecisionSlot <= seal.Index || candidate.ConfigID == 0 {
		return fmt.Errorf("managed checkpoint root identity is invalid")
	}
	candidate.Seal = cloneManagedCheckpointSeal(seal)
	if err := c.validateManagedCheckpointRootLocal(candidate); err != nil {
		return err
	}
	if _, err := c.checkpointMembershipForSeal(seal); err != nil {
		return err
	}
	decision, ok := c.CertifiedValue(candidate.DecisionSlot)
	if !ok || decision.Hash != candidate.Hash || !bytes.Equal(decision.Value, candidate.Value) {
		return fmt.Errorf("managed checkpoint root is not the exact locally certified decision")
	}
	configID, certificate, err := decodeCertificate(decision.Certificate)
	if err != nil || certificate.Slot != candidate.DecisionSlot || certificate.Proposal.Hash != decision.Hash || configID != candidate.ConfigID {
		return fmt.Errorf("managed checkpoint root certificate identity is invalid")
	}
	certified, err := c.certifiedDecision(decision)
	if err != nil {
		return err
	}
	if err := c.validateDecisionForRecovery(certified, true); err != nil {
		return fmt.Errorf("validate managed checkpoint root certificate: %w", err)
	}
	certificateBytes := bytes.Clone(decision.Certificate)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.managedAppendOnly.Verify(ctx, candidate); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.validateManagedCheckpointRootLocal(candidate); err != nil {
		return err
	}
	decision, ok = c.CertifiedValue(candidate.DecisionSlot)
	if !ok || decision.Hash != candidate.Hash || !bytes.Equal(decision.Value, candidate.Value) || !bytes.Equal(decision.Certificate, certificateBytes) {
		return fmt.Errorf("managed checkpoint decision changed during root verification")
	}
	return nil
}

func (c *Core) validateManagedCheckpointRootLocal(candidate ManagedCheckpointCandidate) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	seal := candidate.Seal
	if c.managedAppendOnly == nil || c.walIdentity != hex.EncodeToString(c.managedAppendOnly.Binding.WALIdentity[:]) || seal.GenerationAnchorHash != c.generationAnchorHash {
		return fmt.Errorf("managed checkpoint binding or generation lineage changed")
	}
	if candidate.DecisionSlot <= c.floor || candidate.ConfigID == 0 || c.clusterForSlotLocked(candidate.DecisionSlot).ConfigID != candidate.ConfigID {
		return fmt.Errorf("managed checkpoint decision is outside retained certified history")
	}
	if c.floor > seal.Index {
		return fmt.Errorf("managed checkpoint root is below the recovery floor")
	}
	if prefix, ok := c.prefixes[seal.Index]; ok && prefix != seal.PrefixHash {
		return fmt.Errorf("managed checkpoint prefix differs from available local history")
	}
	if order, following, err := c.checkpointLeaderOrdersLocked(seal.Index); err == nil && (!slices.Equal(order, seal.NextLeaderOrder) || !slices.Equal(following, seal.FollowingLeaderOrder)) {
		return fmt.Errorf("managed checkpoint leader order differs from available local history")
	}
	if c.reconfigEnabled {
		if seal.Membership == nil {
			return fmt.Errorf("managed checkpoint lacks historical membership proof")
		}
		if localMembership, err := c.checkpointMembershipAtLocked(seal.Index); err == nil && !sameMembershipRecord(localMembership, *seal.Membership) {
			return fmt.Errorf("managed checkpoint membership differs from available local history")
		}
		if seal.ConfigID != c.clusterForSlotLocked(seal.Index+1).ConfigID {
			return fmt.Errorf("managed checkpoint cut config differs from available local history")
		}
	} else {
		if seal.Membership != nil || seal.ConfigID != c.config.ConfigID {
			return fmt.Errorf("managed checkpoint cut differs from fixed historical membership")
		}
		localtesthooks.Hit("core:managed-checkpoint-fixed-membership-under-read-lock")
		if !c.validateCheckpointLeaderOrdersLocked(seal.Index, seal.NextLeaderOrder, seal.FollowingLeaderOrder) {
			return fmt.Errorf("managed checkpoint cut differs from fixed historical membership")
		}
	}
	return nil
}

func validateManagedDigest(name, value string) error {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != value {
		return fmt.Errorf("%s must be canonical lowercase 32-byte hex", name)
	}
	return nil
}

// ValidateManagedCheckpointCandidate verifies a checkpoint without preparing
// or persisting it. The trusted verifier is called without Core locks held.
func (c *Core) ValidateManagedCheckpointCandidate(ctx context.Context, candidate ManagedCheckpointCandidate) error {
	if c.managedAppendOnly == nil || c.managedAppendOnly.Verify == nil {
		return ErrManagedAppendOnlyUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	candidate.Value = bytes.Clone(candidate.Value)
	seal, isCheckpoint, err := DecodeCheckpointSeal(candidate.Value)
	if err != nil || !isCheckpoint {
		if err == nil {
			err = fmt.Errorf("candidate value is not a QCKP")
		}
		return err
	}
	if sha256.Sum256(candidate.Value) != candidate.Hash || !sameManagedCheckpointSeal(seal, candidate.Seal) {
		return fmt.Errorf("managed checkpoint candidate bytes and identity differ")
	}
	// Use the decoded identity from the exact byte copy; do not retain mutable
	// slices supplied by the caller or expose Core's copy to the verifier.
	candidate.Seal = cloneManagedCheckpointSeal(seal)
	if candidate.DecisionSlot <= seal.Index || candidate.ConfigID == 0 {
		return fmt.Errorf("managed checkpoint candidate decision slot or config is invalid")
	}
	if err := c.validateManagedCandidateLocal(candidate); err != nil {
		return err
	}
	if _, err := c.checkpointMembershipForSeal(candidate.Seal); err != nil {
		return err
	}
	verified := candidate
	verified.Value = bytes.Clone(candidate.Value)
	verified.Seal = cloneManagedCheckpointSeal(candidate.Seal)
	if err := c.managedAppendOnly.Verify(ctx, verified); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.validateManagedCandidateLocal(candidate)
}

func cloneManagedCheckpointSeal(seal CheckpointSeal) CheckpointSeal {
	seal.NextLeaderOrder = append([]NodeID(nil), seal.NextLeaderOrder...)
	seal.FollowingLeaderOrder = append([]NodeID(nil), seal.FollowingLeaderOrder...)
	if seal.Membership != nil {
		membership := cloneMembershipRecord(*seal.Membership)
		seal.Membership = &membership
	}
	return seal
}

func sameManagedCheckpointSeal(left, right CheckpointSeal) bool {
	return left.ConfigID == right.ConfigID && left.Index == right.Index && left.RootHash == right.RootHash && left.StateHash == right.StateHash && left.PrefixHash == right.PrefixHash && left.GenerationAnchorHash == right.GenerationAnchorHash && slices.Equal(left.NextLeaderOrder, right.NextLeaderOrder) && slices.Equal(left.FollowingLeaderOrder, right.FollowingLeaderOrder) && sameCheckpointMembership(left.Membership, right.Membership)
}

func (c *Core) validateManagedCandidateLocal(candidate ManagedCheckpointCandidate) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.validateManagedCandidateLocalLocked(candidate)
}

func (c *Core) validateManagedCandidateLocalLocked(candidate ManagedCheckpointCandidate) error {
	seal := candidate.Seal
	if c.managedAppendOnly == nil || c.walIdentity != hex.EncodeToString(c.managedAppendOnly.Binding.WALIdentity[:]) || seal.GenerationAnchorHash != c.generationAnchorHash {
		return fmt.Errorf("managed checkpoint binding or generation lineage changed")
	}
	if candidate.DecisionSlot <= c.floor {
		return c.compactedErrorLocked(candidate.DecisionSlot)
	}
	decisionCluster := c.clusterForSlotLocked(candidate.DecisionSlot)
	if decisionCluster.ConfigID != candidate.ConfigID || !c.voterForClusterLocked(decisionCluster) {
		return fmt.Errorf("managed checkpoint decision slot is outside current voter membership")
	}
	if !c.reconfigEnabled && seal.ConfigID != c.clusterForSlotLocked(seal.Index+1).ConfigID {
		return fmt.Errorf("managed checkpoint cut config differs from local history")
	}
	if prefix, ok := c.prefixes[seal.Index]; ok && prefix != seal.PrefixHash {
		return fmt.Errorf("managed checkpoint prefix differs from available local history")
	}
	if order, following, err := c.checkpointLeaderOrdersLocked(seal.Index); err == nil && (!slices.Equal(order, seal.NextLeaderOrder) || !slices.Equal(following, seal.FollowingLeaderOrder)) {
		return fmt.Errorf("managed checkpoint leader order differs from available local history")
	}
	if c.reconfigEnabled {
		if seal.Membership == nil {
			return fmt.Errorf("managed checkpoint lacks historical membership proof")
		}
		if localMembership, err := c.checkpointMembershipAtLocked(seal.Index); err == nil {
			if !sameMembershipRecord(localMembership, *seal.Membership) {
				return fmt.Errorf("managed checkpoint membership differs from available local history")
			}
			if seal.ConfigID != c.clusterForSlotLocked(seal.Index+1).ConfigID {
				return fmt.Errorf("managed checkpoint cut config differs from available local history")
			}
		}
		if c.reconfiguration != nil {
			return fmt.Errorf("managed checkpoint decision is outside stable membership")
		}
	} else if seal.Membership != nil {
		return fmt.Errorf("fixed-membership checkpoint includes membership history")
	}
	return nil
}

func (c *Core) checkManagedCheckpointRecordEligibilityLocked(candidate ManagedCheckpointCandidate) error {
	if c.managedAppendOnly == nil || candidate.DecisionSlot == 0 || candidate.ConfigID == 0 {
		return ErrManagedAppendOnlyUnavailable
	}
	value, ok := c.values[candidate.Hash]
	if !ok || !bytes.Equal(value, candidate.Value) || sha256.Sum256(value) != candidate.Hash {
		return fmt.Errorf("managed checkpoint staged value changed before receipt append")
	}
	if err := c.validateManagedCandidateLocalLocked(candidate); err != nil {
		return err
	}
	return nil
}

// LatestManagedCheckpointSeal selects only actual certified managed QCKPs by
// lexicographic (cut, decision slot), independent of arrival or replay order.
func (c *Core) LatestManagedCheckpointSeal() (SealedCheckpoint, bool, error) {
	if c.managedAppendOnly == nil {
		return SealedCheckpoint{}, false, ErrManagedAppendOnlyUnavailable
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var latest SealedCheckpoint
	var latestHash ValueHash
	for slot, decided := range c.decided {
		if decided.Slot != slot || sha256.Sum256(decided.Value) != decided.Hash {
			return SealedCheckpoint{}, false, fmt.Errorf("managed checkpoint decision identity mismatch at slot %d", slot)
		}
		seal, checkpoint, err := DecodeCheckpointSeal(decided.Value)
		if err != nil {
			return SealedCheckpoint{}, false, err
		}
		if !checkpoint {
			continue
		}
		configID, decision, err := decodeCertificate(decided.Certificate)
		if err != nil || decision.Slot != slot || decision.Proposal.Hash != decided.Hash || configID == 0 {
			return SealedCheckpoint{}, false, fmt.Errorf("managed checkpoint certificate identity mismatch at slot %d", slot)
		}
		hash := decided.Hash
		current := SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: slot}
		if current.Index > latest.Index || current.Index == latest.Index && current.DecisionSlot > latest.DecisionSlot {
			latest, latestHash = current, hash
			continue
		}
		if current.Index == latest.Index && current.DecisionSlot == latest.DecisionSlot && latest.Index != 0 && (current.RootHash != latest.RootHash || hash != latestHash) {
			return SealedCheckpoint{}, false, fmt.Errorf("conflicting managed checkpoint values at rank (%d,%d)", current.Index, current.DecisionSlot)
		}
	}
	return latest, latest.Index != 0, nil
}
