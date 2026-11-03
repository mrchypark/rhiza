package quepaxa

import (
	"context"
	"crypto/sha256"
	"fmt"
)

// ManagedReconstructionCut is an opaque, immutable Core snapshot used while a
// managed application projection is rebuilt outside the Core lock.
type ManagedReconstructionCut struct {
	core        *Core
	tip         Slot
	prefix      [32]byte
	seal        CheckpointSeal
	sealSlot    Slot
	sealHash    ValueHash
	sealCert    [32]byte
	floor       Slot
	floorRoot   [32]byte
	config      *ManagedAppendOnlyConfig
	binding     ManagedAppendOnlyBinding
	walIdentity string
	anchor      [32]byte
	membership  uint64
	cluster     Cluster
	configID    uint
	nodeID      NodeID
	localMode   bool
	observer    bool
	learner     bool
}

// CaptureManagedReconstructionCut binds an exact already-selected checkpoint
// producer to a fixed certified prefix. through==0 selects the current tip;
// otherwise it is the fixed historical tip for an installer-journal resume.
// This method does not require voter or ReadIndex eligibility.
func (c *Core) CaptureManagedReconstructionCut(sealed SealedCheckpoint, through Slot, expectedPrefix [32]byte) (*ManagedReconstructionCut, error) {
	if c == nil {
		return nil, fmt.Errorf("managed reconstruction Core unavailable")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.managedAppendOnly != nil && (c.managedAppendOnly.Verify == nil || c.managedAppendOnly.Binding.ApplicationProtocol != managedGraphRequestAppendOnlyApplicationProtocol) {
		return nil, fmt.Errorf("managed reconstruction profile unavailable")
	}
	if through == 0 {
		through = c.tip
	}
	if sealed.Index == 0 || sealed.DecisionSlot <= sealed.Index || sealed.DecisionSlot > through || through > c.tip || sealed.Index < c.floor || c.floor == sealed.Index && c.floorRoot != sealed.RootHash {
		return nil, fmt.Errorf("managed reconstruction root lies outside certified cut")
	}
	decision, ok := c.decided[sealed.DecisionSlot]
	if !ok || decision.Slot != sealed.DecisionSlot || sha256.Sum256(decision.Value) != decision.Hash {
		return nil, fmt.Errorf("managed reconstruction producer certificate unavailable")
	}
	configID, certified, err := decodeCertificate(decision.Certificate)
	if err != nil || certified.Slot != decision.Slot || certified.Proposal.Hash != decision.Hash || configID == 0 {
		return nil, fmt.Errorf("managed reconstruction producer certificate is invalid")
	}
	decoded, isSeal, err := DecodeCheckpointSeal(decision.Value)
	if err != nil || !isSeal || !sameManagedCheckpointSeal(decoded, sealed.CheckpointSeal) {
		return nil, fmt.Errorf("managed reconstruction producer differs from selected root")
	}
	rootPrefix, rootOK := c.prefixes[sealed.Index]
	prefix, prefixOK := c.prefixes[through]
	if !rootOK || rootPrefix != sealed.PrefixHash || !prefixOK || (expectedPrefix != ([32]byte{}) && expectedPrefix != prefix) {
		return nil, fmt.Errorf("managed reconstruction certified prefix unavailable or changed")
	}
	cluster := cloneCluster(c.clusterForSlotLocked(c.tip + 1))
	var binding ManagedAppendOnlyBinding
	if c.managedAppendOnly != nil {
		binding = c.managedAppendOnly.Binding
	}
	return &ManagedReconstructionCut{
		core: c, tip: through, prefix: prefix, seal: sealed.CheckpointSeal,
		sealSlot: sealed.DecisionSlot, sealHash: decision.Hash, sealCert: sha256.Sum256(decision.Certificate), floor: c.floor,
		floorRoot: c.floorRoot, config: c.managedAppendOnly, binding: binding,
		walIdentity: c.walIdentity,
		anchor:      c.generationAnchorHash, membership: c.membershipVersion,
		cluster: cluster, configID: cluster.ConfigID, nodeID: c.nodeID,
		localMode: c.localMode, observer: c.observer, learner: c.learner,
	}, nil
}

func (cut *ManagedReconstructionCut) CertifiedTip() Slot {
	if cut == nil {
		return 0
	}
	return cut.tip
}

func (cut *ManagedReconstructionCut) ProducerDecisionSlot() Slot {
	if cut == nil {
		return 0
	}
	return cut.sealSlot
}

func (cut *ManagedReconstructionCut) PrefixHash() [32]byte {
	if cut == nil {
		return [32]byte{}
	}
	return cut.prefix
}

func (cut *ManagedReconstructionCut) CompactionFloor() Slot {
	if cut == nil {
		return 0
	}
	return cut.floor
}

func (cut *ManagedReconstructionCut) RecoveryRoot() [32]byte {
	if cut == nil {
		return [32]byte{}
	}
	return cut.floorRoot
}

func (cut *ManagedReconstructionCut) ConfigID() uint {
	if cut == nil {
		return 0
	}
	return cut.configID
}

// RecheckManagedReconstructionCut verifies unchanged authority, recovery base,
// and the exact captured prefix while allowing a monotone certified suffix.
// The returned tip is the single fixed bound callers may replay through.
func (c *Core) RecheckManagedReconstructionCut(ctx context.Context, cut *ManagedReconstructionCut) (Slot, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if c == nil || cut == nil || cut.core != c {
		return 0, fmt.Errorf("managed reconstruction cut belongs to another Core")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	cluster := c.clusterForSlotLocked(c.tip + 1)
	decision, ok := c.decided[cut.sealSlot]
	prefix, prefixOK := c.prefixes[cut.tip]
	rootPrefix, rootOK := c.prefixes[cut.seal.Index]
	decisionConfig, certificate, certErr := decodeCertificate(decision.Certificate)
	decoded, isSeal, sealErr := DecodeCheckpointSeal(decision.Value)
	if c.tip < cut.tip || c.floor != cut.floor || c.floorRoot != cut.floorRoot || !prefixOK || prefix != cut.prefix || !rootOK || rootPrefix != cut.seal.PrefixHash || c.managedAppendOnly != cut.config || c.managedAppendOnly != nil && c.managedAppendOnly.Binding != cut.binding || c.walIdentity != cut.walIdentity || c.generationAnchorHash != cut.anchor || c.membershipVersion != cut.membership || c.nodeID != cut.nodeID || c.localMode != cut.localMode || c.observer != cut.observer || c.learner != cut.learner || cluster.ConfigID != cut.configID || !sameCluster(cluster, cut.cluster) || !ok || decision.Hash != cut.sealHash || sha256.Sum256(decision.Value) != decision.Hash || sha256.Sum256(decision.Certificate) != cut.sealCert || certErr != nil || certificate.Slot != cut.sealSlot || certificate.Proposal.Hash != decision.Hash || decisionConfig == 0 || sealErr != nil || !isSeal || !sameManagedCheckpointSeal(decoded, cut.seal) || c.floor > cut.tip {
		return 0, fmt.Errorf("managed reconstruction authority or captured prefix changed")
	}
	return c.tip, nil
}
