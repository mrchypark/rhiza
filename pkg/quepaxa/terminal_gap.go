package quepaxa

import (
	"bytes"
	"crypto/sha256"
)

// TerminalGapKey identifies one locally known, pending reconfiguration
// terminal. It is an observation key, not authority to accept the terminal.
type TerminalGapKey struct {
	ConfigID   uint
	Freeze     Slot
	Terminal   Slot
	FreezeHash ValueHash
}

// StartupTerminalGapSnapshot returns the locally durable, still-pending
// terminal gap without requiring an incoming Learned decision. The key is
// bound to the active freeze and its exact durable control; the caller must
// still load and apply the certified archive suffix through the normal path.
func (c *Core) StartupTerminalGapSnapshot() (TerminalGapKey, bool) {
	if c == nil {
		return TerminalGapKey{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	state := c.reconfiguration
	if state == nil || state.freeze == 0 || state.terminal <= state.freeze ||
		state.terminal-state.freeze > leaderEpochSize || c.tip < state.freeze || c.tip >= state.terminal {
		return TerminalGapKey{}, false
	}
	configID := c.clusterForSlotLocked(state.freeze).ConfigID
	if configID == 0 {
		return TerminalGapKey{}, false
	}
	freeze, exists := c.decided[state.freeze]
	if !exists || !c.durable[state.freeze] || freeze.Hash != state.id ||
		sha256.Sum256(freeze.Value) != [32]byte(state.id) {
		return TerminalGapKey{}, false
	}
	control, ok, err := decodeReconfiguration(freeze.Value)
	if err != nil || !ok || control.Terminal || control.Abort || control.Freeze != state.freeze ||
		control.TerminalSlot != state.terminal || !sameCluster(control.Target, state.target) {
		return TerminalGapKey{}, false
	}
	if state.engineRemoval == nil {
		if control.EngineRemoval != nil {
			return TerminalGapKey{}, false
		}
	} else if control.EngineRemoval == nil ||
		!sameEngineRemoval(*state.engineRemoval, *control.EngineRemoval, false) {
		return TerminalGapKey{}, false
	}
	return TerminalGapKey{ConfigID: configID, Freeze: state.freeze, Terminal: state.terminal, FreezeHash: state.id}, true
}

// StartupCatchUpRoundEligible atomically verifies that a startup quorum round
// still describes the current voter configuration and no reconfiguration is
// pending. It is a snapshot predicate only; it does not grant readiness.
func (c *Core) StartupCatchUpRoundEligible(captured Cluster) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	current := c.clusterForSlotLocked(c.tip + 1)
	return c.reconfiguration == nil && sameCluster(captured, current) && c.voterForClusterLocked(current)
}

// KnownTerminalGap reports whether decision is the exact terminal for the
// active local freeze and the required prefix is still missing. The caller may
// use the key to request a best-effort archive check; it must still verify and
// apply any fetched decisions through the normal certified path.
func (c *Core) KnownTerminalGap(decision Decision) (TerminalGapKey, bool) {
	if c == nil || decision.Slot <= 1 || len(decision.Proposal.Value) == 0 ||
		decision.Proposal.Hash != ValueHash(sha256.Sum256(decision.Proposal.Value)) {
		return TerminalGapKey{}, false
	}
	control, ok, err := decodeReconfiguration(decision.Proposal.Value)
	if err != nil || !ok || !control.Terminal || control.Abort ||
		control.Freeze == 0 || control.TerminalSlot != decision.Slot {
		return TerminalGapKey{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	state := c.reconfiguration
	if state == nil || c.tip >= state.terminal-1 || state.freeze != control.Freeze ||
		state.terminal != control.TerminalSlot || !sameCluster(state.target, control.Target) ||
		decision.Slot != state.terminal || c.clusterForSlotLocked(state.freeze).ConfigID != decision.ConfigID {
		return TerminalGapKey{}, false
	}

	freeze, exists := c.decided[state.freeze]
	if !exists || !c.durable[state.freeze] || freeze.Hash != state.id ||
		sha256.Sum256(freeze.Value) != [32]byte(state.id) {
		return TerminalGapKey{}, false
	}
	freezeControl, freezeOK, freezeErr := decodeReconfiguration(freeze.Value)
	if freezeErr != nil || !freezeOK || freezeControl.Terminal || freezeControl.Abort ||
		freezeControl.Freeze != state.freeze || freezeControl.TerminalSlot != state.terminal ||
		!sameCluster(freezeControl.Target, state.target) {
		return TerminalGapKey{}, false
	}
	if state.engineRemoval == nil {
		if control.EngineRemoval != nil || freezeControl.EngineRemoval != nil {
			return TerminalGapKey{}, false
		}
	} else if freezeControl.EngineRemoval == nil || control.EngineRemoval == nil ||
		!sameEngineRemoval(*state.engineRemoval, *freezeControl.EngineRemoval, false) ||
		!sameEngineRemoval(*state.engineRemoval, *control.EngineRemoval, true) ||
		control.EngineRemoval.FreezeHash != state.id {
		return TerminalGapKey{}, false
	}

	return TerminalGapKey{
		ConfigID:   decision.ConfigID,
		Freeze:     state.freeze,
		Terminal:   state.terminal,
		FreezeHash: state.id,
	}, true
}

// PendingTerminalGap revalidates a previously observed key against the active
// local freeze. Prefix progress does not invalidate the key; completing,
// aborting, or replacing the transition does.
func (c *Core) PendingTerminalGap(key TerminalGapKey) bool {
	if c == nil || key.ConfigID == 0 || key.Freeze == 0 || key.Terminal <= key.Freeze || key.FreezeHash == (ValueHash{}) {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	state := c.reconfiguration
	return state != nil && state.freeze == key.Freeze && state.terminal == key.Terminal &&
		state.id == key.FreezeHash && c.clusterForSlotLocked(state.freeze).ConfigID == key.ConfigID
}

// AllowsNextReconfigurationFreeze permits an immediately contiguous freeze
// after a committed configuration change, before public readiness catches up.
// AcceptDecision still verifies the quorum certificate and durable transition.
func (c *Core) AllowsNextReconfigurationFreeze(decision Decision) bool {
	if c == nil || decision.Proposal.Hash != ValueHash(sha256.Sum256(decision.Proposal.Value)) {
		return false
	}
	control, ok, err := decodeReconfiguration(decision.Proposal.Value)
	if err != nil || !ok || control.Terminal || control.Abort || control.Freeze != decision.Slot {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	current := c.clusterForSlotLocked(c.tip + 1)
	return c.reconfigEnabled && c.reconfiguration == nil && c.voterForClusterLocked(current) &&
		decision.Slot == c.tip+1 && decision.ConfigID == current.ConfigID &&
		c.validateReconfigurationSlotsLocked(control.Freeze, control.TerminalSlot) == nil &&
		validateReconfigurationTarget(current, control.Target, c.retiredIDs) == nil
}

// AllowsPendingReconfigurationLearn admits only the locally frozen old-config
// prefix and its exact terminal. The normal AcceptDecision path still verifies
// the decision certificate and applies it durably.
func (c *Core) AllowsPendingReconfigurationLearn(decision Decision) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.allowsPendingReconfigurationLearnLocked(decision)
}

// PendingTerminalGapForLearn atomically captures the pending transition that
// admits decision. The returned key is an observation that must be rechecked
// after any blocking catch-up and immediately before accepting the decision.
func (c *Core) PendingTerminalGapForLearn(decision Decision) (TerminalGapKey, bool) {
	if c == nil {
		return TerminalGapKey{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.allowsPendingReconfigurationLearnLocked(decision) {
		return TerminalGapKey{}, false
	}
	state := c.reconfiguration
	return TerminalGapKey{
		ConfigID:   c.clusterForSlotLocked(state.freeze).ConfigID,
		Freeze:     state.freeze,
		Terminal:   state.terminal,
		FreezeHash: state.id,
	}, true
}

func (c *Core) allowsPendingReconfigurationLearnLocked(decision Decision) bool {
	if decision.Slot == 0 || len(decision.Proposal.Value) == 0 ||
		decision.Proposal.Hash != ValueHash(sha256.Sum256(decision.Proposal.Value)) {
		return false
	}
	state := c.reconfiguration
	if state == nil || state.freeze == 0 || state.terminal <= state.freeze ||
		decision.Slot < state.freeze || decision.Slot > state.terminal ||
		decision.ConfigID != c.clusterForSlotLocked(state.freeze).ConfigID {
		return false
	}

	freeze, exists := c.decided[state.freeze]
	if !exists || !c.durable[state.freeze] || freeze.Hash != state.id ||
		sha256.Sum256(freeze.Value) != [32]byte(state.id) {
		return false
	}
	freezeControl, isControl, err := decodeReconfiguration(freeze.Value)
	if err != nil || !isControl || freezeControl.Terminal || freezeControl.Abort ||
		freezeControl.Freeze != state.freeze || freezeControl.TerminalSlot != state.terminal ||
		!sameCluster(freezeControl.Target, state.target) {
		return false
	}

	control, isControl, err := decodeReconfiguration(decision.Proposal.Value)
	if err != nil {
		return false
	}
	if decision.Slot < state.terminal {
		if decision.Slot == state.freeze {
			return isControl && decision.Proposal.Hash == freeze.Hash && bytes.Equal(decision.Proposal.Value, freeze.Value)
		}
		if isControl && (control.Terminal || control.Abort) {
			return false
		}
		// The frozen old-config drain is bounded by this locally certified
		// terminal. Permit sparse delivery inside that exact interval; the
		// ordinary AcceptDecision path still verifies the certificate and
		// buffers/replays decisions in slot order.
		return decision.Slot <= state.terminal-1
	}
	if !isControl || !control.Terminal || control.Freeze != state.freeze ||
		control.TerminalSlot != state.terminal ||
		(c.tip >= state.terminal-1 && control.PrefixHash != c.prefixes[state.terminal-1]) ||
		!sameCluster(control.Target, state.target) {
		return false
	}
	if state.engineRemoval == nil {
		return control.EngineRemoval == nil
	}
	return !control.Abort && control.EngineRemoval != nil &&
		sameEngineRemoval(*state.engineRemoval, *control.EngineRemoval, true) &&
		control.EngineRemoval.FreezeHash == state.id &&
		control.EngineRemoval.StorageEffectsExcludedHash != (ValueHash{})
}

// AllowsInstalledTerminalLearn permits an exact retry of a terminal already
// installed on this Core while the Node re-establishes readiness. It does not
// authorize a different decision or any subsequent mutation.
func (c *Core) AllowsInstalledTerminalLearn(decision Decision) bool {
	if c == nil || decision.Slot == 0 || len(decision.Proposal.Value) == 0 ||
		decision.Proposal.Hash != ValueHash(sha256.Sum256(decision.Proposal.Value)) {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	installed, exists := c.decided[decision.Slot]
	if !exists || !c.durable[decision.Slot] || installed.Hash != decision.Proposal.Hash ||
		!bytes.Equal(installed.Value, decision.Proposal.Value) {
		return false
	}
	configID, stored, err := decodeCertificate(installed.Certificate)
	if err != nil || configID != decision.ConfigID || stored.Slot != decision.Slot ||
		stored.Proposal.Hash != decision.Proposal.Hash {
		return false
	}
	expectedCertificate, err := encodeCertificate(configID, decision)
	if err != nil || !bytes.Equal(installed.Certificate, expectedCertificate) {
		return false
	}
	control, ok, err := decodeReconfiguration(installed.Value)
	if err != nil || !ok || !control.Terminal || control.TerminalSlot != decision.Slot || control.Freeze == 0 ||
		c.clusterForSlotLocked(control.TerminalSlot).ConfigID != configID {
		return false
	}
	old := c.clusterForSlotLocked(control.TerminalSlot)
	if control.Abort {
		return control.EngineRemoval == nil && reconfigurationAdds(old, control.Target) &&
			sameCluster(c.clusterForSlotLocked(control.TerminalSlot+1), old)
	}
	return sameCluster(c.clusterForSlotLocked(control.TerminalSlot+1), control.Target)
}
