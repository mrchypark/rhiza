package quepaxa

import (
	"crypto/sha256"
	"fmt"
)

// ConfigTransition is one completed freeze→terminal reconfiguration pair
// whose certificates were durably committed by this Core.
type ConfigTransition struct {
	Freeze   DecidedValue `json:"freeze"`
	Terminal DecidedValue `json:"terminal"`
}

// MembershipRecord is the ordered genesis cluster followed by every
// completed config transition certified by this Core.
type MembershipRecord struct {
	Genesis     Cluster            `json:"genesis"`
	Transitions []ConfigTransition `json:"transitions"`
	// Abort retains the latest certified addition abort. It is evidence for a
	// management retry round, never authority to activate its target.
	Abort *ConfigTransition `json:"abort,omitempty"`
}

// MembershipHistory returns the deep-copied ordered genesis cluster and
// every completed freeze→terminal config transition.  Every transition in
// configHistory must have both a decided and durable freeze slot and a
// decided and durable terminal slot; the method returns an error rather
// than silently omitting a partially committed transition.
//
// The returned copies protect internal Core state.
func (c *Core) MembershipHistory() (MembershipRecord, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.membershipHistoryLocked(0)
}

// CheckpointMembership returns the completed durable history that a seal at
// index must bind. A checkpoint cannot race an active freeze or place its
// floor on a membership boundary.
func (c *Core) CheckpointMembership(index Slot) (MembershipRecord, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if index == 0 || index > c.tip || c.reconfiguration != nil {
		return MembershipRecord{}, fmt.Errorf("checkpoint membership is not stable at slot %d", index)
	}
	return c.membershipHistoryLocked(index)
}

func (c *Core) membershipHistoryLocked(through Slot) (MembershipRecord, error) {
	record, baseTransitions, err := c.membershipTransitionsLocked(through, through != 0)
	if err != nil {
		return MembershipRecord{}, err
	}
	var baseAbort *ConfigTransition
	if c.baseMembership != nil && len(c.configHistory)-1 == baseTransitions && c.baseMembership.Abort != nil {
		baseAbort = cloneMembershipRecord(*c.baseMembership).Abort
	}
	if through == 0 {
		if c.lastAbort != nil {
			abort := ConfigTransition{Freeze: cloneDecidedValue(c.lastAbort.Freeze), Terminal: cloneDecidedValue(c.lastAbort.Terminal)}
			record.Abort = &abort
		} else if baseAbort != nil {
			record.Abort = baseAbort
		}
		return record, nil
	}
	var lastSuccess Slot
	if len(record.Transitions) != 0 {
		lastSuccess = record.Transitions[len(record.Transitions)-1].Terminal.Slot
	}
	var candidateBaseAbort *ConfigTransition
	if c.baseMembership != nil && c.baseMembership.Abort != nil {
		candidateBaseAbort = cloneMembershipRecord(*c.baseMembership).Abort
	}
	abort, err := c.latestAbortAtLocked(through, lastSuccess, candidateBaseAbort)
	if err != nil {
		return MembershipRecord{}, err
	}
	record.Abort = abort
	return record, nil
}

// checkpointMembershipAtLocked returns the already-recovered membership prefix
// that existed at a checkpoint cut. Later certified transitions must not
// invalidate an older marker; the seal is independently checked against this
// exact prefix and its own genesis-bound certificates.
func (c *Core) checkpointMembershipAtLocked(through Slot) (MembershipRecord, error) {
	if through == 0 {
		return MembershipRecord{}, fmt.Errorf("checkpoint membership cut is zero")
	}
	record, _, err := c.membershipTransitionsLocked(through, false)
	if err != nil {
		return MembershipRecord{}, err
	}
	var lastSuccess Slot
	if len(record.Transitions) != 0 {
		lastSuccess = record.Transitions[len(record.Transitions)-1].Terminal.Slot
	}
	var baseAbort *ConfigTransition
	if c.baseMembership != nil && c.baseMembership.Abort != nil {
		baseAbort = cloneMembershipRecord(*c.baseMembership).Abort
	}
	abort, err := c.latestAbortAtLocked(through, lastSuccess, baseAbort)
	if err != nil {
		return MembershipRecord{}, err
	}
	record.Abort = abort
	return record, nil
}

func (c *Core) membershipTransitionsLocked(through Slot, rejectCrossing bool) (MembershipRecord, int, error) {
	if len(c.configHistory) == 0 {
		return MembershipRecord{}, 0, fmt.Errorf("membership history: no config history")
	}

	genesis := cloneCluster(c.configHistory[0].cluster)
	transitions := make([]ConfigTransition, 0, len(c.configHistory)-1)
	start := 1
	baseTransitions := -1
	if c.baseMembership != nil {
		genesis = cloneCluster(c.baseMembership.Genesis)
		base := cloneMembershipRecord(*c.baseMembership)
		baseTransitions = len(base.Transitions)
		start += baseTransitions
		if through == 0 {
			transitions = base.Transitions
		} else {
			for _, transition := range base.Transitions {
				if transition.Terminal.Slot >= through {
					if rejectCrossing {
						return MembershipRecord{}, 0, fmt.Errorf("checkpoint membership crosses configuration boundary at slot %d", transition.Terminal.Slot)
					}
					return MembershipRecord{Genesis: genesis, Transitions: transitions}, baseTransitions, nil
				}
				transitions = append(transitions, transition)
			}
		}
	}

	for i := start; i < len(c.configHistory); i++ {
		epoch := c.configHistory[i]

		// Every configHistory entry is produced by applyReconfigurationLocked
		// which sets epoch.start = previous terminal + 1. The freeze slot is
		// 17 slots before and the terminal slot is 1 slot before.
		if epoch.start < 18 {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d has start %d < 18", i, epoch.start)
		}
		freezeSlot := epoch.start - 17
		terminalSlot := epoch.start - 1
		if through != 0 && terminalSlot >= through {
			if rejectCrossing {
				return MembershipRecord{}, 0, fmt.Errorf("checkpoint membership crosses configuration boundary at slot %d", terminalSlot)
			}
			break
		}

		freezeDecision, frozen, err := c.loadDecidedLocked(freezeSlot)
		if err != nil {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: load freeze slot %d: %w", freezeSlot, err)
		}
		if !frozen {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: freeze slot %d not decided", freezeSlot)
		}
		if !c.isDurableLocked(freezeSlot) {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: freeze slot %d not durable", freezeSlot)
		}

		terminalDecision, terminated, err := c.loadDecidedLocked(terminalSlot)
		if err != nil {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: load terminal slot %d: %w", terminalSlot, err)
		}
		if !terminated {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: terminal slot %d not decided", terminalSlot)
		}
		if !c.isDurableLocked(terminalSlot) {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: terminal slot %d not durable", terminalSlot)
		}

		// Validate internal invariant: the terminal value must be a terminal
		// reconfiguration control that references this freeze slot and carries
		// the same target cluster the configHistory records.
		terminalControl, ok, err := decodeReconfiguration(terminalDecision.Value)
		if err != nil || !ok {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d terminal is not a reconfiguration control: %w", i, err)
		}
		if !terminalControl.Terminal {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d terminal flag unset", i)
		}
		if terminalControl.Freeze != freezeSlot {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d terminal freeze=%d want %d", i, terminalControl.Freeze, freezeSlot)
		}
		if terminalControl.TerminalSlot != terminalSlot {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d terminal slot=%d want %d", i, terminalControl.TerminalSlot, terminalSlot)
		}
		if sha256.Sum256(terminalDecision.Value) != terminalDecision.Hash {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d terminal value hash mismatch", i)
		}
		if sha256.Sum256(freezeDecision.Value) != freezeDecision.Hash {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d freeze value hash mismatch", i)
		}
		if !sameCluster(terminalControl.Target, epoch.cluster) {
			return MembershipRecord{}, 0, fmt.Errorf("membership history: transition %d terminal target does not match configHistory", i)
		}

		transitions = append(transitions, ConfigTransition{
			Freeze: DecidedValue{
				Slot:        freezeDecision.Slot,
				Hash:        freezeDecision.Hash,
				Value:       append([]byte(nil), freezeDecision.Value...),
				Certificate: append([]byte(nil), freezeDecision.Certificate...),
			},
			Terminal: DecidedValue{
				Slot:        terminalDecision.Slot,
				Hash:        terminalDecision.Hash,
				Value:       append([]byte(nil), terminalDecision.Value...),
				Certificate: append([]byte(nil), terminalDecision.Certificate...),
			},
		})
	}

	return MembershipRecord{Genesis: genesis, Transitions: transitions}, baseTransitions, nil
}

// latestAbortAtLocked resolves the latest certified abort within
// (lastSuccess, through]. Aborts do not change the active epoch, so an abort
// at the exact checkpoint cut is included.
func (c *Core) latestAbortAtLocked(through, lastSuccess Slot, baseAbort *ConfigTransition) (*ConfigTransition, error) {
	var abort *ConfigTransition
	if baseAbort != nil && baseAbort.Terminal.Slot <= through && baseAbort.Terminal.Slot > lastSuccess {
		abort = baseAbort
	}
	if c.lastAbort != nil && c.lastAbort.Terminal.Slot <= through && c.lastAbort.Terminal.Slot > lastSuccess {
		if c.isDurableLocked(c.lastAbort.Freeze.Slot) && c.isDurableLocked(c.lastAbort.Terminal.Slot) {
			if abort == nil || abort.Terminal.Slot < c.lastAbort.Terminal.Slot {
				copyAbort := ConfigTransition{Freeze: cloneDecidedValue(c.lastAbort.Freeze), Terminal: cloneDecidedValue(c.lastAbort.Terminal)}
				abort = &copyAbort
			}
		} else if c.lastAbort.Terminal.Slot <= c.floor && baseAbort != nil && sameMembershipTransition(*c.lastAbort, *baseAbort) {
			if abort == nil || abort.Terminal.Slot < baseAbort.Terminal.Slot {
				abort = baseAbort
			}
		}
	}
	inspectAbortSlot := func(slot Slot) error {
		if slot <= lastSuccess || slot > through || !c.isDurableLocked(slot) {
			return nil
		}
		decision, ok, err := c.loadDecidedLocked(slot)
		if err != nil {
			return fmt.Errorf("membership history: load abort slot %d: %w", slot, err)
		}
		if !ok {
			return nil
		}
		control, ok, err := decodeReconfiguration(decision.Value)
		if err != nil || !ok || !control.Terminal || !control.Abort {
			return nil
		}
		freeze, found, err := c.loadDecidedLocked(control.Freeze)
		if err != nil {
			return fmt.Errorf("membership history: load abort freeze slot %d: %w", control.Freeze, err)
		}
		if !found || !c.isDurableLocked(control.Freeze) {
			return fmt.Errorf("membership history: abort freeze slot %d is not durable", control.Freeze)
		}
		if abort == nil || abort.Terminal.Slot < slot {
			abort = &ConfigTransition{
				Freeze:   cloneDecidedValue(freeze),
				Terminal: cloneDecidedValue(decision),
			}
		}
		return nil
	}
	seenAbort := make(map[Slot]struct{}, len(c.abortTerminalSlots))
	for _, slot := range c.abortTerminalSlots {
		if _, done := seenAbort[slot]; done {
			continue
		}
		seenAbort[slot] = struct{}{}
		if err := inspectAbortSlot(slot); err != nil {
			return nil, err
		}
	}
	for slot := range c.decided {
		if _, done := seenAbort[slot]; done {
			continue
		}
		if err := inspectAbortSlot(slot); err != nil {
			return nil, err
		}
	}
	return abort, nil
}
