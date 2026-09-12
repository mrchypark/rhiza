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

	if len(c.configHistory) == 0 {
		return MembershipRecord{}, fmt.Errorf("membership history: no config history")
	}

	genesis := cloneCluster(c.configHistory[0].cluster)
	transitions := make([]ConfigTransition, 0, len(c.configHistory)-1)
	start := 1
	baseTransitions := -1
	if c.baseMembership != nil {
		genesis = cloneCluster(c.baseMembership.Genesis)
		base := cloneMembershipRecord(*c.baseMembership)
		transitions = base.Transitions
		baseTransitions = len(transitions)
		start += len(transitions)
		if len(c.configHistory)-1 > len(transitions) {
			base.Abort = nil // a later successful transition starts a new revision.
		}
	}

	for i := start; i < len(c.configHistory); i++ {
		epoch := c.configHistory[i]

		// Every configHistory entry is produced by applyReconfigurationLocked
		// which sets epoch.start = previous terminal + 1.  The freeze slot is
		// 17 slots before and the terminal slot is 1 slot before.
		if epoch.start < 18 {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d has start %d < 18", i, epoch.start)
		}
		freezeSlot := epoch.start - 17
		terminalSlot := epoch.start - 1
		if through != 0 && terminalSlot >= through {
			return MembershipRecord{}, fmt.Errorf("checkpoint membership crosses configuration boundary at slot %d", terminalSlot)
		}

		freezeDecision, frozen := c.decided[freezeSlot]
		if !frozen {
			return MembershipRecord{}, fmt.Errorf("membership history: freeze slot %d not decided", freezeSlot)
		}
		if !c.durable[freezeSlot] {
			return MembershipRecord{}, fmt.Errorf("membership history: freeze slot %d not durable", freezeSlot)
		}

		terminalDecision, terminated := c.decided[terminalSlot]
		if !terminated {
			return MembershipRecord{}, fmt.Errorf("membership history: terminal slot %d not decided", terminalSlot)
		}
		if !c.durable[terminalSlot] {
			return MembershipRecord{}, fmt.Errorf("membership history: terminal slot %d not durable", terminalSlot)
		}

		// Validate internal invariant: the terminal value must be a terminal
		// reconfiguration control that references this freeze slot and carries
		// the same target cluster the configHistory records.
		terminalControl, ok, err := decodeReconfiguration(terminalDecision.Value)
		if err != nil || !ok {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d terminal is not a reconfiguration control: %w", i, err)
		}
		if !terminalControl.Terminal {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d terminal flag unset", i)
		}
		if terminalControl.Freeze != freezeSlot {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d terminal freeze=%d want %d", i, terminalControl.Freeze, freezeSlot)
		}
		if terminalControl.TerminalSlot != terminalSlot {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d terminal slot=%d want %d", i, terminalControl.TerminalSlot, terminalSlot)
		}
		if sha256.Sum256(terminalDecision.Value) != terminalDecision.Hash {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d terminal value hash mismatch", i)
		}
		if sha256.Sum256(freezeDecision.Value) != freezeDecision.Hash {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d freeze value hash mismatch", i)
		}
		if !sameCluster(terminalControl.Target, epoch.cluster) {
			return MembershipRecord{}, fmt.Errorf("membership history: transition %d terminal target does not match configHistory", i)
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

	record := MembershipRecord{Genesis: genesis, Transitions: transitions}
	if c.lastAbort != nil {
		abort := ConfigTransition{Freeze: cloneDecidedValue(c.lastAbort.Freeze), Terminal: cloneDecidedValue(c.lastAbort.Terminal)}
		record.Abort = &abort
	} else if c.baseMembership != nil && len(c.configHistory)-1 == baseTransitions && c.baseMembership.Abort != nil {
		abort := cloneMembershipRecord(*c.baseMembership).Abort
		record.Abort = abort
	}
	return record, nil
}
