package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// BeginReconfiguration commits a freeze under the current voter quorum. The
// next fifteen slots are drained under that same configuration; FinishReconfiguration
// then commits the terminal control record and activates target at T+1.
func (c *Core) BeginReconfiguration(ctx context.Context, target Cluster) (Slot, error) {
	if !c.reconfigEnabled || c.observer {
		return 0, ErrQuorumUnavailable
	}
	c.reconfigurationMu.Lock()
	defer c.reconfigurationMu.Unlock()
	if err := c.takePipeline(ctx); err != nil {
		return 0, err
	}
	defer c.releasePipeline()
	c.mu.RLock()
	if c.reconfiguration != nil {
		c.mu.RUnlock()
		return 0, fmt.Errorf("reconfiguration is already frozen")
	}
	current := c.clusterForSlotLocked(c.tip + 1)
	freeze := c.tip + 1
	prefix := c.prefixes[c.tip]
	highest := c.highestKnownSlotLocked()
	retired := make(map[NodeID]struct{}, len(c.retiredIDs))
	for id := range c.retiredIDs {
		retired[id] = struct{}{}
	}
	c.mu.RUnlock()
	if highest > freeze+15 {
		return 0, fmt.Errorf("reconfiguration cannot freeze with legacy slot %d beyond terminal %d", highest, freeze+16)
	}
	if err := validateReconfigurationTarget(current, target, retired); err != nil {
		return 0, err
	}
	value, err := encodeReconfiguration(reconfigurationValue{Freeze: freeze, TerminalSlot: freeze + 16, Target: cloneCluster(target), PrefixHash: prefix})
	if err != nil {
		return 0, err
	}
	decision, err := c.runSlot(ctx, freeze, value, true)
	if err != nil {
		return 0, err
	}
	if decision.Proposal.Hash != reconfigurationHash(value) {
		return 0, fmt.Errorf("freeze slot was decided by another value")
	}
	if err := c.acceptDecision(decision); err != nil {
		return 0, err
	}
	if _, err := c.completeDecision(ctx, freeze, true); err != nil {
		return 0, err
	}
	return freeze, nil
}

// FinishReconfiguration fills the bounded old-configuration drain and commits
// its terminal record. It never changes membership before that record is durable.
func (c *Core) FinishReconfiguration(ctx context.Context) error {
	if !c.reconfigEnabled || c.observer {
		return ErrQuorumUnavailable
	}
	c.reconfigurationMu.Lock()
	defer c.reconfigurationMu.Unlock()
	if err := c.takePipeline(ctx); err != nil {
		return err
	}
	c.mu.RLock()
	state := c.reconfiguration
	var previousTerminal Slot
	if len(c.configHistory) > 1 {
		previousTerminal = c.configHistory[len(c.configHistory)-1].start - 1
	}
	c.mu.RUnlock()
	if state == nil {
		c.releasePipeline()
		if previousTerminal != 0 {
			_, err := c.completeDecision(ctx, previousTerminal, true)
			return err
		}
		return fmt.Errorf("reconfiguration is not frozen")
	}
	c.releasePipeline()
	if _, err := c.completeDecision(ctx, state.freeze, true); err != nil {
		return err
	}
	if err := c.RecoverThrough(ctx, state.terminal-1); err != nil {
		return err
	}
	for slot := state.freeze + 1; slot < state.terminal; slot++ {
		if _, err := c.completeDecision(ctx, slot, true); err != nil {
			return err
		}
	}
	if err := c.takePipeline(ctx); err != nil {
		return err
	}
	defer c.releasePipeline()
	prefix, ok := c.PrefixHash(state.terminal - 1)
	if !ok {
		return fmt.Errorf("reconfiguration drain prefix is unavailable")
	}
	value, err := encodeReconfiguration(reconfigurationValue{Terminal: true, Freeze: state.freeze, TerminalSlot: state.terminal, Target: cloneCluster(state.target), PrefixHash: prefix})
	if err != nil {
		return err
	}
	decision, err := c.runSlot(ctx, state.terminal, value, true)
	if err != nil {
		return err
	}
	if decision.Proposal.Hash != reconfigurationHash(value) {
		return fmt.Errorf("terminal slot was decided by another value")
	}
	if err := c.acceptDecision(decision); err != nil {
		return err
	}
	_, err = c.completeDecision(ctx, state.terminal, true)
	return err
}

func (c *Core) takePipeline(ctx context.Context) error {
	acquired := 0
	for acquired < cap(c.pipeline) {
		select {
		case c.pipeline <- struct{}{}:
			acquired++
		case <-ctx.Done():
			for acquired > 0 {
				<-c.pipeline
				acquired--
			}
			return ctx.Err()
		}
	}
	return nil
}
func (c *Core) releasePipeline() {
	for range cap(c.pipeline) {
		<-c.pipeline
	}
}

func validateReconfigurationTarget(current, target Cluster, retiredIDs map[NodeID]struct{}) error {
	if target.ConfigID != current.ConfigID+1 || len(target.Members) == 0 {
		return fmt.Errorf("invalid reconfiguration target")
	}
	old, next := current.MemberSet(), target.MemberSet()
	if len(next) != len(target.Members) {
		return fmt.Errorf("duplicate target member")
	}
	for _, member := range target.Members {
		if member.ID == "" {
			return fmt.Errorf("target member ID is required")
		}
	}
	added, removed := 0, 0
	for id, oldMember := range old {
		if member, ok := next[id]; !ok {
			removed++
		} else if member != oldMember {
			return fmt.Errorf("retained member %q changed identity", id)
		}
	}
	for id := range next {
		if _, ok := old[id]; !ok {
			added++
			if _, retired := retiredIDs[id]; retired {
				return fmt.Errorf("member ID %q was retired", id)
			}
			identity, err := hex.DecodeString(next[id].WALIdentity)
			if err != nil || len(identity) != 32 || hex.EncodeToString(identity) != next[id].WALIdentity {
				return fmt.Errorf("added member %q has invalid WAL identity", id)
			}
		}
	}
	if added+removed != 1 {
		return fmt.Errorf("reconfiguration must add or remove exactly one voter")
	}
	return nil
}

func (c *Core) applyReconfigurationLocked(decision Decision) error {
	control, ok, err := decodeReconfiguration(decision.Proposal.Value)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if !c.reconfigEnabled {
		return fmt.Errorf("reconfiguration control is disabled")
	}
	if !control.Terminal {
		if state := c.reconfiguration; state != nil && decision.Slot > state.freeze && decision.Slot < state.terminal {
			return nil // any certified later freeze control is a drain no-op.
		}
		if c.reconfiguration != nil || decision.Slot != control.Freeze || c.tip+1 != decision.Slot || c.prefixes[c.tip] != control.PrefixHash {
			return fmt.Errorf("invalid reconfiguration freeze")
		}
		if err := validateReconfigurationTarget(c.clusterForSlotLocked(decision.Slot), control.Target, c.retiredIDs); err != nil {
			return err
		}
		safe := make(map[NodeID]struct{}, len(decision.Summaries))
		for _, summary := range decision.Summaries {
			safe[summary.RecorderID] = struct{}{}
		}
		c.reconfiguration = &reconfigurationState{freeze: control.Freeze, terminal: control.TerminalSlot, id: decision.Proposal.Hash, freezePrefix: control.PrefixHash, target: cloneCluster(control.Target), safe: safe}
		return nil
	}
	state := c.reconfiguration
	if state == nil || decision.Slot != state.terminal || control.Freeze != state.freeze || control.TerminalSlot != state.terminal || control.PrefixHash != c.prefixes[c.tip] || !sameCluster(control.Target, state.target) || c.tip+1 != decision.Slot {
		return fmt.Errorf("invalid reconfiguration terminal")
	}
	for _, summary := range decision.Summaries {
		if summary.ReconfigurationID != state.id {
			return fmt.Errorf("terminal quorum does not bind its freeze")
		}
	}
	old := c.clusterForSlotLocked(decision.Slot)
	for id := range old.MemberSet() {
		if _, kept := state.target.MemberSet()[id]; !kept {
			c.retiredIDs[id] = struct{}{}
		}
	}
	c.configHistory = append(c.configHistory, configEpoch{start: state.terminal + 1, cluster: cloneCluster(state.target)})
	clear(c.epochStart)
	clear(c.timings)
	c.reconfiguration = nil
	return nil
}

func (c *Core) reconfigurationRequest(slot Slot, config Cluster) (uint, ValueHash) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.reconfiguration != nil && slot > c.reconfiguration.freeze && slot <= c.reconfiguration.terminal {
		return config.ConfigID, c.reconfiguration.id
	}
	return config.ConfigID, ValueHash{}
}

func reconfigurationHash(value []byte) ValueHash { return sha256.Sum256(value) }

func sameCluster(left, right Cluster) bool {
	if left.ConfigID != right.ConfigID || len(left.Members) != len(right.Members) {
		return false
	}
	for i := range left.Members {
		if left.Members[i] != right.Members[i] {
			return false
		}
	}
	return true
}

func reconfigurationAdds(current, target Cluster) bool {
	for id := range target.MemberSet() {
		if _, exists := current.MemberSet()[id]; !exists {
			return true
		}
	}
	return false
}

// RecordCatchUpThrough returns the certified prefix needed before handling a
// record. Ordinary traffic retains the 16-slot window; a terminal vote must
// verify the entire drain even when it is inside that window.
func (c *Core) RecordCatchUpThrough(request RecordRequest) Slot {
	if !c.reconfigEnabled || request.Slot == 0 {
		return 0
	}
	c.mu.RLock()
	value := request.Proposal.Value
	if len(value) == 0 {
		value = c.values[request.Proposal.Hash]
	}
	terminal := c.reconfiguration != nil && request.Slot == c.reconfiguration.terminal
	c.mu.RUnlock()
	control, ok, _ := decodeReconfiguration(value)
	if terminal || (ok && control.Terminal) {
		return request.Slot - 1
	}
	if request.Slot > 16 {
		return request.Slot - 16
	}
	return 0
}
