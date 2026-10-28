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
	return c.beginReconfiguration(ctx, target, nil, [32]byte{})
}

// BeginReconfigurationAt starts a new operation only when the latest durable
// abort terminal still matches expectedAbortSlot. Zero means no prior abort.
func (c *Core) BeginReconfigurationAt(ctx context.Context, target Cluster, expectedAbortSlot Slot) (Slot, error) {
	return c.beginReconfiguration(ctx, target, &expectedAbortSlot, [32]byte{})
}

// BeginReconfigurationBoundAt certifies the caller's nonzero immutable
// operation digest in the freeze control alongside the exact target and abort
// revision. Existing unbound APIs remain available for legacy transitions.
func (c *Core) BeginReconfigurationBoundAt(ctx context.Context, target Cluster, expectedAbortSlot Slot, operationDigest [32]byte) (Slot, error) {
	if operationDigest == ([32]byte{}) {
		return 0, fmt.Errorf("reconfiguration operation digest is required")
	}
	return c.beginReconfiguration(ctx, target, &expectedAbortSlot, operationDigest)
}

func (c *Core) beginReconfiguration(ctx context.Context, target Cluster, expectedAbortSlot *Slot, operationDigest [32]byte) (Slot, error) {
	if !c.reconfigEnabled || c.observer {
		return 0, ErrQuorumUnavailable
	}
	c.reconfigurationMu.Lock()
	defer c.reconfigurationMu.Unlock()
	if err := c.takePipeline(ctx); err != nil {
		return 0, err
	}
	defer c.releasePipeline()
	for {
		c.mu.RLock()
		if c.reconfiguration != nil {
			c.mu.RUnlock()
			return 0, fmt.Errorf("reconfiguration is already frozen")
		}
		if expectedAbortSlot != nil {
			actual, err := c.abortRevisionLocked()
			if err != nil {
				c.mu.RUnlock()
				return 0, err
			}
			if actual != *expectedAbortSlot {
				c.mu.RUnlock()
				return 0, fmt.Errorf("reconfiguration abort revision %d does not match expected %d", actual, *expectedAbortSlot)
			}
		}
		current := c.clusterForSlotLocked(c.tip + 1)
		freeze := c.tip + 1
		if freeze == 0 || freeze > ^Slot(0)-17 {
			c.mu.RUnlock()
			return 0, fmt.Errorf("reconfiguration slot range exhausted")
		}
		freezeSchedule := c.isLeaderScheduleSlotLocked(freeze)
		terminalSchedule := c.isLeaderScheduleSlotLocked(freeze + 16)
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
		if freezeSchedule || terminalSchedule {
			if err := c.RecoverThrough(ctx, freeze); err != nil {
				return 0, err
			}
			decided, err := c.completeDecision(ctx, freeze, true)
			if err != nil {
				return 0, err
			}
			if freezeSchedule {
				order, scheduled, err := DecodeLeaderSchedule(decided.Value)
				if err != nil || !scheduled || !validateLeaderSchedule(current, order) {
					return 0, fmt.Errorf("incompatible leader schedule history at slot %d", freeze)
				}
			}
			continue
		}
		value, err := encodeReconfiguration(reconfigurationValue{Freeze: freeze, TerminalSlot: freeze + 16, Target: cloneCluster(target), PrefixHash: prefix, OperationDigest: operationDigest})
		if err != nil {
			return 0, err
		}
		decision, err := c.runSlot(ctx, freeze, value, false)
		if err != nil {
			return 0, err
		}
		if err := c.acceptDecision(decision); err != nil {
			return 0, err
		}
		if _, err := c.completeDecision(ctx, freeze, true); err != nil {
			return 0, err
		}
		if decision.Proposal.Hash != reconfigurationHash(value) {
			return 0, fmt.Errorf("freeze slot was decided by another value")
		}
		return freeze, nil
	}
}

// abortRevisionLocked returns zero only when no abort revision exists. A
// terminal observed before its durable marker must block a compare-and-start.
func (c *Core) abortRevisionLocked() (Slot, error) {
	if c.lastAbort == nil {
		return 0, nil
	}
	freeze, terminal := c.lastAbort.Freeze, c.lastAbort.Terminal
	if terminal.Slot <= c.floor {
		if c.baseMembership != nil && c.baseMembership.Abort != nil && sameMembershipTransition(*c.lastAbort, *c.baseMembership.Abort) {
			return terminal.Slot, nil
		}
		return 0, fmt.Errorf("retained abort revision is not anchored in recovery base")
	}
	if !c.durable[freeze.Slot] || !c.durable[terminal.Slot] {
		return 0, fmt.Errorf("abort revision is not durable")
	}
	return terminal.Slot, nil
}

// FinishReconfiguration fills the bounded old-configuration drain and commits
// its terminal record. It never changes membership before that record is durable.
func (c *Core) FinishReconfiguration(ctx context.Context) error {
	return c.finishReconfiguration(ctx, false, nil, nil, [32]byte{})
}

// FinishReconfigurationAt commits a terminal only for the frozen target and
// abort revision that the caller previously recorded. Zero means no prior
// abort revision.
func (c *Core) FinishReconfigurationAt(ctx context.Context, target Cluster, expectedAbortSlot Slot) error {
	return c.finishReconfiguration(ctx, false, &target, &expectedAbortSlot, [32]byte{})
}

// FinishReconfigurationBoundAt commits only the exact bound freeze identified
// by target, abort revision, and operation digest.
func (c *Core) FinishReconfigurationBoundAt(ctx context.Context, target Cluster, expectedAbortSlot Slot, operationDigest [32]byte) error {
	if operationDigest == ([32]byte{}) {
		return fmt.Errorf("reconfiguration operation digest is required")
	}
	return c.finishReconfiguration(ctx, false, &target, &expectedAbortSlot, operationDigest)
}

// AbortReconfiguration certifies the frozen terminal under the old voter
// quorum without activating an added learner. It is only available for a
// pending addition; the original learner identity remains untouched.
func (c *Core) AbortReconfiguration(ctx context.Context) error {
	return c.finishReconfiguration(ctx, true, nil, nil, [32]byte{})
}

// AbortReconfigurationAt certifies an abort only for the frozen target and
// abort revision that the caller previously recorded. It rejects a stale
// management round before draining or proposing a terminal control.
func (c *Core) AbortReconfigurationAt(ctx context.Context, target Cluster, expectedAbortSlot Slot) error {
	return c.finishReconfiguration(ctx, true, &target, &expectedAbortSlot, [32]byte{})
}

// AbortReconfigurationBoundAt commits only the exact bound addition freeze
// identified by target, abort revision, and operation digest.
func (c *Core) AbortReconfigurationBoundAt(ctx context.Context, target Cluster, expectedAbortSlot Slot, operationDigest [32]byte) error {
	if operationDigest == ([32]byte{}) {
		return fmt.Errorf("reconfiguration operation digest is required")
	}
	return c.finishReconfiguration(ctx, true, &target, &expectedAbortSlot, operationDigest)
}

func (c *Core) finishReconfiguration(ctx context.Context, abort bool, expectedTarget *Cluster, expectedAbortSlot *Slot, operationDigest [32]byte) error {
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
	if expectedAbortSlot != nil && expectedTarget != nil && operationDigest != ([32]byte{}) {
		activeExact := false
		if state != nil {
			freeze, ok := c.decided[state.freeze]
			control, configured, decodeErr := decodeReconfiguration(freeze.Value)
			activeExact = ok && decodeErr == nil && configured && !control.Terminal && control.OperationDigest == operationDigest && sameCluster(state.target, *expectedTarget)
		}
		if !activeExact {
			terminal, found, err := c.boundHistoricalTerminalLocked(*expectedTarget, *expectedAbortSlot, operationDigest, abort)
			if err != nil || found {
				c.mu.RUnlock()
				c.releasePipeline()
				if err != nil {
					return err
				}
				_, err = c.CompleteDecision(ctx, terminal)
				return err
			}
		}
	}
	if expectedAbortSlot != nil {
		actual, err := c.abortRevisionLocked()
		if err != nil {
			c.mu.RUnlock()
			c.releasePipeline()
			return err
		}
		if actual != *expectedAbortSlot {
			c.mu.RUnlock()
			c.releasePipeline()
			return fmt.Errorf("reconfiguration abort revision %d does not match expected %d", actual, *expectedAbortSlot)
		}
	}
	if expectedTarget != nil && (state == nil || !sameCluster(state.target, *expectedTarget)) {
		c.mu.RUnlock()
		c.releasePipeline()
		return fmt.Errorf("reconfiguration target no longer matches the requested round")
	}
	if state != nil {
		freeze, ok := c.decided[state.freeze]
		control, configured, err := decodeReconfiguration(freeze.Value)
		if !ok || err != nil || !configured || control.Terminal || control.OperationDigest != operationDigest {
			c.mu.RUnlock()
			c.releasePipeline()
			return fmt.Errorf("reconfiguration operation digest does not match the frozen round")
		}
	}
	c.mu.RUnlock()
	if state == nil {
		c.releasePipeline()
		if operationDigest != ([32]byte{}) {
			return fmt.Errorf("bound reconfiguration is not frozen")
		}
		if previousTerminal != 0 {
			_, err := c.completeDecision(ctx, previousTerminal, true)
			return err
		}
		return fmt.Errorf("reconfiguration is not frozen")
	}
	if abort && !reconfigurationAdds(c.clusterForSlot(state.freeze), state.target) {
		c.releasePipeline()
		return fmt.Errorf("only a pending addition may be aborted")
	}
	c.mu.RLock()
	geometryErr := c.validateReconfigurationSlotsLocked(state.freeze, state.terminal)
	c.mu.RUnlock()
	if geometryErr != nil {
		c.releasePipeline()
		return geometryErr
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
	c.mu.RLock()
	unchanged := c.reconfiguration == state
	c.mu.RUnlock()
	if !unchanged {
		return fmt.Errorf("reconfiguration changed while draining")
	}
	prefix, ok := c.PrefixHash(state.terminal - 1)
	if !ok {
		return fmt.Errorf("reconfiguration drain prefix is unavailable")
	}
	value, err := encodeReconfiguration(reconfigurationValue{Terminal: true, Abort: abort, Freeze: state.freeze, TerminalSlot: state.terminal, Target: cloneCluster(state.target), PrefixHash: prefix, OperationDigest: operationDigest})
	if err != nil {
		return err
	}
	decision, err := c.runSlot(ctx, state.terminal, value, false)
	if err != nil {
		return err
	}
	if err := c.acceptDecision(decision); err != nil {
		return err
	}
	_, err = c.completeDecision(ctx, state.terminal, true)
	if err == nil && decision.Proposal.Hash != reconfigurationHash(value) {
		return fmt.Errorf("terminal slot was decided by another value")
	}
	return err
}

// boundHistoricalTerminalLocked finds a retained, certified terminal for an
// exact completed bound operation. It deliberately refuses compacted history:
// without the freeze and preceding abort revision, a retry cannot be
// distinguished from a later or unrelated transition.
func (c *Core) boundHistoricalTerminalLocked(target Cluster, expectedAbortSlot Slot, operationDigest [32]byte, abort bool) (Slot, bool, error) {
	if c.floor != 0 {
		return 0, false, fmt.Errorf("bound reconfiguration retry history is compacted")
	}
	history, err := c.membershipHistoryLocked(0)
	if err != nil {
		return 0, false, fmt.Errorf("bound reconfiguration retry history: %w", err)
	}
	var matches []ConfigTransition
	matches = append(matches, history.Transitions...)
	if history.Abort != nil {
		matches = append(matches, *history.Abort)
	}
	var found *ConfigTransition
	for i := range matches {
		transition := &matches[i]
		freeze, freezeOK, freezeErr := decodeReconfiguration(transition.Freeze.Value)
		terminal, terminalOK, terminalErr := decodeReconfiguration(transition.Terminal.Value)
		if freezeErr != nil || terminalErr != nil || !freezeOK || !terminalOK || freeze.Terminal || !terminal.Terminal {
			continue
		}
		if freeze.OperationDigest != operationDigest || terminal.OperationDigest != operationDigest || !sameCluster(freeze.Target, target) || !sameCluster(terminal.Target, target) || terminal.Abort != abort {
			continue
		}
		if found != nil {
			return 0, false, fmt.Errorf("bound reconfiguration retry is ambiguous")
		}
		found = transition
	}
	if found == nil {
		return 0, false, nil
	}
	freezeSlot := found.Freeze.Slot
	if found.Freeze.Slot == 0 || found.Terminal.Slot <= freezeSlot || !c.durable[freezeSlot] || !c.durable[found.Terminal.Slot] {
		return 0, false, fmt.Errorf("bound reconfiguration terminal is not durably retained")
	}
	var priorAbort Slot
	for slot, value := range c.decided {
		if slot >= freezeSlot {
			continue
		}
		control, ok, decodeErr := decodeReconfiguration(value.Value)
		if decodeErr == nil && ok && control.Terminal && control.Abort && slot > priorAbort {
			priorAbort = slot
		}
	}
	if priorAbort != expectedAbortSlot {
		return 0, false, fmt.Errorf("bound reconfiguration retry abort revision %d does not match expected %d", priorAbort, expectedAbortSlot)
	}
	return found.Terminal.Slot, true, nil
}

func (c *Core) takePipeline(ctx context.Context) error {
	select {
	case c.pipelineExclusive <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
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
			<-c.pipelineExclusive
			return ctx.Err()
		}
	}
	return nil
}
func (c *Core) releasePipeline() {
	for range cap(c.pipeline) {
		<-c.pipeline
	}
	<-c.pipelineExclusive
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

// ValidateReconfigurationTarget checks a proposed next configuration against
// the active cluster and retired identities without changing Core state.
func (c *Core) ValidateReconfigurationTarget(target Cluster) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	current := c.clusterForSlotLocked(c.tip + 1)
	retired := make(map[NodeID]struct{}, len(c.retiredIDs))
	for id := range c.retiredIDs {
		retired[id] = struct{}{}
	}
	return validateReconfigurationTarget(current, target, retired)
}

func (c *Core) validateReconfigurationSlotsLocked(freeze, terminal Slot) error {
	if freeze == 0 || freeze > ^Slot(0)-17 || terminal != freeze+16 || c.isLeaderScheduleSlotLocked(freeze) || c.isLeaderScheduleSlotLocked(terminal) {
		return fmt.Errorf("incompatible reconfiguration schedule boundary: freeze=%d terminal=%d", freeze, terminal)
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
		if err := c.validateReconfigurationSlotsLocked(control.Freeze, control.TerminalSlot); err != nil {
			return err
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
	freezeDecision, freezeOK := c.decided[state.freeze]
	freezeControl, freezeConfigured, freezeErr := decodeReconfiguration(freezeDecision.Value)
	if !freezeOK || freezeErr != nil || !freezeConfigured || freezeControl.Terminal || control.OperationDigest != freezeControl.OperationDigest {
		return fmt.Errorf("reconfiguration terminal operation digest does not match its freeze")
	}
	for _, summary := range decision.Summaries {
		if summary.ReconfigurationID != state.id {
			return fmt.Errorf("terminal quorum does not bind its freeze")
		}
	}
	if control.Abort {
		if !reconfigurationAdds(c.clusterForSlotLocked(decision.Slot), state.target) {
			return fmt.Errorf("only a pending addition may be aborted")
		}
		freeze, ok := c.decided[state.freeze]
		if !ok {
			return fmt.Errorf("abort terminal has no freeze decision")
		}
		c.lastAbort = &ConfigTransition{Freeze: cloneDecidedValue(freeze), Terminal: DecidedValue{Slot: decision.Slot, Hash: decision.Proposal.Hash, Value: append([]byte(nil), decision.Proposal.Value...), Certificate: append([]byte(nil), c.decided[decision.Slot].Certificate...)}}
		c.reconfiguration = nil
		return nil
	}
	old := c.clusterForSlotLocked(decision.Slot)
	for id := range old.MemberSet() {
		if _, kept := state.target.MemberSet()[id]; !kept {
			c.retiredIDs[id] = struct{}{}
		}
	}
	c.configHistory = append(c.configHistory, configEpoch{start: state.terminal + 1, cluster: cloneCluster(state.target)})
	c.lastAbort = nil
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
