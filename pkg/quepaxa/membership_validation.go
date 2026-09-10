package quepaxa

import (
	"crypto/sha256"
	"fmt"
)

// validateMembershipHistory reconstructs configuration authority from an
// explicitly trusted bootstrap configuration. It does not install a log floor
// or grant learner admission; checkpoint validation must bind the result to its
// certified seal before either action.
func validateMembershipHistory(initial Cluster, history MembershipRecord) (*Core, error) {
	if !sameCluster(initial, history.Genesis) || len(initial.Members) == 0 {
		return nil, fmt.Errorf("membership history does not match bootstrap configuration")
	}
	members := initial.MemberSet()
	if len(members) != len(initial.Members) {
		return nil, fmt.Errorf("duplicate bootstrap member")
	}
	for id := range members {
		if id == "" {
			return nil, fmt.Errorf("empty bootstrap member")
		}
	}
	initial = cloneCluster(initial)
	verifier := &Core{config: &initial, reconfigEnabled: true, configHistory: []configEpoch{{start: 1, cluster: initial}}, decided: make(map[Slot]DecidedValue), retiredIDs: make(map[NodeID]struct{})}
	current := initial
	var previousTerminal Slot
	for i, transition := range history.Transitions {
		freeze, err := historyDecision(current.ConfigID, transition.Freeze)
		if err != nil {
			return nil, fmt.Errorf("transition %d freeze: %w", i, err)
		}
		f, ok, err := decodeReconfiguration(freeze.Proposal.Value)
		if err != nil || !ok || f.Terminal || freeze.Slot != f.Freeze || f.Freeze <= previousTerminal || f.TerminalSlot <= f.Freeze {
			return nil, fmt.Errorf("transition %d has invalid freeze boundary", i)
		}
		if err := validateReconfigurationTarget(current, f.Target, verifier.retiredIDs); err != nil {
			return nil, err
		}
		if err := verifier.validateDecisionForRecovery(freeze, true); err != nil {
			return nil, fmt.Errorf("transition %d freeze quorum: %w", i, err)
		}
		verifier.decided[freeze.Slot] = transition.Freeze
		terminal, err := historyDecision(current.ConfigID, transition.Terminal)
		if err != nil {
			return nil, fmt.Errorf("transition %d terminal: %w", i, err)
		}
		term, ok, err := decodeReconfiguration(terminal.Proposal.Value)
		if err != nil || !ok || !term.Terminal || term.Abort || terminal.Slot != f.TerminalSlot || term.Freeze != f.Freeze || term.TerminalSlot != f.TerminalSlot || !sameCluster(term.Target, f.Target) {
			return nil, fmt.Errorf("transition %d has invalid terminal binding", i)
		}
		if err := verifier.validateDecisionForRecovery(terminal, true); err != nil {
			return nil, fmt.Errorf("transition %d terminal quorum: %w", i, err)
		}
		safe := make(map[NodeID]bool, len(freeze.Summaries))
		for _, summary := range freeze.Summaries {
			safe[summary.RecorderID] = true
		}
		for _, summary := range terminal.Summaries {
			if !safe[summary.RecorderID] || summary.ReconfigurationID != freeze.Proposal.Hash {
				return nil, fmt.Errorf("terminal recorder is outside its freeze quorum")
			}
		}
		nextMembers := f.Target.MemberSet()
		for id := range current.MemberSet() {
			if _, retained := nextMembers[id]; !retained {
				verifier.retiredIDs[id] = struct{}{}
			}
		}
		current = cloneCluster(f.Target)
		verifier.configHistory = append(verifier.configHistory, configEpoch{start: terminal.Slot + 1, cluster: current})
		verifier.decided[terminal.Slot] = transition.Terminal
		previousTerminal = terminal.Slot
	}
	if history.Abort != nil {
		abort := *history.Abort
		freeze, err := historyDecision(current.ConfigID, abort.Freeze)
		if err != nil {
			return nil, fmt.Errorf("abort freeze: %w", err)
		}
		f, ok, err := decodeReconfiguration(freeze.Proposal.Value)
		if err != nil || !ok || f.Terminal || freeze.Slot != f.Freeze || f.Freeze <= previousTerminal || f.TerminalSlot <= f.Freeze {
			return nil, fmt.Errorf("abort has invalid freeze boundary")
		}
		if err := validateReconfigurationTarget(current, f.Target, verifier.retiredIDs); err != nil || !reconfigurationAdds(current, f.Target) {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("abort target is not an addition")
		}
		if err := verifier.validateDecisionForRecovery(freeze, true); err != nil {
			return nil, fmt.Errorf("abort freeze quorum: %w", err)
		}
		verifier.decided[freeze.Slot] = abort.Freeze
		terminal, err := historyDecision(current.ConfigID, abort.Terminal)
		if err != nil {
			return nil, fmt.Errorf("abort terminal: %w", err)
		}
		term, ok, err := decodeReconfiguration(terminal.Proposal.Value)
		if err != nil || !ok || !term.Terminal || !term.Abort || terminal.Slot != f.TerminalSlot || term.Freeze != f.Freeze || term.TerminalSlot != f.TerminalSlot || !sameCluster(term.Target, f.Target) {
			return nil, fmt.Errorf("abort has invalid terminal binding")
		}
		if err := verifier.validateDecisionForRecovery(terminal, true); err != nil {
			return nil, fmt.Errorf("abort terminal quorum: %w", err)
		}
		safe := make(map[NodeID]bool, len(freeze.Summaries))
		for _, summary := range freeze.Summaries {
			safe[summary.RecorderID] = true
		}
		for _, summary := range terminal.Summaries {
			if !safe[summary.RecorderID] || summary.ReconfigurationID != freeze.Proposal.Hash {
				return nil, fmt.Errorf("abort terminal recorder is outside its freeze quorum")
			}
		}
		verifier.decided[terminal.Slot] = abort.Terminal
		verifier.lastAbort = &ConfigTransition{Freeze: cloneDecidedValue(abort.Freeze), Terminal: cloneDecidedValue(abort.Terminal)}
	}
	return verifier, nil
}

func historyDecision(configID uint, value DecidedValue) (Decision, error) {
	if len(value.Value) == 0 || len(value.Value) > MaxReplicatedValueBytes {
		return Decision{}, fmt.Errorf("membership decision value length is invalid")
	}
	id, decision, err := decodeCertificate(value.Certificate)
	if err != nil {
		return Decision{}, err
	}
	if id != configID || decision.Slot != value.Slot || decision.Proposal.Hash != value.Hash || sha256.Sum256(value.Value) != value.Hash {
		return Decision{}, fmt.Errorf("membership decision identity mismatch")
	}
	decision.ConfigID = id
	decision.Proposal.Value = value.Value
	return decision, nil
}
