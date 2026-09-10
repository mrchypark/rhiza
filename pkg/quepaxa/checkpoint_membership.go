package quepaxa

import (
	"fmt"
	"slices"
)

func cloneMembershipRecord(record MembershipRecord) MembershipRecord {
	copy := MembershipRecord{Genesis: cloneCluster(record.Genesis), Transitions: make([]ConfigTransition, len(record.Transitions))}
	for i, transition := range record.Transitions {
		copy.Transitions[i] = ConfigTransition{Freeze: cloneDecidedValue(transition.Freeze), Terminal: cloneDecidedValue(transition.Terminal)}
	}
	if record.Abort != nil {
		abort := ConfigTransition{Freeze: cloneDecidedValue(record.Abort.Freeze), Terminal: cloneDecidedValue(record.Abort.Terminal)}
		copy.Abort = &abort
	}
	return copy
}

func cloneDecidedValue(value DecidedValue) DecidedValue {
	value.Value = append([]byte(nil), value.Value...)
	value.Certificate = append([]byte(nil), value.Certificate...)
	return value
}

func sameMembershipRecord(left, right MembershipRecord) bool {
	if !sameCluster(left.Genesis, right.Genesis) || len(left.Transitions) != len(right.Transitions) || (left.Abort == nil) != (right.Abort == nil) {
		return false
	}
	for i := range left.Transitions {
		for _, pair := range [][2]DecidedValue{{left.Transitions[i].Freeze, right.Transitions[i].Freeze}, {left.Transitions[i].Terminal, right.Transitions[i].Terminal}} {
			if pair[0].Slot != pair[1].Slot || pair[0].Hash != pair[1].Hash || !slices.Equal(pair[0].Value, pair[1].Value) || !slices.Equal(pair[0].Certificate, pair[1].Certificate) {
				return false
			}
		}
	}
	if left.Abort != nil && !sameMembershipTransition(*left.Abort, *right.Abort) {
		return false
	}
	return true
}

func sameMembershipTransition(left, right ConfigTransition) bool {
	for _, pair := range [][2]DecidedValue{{left.Freeze, right.Freeze}, {left.Terminal, right.Terminal}} {
		if pair[0].Slot != pair[1].Slot || pair[0].Hash != pair[1].Hash || !slices.Equal(pair[0].Value, pair[1].Value) || !slices.Equal(pair[0].Certificate, pair[1].Certificate) {
			return false
		}
	}
	return true
}

// checkpointMembershipForSeal verifies supplied history against immutable
// bootstrap membership before it can select a certificate epoch.
func (c *Core) checkpointMembershipForSeal(seal CheckpointSeal) (*Core, error) {
	if !c.reconfigEnabled {
		if seal.Membership != nil {
			return nil, fmt.Errorf("fixed-membership checkpoint includes membership history")
		}
		return nil, nil
	}
	if seal.Membership == nil {
		return nil, fmt.Errorf("reconfiguration checkpoint lacks membership history")
	}
	verifier, err := validateMembershipHistory(*c.config, cloneMembershipRecord(*seal.Membership))
	if err != nil {
		return nil, fmt.Errorf("checkpoint membership: %w", err)
	}
	if verifier.reconfiguration != nil || verifier.clusterForSlot(seal.Index+1).ConfigID != seal.ConfigID || !verifier.validateCheckpointLeaderOrders(seal.Index, seal.NextLeaderOrder, seal.FollowingLeaderOrder) {
		return nil, fmt.Errorf("checkpoint membership does not certify checkpoint epoch")
	}
	for _, transition := range seal.Membership.Transitions {
		if transition.Terminal.Slot >= seal.Index {
			return nil, fmt.Errorf("checkpoint membership crosses configuration boundary")
		}
	}
	return verifier, nil
}
