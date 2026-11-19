package quepaxa

import (
	"context"
	"fmt"
	"slices"
)

func cloneMembershipRecord(record MembershipRecord) MembershipRecord {
	copy := MembershipRecord{Genesis: cloneCluster(record.Genesis), Transitions: make([]ConfigTransition, len(record.Transitions))}
	if record.Anchor != nil {
		anchor := *record.Anchor
		anchor.Cluster = cloneCluster(record.Anchor.Cluster)
		anchor.RetiredIDs = append([]NodeID(nil), record.Anchor.RetiredIDs...)
		copy.Anchor = &anchor
	}
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
	if !sameCluster(left.Genesis, right.Genesis) || len(left.Transitions) != len(right.Transitions) || (left.Abort == nil) != (right.Abort == nil) || (left.Anchor == nil) != (right.Anchor == nil) {
		return false
	}
	if left.Anchor != nil && (left.Anchor.Through != right.Anchor.Through || left.Anchor.Digest != right.Anchor.Digest || !sameCluster(left.Anchor.Cluster, right.Anchor.Cluster) || !slices.Equal(left.Anchor.RetiredIDs, right.Anchor.RetiredIDs)) {
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

func (c *Core) verifyMembershipAnchorContext(ctx context.Context, seal CheckpointSeal) error {
	if !c.reconfigEnabled || seal.Membership == nil || seal.Membership.Anchor == nil {
		return nil
	}
	c.mu.RLock()
	local, err := c.membershipHistoryLocked(seal.Index)
	c.mu.RUnlock()
	if err == nil && membershipRecordMatches(local, *seal.Membership) {
		if c.storeMembershipAnchor != nil {
			return c.storeMembershipAnchor(ctx, seal.Membership.Anchor.Digest, local)
		}
		return nil
	}
	if c.verifyMembershipAnchor == nil {
		return fmt.Errorf("checkpoint membership anchor verifier is unavailable")
	}
	return c.verifyMembershipAnchor(ctx, cloneCluster(*c.config), *seal.Membership.Anchor)
}
