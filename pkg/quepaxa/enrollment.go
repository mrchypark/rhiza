package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

type EnrollmentReadRound struct {
	Source      NodeID
	Index       Slot
	Cluster     Cluster
	AbortSlot   Slot
	History     MembershipRecord
	HistoryHash [32]byte
}

type EnrollmentCatchUp func(context.Context, NodeID, Slot) error

// EnrollmentReadRound captures a stable membership view after a quorum read.
func (c *Core) EnrollmentReadRound(ctx context.Context, catchUp EnrollmentCatchUp) (EnrollmentReadRound, error) {
	c.mu.RLock()
	cluster := cloneCluster(c.clusterForSlotLocked(c.tip + 1))
	abort := Slot(0)
	if c.lastAbort != nil {
		abort = c.lastAbort.Terminal.Slot
	}
	eligible := c.reconfiguration == nil && c.voterForClusterLocked(cluster)
	c.mu.RUnlock()
	if !eligible {
		return EnrollmentReadRound{}, fmt.Errorf("enrollment source is not an eligible unfrozen voter")
	}

	index, source, err := c.ReadIndex(ctx)
	if err != nil {
		return EnrollmentReadRound{}, err
	}
	c.mu.RLock()
	behind := c.tip < index
	c.mu.RUnlock()
	if behind {
		if catchUp == nil {
			return EnrollmentReadRound{}, fmt.Errorf("enrollment source is behind read index %d", index)
		}
		if err := catchUp(ctx, source, index); err != nil {
			return EnrollmentReadRound{}, fmt.Errorf("enrollment catch-up: %w", err)
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	current := c.clusterForSlotLocked(c.tip + 1)
	currentAbort := Slot(0)
	if c.lastAbort != nil {
		currentAbort = c.lastAbort.Terminal.Slot
	}
	if !sameCluster(cluster, current) || abort != currentAbort || c.reconfiguration != nil || !c.voterForClusterLocked(current) || c.tip < index {
		return EnrollmentReadRound{}, fmt.Errorf("enrollment membership changed or source remains behind")
	}
	history, err := c.membershipHistoryLocked(0)
	if err != nil {
		return EnrollmentReadRound{}, err
	}
	encoded, err := json.Marshal(history)
	if err != nil {
		return EnrollmentReadRound{}, fmt.Errorf("encode enrollment history: %w", err)
	}
	return EnrollmentReadRound{Source: c.nodeID, Index: index, Cluster: cluster, AbortSlot: abort, History: history, HistoryHash: sha256.Sum256(encoded)}, nil
}

// ValidateEnrollmentHistory validates the transcript, not its origin: the
// transcript is unsigned and requires independently authenticated transport.
func ValidateEnrollmentHistory(initial Cluster, history MembershipRecord) (Cluster, error) {
	verifier, err := validateMembershipHistory(initial, history)
	if err != nil {
		return Cluster{}, err
	}
	return cloneCluster(verifier.configHistory[len(verifier.configHistory)-1].cluster), nil
}
