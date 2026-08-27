package quepaxa

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// RestoreCheckpointBase installs an object-store recovery floor only after
// verifying both the checkpoint bytes and the consensus certificate that
// sealed that exact root.
func (c *Core) RestoreCheckpointBase(ctx context.Context, seal CheckpointSeal, certified DecidedValue) error {
	if c.Tip() >= seal.Index || seal.ConfigID != c.config.ConfigID || seal.Index == 0 || seal.RootHash == ([32]byte{}) || seal.PrefixHash == ([32]byte{}) || !c.validateCheckpointLeaderOrders(seal.Index, seal.NextLeaderOrder, seal.FollowingLeaderOrder) {
		return fmt.Errorf("invalid checkpoint recovery base")
	}
	value, checkpoint, err := DecodeCheckpointSeal(certified.Value)
	if err != nil || !checkpoint || value.ConfigID != seal.ConfigID || value.Index != seal.Index || value.RootHash != seal.RootHash || value.StateHash != seal.StateHash || value.PrefixHash != seal.PrefixHash || !slices.Equal(value.NextLeaderOrder, seal.NextLeaderOrder) || !slices.Equal(value.FollowingLeaderOrder, seal.FollowingLeaderOrder) {
		return fmt.Errorf("checkpoint recovery decision does not match its seal")
	}
	decision, err := c.certifiedDecision(certified)
	if err != nil {
		return err
	}
	validator := c.checkpointValidator
	if validator == nil {
		return fmt.Errorf("checkpoint validation is unavailable")
	}
	if err := validator(ctx, seal); err != nil {
		return err
	}
	base := consensusBase{ConfigID: seal.ConfigID, ClosedThrough: seal.Index, PrefixHash: seal.PrefixHash, RecoveryRoot: seal.RootHash, LeaderEpoch: leaderEpoch(seal.Index + 1), NextLeaderOrder: append([]NodeID(nil), seal.NextLeaderOrder...), FollowingLeaderOrder: append([]NodeID(nil), seal.FollowingLeaderOrder...)}
	c.mu.Lock()
	c.installBaseLocked(base)
	c.mu.Unlock()
	if err := c.validateDecisionForRecovery(decision, true); err != nil {
		return fmt.Errorf("validate checkpoint recovery decision: %w", err)
	}
	payload, err := json.Marshal(base)
	if err != nil {
		return err
	}
	if err := c.wal.Compact(qlog.Entry{Slot: uint64(seal.Index), Hash: seal.RootHash, Type: qlog.EntryCheckpoint, Payload: payload}, nil); err != nil {
		return err
	}
	c.mu.Lock()
	c.preparedCheckpoints[seal.Index] = seal.RootHash
	c.mu.Unlock()
	return nil
}

type consensusBase struct {
	ConfigID             uint     `json:"config_id"`
	ClosedThrough        Slot     `json:"closed_through"`
	PrefixHash           [32]byte `json:"prefix_hash"`
	RecoveryRoot         [32]byte `json:"recovery_root"`
	LeaderEpoch          uint64   `json:"leader_epoch"`
	NextLeaderOrder      []NodeID `json:"next_leader_order"`
	FollowingLeaderOrder []NodeID `json:"following_leader_order,omitempty"`
}

// CompactThrough installs a certified local recovery floor. Callers must have
// independently verified and quorum-sealed recoveryRoot before invoking it.
func (c *Core) CompactThrough(through Slot, recoveryRoot [32]byte) error {
	if through == 0 || recoveryRoot == ([32]byte{}) {
		return fmt.Errorf("invalid consensus compaction floor")
	}
	for range cap(c.pipeline) {
		c.pipeline <- struct{}{}
	}
	defer func() {
		for range cap(c.pipeline) {
			<-c.pipeline
		}
	}()
	c.slotMu.Lock()
	defer c.slotMu.Unlock()
	for i := range c.recordLocks {
		c.recordLocks[i].Lock()
	}
	defer func() {
		for i := len(c.recordLocks) - 1; i >= 0; i-- {
			c.recordLocks[i].Unlock()
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()
	if through <= c.floor || through > c.tip {
		return fmt.Errorf("compaction floor %d is outside retained range (%d,%d]", through, c.floor, c.tip)
	}
	seal, sealed := c.sealedRoots[recoveryRoot]
	if preparedRoot, prepared := c.preparedCheckpoints[through]; !sealed || seal.Index != through || !prepared || preparedRoot != recoveryRoot {
		return fmt.Errorf("recovery root is not locally verified and quorum sealed through %d", through)
	}
	prefix, ok := c.prefixes[through]
	if !ok {
		return fmt.Errorf("prefix hash at slot %d is unavailable", through)
	}
	order, following, err := c.checkpointLeaderOrdersLocked(through)
	if err != nil {
		return err
	}
	base := consensusBase{
		ConfigID: c.config.ConfigID, ClosedThrough: through,
		PrefixHash: prefix, RecoveryRoot: recoveryRoot, LeaderEpoch: leaderEpoch(through + 1), NextLeaderOrder: order, FollowingLeaderOrder: following,
	}
	payload, err := json.Marshal(base)
	if err != nil {
		return err
	}
	keep := make(map[[32]byte]struct{})
	for slot, decision := range c.decided {
		if slot > through {
			keep[decision.Hash] = struct{}{}
		}
	}
	for slot, state := range c.recorders {
		if slot <= through {
			continue
		}
		for _, proposal := range []*Proposal{state.FirstCurrent, state.AggregateCurrent, state.AggregatePrior} {
			if proposal != nil {
				keep[proposal.Hash] = struct{}{}
			}
		}
	}
	if err := c.wal.Compact(qlog.Entry{Slot: uint64(through), Hash: recoveryRoot, Type: qlog.EntryCheckpoint, Payload: payload}, keep); err != nil {
		return err
	}
	c.installBaseLocked(base)
	c.advanceTipLocked()
	c.pruneSlotAllocatorLocked()
	return nil
}

func (c *Core) LatestCheckpointSeal() (SealedCheckpoint, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var latest SealedCheckpoint
	for root, seal := range c.sealedRoots {
		if seal.Index > latest.Index {
			latest = seal
		} else if seal.Index != 0 && seal.Index == latest.Index && root != latest.RootHash {
			return SealedCheckpoint{}, false, fmt.Errorf("conflicting checkpoint seals at index %d", seal.Index)
		}
	}
	return latest, latest.Index != 0, nil
}

// LatestPreparedCheckpoint returns the newest locally verified immutable root.
func (c *Core) LatestPreparedCheckpoint() (Slot, [32]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latestPreparedCheckpointLocked()
}

func (c *Core) latestPreparedCheckpointLocked() (Slot, [32]byte, bool) {
	var latest Slot
	var root [32]byte
	for slot, candidate := range c.preparedCheckpoints {
		if slot > latest {
			latest, root = slot, candidate
		}
	}
	return latest, root, latest != 0
}

func (c *Core) CompactionFloor() Slot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.floor
}

func (c *Core) RecoveryRoot() (Slot, [32]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.floor, c.floorRoot, c.floor != 0 && c.floorRoot != ([32]byte{})
}

func (c *Core) installBaseLocked(base consensusBase) {
	c.floor = base.ClosedThrough
	c.floorRoot = base.RecoveryRoot
	c.baseLeaderEpoch = base.LeaderEpoch
	c.baseLeaderOrder = append([]NodeID(nil), base.NextLeaderOrder...)
	c.baseFollowingEpoch = 0
	c.baseFollowingOrder = nil
	if len(base.FollowingLeaderOrder) != 0 {
		c.baseFollowingEpoch = base.LeaderEpoch + 1
		c.baseFollowingOrder = append([]NodeID(nil), base.FollowingLeaderOrder...)
	}
	c.tip = base.ClosedThrough
	clear(c.prefixes)
	c.prefixes[base.ClosedThrough] = base.PrefixHash
	for slot := range c.decided {
		if slot <= base.ClosedThrough {
			delete(c.decided, slot)
			delete(c.durable, slot)
			delete(c.logged, slot)
		}
	}
	clear(c.byHash)
	for slot, decision := range c.decided {
		c.updateHashIndexLocked(decision.Hash, slot)
	}
	for slot := range c.recorders {
		if slot <= base.ClosedThrough {
			delete(c.recorders, slot)
		}
	}
	for slot := range c.preparedCheckpoints {
		if slot < base.ClosedThrough {
			delete(c.preparedCheckpoints, slot)
		}
	}
	for root, seal := range c.sealedRoots {
		if seal.Index < base.ClosedThrough {
			delete(c.sealedRoots, root)
		}
	}
	used := make(map[ValueHash]struct{})
	for _, decision := range c.decided {
		used[decision.Hash] = struct{}{}
	}
	for _, state := range c.recorders {
		for _, proposal := range []*Proposal{state.FirstCurrent, state.AggregateCurrent, state.AggregatePrior} {
			if proposal != nil {
				used[proposal.Hash] = struct{}{}
			}
		}
	}
	for hash := range c.values {
		if _, ok := used[hash]; !ok {
			delete(c.values, hash)
			delete(c.valueDurable, hash)
		}
	}
}

// pruneSlotAllocatorLocked requires slotMu and mu.
func (c *Core) pruneSlotAllocatorLocked() {
	kept := c.vacant[:0]
	for _, slot := range c.vacant {
		if slot > c.floor {
			kept = append(kept, slot)
		}
	}
	c.vacant = kept
	next := c.tip + 1
	if floorNext := c.floor + 1; next < floorNext {
		next = floorNext
	}
	if c.nextSlot < next {
		c.nextSlot = next
	}
}

func decodeConsensusBase(data []byte) (consensusBase, error) {
	var base consensusBase
	if err := decodeStrictJSON(data, &base); err != nil {
		return consensusBase{}, err
	}
	if base.ClosedThrough == 0 || base.PrefixHash == ([32]byte{}) || base.RecoveryRoot == ([32]byte{}) || len(base.NextLeaderOrder) == 0 {
		return consensusBase{}, fmt.Errorf("invalid consensus base")
	}
	return base, nil
}
