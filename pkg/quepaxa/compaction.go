package quepaxa

import (
	"encoding/json"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

const consensusBaseVersion = 1

type consensusBase struct {
	Version         int      `json:"version"`
	ConfigID        uint     `json:"config_id"`
	ClosedThrough   Slot     `json:"closed_through"`
	PrefixHash      [32]byte `json:"prefix_hash"`
	RecoveryRoot    [32]byte `json:"recovery_root"`
	LeaderEpoch     uint64   `json:"leader_epoch"`
	NextLeaderOrder []NodeID `json:"next_leader_order"`
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
	order, err := c.leaderOrderLocked(through + 1)
	if err != nil {
		return err
	}
	base := consensusBase{
		Version: consensusBaseVersion, ConfigID: c.config.ConfigID, ClosedThrough: through,
		PrefixHash: prefix, RecoveryRoot: recoveryRoot, LeaderEpoch: leaderEpoch(through + 1), NextLeaderOrder: order,
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

func (c *Core) LatestCheckpointSeal() (SealedCheckpoint, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var latest SealedCheckpoint
	for _, seal := range c.sealedRoots {
		if seal.Index > latest.Index {
			latest = seal
		}
	}
	return latest, latest.Index != 0
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
	if err := json.Unmarshal(data, &base); err != nil {
		return consensusBase{}, err
	}
	if base.Version != consensusBaseVersion || base.ClosedThrough == 0 || base.PrefixHash == ([32]byte{}) || base.RecoveryRoot == ([32]byte{}) || len(base.NextLeaderOrder) == 0 {
		return consensusBase{}, fmt.Errorf("invalid consensus base")
	}
	return base, nil
}
