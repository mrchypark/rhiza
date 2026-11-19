package quepaxa

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

const (
	// DefaultMaxRetainedDecisionBytes bounds hot in-memory DecidedValue payloads
	// kept by Core when CompactThrough does not run (for example Managed HA).
	DefaultMaxRetainedDecisionBytes int64 = 64 << 20

	// DefaultMaxRetainedDecisionSlots bounds the number of hot decided slots
	// retained in memory per Core when CompactThrough does not run.
	DefaultMaxRetainedDecisionSlots = 4096

	// coldPrefixStride keeps a sparse anchor in Core.prefixes so any evicted
	// PrefixHash can be recomputed from cold hashes with bounded hashing.
	coldPrefixStride        Slot = 256
	coldLeaderScheduleLimit      = 256
)

type coldDecision struct {
	loc  qlog.EntryLocation
	hash ValueHash
}

func decisionPayloadBytes(d DecidedValue) int64 {
	return int64(len(d.Value)) + int64(len(d.Certificate))
}

func (c *Core) putDecidedLocked(slot Slot, d DecidedValue) {
	if old, ok := c.decided[slot]; ok {
		c.hotDecisionBytes -= decisionPayloadBytes(old)
	}
	c.decided[slot] = d
	c.hotDecisionBytes += decisionPayloadBytes(d)
}

func (c *Core) deleteDecidedLocked(slot Slot) {
	if old, ok := c.decided[slot]; ok {
		c.hotDecisionBytes -= decisionPayloadBytes(old)
		delete(c.decided, slot)
	}
}

func (c *Core) recomputeHotDecisionBytesLocked() {
	var total int64
	for _, d := range c.decided {
		total += decisionPayloadBytes(d)
	}
	c.hotDecisionBytes = total
}

func (c *Core) isDurableLocked(slot Slot) bool {
	if slot <= c.floor {
		return false
	}
	if d, exists := c.durable[slot]; exists {
		return d
	}
	return slot <= c.evictedThrough
}

func (c *Core) isLoggedLocked(slot Slot) bool {
	if slot <= c.floor {
		return false
	}
	if l, exists := c.logged[slot]; exists {
		return l
	}
	return slot <= c.evictedThrough
}

func (c *Core) coldEntryLocked(slot Slot) (coldDecision, bool) {
	if slot <= c.floor || slot < c.coldBase || slot > c.evictedThrough {
		return coldDecision{}, false
	}
	idx := int(slot - c.coldBase)
	if idx < 0 || idx >= len(c.cold) {
		return coldDecision{}, false
	}
	entry := c.cold[idx]
	if entry.loc.Length <= 0 {
		return coldDecision{}, false
	}
	return entry, true
}

func (c *Core) isDecidedLocked(slot Slot) bool {
	if _, ok := c.decided[slot]; ok {
		return true
	}
	_, ok := c.coldEntryLocked(slot)
	return ok
}

func (c *Core) decisionHashLocked(slot Slot) (ValueHash, bool) {
	if d, ok := c.decided[slot]; ok {
		return d.Hash, true
	}
	if cold, ok := c.coldEntryLocked(slot); ok {
		return cold.hash, true
	}
	return ValueHash{}, false
}

func (c *Core) readColdDecision(slot Slot, cold coldDecision) (DecidedValue, error) {
	if c.wal == nil {
		return DecidedValue{}, fmt.Errorf("WAL unavailable to read evicted decision at slot %d", slot)
	}
	entry, err := c.wal.ReadEntryAt(cold.loc)
	if err != nil {
		return DecidedValue{}, fmt.Errorf("read cold decision slot %d: %w", slot, err)
	}
	if entry.Type != qlog.EntryDecide || entry.Slot != uint64(slot) || entry.Hash != cold.hash {
		return DecidedValue{}, fmt.Errorf("cold WAL entry identity mismatch at slot %d", slot)
	}
	if !bytes.HasPrefix(entry.Payload, decisionEntryMagic) {
		return DecidedValue{}, fmt.Errorf("cold WAL entry at slot %d is not a decision record", slot)
	}
	value, certificate, err := decodeDecisionRecord(entry.Payload[len(decisionEntryMagic):])
	if err != nil {
		return DecidedValue{}, fmt.Errorf("decode cold decision slot %d: %w", slot, err)
	}
	if sha256.Sum256(value) != cold.hash {
		return DecidedValue{}, fmt.Errorf("cold decision hash mismatch at slot %d", slot)
	}
	return DecidedValue{Slot: slot, Hash: cold.hash, Value: value, Certificate: certificate}, nil
}

func (c *Core) loadDecidedLocked(slot Slot) (DecidedValue, bool, error) {
	if d, ok := c.decided[slot]; ok {
		return d, true, nil
	}
	cold, ok := c.coldEntryLocked(slot)
	if !ok {
		return DecidedValue{}, false, nil
	}
	d, err := c.readColdDecision(slot, cold)
	if err != nil {
		return DecidedValue{}, false, err
	}
	return d, true, nil
}

func (c *Core) prefixHashLocked(slot Slot) ([32]byte, bool) {
	if h, ok := c.prefixes[slot]; ok {
		return h, true
	}
	if slot <= c.floor || slot > c.tip {
		return [32]byte{}, false
	}
	anchor := (slot / coldPrefixStride) * coldPrefixStride
	if anchor < c.floor {
		anchor = c.floor
	}
	var h [32]byte
	found := false
	for a := anchor; ; {
		if a == 0 && c.floor == 0 {
			found = true
			anchor = 0
			break
		}
		if existing, ok := c.prefixes[a]; ok {
			h = existing
			anchor = a
			found = true
			break
		}
		if a <= c.floor {
			break
		}
		a--
	}
	if !found {
		return [32]byte{}, false
	}
	for s := anchor + 1; s <= slot; s++ {
		dh, ok := c.decisionHashLocked(s)
		if !ok {
			return [32]byte{}, false
		}
		h = AdvancePrefixHash(h, s, dh)
	}
	return h, true
}

func (c *Core) advanceDurableThroughLocked() {
	for c.durableThrough < c.tip {
		next := c.durableThrough + 1
		if !c.isDecidedLocked(next) || !c.isLoggedLocked(next) || !c.isDurableLocked(next) {
			break
		}
		c.durableThrough = next
	}
}

func (c *Core) recordAbortTerminalSlotLocked(slot Slot) {
	if !slices.Contains(c.abortTerminalSlots, slot) {
		c.abortTerminalSlots = append(c.abortTerminalSlots, slot)
	}
}

func (c *Core) pinValue(hash ValueHash) {
	if hash == (ValueHash{}) {
		return
	}
	c.mu.Lock()
	if c.pinnedValues == nil {
		c.pinnedValues = make(map[ValueHash]int)
	}
	c.pinnedValues[hash]++
	c.mu.Unlock()
}

func (c *Core) unpinValue(hash ValueHash) {
	if hash == (ValueHash{}) {
		return
	}
	c.mu.Lock()
	if c.pinnedValues[hash] <= 1 {
		delete(c.pinnedValues, hash)
	} else {
		c.pinnedValues[hash]--
	}
	c.mu.Unlock()
}

func (c *Core) rememberRecentStagedLocked(hash ValueHash) {
	if c.recentStaged == nil {
		c.recentStaged = make(map[ValueHash]struct{})
	}
	if _, ok := c.recentStaged[hash]; ok {
		return
	}
	c.recentStaged[hash] = struct{}{}
	c.recentStagedOrder = append(c.recentStagedOrder, hash)
	limit := max(256, c.maxRetainedSlots)
	if len(c.recentStagedOrder) <= limit {
		return
	}
	drop := len(c.recentStagedOrder) - limit
	for _, old := range c.recentStagedOrder[:drop] {
		delete(c.recentStaged, old)
	}
	c.recentStagedOrder = append([]ValueHash(nil), c.recentStagedOrder[drop:]...)
}

func (c *Core) cacheColdLeaderScheduleLocked(slot Slot, value []byte) {
	order, scheduled, err := DecodeLeaderSchedule(value)
	if err != nil || !scheduled {
		return
	}
	if c.coldLeaderSchedules == nil {
		c.coldLeaderSchedules = make(map[Slot][]NodeID)
	}
	if _, exists := c.coldLeaderSchedules[slot]; !exists {
		c.coldLeaderOrder = append(c.coldLeaderOrder, slot)
	}
	c.coldLeaderSchedules[slot] = append([]NodeID(nil), order...)
	if len(c.coldLeaderOrder) <= coldLeaderScheduleLimit {
		return
	}
	drop := len(c.coldLeaderOrder) - coldLeaderScheduleLimit
	for _, old := range c.coldLeaderOrder[:drop] {
		delete(c.coldLeaderSchedules, old)
	}
	c.coldLeaderOrder = append([]Slot(nil), c.coldLeaderOrder[drop:]...)
}

func (c *Core) overRetentionBudgetLocked() bool {
	byteOver := c.maxRetainedBytes > 0 && c.hotDecisionBytes > c.maxRetainedBytes
	slotOver := c.maxRetainedSlots > 0 && len(c.decided) > c.maxRetainedSlots
	return byteOver || slotOver
}

func (c *Core) evictColdDecisionsLocked() {
	if c.localMode || c.wal == nil || c.compactionValues != nil || c.hotDecisionBytes < 0 {
		return
	}
	if c.maxRetainedBytes < 0 && c.maxRetainedSlots < 0 {
		return
	}
	if !c.overRetentionBudgetLocked() {
		return
	}
	minHot := 64
	if c.maxRetainedSlots > 0 && c.maxRetainedSlots/2 < minHot {
		minHot = max(1, c.maxRetainedSlots/2)
	}
	// If byte limit forces eviction while slot budget alone would keep minHot,
	// still retain at least one hot decision (specifically c.tip, since the loop
	// requires s < c.tip).
	if c.maxRetainedBytes > 0 && c.hotDecisionBytes > c.maxRetainedBytes && c.maxRetainedSlots <= 0 {
		minHot = 1
	}

	protectedPrefixSlots := make(map[Slot]struct{}, len(c.preparedCheckpoints)+len(c.sealedRoots))
	for slot := range c.preparedCheckpoints {
		protectedPrefixSlots[slot] = struct{}{}
	}
	var latestSeal SealedCheckpoint
	for _, seal := range c.sealedRoots {
		protectedPrefixSlots[seal.Index] = struct{}{}
		if seal.DecisionSlot > c.tip || !c.isDurableLocked(seal.DecisionSlot) {
			continue
		}
		if seal.Index > latestSeal.Index || (seal.Index == latestSeal.Index && seal.DecisionSlot > latestSeal.DecisionSlot) {
			latestSeal = seal
		}
	}

	var candidateDead map[ValueHash]struct{}
	for c.overRetentionBudgetLocked() {
		byteExceeded := c.maxRetainedBytes > 0 && c.hotDecisionBytes > c.maxRetainedBytes && len(c.decided) > 1
		slotExceeded := c.maxRetainedSlots > 0 && len(c.decided) > c.maxRetainedSlots && len(c.decided) > minHot
		if !byteExceeded && !slotExceeded {
			break
		}
		s := max(c.floor, c.evictedThrough) + 1
		if s > c.durableThrough || s >= c.tip {
			break
		}
		if c.reconfiguration != nil && s >= c.reconfiguration.freeze {
			break
		}
		d, ok := c.decided[s]
		if !ok {
			break
		}
		loc, hasLoc := c.decisionLocs[s]
		if !hasLoc || loc.Length <= 0 {
			break
		}
		if len(c.cold) == 0 {
			c.coldBase = s
		}
		if s != c.coldBase+Slot(len(c.cold)) {
			break
		}
		if c.reconfigEnabled {
			if control, isCtrl, err := decodeReconfiguration(d.Value); err == nil && isCtrl && control.Terminal && control.Abort {
				c.recordAbortTerminalSlotLocked(s)
			}
		}
		c.cold = append(c.cold, coldDecision{loc: loc, hash: d.Hash})
		c.cacheColdLeaderScheduleLocked(s, d.Value)
		c.deleteDecidedLocked(s)
		delete(c.decisionLocs, s)
		if candidateDead == nil {
			candidateDead = make(map[ValueHash]struct{})
		}
		candidateDead[d.Hash] = struct{}{}
		if state, hasRec := c.recorders[s]; hasRec {
			for _, p := range []*Proposal{state.FirstCurrent, state.AggregateCurrent, state.AggregatePrior} {
				if p != nil {
					candidateDead[p.Hash] = struct{}{}
				}
			}
			delete(c.recorders, s)
		}
		delete(c.durable, s)
		delete(c.logged, s)
		if indexed, exists := c.byHash[d.Hash]; exists && indexed == s {
			delete(c.byHash, d.Hash)
		}
		if _, keep := protectedPrefixSlots[s]; !keep && s != c.floor && s%coldPrefixStride != 0 {
			delete(c.prefixes, s)
		}
		c.evictedThrough = s
	}

	if len(candidateDead) > 0 {
		c.pruneDeadValuesLocked(candidateDead)
	}
	if latestSeal.Index != 0 {
		for root, seal := range c.sealedRoots {
			if seal.Index < latestSeal.Index && seal.DecisionSlot <= c.evictedThrough && root != c.floorRoot && c.preparedCheckpoints[seal.Index] != root {
				delete(c.sealedRoots, root)
				if seal.Index != c.floor && seal.Index%coldPrefixStride != 0 && c.preparedCheckpoints[seal.Index] == ([32]byte{}) {
					protected := false
					for _, remaining := range c.sealedRoots {
						if remaining.Index == seal.Index {
							protected = true
							break
						}
					}
					if !protected {
						delete(c.prefixes, seal.Index)
					}
				}
			}
		}
	}
}

func (c *Core) pruneDeadValuesLocked(candidateDead map[ValueHash]struct{}) {
	live := make(map[ValueHash]struct{}, len(c.decided)+len(c.recorders))
	for _, d := range c.decided {
		live[d.Hash] = struct{}{}
	}
	for _, state := range c.recorders {
		for _, p := range []*Proposal{state.FirstCurrent, state.AggregateCurrent, state.AggregatePrior} {
			if p != nil {
				live[p.Hash] = struct{}{}
			}
		}
	}
	for hash := range c.pinnedValues {
		live[hash] = struct{}{}
	}
	for hash := range c.recentStaged {
		live[hash] = struct{}{}
	}
	for hash := range candidateDead {
		if _, ok := live[hash]; !ok {
			delete(c.values, hash)
			delete(c.valueDurable, hash)
		}
	}
	sweepThreshold := max(256, 2*max(c.maxRetainedSlots, 64))
	if !c.recovering && len(c.values) > sweepThreshold {
		for hash := range c.values {
			if _, ok := live[hash]; !ok {
				delete(c.values, hash)
				delete(c.valueDurable, hash)
			}
		}
	}
}
