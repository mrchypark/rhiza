package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// LocalCheckpointRoot is the complete authority tuple that a local snapshot
// descriptor must match. StateHash is absent for a WAL base record; RootHash
// still commits the descriptor's state hash.
type LocalCheckpointRoot struct {
	RootHash   [32]byte
	Index      Slot
	PrefixHash [32]byte
	StateHash  [32]byte
	ConfigID   uint
}

// LocalCheckpointRoots returns every checkpoint root referenced by live
// protocol state or the selected WAL, retaining the authority tuple rather
// than trusting slot/prefix values read back from the descriptor. The WAL
// scan runs without c.mu held; decode or scan failures return no partial set.
func (c *Core) LocalCheckpointRoots(ctx context.Context) ([]LocalCheckpointRoot, error) {
	if !c.localMode {
		return nil, errLocalCostsUnavailable
	}
	if err := c.acquireLocalExecution(ctx); err != nil {
		return nil, err
	}
	defer c.releaseLocalExecution()
	roots := make(map[[32]byte]LocalCheckpointRoot)
	values := make([][]byte, 0)
	c.mu.RLock()
	if c.floor != 0 {
		prefix := c.prefixes[c.floor]
		roots[c.floorRoot] = LocalCheckpointRoot{RootHash: c.floorRoot, Index: c.floor, PrefixHash: prefix, ConfigID: c.clusterForSlotLocked(c.floor).ConfigID}
	}
	for root, sealed := range c.sealedRoots {
		seal := sealed.CheckpointSeal
		roots[root] = LocalCheckpointRoot{RootHash: root, Index: seal.Index, PrefixHash: seal.PrefixHash, StateHash: seal.StateHash, ConfigID: seal.ConfigID}
	}
	for _, decided := range c.decided {
		values = append(values, bytes.Clone(decided.Value))
	}
	for _, state := range c.recorders {
		for _, proposal := range []*Proposal{state.FirstCurrent, state.AggregateCurrent, state.AggregatePrior} {
			if proposal == nil {
				continue
			}
			if proposal.Value != nil {
				if sha256.Sum256(proposal.Value) != [32]byte(proposal.Hash) {
					c.mu.RUnlock()
					return nil, fmt.Errorf("Local checkpoint root proposal has invalid value hash")
				}
				values = append(values, bytes.Clone(proposal.Value))
			} else if value, ok := c.values[proposal.Hash]; ok {
				values = append(values, bytes.Clone(value))
			}
		}
	}
	c.mu.RUnlock()
	addSeal := func(value []byte) error {
		seal, ok, err := DecodeCheckpointSeal(value)
		if err != nil {
			return err
		}
		if ok {
			root := LocalCheckpointRoot{RootHash: seal.RootHash, Index: seal.Index, PrefixHash: seal.PrefixHash, StateHash: seal.StateHash, ConfigID: seal.ConfigID}
			if old, exists := roots[seal.RootHash]; exists && (old.Index != root.Index || old.PrefixHash != root.PrefixHash || old.ConfigID != root.ConfigID || old.StateHash != ([32]byte{}) && old.StateHash != root.StateHash) {
				return fmt.Errorf("conflicting Local checkpoint authority tuple")
			}
			roots[seal.RootHash] = root
		}
		return nil
	}
	for _, value := range values {
		if err := addSeal(value); err != nil {
			return nil, fmt.Errorf("decode in-memory Local checkpoint root: %w", err)
		}
	}
	if err := c.wal.Scan(func(entry qlog.Entry) error {
		switch entry.Type {
		case qlog.EntryCheckpoint:
			base, err := decodeConsensusBase(entry.Payload)
			if err != nil {
				return fmt.Errorf("decode Local WAL checkpoint base: %w", err)
			}
			if base.RecoveryRoot != entry.Hash || uint64(base.ClosedThrough) != entry.Slot {
				return fmt.Errorf("Local WAL checkpoint base identity mismatch")
			}
			root := LocalCheckpointRoot{RootHash: base.RecoveryRoot, Index: base.ClosedThrough, PrefixHash: base.PrefixHash, ConfigID: base.ConfigID}
			if old, exists := roots[base.RecoveryRoot]; exists && (old.Index != root.Index || old.PrefixHash != root.PrefixHash || old.ConfigID != root.ConfigID) {
				return fmt.Errorf("conflicting Local checkpoint base authority tuple")
			}
			roots[base.RecoveryRoot] = root
		case qlog.EntryCheckpointVerified:
			seal, ok, err := DecodeCheckpointSeal(entry.Payload)
			if err != nil || !ok || uint64(seal.Index) != entry.Slot || seal.RootHash != entry.Hash {
				if err == nil {
					err = fmt.Errorf("Local WAL prepared checkpoint identity mismatch")
				}
				return err
			}
			root := LocalCheckpointRoot{RootHash: seal.RootHash, Index: seal.Index, PrefixHash: seal.PrefixHash, StateHash: seal.StateHash, ConfigID: seal.ConfigID}
			if old, exists := roots[seal.RootHash]; exists && (old.Index != root.Index || old.PrefixHash != root.PrefixHash || old.ConfigID != root.ConfigID || old.StateHash != ([32]byte{}) && old.StateHash != root.StateHash) {
				return fmt.Errorf("conflicting Local checkpoint seal authority tuple")
			}
			roots[seal.RootHash] = root
		case qlog.EntryDecide:
			if !bytes.HasPrefix(entry.Payload, decisionEntryMagic) {
				return fmt.Errorf("unknown Local WAL decision record format")
			}
			value, certificate, err := decodeDecisionRecord(entry.Payload[len(decisionEntryMagic):])
			if err != nil {
				return fmt.Errorf("decode Local WAL decision: %w", err)
			}
			if err := addSeal(value); err != nil {
				return fmt.Errorf("decode Local WAL decision checkpoint: %w", err)
			}
			if _, ok, err := DecodeCheckpointSeal(value); err != nil {
				return err
			} else if ok {
				_, cert, err := decodeCertificate(certificate)
				if err != nil || cert.Slot != Slot(entry.Slot) {
					return fmt.Errorf("Local checkpoint decision certificate slot mismatch")
				}
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan Local checkpoint roots: %w", err)
	}
	result := make([]LocalCheckpointRoot, 0, len(roots))
	for _, root := range roots {
		if root.RootHash == ([32]byte{}) || root.Index == 0 || root.PrefixHash == ([32]byte{}) || root.ConfigID == 0 {
			return nil, fmt.Errorf("empty Local checkpoint root in live state")
		}
		result = append(result, root)
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].RootHash[:], result[j].RootHash[:]) < 0 })
	return result, nil
}

// ReclaimLocalCheckpoint installs an exact Local checkpoint root, prepares and
// certifies its seal, invokes apply while the Local Core owner is held, then
// compacts through the sealed state index. The callback must apply the
// resulting retained decisions directly; it must not call back into Core.
// This operation is available only for explicit Local mode.
func (c *Core) ReclaimLocalCheckpoint(ctx context.Context, seal CheckpointSeal, apply func(context.Context, Slot) error) (Slot, error) {
	return c.ReclaimLocalCheckpointWithSpacePreflight(ctx, seal, nil, apply)
}

// ReclaimLocalCheckpointWithSpacePreflight is the Node-only extension point
// for checking physical peak space while the same Core owner protects the
// exact WAL rewrite estimate and its pending-write allowance.
func (c *Core) ReclaimLocalCheckpointWithSpacePreflight(ctx context.Context, seal CheckpointSeal, preflight func(uint64, uint64) error, apply func(context.Context, Slot) error) (Slot, error) {
	if !c.localMode {
		return 0, errLocalCostsUnavailable
	}
	if apply == nil {
		return 0, fmt.Errorf("Local checkpoint apply callback is required")
	}
	if err := c.acquireLocalExecution(ctx); err != nil {
		return 0, err
	}
	defer c.releaseLocalExecution()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	_, validator, err := c.checkpointIdentity(seal)
	if err != nil {
		return 0, err
	}
	if validator == nil {
		return 0, fmt.Errorf("Local checkpoint root validator is unavailable")
	}
	if err := validator(ctx, seal); err != nil {
		return 0, fmt.Errorf("validate exact Local checkpoint root: %w", err)
	}
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		return 0, err
	}
	decidedSlot, alreadyDecided := c.DecidedSlot(value)
	cost, err := c.localCost.estimate(len(value))
	if err != nil {
		return 0, err
	}
	through := c.RecorderTip()
	historyBytes, unknownBytes, err := c.localReclaimRecoveryPlanOwned(through)
	if err != nil {
		return 0, err
	}
	var immediate uint64
	if alreadyDecided {
		// Resume an ambiguous prior seal without allocating a replacement slot.
		// historyBytes already includes its exact missing QDEC encoding when
		// the decision has not reached the WAL yet.
		immediate = historyBytes
	} else {
		marker := qlog.Entry{Slot: uint64(seal.Index), Hash: seal.RootHash, Type: qlog.EntryCheckpointVerified, Payload: value}
		immediate, err = checkedAdd(historyBytes, uint64(len(marker.Encode())), cost.ForegroundBytes)
	}
	if err != nil {
		return 0, err
	}
	protected, err := checkedAdd(unknownBytes, cost.CheckpointBytes)
	if err != nil {
		return 0, err
	}
	if err := c.localCapacityAdmissionOwned("reclaim_local_checkpoint", "checkpoint_required", immediate, protected); err != nil {
		return 0, err
	}
	if preflight != nil {
		rewriteBytes, err := c.estimateLocalCompactionOwned(seal)
		if err != nil {
			return 0, err
		}
		remaining := unknownBytes
		remaining, err = checkedAdd(remaining, historyBytes)
		if err == nil && !alreadyDecided {
			remaining, err = checkedAdd(remaining, uint64(len((qlog.Entry{Slot: uint64(seal.Index), Hash: seal.RootHash, Type: qlog.EntryCheckpointVerified, Payload: value}).Encode())))
		}
		if err == nil && !alreadyDecided {
			remaining, err = checkedAdd(remaining, cost.ForegroundBytes)
		}
		if err == nil {
			remaining, err = checkedAdd(remaining, cost.CheckpointBytes)
		}
		if err != nil {
			return 0, fmt.Errorf("Local checkpoint physical-space estimate overflows: %w", err)
		}
		if err := preflight(rewriteBytes, remaining); err != nil {
			return 0, err
		}
	}
	if err := c.prepareCheckpointOwnedWithAdmission(ctx, seal, false); err != nil {
		return 0, err
	}
	var slot Slot
	if alreadyDecided {
		if through > c.Tip() {
			if err := c.recoverThroughOwnedAdmitted(ctx, through); err != nil {
				return decidedSlot, err
			}
		}
		slot = decidedSlot
	} else {
		slot, _, err = c.proposeOwned(ctx, value, true, true, true)
		if err != nil {
			return slot, err
		}
	}
	if err := c.ensureDurableThroughOwned(ctx, slot); err != nil {
		return slot, err
	}
	if err := apply(ctx, slot); err != nil {
		return slot, err
	}
	if err := ctx.Err(); err != nil {
		return slot, err
	}
	if err := validator(ctx, seal); err != nil {
		return slot, fmt.Errorf("revalidate exact Local checkpoint root: %w", err)
	}
	hitLocalCheckpointCrashBoundary("after-seal-decision-durable-before-compact")
	if err := c.compactThroughOwned(seal.Index, seal.RootHash); err != nil {
		return slot, err
	}
	return slot, nil
}

func (c *Core) localReclaimRecoveryPlanOwned(through Slot) (historyBytes, unknownBytes uint64, err error) {
	if !c.localMode || len(c.localExecutionOwner) == 0 {
		return 0, 0, errLocalExternalMutation
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	tip := c.tip
	if through <= tip {
		history, err := c.localMissingDecisionBytesOwnedLocked(c.floor+1, through)
		return history, 0, err
	}
	rangeCount := uint64(through - tip)
	decided := uint64(0)
	for slot := range c.decided {
		if slot > tip && slot <= through {
			decided++
		}
	}
	if decided > rangeCount {
		return 0, 0, fmt.Errorf("Local recovery suffix count exceeds slot range")
	}
	for slot, state := range c.recorders {
		if slot <= tip || slot > through {
			continue
		}
		if _, hasDecision := c.decided[slot]; hasDecision {
			continue
		}
		if err := c.validateLocalISRLocked(slot, state, nil); err != nil {
			return 0, 0, err
		}
	}
	historyBytes, err = c.localMissingDecisionBytesOwnedLocked(c.floor+1, through)
	if err != nil {
		return 0, 0, err
	}
	unknownBytes, err = checkedMul(rangeCount-decided, c.localCost.maxSlotBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("Local recovery byte count overflows: %w", err)
	}
	return historyBytes, unknownBytes, nil
}

func (c *Core) localMissingDecisionBytesOwnedLocked(first, through Slot) (uint64, error) {
	if through < first || through <= c.floor || c.localUnloggedCount == 0 {
		return 0, nil
	}
	var total uint64
	for slot, decided := range c.decided {
		if slot < first || slot > through || slot <= c.floor || c.logged[slot] {
			continue
		}
		entry, err := decisionEntry(decided)
		if err != nil {
			return 0, fmt.Errorf("estimate Local decision slot %d: %w", slot, err)
		}
		total, err = checkedAdd(total, uint64(len(entry.Encode())))
		if err != nil {
			return 0, fmt.Errorf("Local decision byte count overflows: %w", err)
		}
	}
	return total, nil
}
