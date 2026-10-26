package quepaxa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// ImportLocalArchive imports a contiguous archive page while holding the Local
// execution owner. Validation and capacity refusals leave Core and WAL unchanged.
// An append or sync failure has an uncertain outcome: reopen the WAL before retry.
func (c *Core) ImportLocalArchive(ctx context.Context, values []DecidedValue) error {
	if !c.localMode {
		return errLocalExternalMutation
	}
	if err := c.acquireLocalExecution(ctx); err != nil {
		return err
	}
	defer c.releaseLocalExecution()
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(values) == 0 {
		return nil
	}
	// Build a disposable validation view, never recover into the populated Core.
	c.mu.RLock()
	if c.reconfigEnabled || len(c.recorders) != 0 || c.localUnloggedCount != 0 || len(c.decided) != int(c.tip-c.floor) {
		c.mu.RUnlock()
		return fmt.Errorf("local archive import requires a settled Local prefix")
	}
	view := newCore(c.nodeID, c.config, c.wal, nil)
	view.tip, view.floor, view.floorRoot = c.tip, c.floor, c.floorRoot
	view.baseLeaderEpoch, view.baseLeaderOrder = c.baseLeaderEpoch, slices.Clone(c.baseLeaderOrder)
	view.baseFollowingEpoch, view.baseFollowingOrder = c.baseFollowingEpoch, slices.Clone(c.baseFollowingOrder)
	view.generationAnchorHash = c.generationAnchorHash
	view.decided, view.prefixes = maps.Clone(c.decided), maps.Clone(c.prefixes)
	c.mu.RUnlock()
	entries := make([]qlog.Entry, len(values))
	page := make([]DecidedValue, len(values))
	var cost uint64
	for i, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		if view.tip == ^Slot(0) || value.Slot != view.tip+1 {
			return fmt.Errorf("local archive decision gap")
		}
		if err := c.ValidateLocalValueCost(value.Value); err != nil {
			return err
		}
		value.Value, value.Certificate = bytes.Clone(value.Value), bytes.Clone(value.Certificate)
		decision, err := view.certifiedDecision(value)
		if err != nil {
			return err
		}
		if err := view.validateDecisionForRecovery(decision, false); err != nil {
			return err
		}
		if seal, checkpoint, err := DecodeCheckpointSeal(value.Value); err != nil {
			return err
		} else if checkpoint {
			order, following, orderErr := view.checkpointLeaderOrdersLocked(seal.Index)
			prefix, ok := view.prefixes[seal.Index]
			if seal.Index == 0 || seal.Index >= value.Slot || seal.RootHash == ([32]byte{}) || !ok || prefix != seal.PrefixHash || seal.ConfigID != c.config.ConfigID || seal.GenerationAnchorHash != view.generationAnchorHash || seal.Membership != nil || orderErr != nil || !slices.Equal(order, seal.NextLeaderOrder) || !slices.Equal(following, seal.FollowingLeaderOrder) {
				return fmt.Errorf("local archive checkpoint does not bind its prefix")
			}
		}
		if err := view.acceptDecision(decision); err != nil {
			return err
		}
		entry, err := decisionEntry(value)
		if err != nil {
			return err
		}
		cost, err = checkedAdd(cost, uint64(len(entry.Encode())))
		if err != nil {
			return err
		}
		entries[i], page[i] = entry, value
	}
	if err := c.localCapacityAdmissionOwned("import_local_archive", "checkpoint_required", cost, c.localCost.maxCheckpointCostBytes); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := c.wal.Append(entry); err != nil {
			return fmt.Errorf("local archive append outcome uncertain; reopen WAL: %w", err)
		}
	}
	if err := c.wal.Sync(); err != nil {
		return fmt.Errorf("local archive sync outcome uncertain; reopen WAL: %w", err)
	}
	for _, value := range page {
		decision, _ := view.certifiedDecision(value)
		if err := c.acceptDecision(decision); err != nil {
			return fmt.Errorf("local archive installation failed; reopen WAL: %w", err)
		}
		c.mu.Lock()
		// Preserve the original certificate bytes, including the original Local proof.
		c.decided[value.Slot] = value
		c.markDecisionLoggedLocked(value.Slot)
		c.durable[value.Slot], c.valueDurable[value.Hash] = true, true
		c.mu.Unlock()
	}
	return nil
}

// RestoreLocalArchiveBase installs a verified checkpoint into a fresh staging
// WAL, or verifies an exact already-installed base retry without rewriting it.
func (c *Core) RestoreLocalArchiveBase(ctx context.Context, seal CheckpointSeal, decision DecidedValue) error {
	if !c.localMode {
		return errLocalExternalMutation
	}
	if err := c.acquireLocalExecution(ctx); err != nil {
		return err
	}
	defer c.releaseLocalExecution()
	if err := ctx.Err(); err != nil {
		return err
	}
	if seal.Index == ^Slot(0) || decision.Slot <= seal.Index || seal.Membership != nil {
		return fmt.Errorf("invalid Local archive base seal slot or membership")
	}
	if err := c.ValidateCheckpointBase(ctx, seal, decision); err != nil {
		return err
	}
	base := consensusBase{ConfigID: seal.ConfigID, ClosedThrough: seal.Index, PrefixHash: seal.PrefixHash, RecoveryRoot: seal.RootHash, LeaderEpoch: leaderEpoch(seal.Index + 1), NextLeaderOrder: seal.NextLeaderOrder, FollowingLeaderOrder: seal.FollowingLeaderOrder, GenerationAnchorHash: seal.GenerationAnchorHash}
	data, err := json.Marshal(base)
	if err != nil {
		return err
	}
	entry := qlog.Entry{Slot: uint64(seal.Index), Hash: seal.RootHash, Type: qlog.EntryCheckpoint, Payload: data}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reconfigEnabled || len(c.recorders) != 0 || len(c.preparedCheckpoints) != 0 || len(c.decided) != 0 || len(c.values) != 0 || len(c.valueDurable) != 0 || len(c.byHash) != 0 || len(c.logged) != 0 || len(c.durable) != 0 || c.localUnloggedCount != 0 || len(c.sealedRoots) != 0 || len(c.vacant) != 0 {
		return fmt.Errorf("local archive base requires a fresh Core")
	}
	entries, err := c.wal.Read()
	if err != nil {
		return err
	}
	if len(entries) == 1 && bytes.Equal(entries[0].Encode(), entry.Encode()) && c.tip == seal.Index && c.floor == seal.Index && c.floorRoot == seal.RootHash && c.prefixes[seal.Index] == seal.PrefixHash {
		return ctx.Err()
	}
	if c.tip != 0 || c.floor != 0 || c.nextSlot != 1 || len(entries) != 0 || c.wal.Bytes() != 0 {
		return fmt.Errorf("local archive base requires an empty staging WAL")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.wal.Compact(entry, nil); err != nil {
		return fmt.Errorf("local archive base outcome uncertain; reopen WAL: %w", err)
	}
	c.installBaseLocked(base)
	c.pruneSlotAllocatorLocked()
	return nil
}
