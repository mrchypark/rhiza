package recovery

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// VerifyManagedGraphLocator binds an untrusted CURRENT hint to one already
// captured archive HEAD. It does not mutate HEAD.Base, install a Core base, pin
// data, or grant startup authority. The verifier carries trusted membership and
// a real checkpoint file validator, never a no-op callback.
func VerifyManagedGraphLocator(ctx context.Context, snapshot *RecoverySnapshot, cp *checkpoint.Manager, verifier *quepaxa.Core, l checkpoint.ManagedGraphLocator) (quepaxa.CheckpointSeal, quepaxa.DecidedValue, error) {
	return verifyManagedAppendOnlyLocator(ctx, snapshot, cp, verifier, checkpoint.ManagedAppendOnlyLocator(l), sqlpolicy.ManagedGraphRequestMaterializationPolicy)
}

// The expected policy is trusted configuration; CURRENT cannot select it.
func verifyManagedAppendOnlyLocator(ctx context.Context, snapshot *RecoverySnapshot, cp *checkpoint.Manager, verifier *quepaxa.Core, l checkpoint.ManagedAppendOnlyLocator, expected sqlpolicy.MaterializationPolicy) (quepaxa.CheckpointSeal, quepaxa.DecidedValue, error) {
	var zero quepaxa.CheckpointSeal
	if (expected != sqlpolicy.ManagedRequestMaterializationPolicy && expected != sqlpolicy.ManagedGraphRequestMaterializationPolicy) || l.Policy != int(expected) || snapshot == nil || cp == nil || verifier == nil || l.Index == 0 || l.Index >= l.SealDecisionSlot || quepaxa.Slot(l.SealDecisionSlot) > snapshot.Tip() {
		return zero, quepaxa.DecidedValue{}, fmt.Errorf("managed Graph locator lies outside captured history")
	}
	page, _, err := snapshot.DecisionsFrom(ctx, quepaxa.Slot(l.SealDecisionSlot), 1)
	if err != nil || len(page) != 1 || uint64(page[0].Slot) != l.SealDecisionSlot {
		return zero, quepaxa.DecidedValue{}, fmt.Errorf("managed Graph locator seal is unavailable: %w", err)
	}
	d := page[0]
	if d.Hash != l.SealDecisionHash || sha256.Sum256(d.Value) != d.Hash || len(d.Certificate) == 0 {
		return zero, d, fmt.Errorf("managed Graph locator decision hash mismatch")
	}
	seal, isSeal, err := quepaxa.DecodeCheckpointSeal(d.Value)
	if err != nil || !isSeal || uint64(seal.Index) != l.Index || seal.RootHash != l.RootHash {
		return zero, d, fmt.Errorf("managed Graph locator does not identify the exact seal")
	}
	root, err := cp.OpenRoot(ctx, l.Index, l.RootHash)
	if err != nil {
		return zero, d, err
	}
	if root.Hash != seal.StateHash || root.Generation != root.Index || root.SQLExecutionPolicy != l.Policy {
		return zero, d, fmt.Errorf("managed Graph locator root differs from certified seal")
	}
	if err := verifier.ValidateCheckpointBase(ctx, seal, d); err != nil {
		return zero, d, err
	}
	return seal, d, nil
}

// ObserveManagedGraphDecision reads a fixed HEAD and its immutable extent via
// the actual archive decoder. It creates no GC pin/lease and performs no writes.
// Certificate authority is still checked by the caller's actual Core.
func (m *Manager) ObserveManagedGraphDecision(ctx context.Context, slot quepaxa.Slot) (quepaxa.DecidedValue, quepaxa.Slot, error) {
	if !m.appendOnly || slot == 0 {
		return quepaxa.DecidedValue{}, 0, fmt.Errorf("managed Graph observation requires append-only archive")
	}
	r := NewManager(m.bucket, m.prefix, m.configID)
	r.validateValue = types.ValidateManagedGraphExecutionPolicy
	defer r.Close()
	if err := r.loadHead(ctx, make(map[extentObject]Extent), true); err != nil {
		return quepaxa.DecidedValue{}, 0, err
	}
	r.mu.Lock()
	snapshot := &RecoverySnapshot{manager: r, head: r.head, refs: slices.Clone(r.extents)}
	r.mu.Unlock()
	page, tip, err := snapshot.DecisionsFrom(ctx, slot, 1)
	if err != nil || len(page) != 1 || page[0].Slot != slot {
		return quepaxa.DecidedValue{}, tip, fmt.Errorf("exact managed Graph archived decision unavailable: %w", err)
	}
	return page[0], tip, nil
}
