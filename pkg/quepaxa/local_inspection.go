package quepaxa

import (
	"context"
	"fmt"
)

// LocalReadOnlyInspection exposes recovered Local WAL state without exposing
// Core's mutation methods. The caller retains ownership of the read-only WAL.
type LocalReadOnlyInspection struct {
	core *Core
}

// OpenLocalReadOnlyInspection recovers a standalone Local history for
// inspection. It requires an already-open read-only WAL and the exact
// single-member LocalMode configuration; it does not enable mutation.
func OpenLocalReadOnlyInspection(config Config) (*LocalReadOnlyInspection, error) {
	if config.WAL == nil || !config.WAL.ReadOnly() {
		return nil, fmt.Errorf("Local read-only inspection requires a read-only WAL")
	}
	if !config.LocalMode {
		return nil, fmt.Errorf("Local read-only inspection requires LocalMode")
	}
	core, err := newCoreFromConfig(config, false, false, true)
	if err != nil {
		return nil, err
	}
	return &LocalReadOnlyInspection{core: core}, nil
}

func (view *LocalReadOnlyInspection) Tip() Slot { return view.core.Tip() }

func (view *LocalReadOnlyInspection) RecorderTip() Slot { return view.core.RecorderTip() }

func (view *LocalReadOnlyInspection) PrefixHash(slot Slot) ([32]byte, bool) {
	return view.core.PrefixHash(slot)
}

func (view *LocalReadOnlyInspection) CompactionFloor() Slot {
	return view.core.CompactionFloor()
}

func (view *LocalReadOnlyInspection) CertifiedValue(slot Slot) (DecidedValue, bool) {
	return view.core.CertifiedValue(slot)
}

func (view *LocalReadOnlyInspection) RecoveryRoot() (Slot, [32]byte, bool) {
	return view.core.RecoveryRoot()
}

// LocalCheckpointRoots returns the recovered root tuples as values, without
// exposing mutable Core state.
func (view *LocalReadOnlyInspection) LocalCheckpointRoots(ctx context.Context) ([]LocalCheckpointRoot, error) {
	roots, err := view.core.LocalCheckpointRoots(ctx)
	if err != nil {
		return nil, err
	}
	return append([]LocalCheckpointRoot(nil), roots...), nil
}
