package recovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// CertifiedBarrierState is the replayed source state at one exact certified
// read-barrier decision. It is safe to consume only with the caller's trusted
// membership/anchor expectations and a successful archive-stability check.
type CertifiedBarrierState struct {
	Decision      quepaxa.DecidedValue
	Membership    quepaxa.Cluster
	PrefixHash    [32]byte
	AbortSlot     quepaxa.Slot
	AbortTarget   quepaxa.Cluster
	HasAbort      bool
	Pending       bool
	PendingTarget quepaxa.Cluster
}

// ReplayCertifiedThrough replays contiguous certified archive pages into a
// passive observer. The callback must return the same captured archive tip for
// every page; it must not create a recovery pin or mutate archive state.
func ReplayCertifiedThrough(
	ctx context.Context,
	core *quepaxa.Core,
	through quepaxa.Slot,
	readPage func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error),
) error {
	if core == nil || readPage == nil || through == 0 || core.Tip() > through {
		return fmt.Errorf("invalid certified replay boundary")
	}
	for core.Tip() < through {
		if err := ctx.Err(); err != nil {
			return err
		}
		from := core.Tip() + 1
		limit := 256
		if remaining := through - from + 1; remaining < quepaxa.Slot(limit) {
			limit = int(remaining)
		}
		values, tip, err := readPage(ctx, from, limit)
		if err != nil {
			return err
		}
		if tip < through || len(values) == 0 || len(values) > limit || values[0].Slot != from {
			return fmt.Errorf("certified archive omitted slot %d", from)
		}
		for i, value := range values {
			if err := ctx.Err(); err != nil {
				return err
			}
			if value.Slot != from+quepaxa.Slot(i) || value.Slot > through {
				return fmt.Errorf("certified archive page is not contiguous through slot %d", through)
			}
			if err := types.ValidateExecutionPolicy(value.Value); err != nil {
				return fmt.Errorf("certified archive decision %d violates execution policy: %w", value.Slot, err)
			}
		}
		before := core.Tip()
		if err := core.AcceptCertifiedValues(values); err != nil {
			return fmt.Errorf("verify certified archive page: %w", err)
		}
		if core.Tip() <= before {
			return fmt.Errorf("certified archive replay made no progress at slot %d", from)
		}
	}
	if core.Tip() != through {
		return fmt.Errorf("verified tip %d differs from requested slot %d", core.Tip(), through)
	}
	return ctx.Err()
}

// VerifyCertifiedBarrier verifies a read-barrier decision against a fresh,
// base-zero archive replay. The slot is a lookup hint, never proof by itself.
// Compacted or externally anchored archives are refused because this helper
// has no checkpoint/anchor trust context.
func (m *Manager) VerifyCertifiedBarrier(
	ctx context.Context,
	trustedInitial quepaxa.Cluster,
	enableReconfiguration bool,
	slot quepaxa.Slot,
	expectedNonce [quepaxa.ReadBarrierNonceSize]byte,
) (decision quepaxa.DecidedValue, membership quepaxa.Cluster, resultErr error) {
	return m.verifyCertifiedBarrier(ctx, trustedInitial, enableReconfiguration, slot, expectedNonce, nil, nil)
}

// VerifyCertifiedBarrierState returns the exact reconstructed chain and
// membership-transition state at the certified barrier after validating the
// archive snapshot remains unchanged.
func (m *Manager) VerifyCertifiedBarrierState(
	ctx context.Context,
	trustedInitial quepaxa.Cluster,
	enableReconfiguration bool,
	slot quepaxa.Slot,
	expectedNonce [quepaxa.ReadBarrierNonceSize]byte,
) (CertifiedBarrierState, error) {
	var state CertifiedBarrierState
	_, _, err := m.verifyCertifiedBarrier(ctx, trustedInitial, enableReconfiguration, slot, expectedNonce, nil, &state)
	return state, err
}

// VerifyCertifiedBarrierWithEvidence verifies a read barrier after installing
// the generation anchor and certified checkpoint base selected by a fresh
// archive view. expected must come from the approved recovery contract.
func (m *Manager) VerifyCertifiedBarrierWithEvidence(
	ctx context.Context,
	trustedInitial quepaxa.Cluster,
	enableReconfiguration bool,
	slot quepaxa.Slot,
	expectedNonce [quepaxa.ReadBarrierNonceSize]byte,
	expected EvidenceOptions,
) (decision quepaxa.DecidedValue, membership quepaxa.Cluster, resultErr error) {
	return m.verifyCertifiedBarrier(ctx, trustedInitial, enableReconfiguration, slot, expectedNonce, &expected, nil)
}

// VerifyCertifiedBarrierWithEvidenceState is the anchored/checkpoint-aware
// form of VerifyCertifiedBarrierState. Trusted expectations come from the
// retained repair authorization, not the selected archive view.
func (m *Manager) VerifyCertifiedBarrierWithEvidenceState(
	ctx context.Context,
	trustedInitial quepaxa.Cluster,
	enableReconfiguration bool,
	slot quepaxa.Slot,
	expectedNonce [quepaxa.ReadBarrierNonceSize]byte,
	expected EvidenceOptions,
) (CertifiedBarrierState, error) {
	var state CertifiedBarrierState
	_, _, err := m.verifyCertifiedBarrier(ctx, trustedInitial, enableReconfiguration, slot, expectedNonce, &expected, &state)
	return state, err
}

func (m *Manager) verifyCertifiedBarrier(
	ctx context.Context,
	trustedInitial quepaxa.Cluster,
	enableReconfiguration bool,
	slot quepaxa.Slot,
	expectedNonce [quepaxa.ReadBarrierNonceSize]byte,
	expected *EvidenceOptions,
	verified *CertifiedBarrierState,
) (decision quepaxa.DecidedValue, membership quepaxa.Cluster, resultErr error) {
	var zero quepaxa.DecidedValue
	if m == nil || m.bucket == nil || m.configID != 1 || slot == 0 {
		return zero, quepaxa.Cluster{}, fmt.Errorf("certified barrier slot must be positive")
	}
	// Do not let a long-lived caller Manager's extent cache stand in for a
	// current archive read. This verifier owns only this read-only Manager; it
	// never closes or drains the shared bucket.
	reader := NewManager(m.bucket, m.prefix, m.configID)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	reader.mu.Lock()
	head := reader.head
	version := reader.headCAS
	refs := append([]Extent(nil), reader.extents...)
	tip := reader.tip
	reader.mu.Unlock()
	var view CertifiedArchiveView
	if expected == nil {
		if head.Sealed || head.Base != 0 || head.BasePrefix != ([32]byte{}) || head.BaseSeal != nil || head.BaseDecision != nil || head.BaseAnchor != nil || head.LineageAnchor != nil || slot > tip {
			return zero, quepaxa.Cluster{}, fmt.Errorf("certified barrier source is sealed, compacted, anchored, or behind the requested slot")
		}
	} else {
		var err error
		view, err = reader.CaptureCertifiedView()
		if err != nil {
			return zero, quepaxa.Cluster{}, err
		}
		if err := validateEvidenceOptions(*expected); err != nil {
			return zero, quepaxa.Cluster{}, err
		}
		if trustedInitial.ConfigID != 1 || NewMembershipRecord(expected.ExpectedMembership.Cluster, trustedInitial.Members, expected.ExpectedMembership.Durability) != expected.ExpectedMembership {
			return zero, quepaxa.Cluster{}, fmt.Errorf("trusted initial membership differs from expected target membership")
		}
		if _, err := VerifyExpectedAnchorForView(ctx, view, *expected); err != nil {
			return zero, quepaxa.Cluster{}, fmt.Errorf("verify expected generation anchor: %w", err)
		}
		if head.Sealed || slot > tip {
			return zero, quepaxa.Cluster{}, fmt.Errorf("certified barrier source is sealed or behind the requested slot")
		}
	}
	if trustedInitial.ConfigID == 0 || len(trustedInitial.Members) == 0 {
		return zero, quepaxa.Cluster{}, fmt.Errorf("trusted initial membership is unavailable")
	}
	observerDir, err := os.MkdirTemp("", ".rhiza-certified-barrier-")
	if err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	var wal *qlog.WAL
	defer func() {
		if wal != nil {
			resultErr = errors.Join(resultErr, wal.Close())
		}
		resultErr = errors.Join(resultErr, os.RemoveAll(observerDir))
	}()
	wal, err = qlog.Open(observerDir)
	if err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	core, err := quepaxa.NewObserver(quepaxa.Config{
		NodeID: "rhiza-certified-barrier-verifier", Cluster: trustedInitial,
		WAL: wal, EnableReconfiguration: enableReconfiguration,
	})
	if err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	if expected != nil {
		checkpoints := checkpoint.NewManager(reader.bucket, reader.prefix, "", 1)
		core.SetCheckpointValidator(func(checkpointCtx context.Context, seal quepaxa.CheckpointSeal) error {
			return checkpoints.Verify(checkpointCtx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		if _, err := InitializeCertifiedRecoveryBase(ctx, core, view, expected.ExpectedMembership, expected.ExpectedAnchorHash, checkpoints); err != nil {
			return zero, quepaxa.Cluster{}, fmt.Errorf("initialize certified recovery base: %w", err)
		}
	}
	readPage := func(pageCtx context.Context, from quepaxa.Slot, limit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
		values, observedTip, err := reader.DecisionsFrom(pageCtx, from, limit)
		if err != nil {
			return nil, observedTip, err
		}
		if observedTip != tip {
			return nil, observedTip, fmt.Errorf("certified archive tip changed during verification")
		}
		return values, observedTip, nil
	}
	if err := ReplayCertifiedThrough(ctx, core, slot, readPage); err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	decision, ok := core.CertifiedValue(slot)
	if !ok || decision.Slot != slot {
		return zero, quepaxa.Cluster{}, fmt.Errorf("certified barrier decision is unavailable")
	}
	if !bytes.Equal(decision.Value, quepaxa.EncodeReadBarrier(expectedNonce)) {
		return zero, quepaxa.Cluster{}, fmt.Errorf("certified decision does not match the expected read-barrier nonce")
	}
	cluster := core.ClusterForSlot(slot)
	if cluster.ConfigID == 0 || len(cluster.Members) == 0 {
		return zero, quepaxa.Cluster{}, fmt.Errorf("certified barrier membership is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	if err := reader.Load(ctx); err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	reader.mu.Lock()
	stable := archiveHeadsEqual(head, reader.head) && sameNullableObjectVersion(version, reader.headCAS) && archiveRefPrefixEqual(reader.extents, refs)
	reader.mu.Unlock()
	if !stable {
		return zero, quepaxa.Cluster{}, errors.New("certified archive changed during barrier verification")
	}
	if err := ctx.Err(); err != nil {
		return zero, quepaxa.Cluster{}, err
	}
	if verified != nil {
		prefixHash, ok := core.PrefixHash(slot)
		if !ok {
			return zero, quepaxa.Cluster{}, fmt.Errorf("certified barrier prefix is unavailable")
		}
		pendingTarget, pending := core.PendingReconfiguration()
		abortSlot, abortTarget, hasAbort := core.LastReconfigurationAbort()
		*verified = CertifiedBarrierState{
			Decision: decision, Membership: cluster, PrefixHash: prefixHash,
			AbortSlot: abortSlot, AbortTarget: abortTarget, HasAbort: hasAbort,
			Pending: pending, PendingTarget: pendingTarget,
		}
	}
	return decision, cluster, nil
}
