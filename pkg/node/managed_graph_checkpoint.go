package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// The caller starts this only after actual managed startup. Its work is part of
// Node's normal cancellation/drain lifetime. It never enables legacy GC/trim.
func (n *Node) startManagedGraphCheckpointPublisher(ctx context.Context) {
	if n.managedStartup == nil || n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy || n.checkpoints == nil || n.archive == nil {
		return
	}
	interval := n.config.CheckpointInterval
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	period := min(interval, time.Minute)
	tail := n.config.CheckpointTailBytes
	if tail <= 0 {
		tail = 512 << 20
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		var last time.Time
		for {
			// First publication/recovery is attempted immediately. A failed
			// attempt preserves its candidate/prepared seal for the next tick.
			if last.IsZero() || time.Since(last) >= interval || n.wal.Bytes() >= tail {
				if err := n.publishManagedGraphCheckpoint(ctx); err != nil {
					if ctx.Err() == nil {
						log.Printf("managed Graph checkpoint publication deferred: %v", err)
					}
				} else {
					last = time.Now()
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// The existing owned publisher is the only producer of this notification. A
// caller waits without holding compactionMu, so peer/control publication and
// Node cancellation can continue while the application is not yet admitted.
func (n *Node) ensureManagedGraphInitialRoot(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == nil || n.managedGraphInitialRootDone == nil {
		return fmt.Errorf("managed Graph initial root completion is unavailable")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.managedGraphInitialRootDone:
		return ctx.Err()
	}
}

func (n *Node) publishManagedGraphCheckpoint(ctx context.Context) error {
	if n.managedStartup == nil || n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy || n.core == nil || n.checkpoints == nil || n.archive == nil || n.material == nil {
		return fmt.Errorf("managed Graph checkpoint runtime unavailable")
	}
	// Local single-flight only. No external publisher/GC lease is acquired.
	n.compactionMu.Lock()
	defer n.compactionMu.Unlock()
	if err := n.ensureInitialManagedAdmission(ctx); err != nil {
		return err
	}
	if !n.core.IsVoter() {
		return fmt.Errorf("managed Graph checkpoint requires an active voter")
	}
	// Resume publication after seal certification but before CURRENT CAS. Do
	// not create another application command or reinterpret absent metadata.
	if sealed, exists, err := n.core.LatestCheckpointSeal(); err != nil {
		return err
	} else if exists {
		if err := n.promoteManagedGraphSeal(ctx, sealed); err != nil {
			return err
		}
		if uint64(sealed.Index) >= n.material.StateTip() {
			return nil
		}
	}
	// A trusted, actually admitted new runtime may need its first positive cut.
	// The control proposal is not an application write or a bootstrap signal.
	if n.material.Tip() == 0 {
		if err := n.advanceManagedGraphCheckpointCut(ctx); err != nil {
			return err
		}
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return err
	}
	files, cut, cleanup, err := n.material.CheckpointFilesAt(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if cut == 0 || cut > uint64(n.core.Tip()) {
		return fmt.Errorf("managed Graph pair cut lies outside certified history")
	}
	if fence, _, exists := n.core.LatestPreparedCheckpoint(); exists && fence > quepaxa.Slot(cut) {
		return n.reconcileManagedGraphCapturedFence(ctx, quepaxa.Slot(cut), fmt.Errorf("managed Graph captured cut %d is below prepared fence %d", cut, fence))
	}
	actual := make([]checkpoint.File, 0, len(files))
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		actual = append(actual, checkpoint.File{Role: string(file.Role), Path: file.Path})
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	if err := n.validateCheckpointFiles(ctx, actual, cut); err != nil {
		return err
	}
	root, err := n.checkpoints.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		return err
	}
	prefix, ok := n.core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		return fmt.Errorf("managed Graph checkpoint certified prefix unavailable")
	}
	next, following, err := n.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		return err
	}
	seal := quepaxa.CheckpointSeal{ConfigID: n.managedGraphCheckpointCluster(quepaxa.Slot(cut)).ConfigID, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, GenerationAnchorHash: n.core.GenerationAnchorHash()}
	if n.config.EnableReconfiguration {
		history, err := n.core.CheckpointMembership(seal.Index)
		if err != nil {
			return err
		}
		seal.Membership = &history
	}
	if prior, hash, exists := n.core.LatestPreparedCheckpoint(); exists && prior == seal.Index && hash != seal.RootHash {
		// Preserve the conflicting prepared record. Only a real higher
		// control cut can make a new immutable candidate eligible.
		if err := n.advanceManagedGraphCheckpointCut(ctx); err != nil {
			return err
		}
		return fmt.Errorf("managed Graph prepared cut conflict retained; advanced certified control cut")
	}
	if err := n.core.PrepareCheckpoint(ctx, seal); err != nil {
		if errors.Is(err, quepaxa.ErrCheckpointPreparePending) {
			if controlErr := n.advanceManagedGraphCheckpointCut(ctx); controlErr != nil {
				return errors.Join(err, controlErr)
			}
			return fmt.Errorf("managed Graph pending checkpoint preserved; advanced certified control cut for a fresh capture: %w", err)
		}
		return n.reconcileManagedGraphCapturedFence(ctx, seal.Index, err)
	}
	if err := n.transport.PrepareCheckpoint(ctx, seal); err != nil {
		return n.advanceManagedGraphFailedPrepare(ctx, err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		return err
	}
	if _, _, err := n.core.Propose(ctx, value); err != nil {
		return err
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return err
	}
	sealed, exists, err := n.core.LatestCheckpointSeal()
	if err != nil || !exists || sealed.Index != seal.Index || sealed.RootHash != seal.RootHash {
		return fmt.Errorf("managed Graph exact checkpoint seal was not certified: %w", err)
	}
	return n.promoteManagedGraphSeal(ctx, sealed)
}

func (n *Node) reconcileManagedGraphCapturedFence(ctx context.Context, captured quepaxa.Slot, prepareErr error) error {
	fence, _, exists := n.core.LatestPreparedCheckpoint()
	if !exists || fence <= captured {
		return prepareErr
	}
	target, _, err := n.core.ReadIndex(ctx)
	if err != nil {
		return errors.Join(prepareErr, err)
	}
	if n.core.Tip() < max(target, fence) {
		if n.transport == nil {
			return errors.Join(prepareErr, fmt.Errorf("managed Graph prepared fence requires unavailable certified catch-up"))
		}
		cluster := n.core.CurrentCluster()
		if err := n.catchUpQuorum(ctx, n.transport, &cluster); err != nil {
			return errors.Join(prepareErr, err)
		}
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		return errors.Join(prepareErr, err)
	}
	latest, _, _ := n.core.LatestPreparedCheckpoint()
	if n.core.Tip() < max(target, latest) || n.material.Tip() < uint64(max(target, latest)) {
		return errors.Join(prepareErr, fmt.Errorf("managed Graph prepared fence still has unresolved certified history"))
	}
	// The old physical snapshot is never relabelled or prepared. Return its
	// refusal and let the next tick capture the now-replayed common prefix.
	return fmt.Errorf("managed Graph stale capture preserved; replayed certified prefix through %d: %w", n.material.Tip(), prepareErr)
}

func (n *Node) advanceManagedGraphFailedPrepare(ctx context.Context, prepareErr error) error {
	// The existing prepare RPC reports failed durable prepare quorum, not a
	// typed remote conflict. Do not parse peer error text or certify success.
	// A real control decision advances the next capture past partial prepares;
	// if consensus is unavailable, both original and control errors survive.
	if ctx.Err() == nil && errors.Is(prepareErr, quepaxa.ErrQuorumUnavailable) {
		if err := n.advanceManagedGraphCheckpointCut(ctx); err != nil {
			return errors.Join(prepareErr, err)
		}
		return fmt.Errorf("managed Graph peer prepare failed; advanced certified control cut: %w", prepareErr)
	}
	return prepareErr
}

func (n *Node) advanceManagedGraphCheckpointCut(ctx context.Context) error {
	var nonce [types.ReadBarrierNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	value := types.EncodeReadBarrier(nonce)
	ownerBinding := n.managedGraphPublisherBinding.Load()
	if ownerBinding == nil {
		ownerBinding = n.internalApplication.Load()
	}
	op := &managedGraphControlOperation{value: append([]byte(nil), value...), hash: sha256.Sum256(value), binding: ownerBinding, done: make(chan struct{})}
	n.managedGraphControlMu.Lock()
	if n.managedGraphControl != nil {
		n.managedGraphControlMu.Unlock()
		return fmt.Errorf("managed Graph checkpoint control already active")
	}
	n.managedGraphControl = op
	n.managedGraphControlMu.Unlock()
	op.slot, op.err = n.server.ProposeCheckpointBarrier(ctx, value)
	if op.err == nil {
		if decision, ok := n.core.CertifiedValue(op.slot); !ok || decision.Hash != op.hash || !bytes.Equal(decision.Value, op.value) {
			op.err = fmt.Errorf("managed Graph owned checkpoint control certificate did not match its proposal")
		} else {
			op.err = n.replayLocalDecisions(ctx)
		}
	}
	n.managedGraphControlMu.Lock()
	close(op.done)
	n.managedGraphControl = nil
	n.managedGraphControlMu.Unlock()
	return op.err
}

// waitManagedAppendOnlyControl handles only a Core-validated, exact next-slot
// pure-control recorder. The observation is a hint: admission still requires
// the normal certified barrier, prefix replay, and fresh runtime checks.
func (n *Node) waitManagedAppendOnlyControl(ctx context.Context, binding *internalApplicationBinding) error {
	if binding == nil || binding.core == nil || binding.core != n.core || n.server == nil || n.managedStartup == nil || !isManagedAppendOnlyProtocol(n.managedApplicationProtocol()) {
		return fmt.Errorf("managed append-only admission binding unavailable")
	}
	progress := n.core.RuntimeProgressSnapshot()
	n.managedGraphControlMu.Lock()
	if settled := n.managedGraphControl; settled != nil {
		select {
		case <-settled.done:
			// Existing waiters retain the operation and its original result.
			// A later admission gets a fresh observation, but an unresolved
			// recorder value is never retried or reinterpreted as success.
			n.managedGraphControl = nil
			if progress.RecorderTip > progress.CertifiedTip {
				n.managedGraphControlMu.Unlock()
				return n.refuseManagedGraphAdmissionAtReason("completed_operation_recorder_gap", progress.RecorderTip, progress)
			}
		default:
		}
	}
	if op := n.managedGraphControl; op != nil {
		if op.observation == nil {
			isBarrier, decodeErr := types.DecodeReadBarrier(op.value)
			if op.binding == binding && decodeErr == nil && isBarrier && (progress.RecorderTip <= progress.CertifiedTip || progress.RecorderTip == progress.CertifiedTip+1 && progress.RecorderHashPresent && progress.RecorderHash == op.hash) {
				done := op.done
				target := progress.RecorderTip
				if target <= progress.CertifiedTip {
					target = progress.CertifiedTip
				}
				n.managedGraphControlMu.Unlock()
				return n.waitManagedAppendOnlyBarrier(ctx, target, op, done)
			}
			n.managedGraphControlMu.Unlock()
			return n.refuseManagedGraphAdmissionAtReason("unobserved_local_operation_mismatch", progress.RecorderTip, progress)
		}
		if managedAppendOnlyOperationMatchesProgress(op, binding, n.core, progress) {
			done := op.done
			target := op.observation.ObservedSlot
			n.managedGraphControlMu.Unlock()
			return n.waitManagedAppendOnlyBarrier(ctx, target, op, done)
		}
		n.managedGraphControlMu.Unlock()
		return n.refuseManagedGraphAdmissionAtReason("active_observation_mismatch", progress.RecorderTip, progress)
	}
	n.managedGraphControlMu.Unlock()
	if progress.RecorderTip <= progress.CertifiedTip {
		n.clearCompletedManagedAppendOnlyControl(progress.CertifiedTip)
		return nil
	}
	observation, err := n.core.ObserveManagedAppendOnlyControl(ctx)
	if err != nil {
		return n.refuseManagedGraphAdmissionAtReason("core_observation_rejected", progress.RecorderTip, progress)
	}
	if !observation.BelongsTo(n.core) || observation.ApplicationProtocol != binding.identity.ApplicationProtocol || observation.Generation != binding.identity.Member.Generation || observation.CommittedTip != progress.CertifiedTip || observation.ObservedSlot != progress.RecorderTip || observation.ObservedSlot != observation.CommittedTip+1 || len(observation.Proposals) == 0 {
		return n.refuseManagedGraphAdmissionAtReason("observation_shape_mismatch", progress.RecorderTip, progress)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	n.managedGraphControlMu.Lock()
	if err := ctx.Err(); err != nil {
		n.managedGraphControlMu.Unlock()
		return err
	}
	op := n.managedGraphControl
	if op != nil && op.observation == nil {
		isBarrier, decodeErr := types.DecodeReadBarrier(op.value)
		if op.binding == binding && decodeErr == nil && isBarrier && (progress.RecorderTip <= progress.CertifiedTip || progress.RecorderTip == progress.CertifiedTip+1 && progress.RecorderHashPresent && progress.RecorderHash == op.hash) {
			done := op.done
			target := progress.RecorderTip
			if target <= progress.CertifiedTip {
				target = progress.CertifiedTip
			}
			n.managedGraphControlMu.Unlock()
			return n.waitManagedAppendOnlyBarrier(ctx, target, op, done)
		}
		n.managedGraphControlMu.Unlock()
		return n.refuseManagedGraphAdmissionAtReason("concurrent_unobserved_operation_mismatch", progress.RecorderTip, progress)
	}
	if op != nil && op.observation != nil && progress.CertifiedTip >= op.observation.ObservedSlot {
		select {
		case <-op.done:
			n.managedGraphControl = nil
			op = nil
		default:
		}
	}
	if op != nil {
		if !managedAppendOnlyOperationMatchesProgress(op, binding, n.core, progress) {
			n.managedGraphControlMu.Unlock()
			return n.refuseManagedGraphAdmissionAtReason("active_observation_mismatch", progress.RecorderTip, progress)
		}
		done := op.done
		n.managedGraphControlMu.Unlock()
		return n.waitManagedAppendOnlyBarrier(ctx, progress.RecorderTip, op, done)
	}
	var nonce [types.ReadBarrierNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		n.managedGraphControlMu.Unlock()
		return err
	}
	value := types.EncodeReadBarrier(nonce)
	op = &managedGraphControlOperation{
		value: append([]byte(nil), value...), hash: sha256.Sum256(value), binding: binding,
		observation: &observation, done: make(chan struct{}),
	}
	n.managedGraphControl = op
	n.wg.Add(1) // caller still holds a binding borrower; Shutdown drains it before Wait.
	n.managedGraphControlMu.Unlock()
	go n.runManagedAppendOnlyBarrier(ctx, binding, op)
	return n.waitManagedAppendOnlyBarrier(ctx, progress.RecorderTip, op, op.done)
}

func managedAppendOnlyOperationMatchesProgress(op *managedGraphControlOperation, binding *internalApplicationBinding, core *quepaxa.Core, progress quepaxa.RuntimeProgress) bool {
	if op == nil || op.observation == nil || op.binding != binding || !op.observation.BelongsTo(core) || op.observation.ObservedSlot != op.observation.CommittedTip+1 {
		return false
	}
	if progress.CertifiedTip < op.observation.CommittedTip {
		return false
	}
	if progress.CertifiedTip < op.observation.ObservedSlot {
		if progress.RecorderTip == op.observation.ObservedSlot {
			return true
		}
		return progress.RecorderTip > op.observation.ObservedSlot && progress.RecorderHashPresent && progress.RecorderHash == op.hash
	}
	return true
}

func (n *Node) clearCompletedManagedAppendOnlyControl(certified quepaxa.Slot) {
	n.managedGraphControlMu.Lock()
	defer n.managedGraphControlMu.Unlock()
	op := n.managedGraphControl
	if op == nil || op.observation == nil || certified < op.observation.ObservedSlot {
		return
	}
	select {
	case <-op.done:
		n.managedGraphControl = nil
	default:
	}
}

func (n *Node) runManagedAppendOnlyBarrier(startCtx context.Context, binding *internalApplicationBinding, op *managedGraphControlOperation) {
	defer n.wg.Done()
	ctx, cancel := context.WithCancel(startCtx)
	stopLifetime := context.AfterFunc(binding.lifetime, cancel)
	defer func() {
		stopLifetime()
		cancel()
	}()
	started := false
	if startCtx.Err() != nil {
		op.err = startCtx.Err()
	} else if ctx.Err() != nil {
		op.err = ctx.Err()
	} else {
		started = true
		op.slot, op.err = n.server.ProposeCheckpointBarrier(ctx, op.value)
	}
	if !started {
		n.managedGraphControlMu.Lock()
		if n.managedGraphControl == op {
			n.managedGraphControl = nil
		}
		close(op.done)
		n.managedGraphControlMu.Unlock()
		return
	}
	if op.err == nil && op.slot < op.observation.ObservedSlot {
		op.err = fmt.Errorf("managed append-only barrier did not cover observed control slot")
	}
	if op.err == nil {
		op.err = n.core.WaitTip(ctx, op.slot)
	}
	if op.err == nil {
		decision, ok := n.core.CertifiedValue(op.slot)
		if !ok || decision.Hash != op.hash || !bytes.Equal(decision.Value, op.value) {
			op.err = fmt.Errorf("managed append-only barrier certificate did not match its proposal")
		}
	}
	if op.err == nil {
		op.err = n.replayLocalDecisions(ctx)
	}
	n.managedGraphControlMu.Lock()
	close(op.done)
	n.managedGraphControlMu.Unlock()
}

func (n *Node) waitManagedAppendOnlyBarrier(ctx context.Context, target quepaxa.Slot, op *managedGraphControlOperation, done <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if op.err != nil || op.slot < target {
		return n.refuseManagedGraphAdmissionAtReason("observed_control_barrier_failed_or_short", target, n.core.RuntimeProgressSnapshot())
	}
	decision, ok := n.core.CertifiedValue(op.slot)
	if !ok || decision.Hash != op.hash || !bytes.Equal(decision.Value, op.value) {
		return n.refuseManagedGraphAdmissionAtReason("observed_control_certificate_mismatch", target, n.core.RuntimeProgressSnapshot())
	}
	return nil
}

// Wait only when the exact highest recorder value is the live checkpoint
// publisher's operation. Recorder state by itself never grants admission.
func (n *Node) waitManagedGraphOwnedControl(ctx context.Context, binding *internalApplicationBinding, target quepaxa.Slot) error {
	progress := n.core.RuntimeProgressSnapshot()
	n.managedGraphControlMu.Lock()
	op := n.managedGraphControl
	if op == nil {
		n.managedGraphControlMu.Unlock()
		if progress.RecorderTip > progress.CertifiedTip {
			// A remote voter may record the exact next control after the earlier
			// append-only admission check. Reobserve through Core's read-only
			// classifier; it rejects application/unknown winners and only returns
			// success after the existing certified barrier and replay path.
			return n.waitManagedAppendOnlyControl(ctx, binding)
		}
		return nil
	}
	isBarrier, decodeErr := types.DecodeReadBarrier(op.value)
	if decodeErr != nil || !isBarrier || op.binding != binding {
		n.managedGraphControlMu.Unlock()
		return n.refuseManagedGraphAdmissionAtReason("local_owner_identity_mismatch", target, progress)
	}
	switch {
	case progress.RecorderTip > progress.CertifiedTip:
		if progress.RecorderTip-progress.CertifiedTip != 1 || !progress.RecorderHashPresent || progress.RecorderHashIsCertified || progress.RecorderHash != op.hash {
			n.managedGraphControlMu.Unlock()
			return n.refuseManagedGraphAdmissionAtReason("recorder_not_exact_owned_barrier", target, progress)
		}
		observedSlot := progress.RecorderTip
		done := op.done
		n.managedGraphControlMu.Unlock()
		return n.waitAndVerifyManagedGraphOwnedControl(ctx, target, op, done, observedSlot)
	case progress.RecorderTip == progress.CertifiedTip:
		done := op.done
		n.managedGraphControlMu.Unlock()
		return n.waitAndVerifyManagedGraphOwnedControl(ctx, target, op, done, 0)
	default:
		n.managedGraphControlMu.Unlock()
		return n.refuseManagedGraphAdmissionAtReason("recorder_behind_certificate", target, progress)
	}
}

func (n *Node) waitAndVerifyManagedGraphOwnedControl(ctx context.Context, target quepaxa.Slot, op *managedGraphControlOperation, done <-chan struct{}, observedSlot quepaxa.Slot) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	if op.err != nil || op.slot == 0 || observedSlot != 0 && op.slot != observedSlot {
		return n.refuseManagedGraphAdmissionAtReason("owned_checkpoint_barrier_failed_or_wrong_slot", target, n.core.RuntimeProgressSnapshot())
	}
	decision, ok := n.core.CertifiedValue(op.slot)
	if !ok || decision.Hash != op.hash || !bytes.Equal(decision.Value, op.value) {
		return n.refuseManagedGraphAdmissionAtReason("owned_checkpoint_certificate_mismatch", target, n.core.RuntimeProgressSnapshot())
	}
	return nil
}

func (n *Node) refuseManagedGraphAdmissionAt(target quepaxa.Slot, progress quepaxa.RuntimeProgress) error {
	return n.refuseManagedGraphAdmissionAtReason("unclassified", target, progress)
}

func (n *Node) refuseManagedGraphAdmissionAtReason(reason string, target quepaxa.Slot, progress quepaxa.RuntimeProgress) error {
	applied := uint64(n.material.Tip())
	if n.managedApplicationProtocol() == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
		recorderHash := "absent"
		if progress.RecorderHashPresent {
			recorderHash = fmt.Sprintf("%x", progress.RecorderHash)
		}
		decisionHash := "absent"
		if decision, ok := n.core.CertifiedValue(progress.RecorderTip); ok {
			decisionHash = fmt.Sprintf("%x", decision.Hash)
		}
		log.Printf("MANAGED_GRAPH_ADMISSION_REFUSED reason=%s target=%d recorded=%d certified=%d applied=%d recorder_hash=%s recorder_hash_certified=%t certified_recorder_hash=%s", reason, target, progress.RecorderTip, progress.CertifiedTip, applied, recorderHash, progress.RecorderHashIsCertified, decisionHash)
	}
	return fmt.Errorf("internal application has unresolved certified history: target=%d recorded=%d certified=%d applied=%d", target, progress.RecorderTip, progress.CertifiedTip, applied)
}

// Called only from the authenticated existing checkpoint prepare RPC. Unlike
// legacy publication, policy10 has no publisher claim; real current membership,
// paired root validation and durable Core preparation are still mandatory.
func (n *Node) prepareManagedGraphPeerCheckpoint(ctx context.Context, sender quepaxa.NodeID, seal quepaxa.CheckpointSeal) (resultErr error) {
	if n.managedStartup == nil || n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy || n.core == nil || n.checkpoints == nil {
		return fmt.Errorf("managed Graph checkpoint peer runtime unavailable")
	}
	cluster := n.managedGraphCheckpointCluster(seal.Index)
	if _, member := cluster.MemberSet()[sender]; !member || seal.ConfigID != cluster.ConfigID {
		return fmt.Errorf("managed Graph checkpoint sender is outside certified membership")
	}
	if _, err := n.core.WaitCheckpointPrefixIfLagging(ctx, seal); err != nil {
		return err
	}
	root, err := n.checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
	if err != nil || root.Hash != seal.StateHash || root.Generation != uint64(seal.Index) {
		return fmt.Errorf("managed Graph peer paired root mismatch: %w", err)
	}
	dir, err := os.MkdirTemp("", "rhiza-graph-peer-prepare-*")
	if err != nil {
		return err
	}
	defer func() { resultErr = cleanupManagedGraphCheckpointStage(dir, resultErr) }()
	files, err := n.checkpoints.DownloadAndVerifyRootFiles(ctx, root, dir)
	if err != nil {
		return err
	}
	if err := n.validateCheckpointFiles(ctx, files, uint64(seal.Index)); err != nil {
		return err
	}
	return n.core.PrepareCheckpoint(ctx, seal)
}

func (n *Node) managedGraphCheckpointCluster(cut quepaxa.Slot) quepaxa.Cluster {
	return n.core.ClusterForSlot(cut + 1)
}

func cleanupManagedGraphCheckpointStage(dir string, prior error) error {
	if err := os.RemoveAll(dir); err != nil {
		return errors.Join(prior, fmt.Errorf("cleanup managed Graph peer checkpoint stage %s: %w", dir, err))
	}
	return prior
}

func (n *Node) promoteManagedGraphSeal(ctx context.Context, seal quepaxa.SealedCheckpoint) error {
	d, exists := n.core.CertifiedValue(seal.DecisionSlot)
	if !exists || d.Slot <= seal.Index || sha256.Sum256(d.Value) != d.Hash {
		return fmt.Errorf("managed Graph exact certified seal unavailable")
	}
	if err := n.core.ValidateCheckpointBase(ctx, seal.CheckpointSeal, d); err != nil {
		return err
	}
	root, err := n.checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
	if err != nil || root == nil || root.Hash != seal.StateHash || root.Generation != uint64(seal.Index) {
		return fmt.Errorf("managed Graph certified pair root mismatch: %w", err)
	}
	if err := n.archive.SyncThrough(ctx, n.core, seal.DecisionSlot); err != nil {
		return err
	}
	archived, tip, err := n.archive.ObserveManagedGraphDecision(ctx, seal.DecisionSlot)
	if err != nil {
		return fmt.Errorf("managed Graph durable archive seal differs: %w", err)
	}
	if err := n.validateManagedGraphArchivedSeal(ctx, seal, d, archived, tip); err != nil {
		return err
	}
	err = n.checkpoints.PromoteManagedGraphLocator(ctx, checkpoint.ManagedGraphLocator{Policy: int(sqlpolicy.ManagedGraphRequestMaterializationPolicy), Index: uint64(seal.Index), RootHash: seal.RootHash, SealDecisionSlot: uint64(d.Slot), SealDecisionHash: d.Hash})
	if err == nil {
		n.markManagedGraphInitialRootComplete()
		return nil
	}
	if !errors.Is(err, checkpoint.ErrStaleCheckpoint) {
		return err
	}
	// A different healthy publisher may already have advanced the discovery
	// hint beyond this captured startup root. The hint alone is not authority:
	// authenticate its exact immutable root and fixed-HEAD archived certificate
	// with this runtime's trusted membership/lineage. Never roll CURRENT back.
	current, exists, readErr := n.checkpoints.LoadManagedGraphLocator(ctx)
	if readErr != nil || !exists || current.Index <= uint64(seal.Index) {
		return errors.Join(err, readErr, fmt.Errorf("newer managed Graph locator unavailable"))
	}
	newer, tip, readErr := n.archive.ObserveManagedGraphDecision(ctx, quepaxa.Slot(current.SealDecisionSlot))
	if readErr != nil || tip < quepaxa.Slot(current.SealDecisionSlot) || newer.Hash != current.SealDecisionHash || sha256.Sum256(newer.Value) != newer.Hash {
		return errors.Join(err, readErr, fmt.Errorf("newer managed Graph archived seal differs"))
	}
	newSeal, isSeal, decodeErr := quepaxa.DecodeCheckpointSeal(newer.Value)
	if decodeErr != nil || !isSeal || uint64(newSeal.Index) != current.Index || newSeal.RootHash != current.RootHash {
		return errors.Join(err, decodeErr, fmt.Errorf("newer managed Graph locator is not the exact seal"))
	}
	newRoot, rootErr := n.checkpoints.OpenRoot(ctx, current.Index, current.RootHash)
	if rootErr != nil || newRoot == nil || newRoot.Generation != current.Index || newRoot.Hash != newSeal.StateHash || newRoot.SQLExecutionPolicy != int(sqlpolicy.ManagedGraphRequestMaterializationPolicy) {
		return errors.Join(err, rootErr, fmt.Errorf("newer managed Graph root differs"))
	}
	if proofErr := n.core.ValidateCheckpointBase(ctx, newSeal, newer); proofErr != nil {
		return errors.Join(err, proofErr)
	}
	n.markManagedGraphInitialRootComplete()
	return nil
}

func (n *Node) validateManagedGraphArchivedSeal(ctx context.Context, seal quepaxa.SealedCheckpoint, local, archived quepaxa.DecidedValue, tip quepaxa.Slot) error {
	if tip < seal.DecisionSlot || local.Slot != seal.DecisionSlot || archived.Slot != local.Slot || archived.Hash != local.Hash || sha256.Sum256(archived.Value) != archived.Hash || !bytes.Equal(archived.Value, local.Value) {
		return fmt.Errorf("managed Graph durable archive seal differs")
	}
	// Recorder summary order (and even the sufficient quorum subset) is not a
	// decision identity. Authenticate the archived proof independently instead
	// of comparing its physical encoding with the local retained certificate.
	return n.core.ValidateCheckpointBase(ctx, seal.CheckpointSeal, archived)
}
