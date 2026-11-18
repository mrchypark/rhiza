package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func (n *Node) preparePeerCheckpoint(ctx context.Context, sender quepaxa.NodeID, seal quepaxa.CheckpointSeal) error {
	if n.managedStartup != nil && isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) {
		return fmt.Errorf("managed append-only checkpoint profile does not use prepared-fence RPCs")
	}
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return n.prepareManagedGraphPeerCheckpoint(ctx, sender, seal)
	}
	if err := n.checkpoints.ValidatePublisherClaim(ctx, string(sender), uint64(seal.Index), seal.RootHash); err != nil {
		return err
	}
	if _, err := n.core.WaitCheckpointPrefixIfLagging(ctx, seal); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The claim can change during its initial read even if the prefix is already available.
	if err := n.checkpoints.ValidatePublisherClaim(ctx, string(sender), uint64(seal.Index), seal.RootHash); err != nil {
		return err
	}
	if err := n.core.PrepareCheckpoint(ctx, seal); err != nil {
		return err
	}
	log.Printf("checkpoint prepared: index=%d root=%x", seal.Index, seal.RootHash)
	return nil
}

// Initial admission owns a real runtime borrow. Shutdown cancels that binding
// and drains its borrowers before wg.Wait, so this Add cannot race a completed
// Wait or outlive native resources. Starting here avoids losing the first tick
// before a startup-only binding has acquired its actual admission.
func (n *Node) startManagedGraphPublisherAfterAdmission(binding *internalApplicationBinding) {
	if binding == nil || n.managedStartup == nil {
		return
	}
	if isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) {
		if !n.managedGraphPublisherStarted.CompareAndSwap(false, true) {
			return
		}
		n.managedGraphPublisherBinding.Store(binding)
		n.startManagedAppendOnlyPublisher(binding.lifetime)
		return
	}
	if n.materializationPolicy() == sqlpolicy.ManagedGraphRequestMaterializationPolicy && n.managedGraphPublisherStarted.CompareAndSwap(false, true) {
		n.managedGraphPublisherBinding.Store(binding)
		n.startManagedGraphCheckpointPublisher(binding.lifetime)
	}
}

func advanceCheckpointReadBarriers(ctx context.Context, reserved uint64, material *materializer.Materializer, server *network.Server) error {
	for material.Tip() < reserved {
		var nonce [types.ReadBarrierNonceSize]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		if _, err := server.ProposeCheckpointBarrier(ctx, types.EncodeReadBarrier(nonce)); err != nil {
			return err
		}
	}
	return nil
}

// validatedArchiveRecoveryBase returns an archive floor only after both the
// archive chain and its certified checkpoint base have been checked against
// this Core's checkpoint verifier and local lineage. Archive metadata alone
// is not authority to delete checkpoint roots.
func (n *Node) validatedArchiveRecoveryBase(ctx context.Context) (quepaxa.CheckpointSeal, bool) {
	if n.archive == nil || n.core == nil {
		return quepaxa.CheckpointSeal{}, false
	}
	if err := n.archive.Load(ctx); err != nil {
		log.Printf("load shared archive before checkpoint GC failed: %v", err)
		return quepaxa.CheckpointSeal{}, false
	}
	seal, decision, ok := n.archive.RecoveryBase()
	if !ok || decision.Slot <= seal.Index {
		return quepaxa.CheckpointSeal{}, false
	}
	if local, exists := n.core.CertifiedValue(decision.Slot); exists &&
		(local.Hash != decision.Hash || !bytes.Equal(local.Value, decision.Value) || !bytes.Equal(local.Certificate, decision.Certificate)) {
		log.Printf("archive recovery base decision conflicts with local certified history")
		return quepaxa.CheckpointSeal{}, false
	}
	if err := n.core.ValidateCheckpointBase(ctx, seal, decision); err != nil {
		log.Printf("validate shared archive checkpoint base before GC failed: %v", err)
		return quepaxa.CheckpointSeal{}, false
	}
	return seal, true
}

func advanceArchiveFloor(current, base uint64, ok bool) (uint64, bool) {
	if !ok {
		return current, false
	}
	return max(current, base), true
}

// publishCertifiedCheckpoint only makes a checkpoint discoverable after its
// decision archive has been made durable. Startup does not require compaction;
// another recovering voter may still pin the retained history.
func (n *Node) publishCertifiedCheckpoint(ctx context.Context, root *checkpoint.Checkpoint) error {
	if n.archive != nil {
		if err := n.archive.SyncThrough(ctx, n.core, n.core.Tip()); err != nil {
			return err
		}
	}
	return n.checkpoints.PromoteCertifiedCurrent(ctx, root)
}

func (n *Node) compactCertifiedCheckpoint(ctx context.Context) error {
	// Auto-checkpoint completion and the archive ticker can observe the same
	// seal. Keep the whole trim/compact transition single-flight so the later
	// caller rechecks the newly installed floor instead of compacting it twice.
	n.compactionMu.Lock()
	defer n.compactionMu.Unlock()
	if n.core == nil || n.archive == nil || n.checkpoints == nil {
		return nil
	}
	seal, ok, err := n.core.LatestCheckpointSeal()
	if err != nil {
		return err
	}
	if !ok || seal.Index <= n.core.CompactionFloor() {
		return nil
	}
	if quepaxa.Slot(n.material.Tip()) < seal.Index {
		return fmt.Errorf("materializer tip %d is behind checkpoint %d", n.material.Tip(), seal.Index)
	}
	if err := n.core.VerifyCheckpoint(ctx, seal.CheckpointSeal); err != nil {
		return err
	}
	if err := n.archive.SyncThrough(ctx, n.core, seal.DecisionSlot); err != nil {
		return err
	}
	decision, ok := n.core.CertifiedValue(seal.DecisionSlot)
	if !ok {
		return fmt.Errorf("checkpoint seal decision %d is unavailable", seal.DecisionSlot)
	}
	if err := n.archive.TrimThrough(ctx, seal, decision); err != nil {
		if err == recovery.ErrActiveRecoveryPin || err == recovery.ErrActiveGCLock {
			// The checkpoint is already certified and published. Keep the
			// compaction floor and protected history intact; the existing archive
			// ticker will retry this seal after the reader or GC holder releases.
			return nil
		}
		return err
	}
	return n.core.CompactThrough(seal.Index, seal.RootHash)
}

func (n *Node) reclaimLocalCheckpoint(ctx context.Context) (resultErr error) {
	if n.config == nil || !n.config.Local || n.server == nil || n.core == nil || n.material == nil || n.localStore == nil {
		return network.ErrLocalMaintenanceRefused
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: canceled before Local maintenance admission", network.ErrLocalMaintenanceRefused)
	}
	resume, err := n.server.Quiesce(ctx)
	if err != nil {
		if errors.Is(err, network.ErrNotReady) {
			return network.ErrNotReady
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%w: canceled while quiescing before Local maintenance mutation", network.ErrLocalMaintenanceRefused)
		}
		return err
	}
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, network.ErrLocalMaintenanceRefused) && !quepaxa.IsLocalAdmissionDenial(resultErr) && !errors.Is(resultErr, network.ErrNotReady) {
			n.server.FailLocalLifecycle(resultErr)
		}
		resume()
	}()
	n.localSnapshotMu.Lock()
	defer func() {
		n.localSnapshotMu.Unlock()
		if resultErr == nil {
			n.collectLocalBackup(ctx)
		}
	}()
	if err := n.localCheckpointSpacePreflight(); err != nil {
		return fmt.Errorf("%w: %v", network.ErrLocalMaintenanceRefused, err)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("%w: canceled before Local state replay", network.ErrLocalMaintenanceRefused)
	}
	if n.material.Tip() < uint64(n.core.Tip()) {
		if err := n.replayLocalDecisions(ctx); err != nil {
			return fmt.Errorf("replay Local state before checkpoint: %w", err)
		}
	}
	index := n.material.Tip()
	if index == 0 || quepaxa.Slot(index) != n.core.Tip() || quepaxa.Slot(index) <= n.core.CompactionFloor() {
		return fmt.Errorf("%w: no reclaimable Local prefix", network.ErrLocalMaintenanceRefused)
	}
	prefix, ok := n.core.PrefixHash(quepaxa.Slot(index))
	if !ok {
		return fmt.Errorf("Local checkpoint prefix is unavailable")
	}
	files, capturedIndex, cleanup, err := n.material.CheckpointFilesAt(ctx)
	if err != nil {
		return fmt.Errorf("capture Local checkpoint: %w", err)
	}
	defer cleanup()
	if capturedIndex != index {
		return fmt.Errorf("Local checkpoint snapshot moved from slot %d to %d", index, capturedIndex)
	}
	var sourceBytes, graphBytes uint64
	for _, file := range files {
		info, statErr := os.Lstat(file.Path)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("Local checkpoint capture produced an invalid fixed-role file")
		}
		sourceBytes += uint64(info.Size())
		if file.Role == materializer.CheckpointGraphData {
			graphBytes = uint64(info.Size())
		}
	}
	if sourceBytes > localCheckpointMaxBytes {
		return fmt.Errorf("Local checkpoint exceeds the combined %d-byte bound", localCheckpointMaxBytes)
	}
	if err := n.localStore.requireFreeBytes(n.config.DataDir, sourceBytes, graphBytes, uint64(max(int64(0), n.wal.Bytes()))); err != nil {
		return fmt.Errorf("Local checkpoint space changed during capture: %w", err)
	}
	descriptor, err := n.localStore.Publish(ctx, files, index, prefix, n.core.ConfigIDForSlot(quepaxa.Slot(index)))
	if err != nil {
		return fmt.Errorf("publish Local checkpoint: %w", err)
	}
	hitLocalNodeCheckpointCrashBoundary("after-checkpoint-root-durable-before-prepare")
	seal, err := sealFromLocalDescriptor(n.core, descriptor)
	if err != nil {
		return err
	}
	slot, err := n.core.ReclaimLocalCheckpointWithSpacePreflight(ctx, seal, func(rewriteBytes, remainingBytes uint64) error {
		return n.localCheckpointSpacePreflight(rewriteBytes, remainingBytes)
	}, func(applyCtx context.Context, appliedThrough quepaxa.Slot) error {
		if err := n.replayLocalDecisions(applyCtx); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) < appliedThrough {
			return fmt.Errorf("Local materializer stopped at %d before seal slot %d", n.material.Tip(), appliedThrough)
		}
		if err := n.publishLocalBackupCheckpoint(applyCtx, seal, appliedThrough); err != nil {
			return err
		}
		return n.localCheckpointSpacePreflight()
	})
	if err != nil {
		if quepaxa.IsLocalAdmissionDenial(err) {
			return err
		}
		return fmt.Errorf("reclaim Local checkpoint: %w", err)
	}
	if floor, _, ok := n.core.RecoveryRoot(); !ok || floor != seal.Index {
		return fmt.Errorf("Local checkpoint compacted through unexpected floor after seal slot %d", slot)
	}
	n.localRoot = &descriptor
	// Collection is post-commit cleanup: authority and materialized state are
	// already durable. A deletion failure is retained as cleanup debt and must
	// not turn a successful checkpoint into an ambiguous proposal result.
	if roots, rootErr := n.core.LocalCheckpointRoots(ctx); rootErr != nil {
		log.Printf("Local checkpoint GC deferred: cannot resolve live roots: %v", rootErr)
	} else if gcErr := n.localStore.Collect(roots); gcErr != nil {
		log.Printf("Local checkpoint GC deferred: %v", gcErr)
	}
	return nil
}

func (n *Node) resumePreparedLocalCheckpoint(ctx context.Context) (resultErr error) {
	index, root, prepared := n.core.LatestPreparedCheckpoint()
	if !prepared {
		return nil
	}
	if sealed, ok, err := n.core.LatestCheckpointSeal(); err != nil {
		return err
	} else if ok && sealed.Index == index && sealed.RootHash == root && n.core.CompactionFloor() >= index {
		floor, floorRoot, hasFloor := n.core.RecoveryRoot()
		if hasFloor && floor == index && floorRoot == root {
			return nil
		}
	}
	prefix, ok := n.core.PrefixHash(index)
	if !ok {
		return fmt.Errorf("prepared Local checkpoint has no certified prefix")
	}
	descriptor, _, err := n.localStore.OpenExpected(quepaxa.LocalCheckpointRoot{RootHash: root, Index: index, PrefixHash: prefix, ConfigID: n.core.ConfigIDForSlot(index)})
	if err != nil {
		return fmt.Errorf("open exact prepared Local checkpoint: %w", err)
	}
	if n.localRoot == nil || descriptor.Slot >= n.localRoot.Slot {
		n.localRoot = &descriptor
	}
	if err := n.localCheckpointSpacePreflight(); err != nil {
		return err
	}
	resume, err := n.server.Quiesce(ctx)
	if err != nil {
		return err
	}
	defer resume()
	n.localSnapshotMu.Lock()
	defer func() {
		n.localSnapshotMu.Unlock()
		if resultErr == nil {
			n.collectLocalBackup(ctx)
		}
	}()
	seal, err := sealFromLocalDescriptor(n.core, descriptor)
	if err != nil {
		return err
	}
	_, err = n.core.ReclaimLocalCheckpointWithSpacePreflight(ctx, seal, func(rewriteBytes, remainingBytes uint64) error {
		return n.localCheckpointSpacePreflight(rewriteBytes, remainingBytes)
	}, func(applyCtx context.Context, through quepaxa.Slot) error {
		if err := n.replayLocalDecisions(applyCtx); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) < through {
			return fmt.Errorf("resumed Local materializer stopped at %d before seal slot %d", n.material.Tip(), through)
		}
		return n.publishLocalBackupCheckpoint(applyCtx, seal, through)
	})
	if err != nil {
		return fmt.Errorf("complete prepared Local checkpoint: %w", err)
	}
	n.localRoot = &descriptor
	return nil
}

func (n *Node) localCheckpointSpacePreflight(reserve ...uint64) error {
	if n.localStore == nil || n.wal == nil {
		return fmt.Errorf("Local checkpoint storage is unavailable")
	}
	var live uint64
	for _, path := range []string{filepath.Join(n.config.DataDir, "sqlite.db"), filepath.Join(n.config.DataDir, "latticedb", "graph.ltdb")} {
		size, err := localTreeBytes(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("invalid Local materializer data for disk estimate at %s: %w", path, err)
		}
		if live > ^uint64(0)-size {
			return fmt.Errorf("Local checkpoint disk estimate overflow")
		}
		live += size
	}
	if live > localCheckpointMaxBytes {
		return fmt.Errorf("Local state exceeds the supported checkpoint size bound")
	}
	// Backup output, durable publication copy, possible graph restore copy, and
	// a conservative WAL rewrite are all included; existing root files remain
	// charged as cleanup debt until deletion and directory sync succeed.
	values := []uint64{live, live, live, uint64(max(int64(0), n.wal.Bytes()))}
	values = append(values, reserve...)
	required, err := checkedLocalDiskAdd(values...)
	if err != nil {
		return err
	}
	return n.localStore.requireFreeBytes(n.config.DataDir, required)
}

func localTreeBytes(root string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in Local materializer data")
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("non-regular file in Local materializer data")
		}
		size := uint64(info.Size())
		if total > ^uint64(0)-size {
			return fmt.Errorf("Local checkpoint disk estimate overflow")
		}
		total += size
		return nil
	})
	return total, err
}

func checkedLocalDiskAdd(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if total > ^uint64(0)-value {
			return 0, fmt.Errorf("Local checkpoint disk estimate overflow")
		}
		total += value
	}
	return total, nil
}
