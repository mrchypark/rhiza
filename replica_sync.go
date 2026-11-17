package rhiza

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

func (r *ReadReplica) syncObjectStore(ctx context.Context) (resultErr error) {
	if err := r.archive.Load(ctx); err != nil {
		return err
	}
	seal, baseDecision, hasBase := r.archive.RecoveryBase()
	if !hasBase {
		r.clearCheckpointEvidence()
		if r.archive.Tip() < r.core.Tip() {
			return fmt.Errorf("object-store archive tip %d is behind replica tip %d", r.archive.Tip(), r.core.Tip())
		}
		if err := r.acceptArchive(ctx, r.archive.DecisionsFrom, r.archive.Tip()); err != nil {
			return err
		}
		r.setSource("object-store", uint64(r.archive.Tip()))
		return r.applyCertified(ctx)
	}
	if floor := r.core.CompactionFloor(); seal.Index < floor {
		return fmt.Errorf("object-store checkpoint base %d regressed behind replica floor %d", seal.Index, floor)
	}
	version, versioned := r.archive.HeadVersion()
	// The previous successful pass verified this exact history. No object data
	// is consumed here, so there is nothing to protect from GC with a pin.
	if versioned && r.syncedHead != nil && *r.syncedHead == version &&
		r.core.Tip() == r.archive.Tip() && r.material.Tip() == uint64(r.core.Tip()) &&
		r.core.CompactionFloor() == seal.Index {
		r.setSource("object-store", uint64(r.archive.Tip()))
		return nil
	}
	r.syncedHead = nil
	if r.pinOwner == "" {
		var err error
		r.pinOwner, err = replicaOwner(r.config.ReplicaID)
		if err != nil {
			return err
		}
	}
	owner := r.pinOwner
	pinID := sha256.Sum256([]byte(owner))
	var checkpointPinCreated, complete bool
	defer func() {
		if resultErr == nil && complete {
			r.setCheckpointEvidence(seal, version, versioned, hex.EncodeToString(pinID[:]), checkpointPinCreated)
		}
	}()
	// Run after pin cleanup and renewal have completed. A failed release must
	// not allow reuse of an owner whose previous lease may still be active.
	defer func() {
		if resultErr != nil {
			r.pinOwner = ""
		} else if versioned {
			r.syncedHead = &version
		}
	}()
	snapshot, err := r.archive.BeginRecoverySnapshot(ctx, owner, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, closeRecoverySnapshot(snapshot)) }()
	version, versioned = r.archive.HeadVersion()
	seal, baseDecision, hasBase = snapshot.RecoveryBase()
	if !hasBase {
		return fmt.Errorf("recovery snapshot omitted checkpoint base")
	}
	if floor := r.core.CompactionFloor(); seal.Index < floor {
		return fmt.Errorf("recovery snapshot checkpoint base %d regressed behind replica floor %d", seal.Index, floor)
	}
	var checkpointPin *checkpoint.RecoveryPin
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if r.core.Tip() < seal.Index || r.material.Tip() < uint64(seal.Index) {
		root, err := r.checkpoints.OpenRoot(workCtx, uint64(seal.Index), seal.RootHash)
		if err != nil {
			return err
		}
		if root.Hash != seal.StateHash {
			return fmt.Errorf("certified checkpoint state hash mismatch")
		}
		checkpointPin, err = r.checkpoints.PinRecoveryRoot(workCtx, root, owner, 2*time.Minute)
		if err != nil {
			return err
		}
		checkpointPinCreated = true
		defer func() { resultErr = errors.Join(resultErr, closeCheckpointPin(checkpointPin)) }()
	}
	renewDone := make(chan error, 1)
	go renewReplicaPins(workCtx, cancel, snapshot, checkpointPin, renewDone)
	defer func() {
		cancel()
		resultErr = errors.Join(resultErr, <-renewDone)
	}()
	if checkpointPin != nil {
		root, err := checkpointPin.Root()
		if err != nil {
			return err
		}
		dir, err := os.MkdirTemp(r.config.DataDir, ".rhiza-replica-restore-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		files, err := r.checkpoints.DownloadAndVerifyRootFiles(workCtx, root, dir)
		if err != nil {
			return err
		}
		if r.core.Tip() < seal.Index {
			if err := r.core.RestoreCheckpointBase(workCtx, seal, baseDecision); err != nil {
				return err
			}
		} else if err := r.core.ValidateCheckpointBase(workCtx, seal, baseDecision); err != nil {
			return err
		}
		if r.material.Tip() < uint64(seal.Index) {
			materialFiles := make([]materializer.CheckpointFile, 0, len(files))
			for _, file := range files {
				materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
			}
			if err := r.material.RestoreCheckpoint(workCtx, materialFiles); err != nil {
				return err
			}
		}
	} else if err := r.core.ValidateCheckpointBase(workCtx, seal, baseDecision); err != nil {
		return err
	}
	if err := r.acceptArchive(workCtx, snapshot.DecisionsFrom, snapshot.Tip()); err != nil {
		return err
	}
	if err := r.applyCertified(workCtx); err != nil {
		return err
	}
	if r.core.CompactionFloor() < seal.Index && r.core.Tip() >= baseDecision.Slot {
		if err := r.core.PrepareCheckpoint(workCtx, seal); err != nil {
			return err
		}
		if err := r.core.CompactThrough(seal.Index, seal.RootHash); err != nil {
			return err
		}
	}
	r.setSource("object-store", uint64(snapshot.Tip()))
	complete = true
	return nil
}

type archiveReader func(context.Context, quepaxa.Slot, int) ([]quepaxa.DecidedValue, quepaxa.Slot, error)

func (r *ReadReplica) acceptArchive(ctx context.Context, read archiveReader, tip quepaxa.Slot) error {
	for r.core.Tip() < tip {
		values, _, err := read(ctx, r.core.Tip()+1, 256)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			return fmt.Errorf("shared archive omitted slot %d", r.core.Tip()+1)
		}
		for _, decision := range values {
			if err := types.ValidateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := r.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReadReplica) applyCertified(ctx context.Context) error {
	for quepaxa.Slot(r.material.Tip()) < r.core.Tip() {
		values, _, err := r.core.DecisionsFrom(quepaxa.Slot(r.material.Tip())+1, 256)
		if err != nil {
			return err
		}
		if len(values) == 0 {
			return fmt.Errorf("replica decision gap at slot %d", r.material.Tip()+1)
		}
		if err := r.material.ApplyBatch(ctx, values); err != nil {
			return err
		}
	}
	return nil
}

func replicaOwner(id string) (string, error) {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("replica-%s-%x", id, nonce), nil
}

func renewReplicaPins(ctx context.Context, cancel context.CancelFunc, snapshot *recovery.RecoverySnapshot, pin *checkpoint.RecoveryPin, done chan<- error) {
	ticker := time.NewTicker(40 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := snapshot.Renew(ctx, 2*time.Minute); err != nil {
				cancel()
				done <- err
				return
			}
			if pin != nil {
				if err := pin.Renew(ctx, 2*time.Minute); err != nil {
					cancel()
					done <- err
					return
				}
			}
		}
	}
}

func closeRecoverySnapshot(snapshot *recovery.RecoverySnapshot) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return snapshot.Close(ctx)
}

func closeCheckpointPin(pin *checkpoint.RecoveryPin) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pin.Close(ctx)
}

func (r *ReadReplica) setSource(source string, tip uint64) {
	r.statusMu.Lock()
	r.status.Source, r.status.SourceTip = source, tip
	r.statusMu.Unlock()
}

func (r *ReadReplica) clearCheckpointEvidence() {
	r.statusMu.Lock()
	r.status.CheckpointBaseSlot = 0
	r.status.CheckpointRootHash = ""
	r.status.CheckpointStateHash = ""
	r.status.CheckpointPrefixHash = ""
	r.status.ArchiveHeadVersionType = ""
	r.status.ArchiveHeadVersion = ""
	r.status.RecoveryPinID = ""
	r.status.RecoveryPinsCreated = 0
	r.status.RecoveryPinsReleased = 0
	r.statusMu.Unlock()
}

func (r *ReadReplica) setCheckpointEvidence(seal quepaxa.CheckpointSeal, version thanosobjstore.ObjectVersion, versioned bool, pinID string, checkpointPin bool) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	r.status.CheckpointBaseSlot = uint64(seal.Index)
	r.status.CheckpointRootHash = hex.EncodeToString(seal.RootHash[:])
	r.status.CheckpointStateHash = hex.EncodeToString(seal.StateHash[:])
	r.status.CheckpointPrefixHash = hex.EncodeToString(seal.PrefixHash[:])
	if versioned {
		r.status.ArchiveHeadVersion = version.Value
		switch version.Type {
		case thanosobjstore.Generation:
			r.status.ArchiveHeadVersionType = "generation"
		case thanosobjstore.ETag:
			r.status.ArchiveHeadVersionType = "etag"
		default:
			r.status.ArchiveHeadVersionType = "unknown"
		}
	}
	r.status.RecoveryPinID = pinID
	r.status.RecoveryPinsCreated = 1
	r.status.RecoveryPinsReleased = 1
	if checkpointPin {
		r.status.RecoveryPinsCreated++
		r.status.RecoveryPinsReleased++
	}
}
