package qlog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"

	"github.com/thanos-io/objstore"
)

// Recovery handles crash recovery from WAL and object storage.
type Recovery struct {
	wal      *WAL
	manifest *Manifest
	syncer   *ObjStoreSyncer
}

// NewRecovery creates a new recovery handler.
func NewRecovery(wal *WAL, manifest *Manifest, bucket objstore.Bucket, prefix string) *Recovery {
	return &Recovery{
		wal:      wal,
		manifest: manifest,
		syncer:   NewObjStoreSyncer(wal, manifest, bucket, prefix, 0),
	}
}

// Recover recovers state from WAL and object storage.
// Returns the last known slot.
func (r *Recovery) Recover(ctx context.Context) (uint64, error) {
	// 1. Try to load manifest from object storage
	if err := r.syncer.LoadManifest(ctx); err != nil {
		return 0, fmt.Errorf("load object-store manifest: %w", err)
	}

	// 2. Read local WAL
	entries, err := r.wal.Read()
	if err != nil {
		return 0, fmt.Errorf("read WAL: %w", err)
	}

	// 3. Find tip slot
	var tip uint64
	for _, e := range entries {
		if e.Slot > tip {
			tip = e.Slot
		}
	}

	// 4. Check if we need to restore from object storage
	if r.shouldRestore(ctx, tip) {
		restoredTip, err := r.restoreFromObjectStorage(ctx)
		if err != nil {
			return 0, fmt.Errorf("restore from object storage: %w", err)
		}
		if restoredTip > tip {
			tip = restoredTip
		}
	}

	log.Printf("recovery complete: tip=%d", tip)
	return tip, nil
}

// shouldRestore returns true if we need to restore from object storage.
func (r *Recovery) shouldRestore(ctx context.Context, localTip uint64) bool {
	// If local WAL is empty, restore from object storage
	if localTip == 0 {
		return true
	}

	// If object storage has a newer manifest, restore
	remoteTip := r.manifest.TipSlot
	if remoteTip > localTip {
		return true
	}

	return false
}

// restoreFromObjectStorage restores state from object storage.
func (r *Recovery) restoreFromObjectStorage(ctx context.Context) (uint64, error) {
	for _, seg := range r.manifest.Segments {
		data, err := r.syncer.DownloadSegment(ctx, seg.Index)
		if err != nil {
			return 0, fmt.Errorf("download segment %d: %w", seg.Index, err)
		}
		if seg.Size > 0 && int64(len(data)) != seg.Size {
			return 0, fmt.Errorf("segment %d size mismatch: got %d want %d", seg.Index, len(data), seg.Size)
		}
		hash := sha256.Sum256(data)
		if seg.Hash != ([32]byte{}) && hash != seg.Hash {
			return 0, fmt.Errorf("segment %d hash mismatch", seg.Index)
		}
		if err := r.wal.RestoreSegment(seg.Index, data); err != nil {
			return 0, fmt.Errorf("restore segment %d: %w", seg.Index, err)
		}
	}
	entries, err := r.wal.Read()
	if err != nil {
		return 0, fmt.Errorf("read restored WAL: %w", err)
	}
	var tip uint64
	for _, entry := range entries {
		if entry.Slot > tip {
			tip = entry.Slot
		}
	}
	if tip < r.manifest.TipSlot {
		return 0, fmt.Errorf("restored WAL tip %d is behind manifest tip %d", tip, r.manifest.TipSlot)
	}
	return tip, nil
}

// RecoverFromCheckpoint restores from a checkpoint and replays WAL.
func (r *Recovery) RecoverFromCheckpoint(ctx context.Context, checkpointData []byte, checkpointSlot uint64) error {
	// 1. Restore checkpoint to local database
	// This is handled by the materializer

	// 2. Replay WAL from checkpoint slot
	entries, err := r.wal.Read()
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}

	for _, e := range entries {
		if e.Slot > checkpointSlot {
			// This entry is after the checkpoint, apply it
			log.Printf("replaying entry: slot=%d", e.Slot)
			// Application is handled by the materializer
		}
	}

	return nil
}
