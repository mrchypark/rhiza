package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

// Checkpoint represents a snapshot of materialized state.
type Checkpoint struct {
	Index       types.Slot     `json:"index"`
	Hash        types.ValueHash `json:"hash"`
	Size        int64          `json:"size"`
	Profile     string         `json:"profile"`
	Fingerprint string         `json:"fingerprint"`
	CreatedAt   time.Time      `json:"created_at"`
}

// CheckpointManifest tracks checkpoint metadata.
type CheckpointManifest struct {
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// CheckpointStore manages checkpoints in object storage.
type CheckpointStore struct {
	store    *Store
	mu       sync.RWMutex
	manifest *CheckpointManifest
}

// NewCheckpointStore creates a new checkpoint store.
func NewCheckpointStore(store *Store) *CheckpointStore {
	return &CheckpointStore{
		store: store,
		manifest: &CheckpointManifest{
			Checkpoints: make([]Checkpoint, 0),
		},
	}
}

// Load reads the checkpoint manifest from object storage.
func (cs *CheckpointStore) Load(ctx context.Context) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	r, err := cs.store.Get(ctx, "checkpoint/manifest.json")
	if err != nil {
		if cs.store.IsNotFound(err) {
			cs.manifest = &CheckpointManifest{Checkpoints: make([]Checkpoint, 0)}
			return nil
		}
		return fmt.Errorf("get checkpoint manifest: %w", err)
	}
	defer r.Close()

	var m CheckpointManifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return fmt.Errorf("decode checkpoint manifest: %w", err)
	}
	cs.manifest = &m
	return nil
}

// Persist writes the checkpoint manifest to object storage.
func (cs *CheckpointStore) Persist(ctx context.Context) error {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	data, err := json.Marshal(cs.manifest)
	if err != nil {
		return fmt.Errorf("marshal checkpoint manifest: %w", err)
	}

	return cs.store.Upload(ctx, "checkpoint/manifest.json", bytes.NewReader(data))
}

// Upload uploads checkpoint data and registers it.
func (cs *CheckpointStore) Upload(ctx context.Context, cp Checkpoint, data []byte) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	key := fmt.Sprintf("checkpoint/%d-%x", cp.Index, cp.Hash[:8])
	if err := cs.store.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("upload checkpoint: %w", err)
	}

	cp.Size = int64(len(data))
	cp.CreatedAt = time.Now()
	cs.manifest.Checkpoints = append(cs.manifest.Checkpoints, cp)
	return nil
}

// Latest returns the most recent checkpoint, or nil if none exists.
func (cs *CheckpointStore) Latest() *Checkpoint {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if len(cs.manifest.Checkpoints) == 0 {
		return nil
	}
	cp := cs.manifest.Checkpoints[len(cs.manifest.Checkpoints)-1]
	return &cp
}

// Download retrieves checkpoint data by index.
func (cs *CheckpointStore) Download(ctx context.Context, index types.Slot) ([]byte, error) {
	cs.mu.RLock()
	var target *Checkpoint
	for _, cp := range cs.manifest.Checkpoints {
		if cp.Index == index {
			target = &cp
			break
		}
	}
	cs.mu.RUnlock()

	if target == nil {
		return nil, fmt.Errorf("checkpoint %d not found", index)
	}

	key := fmt.Sprintf("checkpoint/%d-%x", target.Index, target.Hash[:8])
	r, err := cs.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download checkpoint: %w", err)
	}
	defer r.Close()

	return json.Marshal(r)
}
