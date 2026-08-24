package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/thanos-io/objstore"
)

// Checkpoint represents a snapshot of materialized state.
type Checkpoint struct {
	Index     uint64    `json:"index"`
	Hash      [32]byte  `json:"hash"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

// Manifest tracks checkpoint metadata.
type Manifest struct {
	Checkpoints []Checkpoint `json:"checkpoints"`
}

// Manager manages checkpoints in object storage.
type Manager struct {
	bucket   objstore.Bucket
	prefix   string
	manifest *Manifest
	mu       sync.Mutex
}

// NewManager creates a new checkpoint manager.
func NewManager(bucket objstore.Bucket, prefix, localDir string) *Manager {
	return &Manager{
		bucket:   bucket,
		prefix:   prefix,
		manifest: &Manifest{Checkpoints: make([]Checkpoint, 0)},
	}
}

// Load loads the manifest from object storage.
func (m *Manager) Load(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key("checkpoint/manifest.json")
	r, err := m.bucket.Get(ctx, key)
	if err != nil {
		if m.bucket.IsObjNotFoundErr(err) {
			return nil // no manifest yet
		}
		return err
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, m.manifest)
}

// Save saves the manifest to object storage.
func (m *Manager) Save(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.save(ctx)
}

func (m *Manager) save(ctx context.Context) error {
	data, err := json.MarshalIndent(m.manifest, "", "  ")
	if err != nil {
		return err
	}

	key := m.key("checkpoint/manifest.json")
	return m.bucket.Upload(ctx, key, bytes.NewReader(data))
}

// Create uploads a transactionally consistent materializer snapshot.
func (m *Manager) Create(ctx context.Context, data []byte, index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(data) == 0 {
		return fmt.Errorf("empty checkpoint")
	}
	// Calculate hash
	hash := sha256.Sum256(data)
	if count := len(m.manifest.Checkpoints); count > 0 {
		latest := m.manifest.Checkpoints[count-1]
		if latest.Index == index && latest.Hash == hash {
			return nil
		}
	}

	// Upload to object storage
	key := m.key(fmt.Sprintf("checkpoint/%d-%x", index, hash[:8]))
	if err := m.bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("upload checkpoint: %w", err)
	}

	// Update manifest
	cp := Checkpoint{
		Index:     index,
		Hash:      hash,
		Size:      int64(len(data)),
		CreatedAt: time.Now(),
	}
	m.manifest.Checkpoints = append(m.manifest.Checkpoints, cp)

	log.Printf("checkpoint created: index=%d size=%d", index, len(data))
	return m.save(ctx)
}

// Latest returns the most recent checkpoint.
func (m *Manager) Latest() *Checkpoint {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.manifest.Checkpoints) == 0 {
		return nil
	}
	cp := m.manifest.Checkpoints[len(m.manifest.Checkpoints)-1]
	return &cp
}

// Download downloads a checkpoint to local directory.
func (m *Manager) Download(ctx context.Context, index uint64, dstPath string) error {
	data, err := m.Read(ctx, index)
	if err != nil {
		return err
	}
	return os.WriteFile(dstPath, data, 0600)
}

// Read downloads and verifies a checkpoint before it can be restored.
func (m *Manager) Read(ctx context.Context, index uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Find checkpoint in manifest
	var target *Checkpoint
	for _, cp := range m.manifest.Checkpoints {
		if cp.Index == index {
			target = &cp
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("checkpoint %d not found", index)
	}

	// Download from object storage
	key := m.key(fmt.Sprintf("checkpoint/%d-%x", target.Index, target.Hash[:8]))
	r, err := m.bucket.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download checkpoint: %w", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	if int64(len(data)) != target.Size {
		return nil, fmt.Errorf("checkpoint %d size mismatch: got %d want %d", index, len(data), target.Size)
	}
	if sha256.Sum256(data) != target.Hash {
		return nil, fmt.Errorf("checkpoint %d hash mismatch", index)
	}
	return data, nil
}

// Cleanup removes old checkpoints, keeping the latest N.
func (m *Manager) Cleanup(ctx context.Context, keep int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.manifest.Checkpoints) <= keep {
		return nil
	}

	// Remove old checkpoints
	toRemove := m.manifest.Checkpoints[:len(m.manifest.Checkpoints)-keep]
	for _, cp := range toRemove {
		key := m.key(fmt.Sprintf("checkpoint/%d-%x", cp.Index, cp.Hash[:8]))
		if err := m.bucket.Delete(ctx, key); err != nil {
			log.Printf("failed to delete checkpoint %d: %v", cp.Index, err)
		}
	}

	m.manifest.Checkpoints = m.manifest.Checkpoints[len(toRemove):]
	return m.save(ctx)
}

// key prepends the prefix to the path.
func (m *Manager) key(path string) string {
	if m.prefix == "" {
		return path
	}
	return m.prefix + "/" + path
}
