package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/thanos-io/objstore"
)

// CreateLocalFiles writes the existing portable snapshot format for an externally
// exclusive Local writer. It does not publish CURRENT or acquire a voter claim.
func (m *Manager) CreateLocalFiles(ctx context.Context, sources []Source, index uint64) (*Checkpoint, error) {
	var root *Checkpoint
	err := m.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		var err error
		worker := NewManager(LocalBucket(m.bucket), m.prefix, m.localDir, m.configID)
		root, err = worker.createLocalFiles(ctx, sources, index)
		return err
	})
	return root, err
}

func (m *Manager) createLocalFiles(ctx context.Context, sources []Source, index uint64) (*Checkpoint, error) {
	if index == 0 {
		return nil, fmt.Errorf("local checkpoint index is required")
	}
	if err := validateSources(sources); err != nil {
		return nil, err
	}
	for _, source := range sources {
		if source.Role == RoleSQLite {
			if err := sqlpolicy.CheckSnapshotFile(ctx, source.Path); err != nil {
				return nil, err
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	root := Checkpoint{SQLExecutionPolicy: sqlpolicy.Version, ConfigID: m.configID, Generation: 1, Index: index}
	blocks := 0
	for _, source := range sources {
		file, err := m.uploadFile(ctx, source, 1, true, nil, nil)
		if err != nil {
			return nil, err
		}
		blocks += len(file.Blocks)
		root.Size += file.Size
		root.Files = append(root.Files, file)
	}
	if blocks > maxBlocks || root.Size > maxCheckpointSize {
		return nil, fmt.Errorf("local checkpoint exceeds portable snapshot bounds")
	}
	root.Hash = stateHash(root.Files)
	data, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	if len(data) > maxRootSize {
		return nil, fmt.Errorf("local checkpoint root too large")
	}
	root.RootHash = sha256.Sum256(data)
	if err := m.bucket.Upload(ctx, m.key(rootName(index, root.RootHash)), bytes.NewReader(data), objstore.WithIfNotExists()); err != nil && !m.bucket.IsConditionNotMetErr(err) {
		return nil, err
	}
	// OpenRoot checks the actual persisted bytes, including conditional-write reuse.
	opened, err := m.readRoot(ctx, m.key(rootName(index, root.RootHash)), root.RootHash)
	if err != nil {
		return nil, err
	}
	return &opened, nil
}
