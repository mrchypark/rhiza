package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// PreparedReadOnlyRestore describes local data, not authority to start serving.
type PreparedReadOnlyRestore struct {
	TargetDir  string
	SQLitePath string
	SuffixPath string
	HeadHash   [32]byte
	Base       quepaxa.Slot
	Tip        quepaxa.Slot
	PrefixHash [32]byte
}

// PrepareReadOnlyRestore claims a new target directory and materializes a
// verified view. On failure, the claimed directory and partial data remain;
// callers must not treat their presence as evidence of success.
// The caller must control the parent and prevent directory replacement during
// preparation. The result is data, not a consensus WAL or admission authority.
func PrepareReadOnlyRestore(ctx context.Context, view *ReadOnlyView, targetDir string) (result PreparedReadOnlyRestore, resultErr error) {
	if view == nil || !view.verified {
		return result, fmt.Errorf("read-only view is not verified")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := os.Mkdir(targetDir, 0o700); err != nil {
		return result, fmt.Errorf("claim restore directory: %w", err)
	}
	checkpointDir := filepath.Join(targetDir, "checkpoint")
	if err := os.Mkdir(checkpointDir, 0o700); err != nil {
		return result, fmt.Errorf("create checkpoint directory: %w", err)
	}
	files, err := view.DownloadCheckpointFiles(ctx, checkpointDir)
	if err != nil {
		return result, fmt.Errorf("download checkpoint: %w", err)
	}
	sqlitePath := filepath.Join(targetDir, "sqlite.db")
	mat, err := materializer.Open(sqlitePath, 1)
	if err != nil {
		return result, fmt.Errorf("open materializer: %w", err)
	}
	defer func() {
		if mat != nil {
			resultErr = errors.Join(resultErr, mat.Close())
		}
	}()
	if len(files) > 0 {
		restore := make([]materializer.CheckpointFile, 0, len(files))
		for _, file := range files {
			restore = append(restore, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
		}
		if err := mat.RestoreCheckpoint(ctx, restore); err != nil {
			return result, fmt.Errorf("restore checkpoint: %w", err)
		}
	}
	if mat.Tip() != uint64(view.base) {
		return result, fmt.Errorf("checkpoint tip %d does not match base %d", mat.Tip(), view.base)
	}
	const batchSize = 256
	for start := 0; start < len(view.decisions); start += batchSize {
		end := min(start+batchSize, len(view.decisions))
		if err := mat.ApplyBatch(ctx, view.decisions[start:end]); err != nil {
			return result, fmt.Errorf("apply certified suffix: %w", err)
		}
	}
	if mat.Tip() != uint64(view.tip) {
		return result, fmt.Errorf("materialized tip %d does not match captured tip %d", mat.Tip(), view.tip)
	}
	if err := mat.Close(); err != nil {
		mat = nil
		return result, fmt.Errorf("close materializer: %w", err)
	}
	mat = nil
	if err := ctx.Err(); err != nil {
		return result, err
	}
	data, err := json.Marshal(view.decisions)
	if err != nil {
		return result, fmt.Errorf("encode certified suffix: %w", err)
	}
	suffixPath := filepath.Join(targetDir, "certified-suffix.json")
	if err := os.WriteFile(suffixPath, data, 0o600); err != nil {
		return result, fmt.Errorf("write certified suffix: %w", err)
	}
	return PreparedReadOnlyRestore{TargetDir: targetDir, SQLitePath: sqlitePath, SuffixPath: suffixPath, HeadHash: view.headHash, Base: view.base, Tip: view.tip, PrefixHash: view.prefixHash}, nil
}
