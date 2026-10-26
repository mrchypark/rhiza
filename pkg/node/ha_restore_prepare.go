package node

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// PrepareHAInitRestore verifies the configured, existing HA archive and stages
// its selected native checkpoint into HAInitRestoreDir. It does not open a
// Node, DataDir, WAL, materializer, peer transport, or listener. The supplied
// bucket is wrapped so verifier or stager cleanup can never mutate remote
// storage, even if those implementations change later.
func PrepareHAInitRestore(ctx context.Context, config *types.ExecutionConfig, bucket objstore.Bucket) error {
	if config == nil || bucket == nil {
		return fmt.Errorf("HA init restore requires configuration and object storage")
	}
	if config.Local {
		return fmt.Errorf("HA init restore is unavailable in Local mode")
	}
	if config.ClusterID == "" || len(config.Members) == 0 {
		return fmt.Errorf("HA init restore requires the configured bootstrap cluster and membership")
	}
	if config.ObjStoreProvider == "" || config.HAInitRestoreDir == "" {
		return fmt.Errorf("HA init restore requires configured object storage and HAInitRestoreDir")
	}
	if config.DataDir == "" || initStageOverlapsDataDir(config.DataDir, config.HAInitRestoreDir) {
		return fmt.Errorf("HA init restore staging must be separate from DataDir")
	}

	prefix := path.Join(config.ObjStorePrefix, string(config.ClusterID))
	bootstrap := quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: append([]quepaxa.Member(nil), config.Members...)}
	membership := recovery.NewMembershipRecord(string(config.ClusterID), bootstrap.Members, string(config.ObjStoreDurability))
	readOnly := haRestoreReadOnlyBucket{Bucket: bucket}
	selection, err := recovery.VerifyHAArchiveForRestore(ctx, readOnly, prefix, bootstrap, membership)
	if err != nil {
		return fmt.Errorf("verify configured HA archive: %w", err)
	}
	if err := stageHACheckpointFiles(ctx, readOnly, selection, config.HAInitRestoreDir); err != nil {
		return fmt.Errorf("stage verified HA checkpoint: %w", err)
	}
	return nil
}

// initStageOverlapsDataDir resolves existing path ancestors to reject a stage
// path that aliases DataDir through a symlink. It reads filesystem metadata
// only; it does not open DataDir files or create anything in DataDir.
func initStageOverlapsDataDir(dataDir, stageDir string) bool {
	data, err := resolveInitFuturePath(dataDir)
	if err != nil {
		return true
	}
	stage, err := resolveInitFuturePath(stageDir)
	if err != nil {
		return true
	}
	return pathsOverlap(data, stage)
}

func resolveInitFuturePath(configured string) (string, error) {
	absolute, err := filepath.Abs(configured)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	ancestor := absolute
	for {
		if _, err := os.Lstat(ancestor); err == nil {
			resolved, err := filepath.EvalSymlinks(ancestor)
			if err != nil {
				return "", err
			}
			relative, err := filepath.Rel(ancestor, absolute)
			if err != nil {
				return "", err
			}
			if relative == "." {
				return filepath.Clean(resolved), nil
			}
			return filepath.Clean(filepath.Join(resolved, relative)), nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", fmt.Errorf("no existing ancestor for configured restore path")
		}
		ancestor = parent
	}
}

type haRestoreReadOnlyBucket struct{ objstore.Bucket }

func (haRestoreReadOnlyBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	return fmt.Errorf("HA restore init object storage is read-only")
}

func (haRestoreReadOnlyBucket) Delete(context.Context, string) error {
	return fmt.Errorf("HA restore init object storage is read-only")
}
