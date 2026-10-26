package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/recovery"
)

// The sibling intent is durable before the journal directory can contain any
// payload. Only pre-install payload covered by this exact owner may be discarded.
type localBackupStageOwner struct {
	Version uint                  `json:"version"`
	DataDir string                `json:"data_dir"`
	Cluster string                `json:"cluster"`
	Node    string                `json:"node"`
	Local   bool                  `json:"local"`
	Binding recovery.LocalBinding `json:"binding"`
}

func (n *Node) localBackupStageOwnerPath() string {
	return filepath.Join(n.config.DataDir, ".local-backup-import-owner.json")
}

func (n *Node) localBackupStageOwnership(binding recovery.LocalBinding) (localBackupStageOwner, error) {
	dir, err := filepath.Abs(n.config.DataDir)
	if err != nil {
		return localBackupStageOwner{}, err
	}
	if !n.config.Local || binding.Bootstrap.ConfigID != n.loadClusterConfig().ConfigID || len(binding.Bootstrap.Members) != 1 || binding.Bootstrap.Members[0].ID != n.config.NodeID {
		return localBackupStageOwner{}, fmt.Errorf("Local import owner does not match configuration")
	}
	return localBackupStageOwner{Version: 1, DataDir: dir, Cluster: string(n.config.ClusterID), Node: string(n.config.NodeID), Local: true, Binding: binding}, nil
}

func (n *Node) readLocalBackupStageOwner() (*localBackupStageOwner, error) {
	name := n.localBackupStageOwnerPath()
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return nil, fmt.Errorf("invalid Local import ownership intent")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	var owner localBackupStageOwner
	if err := decodeLocalJSON(data, &owner); err != nil {
		return nil, err
	}
	expected, err := n.localBackupStageOwnership(owner.Binding)
	if err != nil || !reflect.DeepEqual(owner, expected) {
		return nil, fmt.Errorf("foreign Local import ownership intent")
	}
	return &owner, nil
}

func (n *Node) verifyLocalBackupStageOwner(owner *localBackupStageOwner) error {
	if n.bucket == nil {
		return fmt.Errorf("Local import owner requires remote binding verification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	archive, err := recovery.OpenLocalArchive(ctx, n.bucket, n.localBackupPrefix())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(owner.Binding, archive.Binding) {
		return fmt.Errorf("Local import ownership binding changed")
	}
	return nil
}

func (n *Node) removeLocalBackupStageOwner() error {
	if err := os.Remove(n.localBackupStageOwnerPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncLocalDirOS(n.config.DataDir)
}

// completeLocalBackupImport is called only after normal Open has verified the
// installed state and initialized backup successfully. Preserve the owner until
// deletion of committed recovery evidence is durable.
func (n *Node) completeLocalBackupImport() error {
	if err := os.RemoveAll(filepath.Join(n.config.DataDir, ".local-backup-import")); err != nil {
		return err
	}
	if err := syncLocalDirOS(n.config.DataDir); err != nil {
		return err
	}
	return n.removeLocalBackupStageOwner()
}

func (n *Node) discardLocalBackupPreinstall() error {
	journal := filepath.Join(n.config.DataDir, ".local-backup-import")
	owner, err := n.readLocalBackupStageOwner()
	if err != nil {
		return err
	}
	info, stageErr := os.Lstat(journal)
	if owner == nil {
		if errors.Is(stageErr, os.ErrNotExist) {
			return nil
		}
		if stageErr != nil {
			return stageErr
		}
		return fmt.Errorf("unowned Local import staging requires inspection")
	}
	if stageErr != nil && !errors.Is(stageErr, os.ErrNotExist) {
		return stageErr
	}
	if stageErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid Local import staging directory")
		}
		entries, err := os.ReadDir(journal)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "next" && !(strings.HasPrefix(entry.Name(), ".local-policy-") && entry.Type().IsRegular()) {
				return fmt.Errorf("unexpected pre-install Local import evidence %q", entry.Name())
			}
		}
	}
	if err := n.verifyLocalBackupStageOwner(owner); err != nil {
		return err
	}
	// Keep the durable owner until removal of its payload is itself durable.
	if stageErr == nil {
		if err := os.RemoveAll(journal); err != nil {
			return err
		}
		if err := syncLocalDirOS(n.config.DataDir); err != nil {
			return err
		}
	}
	return n.removeLocalBackupStageOwner()
}

func (n *Node) beginLocalBackupStage(binding recovery.LocalBinding) error {
	if err := n.discardLocalBackupPreinstall(); err != nil {
		return err
	}
	owner, err := n.localBackupStageOwnership(binding)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	if err := atomicLocalFile(n.config.DataDir, n.localBackupStageOwnerPath(), encoded); err != nil {
		return err
	}
	journal := filepath.Join(n.config.DataDir, ".local-backup-import")
	if err := os.Mkdir(journal, 0700); err != nil {
		return err
	}
	if err := syncLocalDirOS(n.config.DataDir); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(journal, "next"), 0700); err != nil {
		return err
	}
	return syncLocalDirOS(journal)
}
