package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// PreparedStartupIdentity binds prepared data to caller-supplied identities;
// it does not prove authority, source ownership, or permission to start.
type PreparedStartupIdentity struct {
	ClusterUID    string `json:"clusterUID"`
	Generation    int64  `json:"generation"`
	DeploymentUID string `json:"deploymentUID"`
	PodUID        string `json:"podUID"`
	NodeID        string `json:"nodeID"`
	SourceBinding string `json:"sourceBinding"`
	ConfigDigest  string `json:"configDigest"`
}

type preparedStartupManifest struct {
	Version    int                     `json:"version"`
	Identity   PreparedStartupIdentity `json:"identity"`
	Head       []byte                  `json:"head"`
	SQLiteHash [32]byte                `json:"sqliteHash"`
	SuffixHash [32]byte                `json:"suffixHash"`
}

// VerifiedPreparedStartup is verified local data, never startup authority.
type VerifiedPreparedStartup struct {
	dir  string
	view *ReadOnlyView
}

func (p *VerifiedPreparedStartup) Directory() string   { return p.dir }
func (p *VerifiedPreparedStartup) View() *ReadOnlyView { return p.view }

func validPreparedIdentity(i PreparedStartupIdentity) bool {
	return i.ClusterUID != "" && i.Generation >= 0 && i.DeploymentUID != "" && i.PodUID != "" && i.NodeID != "" && i.SourceBinding != "" && i.ConfigDigest != ""
}

// PublishPreparedStartup prepares a new directory, then syncs data and writes a
// completion marker. The caller controls its parent and excludes replacement.
// A failed attempt remains in place. An error after marker publication is an
// uncertain outcome and must be resolved by loading and verifying the result.
func PublishPreparedStartup(ctx context.Context, view *ReadOnlyView, targetDir string, identity PreparedStartupIdentity) error {
	if !validPreparedIdentity(identity) {
		return fmt.Errorf("invalid prepared startup identity")
	}
	result, err := PrepareReadOnlyRestore(ctx, view, targetDir)
	if err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(targetDir))
	if err != nil {
		return fmt.Errorf("sync preparation parent: %w", err)
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return fmt.Errorf("sync preparation parent: %w", err)
	}
	m := preparedStartupManifest{Version: 1, Identity: identity, Head: view.CapturedHead()}
	if m.SQLiteHash, err = preparedFileHash(result.SQLitePath, true); err != nil {
		return err
	}
	if m.SuffixHash, err = preparedFileHash(result.SuffixPath, true); err != nil {
		return err
	}
	if _, err = preparedGraphTree(targetDir, true); err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(targetDir, "completion.pending"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Link(filepath.Join(targetDir, "completion.pending"), filepath.Join(targetDir, "completed.json")); err != nil {
		return err
	}
	dir, err := os.Open(targetDir)
	if err != nil {
		return fmt.Errorf("completion publication outcome unknown: %w", err)
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err != nil {
		return fmt.Errorf("completion publication outcome unknown: %w", errors.Join(err, closeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("completion publication outcome unknown: %w", closeErr)
	}
	return nil
}

// LoadPreparedStartup verifies the captured history and integrity of prepared
// files. It requires a caller-controlled directory, including against races;
// it does not authenticate a mutable manifest or grant permission to write.
func LoadPreparedStartup(ctx context.Context, bucket objstore.Bucket, prefix string, configID uint, bootstrap quepaxa.Cluster, targetDir string, expected PreparedStartupIdentity) (result *VerifiedPreparedStartup, resultErr error) {
	if !validPreparedIdentity(expected) {
		return nil, fmt.Errorf("invalid expected identity")
	}
	info, err := os.Lstat(targetDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("invalid preparation directory")
	}
	marker := filepath.Join(targetDir, "completed.json")
	data, err := readPreparedMarker(marker)
	if err != nil {
		return nil, err
	}
	if len(data) > maxArchiveHeadSize*2 {
		return nil, fmt.Errorf("preparation marker too large")
	}
	var m preparedStartupManifest
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		return nil, err
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("invalid preparation marker")
	}
	if m.Version != 1 || m.Identity != expected {
		return nil, fmt.Errorf("preparation identity mismatch")
	}
	view, err := OpenReadOnlyViewAtHead(ctx, bucket, prefix, configID, bootstrap, m.Head)
	if err != nil {
		return nil, err
	}
	sqlHash, err := preparedFileHash(filepath.Join(targetDir, "sqlite.db"), false)
	if err != nil || sqlHash != m.SQLiteHash {
		return nil, fmt.Errorf("prepared SQLite integrity mismatch")
	}
	verifyDir, err := os.MkdirTemp("", "rhiza-prepared-verify-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := os.RemoveAll(verifyDir); err != nil {
			result = nil
			resultErr = errors.Join(resultErr, fmt.Errorf("remove verification directory %s: %w", verifyDir, err))
		}
	}()
	verified, err := PrepareReadOnlyRestore(ctx, view, filepath.Join(verifyDir, "data"))
	if err != nil {
		return nil, fmt.Errorf("reconstruct prepared state: %w", err)
	}
	verifiedHash, err := preparedFileHash(verified.SQLitePath, false)
	if err != nil || verifiedHash != sqlHash {
		return nil, fmt.Errorf("prepared SQLite differs from verified source")
	}
	graph, err := preparedGraphTree(targetDir, false)
	if err != nil {
		return nil, err
	}
	expectedGraph, err := preparedGraphTree(verified.TargetDir, false)
	if err != nil || !reflect.DeepEqual(graph, expectedGraph) {
		return nil, fmt.Errorf("prepared graph differs from verified source")
	}
	suffix := filepath.Join(targetDir, "certified-suffix.json")
	suffixHash, err := preparedFileHash(suffix, false)
	if err != nil || suffixHash != m.SuffixHash {
		return nil, fmt.Errorf("prepared suffix integrity mismatch")
	}
	want, err := json.Marshal(view.Decisions())
	if err != nil {
		return nil, err
	}
	got, err := os.ReadFile(suffix)
	if err != nil || !bytes.Equal(got, want) {
		return nil, fmt.Errorf("prepared suffix differs from verified history")
	}
	return &VerifiedPreparedStartup{dir: targetDir, view: view}, nil
}

func readPreparedMarker(name string) (data []byte, resultErr error) {
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("invalid preparation marker")
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("preparation marker changed")
	}
	limit := int64(maxArchiveHeadSize * 2)
	data, err = io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("preparation marker too large")
	}
	return data, nil
}

func preparedGraphTree(root string, sync bool) (map[string][32]byte, error) {
	graph := filepath.Join(root, "latticedb")
	result := make(map[string][32]byte)
	if _, err := os.Lstat(graph); errors.Is(err, os.ErrNotExist) {
		return result, nil
	} else if err != nil {
		return nil, err
	}
	var dirs []string
	err := filepath.WalkDir(graph, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid graph symlink")
		}
		if entry.IsDir() {
			dirs = append(dirs, name)
			return nil
		}
		rel, err := filepath.Rel(graph, name)
		if err != nil {
			return err
		}
		hash, err := preparedFileHash(name, sync)
		if err != nil {
			return err
		}
		result[rel] = hash
		return nil
	})
	if err != nil {
		return nil, err
	}
	if sync {
		for i := len(dirs) - 1; i >= 0; i-- {
			f, err := os.Open(dirs[i])
			if err != nil {
				return nil, err
			}
			syncErr := f.Sync()
			closeErr := f.Close()
			if err = errors.Join(syncErr, closeErr); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func preparedFileHash(name string, sync bool) (sum [32]byte, resultErr error) {
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<30 {
		return sum, fmt.Errorf("invalid prepared file")
	}
	f, err := os.Open(name)
	if err != nil {
		return sum, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return sum, fmt.Errorf("prepared file changed")
	}
	h := sha256.New()
	if _, err = io.Copy(h, io.LimitReader(f, 1<<30+1)); err != nil {
		return sum, err
	}
	if sync {
		if err = f.Sync(); err != nil {
			return sum, err
		}
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}
