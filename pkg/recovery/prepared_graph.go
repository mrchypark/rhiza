package recovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const maxPreparedGraphBytes int64 = 32 << 30

type preparedGraphArtifact struct {
	Role   string   `json:"role"`
	Path   string   `json:"path"`
	Size   int64    `json:"size"`
	SHA256 [32]byte `json:"sha256"`
}

func equalGraphArtifacts(left, right []preparedGraphArtifact) bool {
	if len(left) < 2 || len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// v4 describes exact final artifacts, not a canonical live Graph export.
// The caller owns the parent and excludes source replacement for the operation.
func preparedGraphArtifacts(ctx context.Context, root string, sync bool) ([]preparedGraphArtifact, error) {
	for _, dir := range []string{root, filepath.Join(root, "latticedb")} {
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("invalid prepared Graph parent")
		}
	}
	out := []preparedGraphArtifact{{Role: "sqlite", Path: "sqlite.db"}}
	entries, err := os.ReadDir(filepath.Join(root, "latticedb"))
	if err != nil {
		return nil, err
	}
	seenGraph := false
	for _, entry := range entries {
		if !preparedGraphNativeName(entry.Name()) {
			return nil, fmt.Errorf("unsupported prepared native artifact %q", entry.Name())
		}
		if entry.Name() == "graph.ltdb" {
			seenGraph = true
		}
		out = append(out, preparedGraphArtifact{Role: "graph-data", Path: "latticedb/" + entry.Name()})
	}
	if !seenGraph {
		return nil, fmt.Errorf("prepared native base artifact missing")
	}
	var total int64
	for i := range out {
		size, hash, err := preparedGraphFileHash(ctx, filepath.Join(root, filepath.FromSlash(out[i].Path)), maxPreparedGraphBytes-total, sync)
		if err != nil || (size == 0 && out[i].Path != "latticedb/graph.ltdb.lock" && out[i].Path != "latticedb/graph.ltdb-wal") {
			if err == nil {
				err = fmt.Errorf("empty prepared data artifact")
			}
			return nil, err
		}
		out[i].Size = size
		out[i].SHA256 = hash
		total += size
	}
	if sync {
		dir, err := os.Open(filepath.Join(root, "latticedb"))
		if err != nil {
			return nil, err
		}
		if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
			return nil, err
		}
	}
	suffix, err := os.Lstat(filepath.Join(root, "certified-suffix.json"))
	if err != nil || !suffix.Mode().IsRegular() || suffix.Size() < 0 || suffix.Size() > maxPreparedGraphBytes-total {
		return nil, fmt.Errorf("prepared Graph artifacts exceed envelope")
	}
	if total <= 0 || total > maxPreparedGraphBytes {
		return nil, fmt.Errorf("prepared Graph pair exceeds artifact envelope")
	}
	return out, nil
}

func preparedGraphFileHash(ctx context.Context, name string, limit int64, sync bool) (size int64, sum [32]byte, resultErr error) {
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || limit < 0 || info.Size() > limit {
		return 0, sum, fmt.Errorf("invalid prepared Graph artifact")
	}
	file, err := os.Open(name)
	if err != nil {
		return 0, sum, err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	before, err := file.Stat()
	if err != nil || !os.SameFile(info, before) {
		return 0, sum, fmt.Errorf("prepared Graph artifact changed")
	}
	hash := sha256.New()
	size, err = io.Copy(hash, io.LimitReader(preparedGraphReader{ctx, file}, limit+1))
	if err != nil {
		return 0, sum, err
	}
	after, err := file.Stat()
	if err != nil || size != before.Size() || size > limit || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return 0, sum, fmt.Errorf("prepared Graph artifact changed during hashing")
	}
	current, err := os.Lstat(name)
	if err != nil || !os.SameFile(before, current) || current.Size() != before.Size() {
		return 0, sum, fmt.Errorf("prepared Graph artifact replaced")
	}
	if sync {
		if err := file.Sync(); err != nil {
			return 0, sum, err
		}
	}
	copy(sum[:], hash.Sum(nil))
	return size, sum, nil
}

func validateGraphPreparedInventory(root string, completed bool) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("invalid Graph preparation directory")
	}
	allowed := map[string]bool{"sqlite.db": false, "certified-suffix.json": false, "completed.json": false, "completion.pending": false, "checkpoint": true, "latticedb": true, "latticedb/graph.ltdb": false, "checkpoint/sqlite.db": false, "checkpoint/graph-data.db": false}
	seen := map[string]bool{}
	err = filepath.WalkDir(root, func(name string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == root {
			return nil
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		directory, ok := allowed[rel]
		if filepath.ToSlash(filepath.Dir(rel)) == "latticedb" && preparedGraphNativeName(filepath.Base(rel)) {
			ok = true
			directory = false
		}
		if rel == "checkpoint/sqlite.db-wal" || rel == "checkpoint/sqlite.db-shm" {
			ok = true
			directory = false
		}
		if !ok {
			return fmt.Errorf("unsupported Graph preparation artifact %q", rel)
		}
		info, err := os.Lstat(name)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || directory != info.IsDir() || !directory && !info.Mode().IsRegular() {
			return fmt.Errorf("invalid Graph preparation artifact")
		}
		if rel == "checkpoint/sqlite.db-wal" && info.Size() != 0 {
			return fmt.Errorf("checkpoint cache has unexpected WAL content")
		}
		seen[rel] = true
		return nil
	})
	if err != nil {
		return err
	}
	for _, name := range []string{"sqlite.db", "certified-suffix.json", "checkpoint", "latticedb", "latticedb/graph.ltdb"} {
		if !seen[name] {
			return fmt.Errorf("missing Graph preparation artifact")
		}
	}
	if seen["checkpoint/sqlite.db"] != seen["checkpoint/graph-data.db"] {
		return fmt.Errorf("incomplete Graph checkpoint pair")
	}
	if completed && !seen["completed.json"] {
		return fmt.Errorf("Graph completion marker missing")
	}
	return nil
}

// These are opaque files produced by the pinned native engine in flat mode.
// They remain part of the graph-data role; none is parsed as a logical export.
func preparedGraphNativeName(name string) bool {
	switch name {
	case "graph.ltdb", "graph.ltdb-ids", "graph.ltdb-wal", "graph.ltdb-wal.base", "graph.ltdb.layout", "graph.ltdb.lock", "graph.ltdb.pages", "graph.ltdb.pages.layout":
		return true
	default:
		return false
	}
}

type preparedGraphReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r preparedGraphReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
