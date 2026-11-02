package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/ncruces/go-sqlite3/driver"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

const baseline = "04a6006e9c29cf9a652397f209896e79984ce344"

type fixture struct {
	Identity  recovery.PreparedStartupIdentity
	Bootstrap quepaxa.Cluster
	Prefix    string
	Tip       uint64
	Window    uint64
}

type observedBucket struct {
	objstore.Bucket
	reads, writes atomic.Int64
}

func (b *observedBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.reads.Add(1)
	return b.Bucket.Get(ctx, name)
}
func (b *observedBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.reads.Add(1)
	return b.Bucket.GetRange(ctx, name, off, length)
}
func (b *observedBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	b.reads.Add(1)
	return b.Bucket.Attributes(ctx, name)
}
func (b *observedBucket) Exists(ctx context.Context, name string) (bool, error) {
	b.reads.Add(1)
	return b.Bucket.Exists(ctx, name)
}
func (b *observedBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	b.reads.Add(1)
	return b.Bucket.Iter(ctx, dir, f, options...)
}
func (b *observedBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	b.reads.Add(1)
	return b.Bucket.IterWithAttributes(ctx, dir, f, options...)
}
func (b *observedBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	b.writes.Add(1)
	return errors.New("qualification source is read-only")
}
func (b *observedBucket) Delete(context.Context, string) error {
	b.writes.Add(1)
	return errors.New("qualification source is read-only")
}

func inspect(path string) (policy string, window, tip, rows uint64, err error) {
	db, err := driver.Open("file:" + filepath.ToSlash(path) + "?mode=ro&immutable=1")
	if err != nil {
		return "", 0, 0, 0, err
	}
	defer db.Close()
	err = db.QueryRow(`SELECT (SELECT value FROM _rhiza_meta WHERE key='sql_execution_policy'),(SELECT value FROM _rhiza_meta WHERE key='sql_idempotency_window'),(SELECT value FROM _rhiza_meta WHERE key='applied_slot'),(SELECT COUNT(*) FROM records)`).Scan(&policy, &window, &tip, &rows)
	return
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("expected owned fixture directory")
	}
	root := os.Args[1]
	raw, err := os.ReadFile(filepath.Join(root, "fixture.json"))
	if err != nil {
		return err
	}
	var f fixture
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&f); err != nil {
		return err
	}
	if f.Window != 1024 || f.Tip == 0 {
		return errors.New("unexpected counterexample fixture")
	}
	sourcePolicy, sourceWindow, sourceTip, sourceRows, err := inspect(filepath.Join(root, "handoff-v2", "sqlite.db"))
	if err != nil || sourcePolicy != "8" || sourceWindow != f.Window || sourceTip != f.Tip || sourceRows != 1 {
		return fmt.Errorf("invalid actual mixed source: policy=%s window=%d tip=%d rows=%d error=%v", sourcePolicy, sourceWindow, sourceTip, sourceRows, err)
	}
	bucket, err := filesystem.NewBucket(filepath.Join(root, "bucket"))
	if err != nil {
		return err
	}
	defer bucket.Close()
	observed := &observedBucket{Bucket: bucket}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	v1Target := filepath.Join(root, "old-v1-target")
	// This is the real baseline public loader, linked through the launcher's
	// exact git-archive replacement. No copied decoder or new facade is used.
	prepared, err := recovery.LoadPreparedStartup(ctx, observed, f.Prefix, 1, f.Bootstrap, filepath.Join(root, "handoff-v1"), v1Target, f.Identity)
	if err != nil {
		return fmt.Errorf("baseline Version1 counterexample did not reconstruct: %w", err)
	}
	policy, window, tip, rows, err := inspect(filepath.Join(prepared.Directory(), "sqlite.db"))
	if err != nil || policy != "7" || window != 65536 || tip != f.Tip || rows != 1 || observed.reads.Load() == 0 || observed.writes.Load() != 0 {
		return fmt.Errorf("baseline counterexample invalid: policy=%s window=%d tip=%d rows=%d reads=%d writes=%d error=%v", policy, window, tip, rows, observed.reads.Load(), observed.writes.Load(), err)
	}
	v1Reads := observed.reads.Load()
	observed.reads.Store(0)
	observed.writes.Store(0)
	v2Target := filepath.Join(root, "old-v2-target")
	_, err = recovery.LoadPreparedStartup(ctx, observed, f.Prefix, 1, f.Bootstrap, filepath.Join(root, "handoff-v2"), v2Target, f.Identity)
	if err == nil || !strings.Contains(err.Error(), "preparation identity mismatch") || observed.reads.Load() != 0 || observed.writes.Load() != 0 {
		return fmt.Errorf("baseline Version2 guard failed: reads=%d writes=%d error=%v", observed.reads.Load(), observed.writes.Load(), err)
	}
	if _, err := os.Lstat(v2Target); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("Version2 created a target: %v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, "handoff-v2", "completed.json"))
	if err != nil {
		return err
	}
	result := struct {
		Baseline       string `json:"baseline"`
		SourcePolicy   string `json:"source_policy"`
		SourceWindow   uint64 `json:"source_window"`
		RebuiltPolicy  string `json:"v1_rebuilt_policy"`
		RebuiltWindow  uint64 `json:"v1_rebuilt_window"`
		Tip            uint64 `json:"tip"`
		V1SourceReads  int64  `json:"v1_source_reads"`
		V2SourceReads  int64  `json:"v2_source_reads"`
		V2TargetAbsent bool   `json:"v2_target_absent"`
		ManifestSHA    string `json:"manifest_sha256"`
	}{baseline, sourcePolicy, sourceWindow, policy, window, tip, v1Reads, observed.reads.Load(), true, fmt.Sprintf("%x", sha256.Sum256(manifest))}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
