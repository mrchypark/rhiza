package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func haPrepareView(t *testing.T) (*largeProofFixture, *haReadOnlyRecordingBucket, *ReadOnlyView) {
	t.Helper()
	f := newLargeProofFixture(t, 64, "prepare-readonly", false)
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "prepare-suffix", Cypher: `CREATE (:Item {id: 'prepare-suffix'})`, Events: []types.GraphStreamEvent{{Stream: "readonly", Kind: "created", Payload: "prepare-suffix"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.core.Propose(f.ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := f.archive.SyncThrough(f.ctx, f.core, f.core.Tip()); err != nil {
		t.Fatal(err)
	}
	b := &haReadOnlyRecordingBucket{Bucket: f.bucket}
	v, err := OpenReadOnlyView(f.ctx, b, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	return f, b, v
}

func TestHAPrepareReadOnlyRestoreMaterializesCertifiedView(t *testing.T) {
	f, b, v := haPrepareView(t)
	target := filepath.Join(t.TempDir(), "restore")
	result, err := PrepareReadOnlyRestore(f.ctx, v, target)
	if err != nil {
		t.Fatal(err)
	}
	if result.TargetDir != target || result.HeadHash != v.HeadHash() || result.Base != v.Base() || result.Tip != v.Tip() || result.PrefixHash != v.PrefixHash() {
		t.Fatalf("incorrect prepared identity: %+v", result)
	}
	if filepath.Dir(result.SQLitePath) != target || filepath.Dir(result.SuffixPath) != target {
		t.Fatalf("paths outside target: %+v", result)
	}
	data, err := os.ReadFile(result.SuffixPath)
	if err != nil {
		t.Fatal(err)
	}
	var suffix []quepaxa.DecidedValue
	if err := json.Unmarshal(data, &suffix); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(suffix, v.Decisions()) {
		t.Fatal("certified suffix differs from verified view")
	}
	state, err := materializer.Open(result.SQLitePath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if state.Tip() != uint64(v.Tip()) {
		t.Fatalf("tip = %d, want %d", state.Tip(), v.Tip())
	}
	for _, id := range []string{"large-proof-checkpoint", "prepare-suffix"} {
		receipt, found, err := state.GraphMutationReceipt(f.ctx, id)
		if err != nil || !found {
			t.Fatalf("receipt %s: found=%v err=%v", id, found, err)
		}
		if id == "prepare-suffix" && (receipt.Status != types.MutationCommitted || !receipt.Applied) {
			t.Fatalf("suffix receipt not committed: %+v", receipt)
		}
	}
	for _, stream := range []struct{ name, payload string }{{"readonly", "prepare-suffix"}} {
		records, _, err := state.GraphReadStream(f.ctx, stream.name, 0, 10, 0)
		if err != nil || len(records) != 1 || records[0].Payload != stream.payload {
			t.Errorf("stream %s: %+v, %v", stream.name, records, err)
		}
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "wal" || strings.Contains(strings.ToLower(entry.Name()), "membership") {
			t.Fatalf("unexpected authority or startup artifact: %s", entry.Name())
		}
	}
	if b.mutations != 0 {
		t.Fatalf("source mutations: %d", b.mutations)
	}
}

func TestHAPrepareReadOnlyRestoreExistingTargets(t *testing.T) {
	f, b, v := haPrepareView(t)
	parent := t.TempDir()
	for _, kind := range []string{"empty", "nonempty", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			target := filepath.Join(parent, kind)
			switch kind {
			case "empty":
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			case "nonempty":
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(target, "sentinel"), []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(parent, "nonempty"), target); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := PrepareReadOnlyRestore(f.ctx, v, target); !errors.Is(err, os.ErrExist) {
				t.Fatalf("existing target error = %v, want os.ErrExist", err)
			}
			if kind == "empty" {
				entries, err := os.ReadDir(target)
				if err != nil || len(entries) != 0 {
					t.Fatalf("empty target changed: %v, %v", entries, err)
				}
			}
			if kind == "symlink" {
				info, err := os.Lstat(target)
				if err != nil || info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("symlink changed: %v, %v", info, err)
				}
			}
			if kind != "empty" {
				data, err := os.ReadFile(filepath.Join(target, "sentinel"))
				if err != nil || string(data) != "keep" {
					t.Fatalf("existing data changed: %q %v", data, err)
				}
			}
		})
	}
	if b.mutations != 0 {
		t.Fatalf("source mutations: %d", b.mutations)
	}
}

func TestHAPrepareReadOnlyRestoreInvalidAndCanceled(t *testing.T) {
	f, b, v := haPrepareView(t)
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		view *ReadOnlyView
	}{
		{"nil", f.ctx, nil}, {"zero", f.ctx, &ReadOnlyView{}}, {"canceled", canceled, v},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "restore")
			result, err := PrepareReadOnlyRestore(tc.ctx, tc.view, target)
			if err == nil {
				t.Fatal("invalid preparation succeeded")
			}
			if tc.name == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled error = %v", err)
			}
			if tc.name != "canceled" && err.Error() != "read-only view is not verified" {
				t.Fatalf("invalid view error = %v", err)
			}
			if _, statErr := os.Lstat(target); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid request created target: %v", statErr)
			}
			if !reflect.DeepEqual(result, PreparedReadOnlyRestore{}) {
				t.Fatalf("failure returned artifact: %+v", result)
			}
		})
	}
	if b.mutations != 0 {
		t.Fatalf("source mutations: %d", b.mutations)
	}
}

type haPrepareFailBucket struct {
	*haReadOnlyRecordingBucket
	fail bool
}

func (b *haPrepareFailBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.fail {
		return nil, errors.New("injected download failure")
	}
	return b.haReadOnlyRecordingBucket.Get(ctx, name)
}

func TestHAPrepareReadOnlyRestoreDownloadFailures(t *testing.T) {
	for _, mode := range []string{"corrupt", "download"} {
		t.Run(mode, func(t *testing.T) {
			f := newLargeProofFixture(t, 64, "prepare-failure", false)
			b := &haPrepareFailBucket{haReadOnlyRecordingBucket: &haReadOnlyRecordingBucket{Bucket: f.bucket}}
			v, err := OpenReadOnlyView(f.ctx, b, f.prefix, 1, f.bootstrap)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "download" {
				b.fail = true
			} else {
				var key string
				if err := f.bucket.Iter(f.ctx, f.prefix+"/checkpoint/blocks/", func(name string) error {
					if key == "" {
						key = name
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if key == "" {
					t.Fatal("no checkpoint block")
				}
				data := haReadOnlyObject(t, f.bucket, f.ctx, key)
				data[0] ^= 0xff
				if err := f.bucket.Upload(f.ctx, key, bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}
			target := filepath.Join(t.TempDir(), "restore")
			result, err := PrepareReadOnlyRestore(f.ctx, v, target)
			if err == nil {
				t.Fatal("damaged download succeeded")
			}
			if !reflect.DeepEqual(result, PreparedReadOnlyRestore{}) {
				t.Fatalf("failure returned artifact: %+v", result)
			}
			if b.mutations != 0 {
				t.Fatalf("source mutations: %d", b.mutations)
			}
		})
	}
}

func TestHAPrepareReadOnlyRestoreConcurrent(t *testing.T) {
	f := newLargeProofFixture(t, 64, "prepare-concurrent", false)
	v, err := OpenReadOnlyView(f.ctx, f.bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _, err := PrepareReadOnlyRestore(f.ctx, v, target); results <- err }()
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("successful preparations = %d, want 1", wins)
	}
}
