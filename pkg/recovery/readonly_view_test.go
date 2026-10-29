package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// All object mutations, including conditional uploads, pass through these methods.
type haReadOnlyRecordingBucket struct {
	objstore.Bucket
	mutations   int
	extentReads int
}

func (b *haReadOnlyRecordingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.Contains(name, "/archive/blocks/") {
		b.extentReads++
	}
	return b.Bucket.Get(ctx, name)
}

func (b *haReadOnlyRecordingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if strings.Contains(name, "/archive/blocks/") {
		b.extentReads++
	}
	return b.Bucket.GetRange(ctx, name, off, length)
}

func TestHAReadOnlyViewRejectsOversizedHeadBeforeExtentReads(t *testing.T) {
	f := newLargeProofFixture(t, 64, "readonly-bound", false)
	haReadOnlyAppend(t, f, "bounded")
	bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
	if _, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap); err != nil {
		t.Fatalf("valid source: %v", err)
	}
	if bucket.extentReads == 0 {
		t.Fatal("valid control did not read an extent")
	}
	headName := f.prefix + "/archive/head.bin"
	head, err := decodeHead(haReadOnlyObject(t, f.bucket, f.ctx, headName))
	if err != nil {
		t.Fatal(err)
	}
	head.Tip = head.Base + maxReadOnlyDecisions + 1
	data, err := encodeHead(head)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.bucket.Upload(f.ctx, headName, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	bucket.extentReads = 0
	if _, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap); err == nil {
		t.Fatal("oversized head accepted")
	}
	if bucket.extentReads != 0 {
		t.Fatalf("oversized head caused %d extent reads", bucket.extentReads)
	}
	if bucket.mutations != 0 {
		t.Fatalf("source mutations: %d", bucket.mutations)
	}
}

func haReadOnlyAppend(t *testing.T, f *largeProofFixture, request string) {
	t.Helper()
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: request, Events: []types.GraphStreamEvent{{Stream: "readonly", Kind: "created", Payload: request}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.core.Propose(f.ctx, value); err != nil {
		t.Fatal(err)
	}
	if err := f.archive.SyncThrough(f.ctx, f.core, f.core.Tip()); err != nil {
		t.Fatal(err)
	}
}

func TestHAReadOnlyViewFixedHeadAndMaterialization(t *testing.T) {
	f := newLargeProofFixture(t, 64, "readonly-fixed", false)
	haReadOnlyAppend(t, f, "first")
	bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
	view, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	headBytes := haReadOnlyObject(t, f.bucket, f.ctx, f.prefix+"/archive/head.bin")
	if view.HeadHash() != sha256.Sum256(headBytes) || view.Base() != f.seal.Index || view.Tip() != f.core.Tip() {
		t.Fatalf("incorrect captured identity: base=%d tip=%d", view.Base(), view.Tip())
	}
	prefix, ok := f.core.PrefixHash(f.core.Tip())
	if !ok || view.PrefixHash() != prefix {
		t.Fatal("incorrect captured prefix")
	}
	original := view.Decisions()
	if len(original) == 0 {
		t.Fatal("missing certified suffix")
	}
	copyOf := view.Decisions()
	copyOf[0].Value[0] ^= 0xff
	copyOf[0].Certificate[0] ^= 0xff
	if !bytes.Equal(view.Decisions()[0].Value, original[0].Value) || !bytes.Equal(view.Decisions()[0].Certificate, original[0].Certificate) {
		t.Fatal("decisions share mutable storage")
	}
	haReadOnlyAppend(t, f, "second")
	if view.HeadHash() != sha256.Sum256(headBytes) || view.Tip() == f.core.Tip() || view.PrefixHash() != prefix || len(view.Decisions()) != len(original) {
		t.Fatal("view retargeted after HEAD advanced")
	}
	dir := t.TempDir()
	files, err := view.DownloadCheckpointFiles(f.ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "restored.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	restore := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		restore = append(restore, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if err := state.RestoreCheckpoint(f.ctx, restore); err != nil {
		t.Fatal(err)
	}
	if err := state.ApplyBatch(f.ctx, view.Decisions()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := state.GraphMutationReceipt(f.ctx, "first"); err != nil || !found {
		t.Fatalf("suffix not materialized: found=%v err=%v", found, err)
	}
	if bucket.mutations != 0 {
		t.Fatalf("source mutations: %d", bucket.mutations)
	}
}

func haReadOnlyObject(t *testing.T, bucket objstore.Bucket, ctx context.Context, name string) []byte {
	t.Helper()
	r, err := bucket.Get(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if closeErr := r.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestHAReadOnlyViewRejectsCorruptSuffix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*archiveHead, *Extent)
	}{
		{"certificate", func(_ *archiveHead, e *Extent) { e.Decisions[0].Certificate[0] ^= 0xff }},
		{"gap", func(h *archiveHead, e *Extent) {
			e.Start++
			e.End++
			for i := range e.Decisions {
				e.Decisions[i].Slot++
			}
			h.Tip++
		}},
		{"duplicate base slot", func(h *archiveHead, e *Extent) {
			e.Start--
			e.End--
			for i := range e.Decisions {
				e.Decisions[i].Slot--
			}
			h.Tip--
		}},
		{"prefix", func(_ *archiveHead, e *Extent) { e.StartPrefix[0] ^= 0xff }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLargeProofFixture(t, 64, "readonly-suffix", false)
			haReadOnlyAppend(t, f, "suffix")
			headName := f.prefix + "/archive/head.bin"
			head, err := decodeHead(haReadOnlyObject(t, f.bucket, f.ctx, headName))
			if err != nil {
				t.Fatal(err)
			}
			name := f.prefix + "/" + extentObjectKey(head.TailHash, head.TailObject)
			extent, err := decodeExtent(haReadOnlyObject(t, f.bucket, f.ctx, name))
			if err != nil {
				t.Fatal(err)
			}
			tc.change(&head, &extent)
			extent.EndPrefix = extent.StartPrefix
			for _, d := range extent.Decisions {
				extent.EndPrefix = quepaxa.AdvancePrefixHash(extent.EndPrefix, d.Slot, d.Hash)
			}
			data, err := encodeExtent(extent)
			if err != nil {
				t.Fatal(err)
			}
			head.TailHash = sha256.Sum256(data)
			if err := f.bucket.Upload(f.ctx, f.prefix+"/"+extentObjectKey(head.TailHash, head.TailObject), bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			data, err = encodeHead(head)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.bucket.Upload(f.ctx, headName, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
			if _, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap); err == nil {
				t.Fatal("corrupt suffix accepted")
			}
			if bucket.mutations != 0 {
				t.Fatalf("source mutations: %d", bucket.mutations)
			}
		})
	}
}

func TestHAReadOnlyViewRejectsChangedCheckpointBlock(t *testing.T) {
	f := newLargeProofFixture(t, 64, "readonly-block", false)
	var name string
	if err := f.bucket.Iter(f.ctx, f.prefix+"/checkpoint/blocks/", func(key string) error {
		if name == "" {
			name = key
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if name == "" {
		t.Fatal("fixture has no checkpoint block")
	}
	bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
	view, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	data := haReadOnlyObject(t, f.bucket, f.ctx, name)
	data[0] ^= 0xff
	if err := f.bucket.Upload(f.ctx, name, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if _, err := view.DownloadCheckpointFiles(f.ctx, t.TempDir()); err == nil {
		t.Fatal("changed checkpoint block accepted")
	}
	if _, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap); err == nil {
		t.Fatal("corrupt checkpoint block accepted at open")
	}
	if bucket.mutations != 0 {
		t.Fatalf("source mutations: %d", bucket.mutations)
	}
}

func (b *haReadOnlyRecordingBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	b.mutations++
	return errors.New("unexpected source upload")
}

func (b *haReadOnlyRecordingBucket) Delete(context.Context, string) error {
	b.mutations++
	return errors.New("unexpected source delete")
}

func TestHAReadOnlyViewConstructor(t *testing.T) {
	t.Run("valid certified source performs no mutations", func(t *testing.T) {
		f := newLargeProofFixture(t, 64, "readonly-valid", false)
		bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
		view, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap)
		if err != nil || view == nil {
			t.Fatalf("open certified source: view=%v err=%v", view, err)
		}
		if bucket.mutations != 0 {
			t.Fatalf("source mutations: %d", bucket.mutations)
		}
	})

	t.Run("missing head", func(t *testing.T) {
		f := newLargeProofFixture(t, 64, "readonly-missing", false)
		bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
		if _, err := OpenReadOnlyView(f.ctx, bucket, "missing-source", 1, f.bootstrap); err == nil {
			t.Fatal("missing HEAD accepted")
		}
		if bucket.mutations != 0 {
			t.Fatalf("source mutations: %d", bucket.mutations)
		}
	})

	t.Run("wrong configuration", func(t *testing.T) {
		f := newLargeProofFixture(t, 64, "readonly-config", false)
		bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
		if _, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 2, f.bootstrap); err == nil {
			t.Fatal("wrong configuration accepted")
		}
		if bucket.mutations != 0 {
			t.Fatalf("source mutations: %d", bucket.mutations)
		}
	})

	t.Run("wrong bootstrap membership", func(t *testing.T) {
		f := newLargeProofFixture(t, 64, "readonly-membership", false)
		bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
		wrong := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "other"}}}
		if _, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, wrong); err == nil {
			t.Fatal("wrong bootstrap membership accepted")
		}
		if bucket.mutations != 0 {
			t.Fatalf("source mutations: %d", bucket.mutations)
		}
	})

	for _, tc := range []struct {
		name    string
		corrupt func(*testing.T, *archiveHead)
	}{
		{name: "base certificate", corrupt: func(t *testing.T, head *archiveHead) {
			if head.BaseDecision == nil || len(head.BaseDecision.Certificate) == 0 {
				t.Fatal("fixture has no certified base")
			}
			head.BaseDecision.Certificate[0] ^= 0xff
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLargeProofFixture(t, 64, "readonly-corrupt", false)
			name := f.prefix + "/archive/head.bin"
			r, err := f.bucket.Get(f.ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(r)
			if closeErr := r.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				t.Fatal(err)
			}
			head, err := decodeHead(data)
			if err != nil {
				t.Fatal(err)
			}
			tc.corrupt(t, &head)
			data, err = encodeHead(head)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.bucket.Upload(f.ctx, name, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			bucket := &haReadOnlyRecordingBucket{Bucket: f.bucket}
			if _, err := OpenReadOnlyView(f.ctx, bucket, f.prefix, 1, f.bootstrap); err == nil {
				t.Fatal("corrupt base accepted")
			}
			if bucket.mutations != 0 {
				t.Fatalf("source mutations: %d", bucket.mutations)
			}
		})
	}
}
