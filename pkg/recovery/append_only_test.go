package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/thanos-io/objstore"
)

type appendOnlyRecordingBucket struct {
	objstore.Bucket
	mutations int
	reads     int
}

func (b *appendOnlyRecordingBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.reads++
	return b.Bucket.Get(ctx, key)
}
func (b *appendOnlyRecordingBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	b.reads++
	return b.Bucket.GetRange(ctx, key, off, length)
}
func (b *appendOnlyRecordingBucket) Exists(ctx context.Context, key string) (bool, error) {
	b.reads++
	return b.Bucket.Exists(ctx, key)
}
func (b *appendOnlyRecordingBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	b.reads++
	return b.Bucket.Attributes(ctx, key)
}
func (b *appendOnlyRecordingBucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	b.reads++
	return b.Bucket.Iter(ctx, dir, f, opts...)
}

func (b *appendOnlyRecordingBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mutations++
	return b.Bucket.Upload(ctx, key, r, opts...)
}
func (b *appendOnlyRecordingBucket) Delete(ctx context.Context, key string) error {
	b.mutations++
	return b.Bucket.Delete(ctx, key)
}

func TestHAAppendOnlyArchive(t *testing.T) {
	ctx := context.Background()
	if _, err := NewAppendOnlyManager(nil, "ha", 1); err == nil {
		t.Fatal("nil bucket accepted")
	}
	unsupported := localCapabilityBucket{objstore.NewInMemBucket()}
	if _, err := NewAppendOnlyManager(unsupported, "ha", 1); err == nil {
		t.Fatal("missing IfMatch accepted")
	}
	b := &appendOnlyRecordingBucket{Bucket: objstore.NewInMemBucket()}
	m, err := NewAppendOnlyManager(b, "ha", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Cleanup(ctx, 0); err == nil || b.mutations != 0 || b.reads != 0 {
		t.Fatalf("GC err=%v mutations=%d reads=%d", err, b.mutations, b.reads)
	}
	data := []byte("extent")
	hash := sha256.Sum256(data)
	key := m.key(extentObjectKey(hash, 7))
	if err := b.Bucket.Upload(ctx, key, bytes.NewReader([]byte("corrupt"))); err != nil {
		t.Fatal(err)
	}
	if err := m.uploadExtent(ctx, hash, data, 7); err == nil {
		t.Fatal("corrupt extent collision accepted")
	}
	if got := haReadOnlyObject(t, b.Bucket, ctx, key); !bytes.Equal(got, []byte("corrupt")) {
		t.Fatalf("extent overwritten: %q", got)
	}
}

func TestHAAppendOnlyCapturedView(t *testing.T) {
	f := newLargeProofFixture(t, 64, "append-view", false)
	haReadOnlyAppend(t, f, "first")
	view, err := OpenReadOnlyView(f.ctx, f.bucket, f.prefix, 1, f.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := NewAppendOnlyManager(f.bucket, f.prefix, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if err := archive.Load(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.archive.SyncThrough(f.ctx, f.core, f.core.Tip()); err != nil {
		t.Fatal(err)
	}
	haReadOnlyAppend(t, f, "second")
	if err := archive.Cleanup(f.ctx, 0); err == nil {
		t.Fatal("HA GC accepted")
	}
	if _, err := view.DownloadCheckpointFiles(f.ctx, t.TempDir()); err != nil {
		t.Fatalf("captured checkpoint unreadable: %v", err)
	}
	if len(view.Decisions()) == 0 || view.Tip() == f.core.Tip() {
		t.Fatal("captured suffix changed after HEAD advanced")
	}
}
