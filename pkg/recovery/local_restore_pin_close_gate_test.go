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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

type localViewUploadBarrier struct {
	objstore.Bucket
	mu        sync.Mutex
	key       string
	active    int
	blockNext bool
	started   chan struct{}
	release   chan struct{}
	overlap   bool
}

func (b *localViewUploadBarrier) arm(key string) {
	b.mu.Lock()
	b.key = key
	b.blockNext = true
	b.mu.Unlock()
}

func (b *localViewUploadBarrier) Upload(ctx context.Context, name string, reader io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	key := b.key
	b.mu.Unlock()
	if name != key {
		return b.Bucket.Upload(ctx, name, reader, opts...)
	}
	b.mu.Lock()
	if b.active != 0 {
		b.overlap = true
	}
	b.active++
	block := b.blockNext
	b.blockNext = false
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.active--
		b.mu.Unlock()
	}()
	if block {
		reader = &localViewUploadReader{Reader: reader, started: b.started, release: b.release}
	}
	return b.Bucket.Upload(ctx, name, reader, opts...)
}

type localViewUploadReader struct {
	io.Reader
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (r *localViewUploadReader) Read(p []byte) (int, error) {
	r.once.Do(func() {
		close(r.started)
		<-r.release
	})
	return r.Reader.Read(p)
}

func (b *localViewUploadBarrier) overlapped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.overlap
}

func TestLocalRestorePinCloseSerializesWithExpiredViewCleanup(t *testing.T) {
	ctx := context.Background()
	storageRoot := t.TempDir()
	base, err := filesystem.NewBucket(storageRoot)
	if err != nil {
		t.Fatal(err)
	}
	bucket := &localViewUploadBarrier{
		Bucket: base, started: make(chan struct{}), release: make(chan struct{}),
	}
	archive, err := CreateLocalArchive(ctx, bucket, "local", localTestBinding(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	pin, err := archive.PinView(ctx, "close-vs-expiry", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	orphanData := []byte("orphan extent")
	orphanHash := sha256.Sum256(orphanData)
	orphanKey := archive.key(fmt.Sprintf("extents/%x", orphanHash))
	if err := base.Upload(ctx, orphanKey, bytes.NewReader(orphanData)); err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(storageRoot, filepath.FromSlash(orphanKey))
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(orphanPath, old, old); err != nil {
		t.Fatal(err)
	}
	if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, err := base.Exists(ctx, orphanKey); err != nil || !exists {
		t.Fatalf("active restore view did not protect old orphan: exists=%v err=%v", exists, err)
	}
	record, version, err := archive.readLocalView(ctx, pin.key)
	if err != nil {
		t.Fatal(err)
	}
	record.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Upload(ctx, archive.key(pin.key), bytes.NewReader(data), objstore.WithIfMatch(version)); err != nil {
		t.Fatal(err)
	}
	bucket.arm(archive.key(pin.key))

	gcResult := make(chan error, 1)
	go func() { gcResult <- archive.GarbageCollect(ctx, 1, 24*time.Hour) }()
	select {
	case <-bucket.started:
	case <-time.After(3 * time.Second):
		close(bucket.release)
		t.Fatal("GC did not reach expired view-pin cleanup")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	closeErr := pin.Close(closeCtx)
	cancel()
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		close(bucket.release)
		t.Fatalf("Close while GC owns the gate = %v, want gate wait deadline", closeErr)
	}
	if bucket.overlapped() {
		close(bucket.release)
		t.Fatal("pin Close overlapped the GC expiry rewrite")
	}

	close(bucket.release)
	select {
	case err := <-gcResult:
		if err != nil {
			t.Fatalf("GC after release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GC did not finish after releasing upload barrier")
	}
	if err := pin.Close(ctx); err != nil {
		t.Fatalf("Close after GC release: %v", err)
	}
	if bucket.overlapped() {
		t.Fatal("same-key view-pin uploads overlapped")
	}
	closed, _, err := archive.readLocalView(ctx, pin.key)
	if err != nil || closed.LeaseUntilMS != 0 {
		t.Fatalf("closed view lease=%d err=%v", closed.LeaseUntilMS, err)
	}
	old = time.Now().Add(-time.Hour)
	if err := os.Chtimes(orphanPath, old, old); err != nil {
		t.Fatal(err)
	}
	if err := archive.GarbageCollect(ctx, 1, 0); err != nil {
		t.Fatalf("GC after restore close: %v", err)
	}
	if exists, err := base.Exists(ctx, orphanKey); err != nil || exists {
		t.Fatalf("closed restore view did not release old orphan: exists=%v err=%v", exists, err)
	}
}

func TestFilesystemBucketRejectsConcurrentSameKeyUploadAtSwap(t *testing.T) {
	ctx := context.Background()
	bucket, err := filesystem.NewBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- bucket.Upload(ctx, "local/view-pins/key.json", &localViewUploadReader{
			Reader: bytes.NewReader([]byte("first")), started: started, release: release,
		})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("first upload did not reach filesystem copy with its swap open")
	}
	if err := bucket.Upload(ctx, "local/view-pins/key.json", bytes.NewReader([]byte("second"))); err == nil || !strings.Contains(err.Error(), "creating swap file failed") {
		close(release)
		t.Fatalf("overlapping filesystem upload error=%v, want real swap-file exclusion", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first filesystem upload: %v", err)
	}
}
