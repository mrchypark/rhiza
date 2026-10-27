package objstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	thanosobjstore "github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"golang.org/x/sys/unix"
)

func TestLocalFilesystemOwnerSerializesAndReopens(t *testing.T) {
	base := t.TempDir()
	root, data := filepath.Join(base, "objects"), filepath.Join(base, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	otherData := filepath.Join(base, "other-data")
	if err := os.Mkdir(otherData, 0700); err != nil {
		t.Fatal(err)
	}
	first, err := AcquireLocalFilesystemOwner(root, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLocalFilesystemOwner(root, otherData); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("second owner error = %v, want flock would-block", err)
	}
	alias := filepath.Join(base, "objects-alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLocalFilesystemOwner(alias, otherData); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("symlink-alias owner error = %v, want flock would-block", err)
	}
	if err := first.Close(time.Second); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireLocalFilesystemOwner(root, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestLocalFilesystemOwnerRetainsUncertainDescriptor(t *testing.T) {
	base := t.TempDir()
	root, data := filepath.Join(base, "objects"), filepath.Join(base, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	owner, err := AcquireLocalFilesystemOwner(root, data)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := filesystem.NewBucket(root)
	if err != nil {
		t.Fatal(err)
	}
	blocked := &blockingUploadReader{entered: make(chan struct{}), release: make(chan struct{})}
	callDone := make(chan error, 1)
	go func() { callDone <- owner.Wrap(bucket).Upload(context.Background(), "object", blocked) }()
	<-blocked.entered
	if err := owner.Close(5 * time.Millisecond); err == nil {
		t.Fatal("owner closed with an active filesystem call")
	}
	close(blocked.release)
	if err := <-callDone; err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLocalFilesystemOwner(root, data); err == nil {
		t.Fatal("poisoned owner released its lock")
	}
}

func TestLocalFilesystemOwnerDrainsBlockedReaderBeforeReopen(t *testing.T) {
	base := t.TempDir()
	root, data := filepath.Join(base, "objects"), filepath.Join(base, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	owner, err := AcquireLocalFilesystemOwner(root, data)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := filesystem.NewBucket(root)
	if err != nil {
		t.Fatal(err)
	}
	underlying := &blockedOwnerReadCloser{
		entered: make(chan struct{}), release: make(chan struct{}), readDone: make(chan struct{}),
	}
	reader, err := owner.Wrap(&blockingGetBucket{Bucket: bucket, reader: underlying}).Get(context.Background(), "blocked")
	if err != nil {
		t.Fatal(err)
	}
	readResult := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		readResult <- err
	}()
	<-underlying.entered

	ownerClose := make(chan error, 1)
	go func() { ownerClose <- owner.Close(3 * time.Second) }()
	waitOwnerClosing(t, owner)
	readerClose := make(chan error, 1)
	go func() { readerClose <- reader.Close() }()
	select {
	case err := <-readResult:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("blocked read returned %v, want EOF", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("underlying read did not finish after reader Close")
	}
	if err := <-readerClose; err != nil {
		t.Fatal(err)
	}
	if err := <-ownerClose; err != nil {
		t.Fatal(err)
	}

	neighbor, err := AcquireLocalFilesystemOwner(root, data)
	if err != nil {
		t.Fatalf("clean reader drain did not release ownership: %v", err)
	}
	if err := neighbor.Wrap(bucket).Upload(context.Background(), "neighbor", strings.NewReader("ok")); err != nil {
		t.Fatalf("neighbor filesystem operation failed: %v", err)
	}
	if err := neighbor.Close(time.Second); err != nil {
		t.Fatal(err)
	}
}

func waitOwnerClosing(t *testing.T, owner *LocalFilesystemOwner) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		owner.mu.Lock()
		closing := owner.closing
		owner.mu.Unlock()
		if closing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("owner Close did not stop admission")
}

type blockingGetBucket struct {
	thanosobjstore.Bucket
	reader io.ReadCloser
}

func (b *blockingGetBucket) Get(context.Context, string) (io.ReadCloser, error) {
	return b.reader, nil
}

type blockedOwnerReadCloser struct {
	entered  chan struct{}
	release  chan struct{}
	readDone chan struct{}
	once     sync.Once
}

func (r *blockedOwnerReadCloser) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	select {
	case <-r.readDone:
	default:
		close(r.readDone)
	}
	return 0, io.EOF
}

func (r *blockedOwnerReadCloser) Close() error {
	select {
	case <-r.release:
	default:
		close(r.release)
	}
	<-r.readDone
	return nil
}

type blockingUploadReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	done    bool
}

func (r *blockingUploadReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.once.Do(func() { close(r.entered) })
	<-r.release
	r.done = true
	p[0] = 'x'
	return 1, nil
}

func TestLocalFilesystemOwnerRejectsDataDirOverlap(t *testing.T) {
	base := t.TempDir()
	if _, err := AcquireLocalFilesystemOwner(filepath.Join(base, "objects"), base); err == nil {
		t.Fatal("archive root inside DataDir was accepted")
	}
}
