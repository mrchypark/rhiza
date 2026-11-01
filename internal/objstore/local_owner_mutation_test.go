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
)

func mutationOwner(t *testing.T) (*LocalFilesystemOwner, thanosobjstore.Bucket) {
	t.Helper()
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
	t.Cleanup(func() {
		if err := owner.Close(time.Second); err != nil {
			t.Error(err)
		}
	})
	return owner, bucket
}

// Force the pinned provider's actual Delete between swap creation and xattr.Set.
func TestFilesystemProviderDeleteCanRemoveActiveSwap(t *testing.T) {
	_, bucket := mutationOwner(t)
	blocked := &blockingUploadReader{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- bucket.Upload(context.Background(), "parent/control", blocked) }()
	<-blocked.entered
	if err := bucket.Delete(context.Background(), "parent"); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	if err := <-done; !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upstream removed-swap error=%v", err)
	} else {
		t.Logf("actual upstream failure: %v", err)
	}
}

type observedMutationBucket struct {
	thanosobjstore.Bucket
	entered chan string
}

func (b *observedMutationBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...thanosobjstore.ObjectUploadOption) error {
	b.entered <- "upload:" + key
	return b.Bucket.Upload(ctx, key, r, opts...)
}
func (b *observedMutationBucket) Delete(ctx context.Context, key string) error {
	b.entered <- "delete:" + key
	return b.Bucket.Delete(ctx, key)
}

func TestLocalOwnerSerializesMutationLifetimeAcrossWrappers(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		name := "same-key-upload"
		if deletion {
			name = "ancestor-delete"
		}
		t.Run(name, func(t *testing.T) {
			owner, bucket := mutationOwner(t)
			observed := &observedMutationBucket{Bucket: bucket, entered: make(chan string, 4)}
			first, second := owner.Wrap(observed), owner.Wrap(observed)
			blocked := &blockingUploadReader{entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
			defer release()
			firstDone, secondDone := make(chan error, 1), make(chan error, 1)
			go func() { firstDone <- first.Upload(context.Background(), "parent/control", blocked) }()
			<-observed.entered
			<-blocked.entered
			go func() {
				if deletion {
					secondDone <- second.Delete(context.Background(), "parent")
				} else {
					secondDone <- second.Upload(context.Background(), "parent/control", strings.NewReader("second"))
				}
			}()
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for {
				owner.mu.Lock()
				active := owner.active
				owner.mu.Unlock()
				if active == 2 {
					break
				}
				select {
				case call := <-observed.entered:
					release()
					firstErr := <-firstDone
					secondErr := <-secondDone
					t.Fatalf("overlapping provider mutation %s: first=%v second=%v", call, firstErr, secondErr)
				case <-deadline.C:
					t.Fatal("second mutation did not enter owner")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			select {
			case call := <-observed.entered:
				release()
				firstErr := <-firstDone
				secondErr := <-secondDone
				t.Fatalf("overlapping provider mutation %s: first=%v second=%v", call, firstErr, secondErr)
			default:
			}
			release()
			if err := <-firstDone; err != nil {
				t.Fatal(err)
			}
			if err := <-secondDone; err != nil {
				t.Fatal(err)
			}
			if !deletion {
				r, err := second.Get(context.Background(), "parent/control")
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(r)
				closeErr := r.Close()
				if err != nil || closeErr != nil || string(body) != "second" {
					t.Fatalf("final object=%q read=%v close=%v", body, err, closeErr)
				}
			}
		})
	}
}

func TestLocalOwnerCancelledMutationWaitDoesNotCallOrPoison(t *testing.T) {
	owner, bucket := mutationOwner(t)
	observed := &observedMutationBucket{Bucket: bucket, entered: make(chan string, 4)}
	wrapped := owner.Wrap(observed)
	blocked := &blockingUploadReader{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- wrapped.Upload(context.Background(), "parent/first", blocked) }()
	<-blocked.entered
	<-observed.entered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan error, 1)
	go func() { waiting <- owner.Wrap(observed).Delete(ctx, "parent") }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		owner.mu.Lock()
		active := owner.active
		owner.mu.Unlock()
		if active == 2 {
			break
		}
		select {
		case err := <-waiting:
			close(blocked.release)
			<-done
			t.Fatalf("waiter completed before cancellation: %v", err)
		case <-deadline.C:
			close(blocked.release)
			<-done
			t.Fatal("mutation waiter was not admitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := <-waiting; !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled wait=%v", err)
	}
	select {
	case call := <-observed.entered:
		t.Errorf("cancelled waiter called provider: %s", call)
	default:
	}
	if !owner.Held() {
		t.Error("cancelled waiter poisoned owner")
	}
	close(blocked.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
