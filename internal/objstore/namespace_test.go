package objstore

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/thanos-io/objstore"
)

type namespaceCountBucket struct {
	objstore.Bucket
	calls int
}

func (b *namespaceCountBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.calls++
	return b.Bucket.Upload(ctx, key, r, opts...)
}
func (b *namespaceCountBucket) Delete(ctx context.Context, key string) error {
	b.calls++
	return b.Bucket.Delete(ctx, key)
}
func (b *namespaceCountBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.calls++
	return b.Bucket.Get(ctx, key)
}
func (b *namespaceCountBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	b.calls++
	return b.Bucket.GetRange(ctx, key, off, length)
}
func (b *namespaceCountBucket) Exists(ctx context.Context, key string) (bool, error) {
	b.calls++
	return b.Bucket.Exists(ctx, key)
}
func (b *namespaceCountBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	b.calls++
	return b.Bucket.Attributes(ctx, key)
}
func (b *namespaceCountBucket) Iter(ctx context.Context, key string, f func(string) error, opts ...objstore.IterOption) error {
	b.calls++
	return b.Bucket.Iter(ctx, key, f, opts...)
}
func (b *namespaceCountBucket) IterWithAttributes(ctx context.Context, key string, f func(objstore.IterObjectAttributes) error, opts ...objstore.IterOption) error {
	b.calls++
	return b.Bucket.IterWithAttributes(ctx, key, f, opts...)
}

func TestManagedNamespaceRejectsForeignBeforeAdapter(t *testing.T) {
	ctx := context.Background()
	base := &namespaceCountBucket{Bucket: objstore.NewInMemBucket()}
	_, prefix, err := ManagedNamespacePrefixes("installed", "full-cr-uid", 0, "claim-1", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	b, err := BindNamespace(base, prefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"foreign/key", prefix + "-similar/key", prefix + "/../escape", prefix + "//key", prefix + "/./key", prefix + "/%2e%2e/key", prefix + "/\\escape"} {
		t.Run(key, func(t *testing.T) {
			if b.Upload(ctx, key, strings.NewReader("x"), objstore.WithIfNotExists()) == nil {
				t.Fatal("foreign upload accepted")
			}
			if b.Delete(ctx, key) == nil {
				t.Fatal("foreign delete accepted")
			}
			if _, e := b.Get(ctx, key); e == nil {
				t.Fatal("foreign get accepted")
			}
			if _, e := b.GetRange(ctx, key, 0, 1); e == nil {
				t.Fatal("foreign range accepted")
			}
			if _, e := b.Exists(ctx, key); e == nil {
				t.Fatal("foreign exists accepted")
			}
			if _, e := b.Attributes(ctx, key); e == nil {
				t.Fatal("foreign attributes accepted")
			}
			if b.Iter(ctx, key, func(string) error { return nil }) == nil {
				t.Fatal("foreign list accepted")
			}
			if b.IterWithAttributes(ctx, key, func(objstore.IterObjectAttributes) error { return nil }) == nil {
				t.Fatal("foreign attribute list accepted")
			}
		})
	}
	if base.calls != 0 {
		t.Fatalf("rejected names reached adapter %d times", base.calls)
	}
	if err := b.Upload(ctx, prefix+"/archive/head.bin", strings.NewReader("old"), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if base.calls != 1 {
		t.Fatal("valid neighbor did not reach adapter")
	}
	source, err := BindReadOnlyNamespace(base, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if source.Upload(ctx, prefix+"/archive/head.bin", strings.NewReader("forbidden")) == nil || source.Delete(ctx, prefix+"/archive/head.bin") == nil {
		t.Fatal("source handle admitted mutation")
	}
	if base.calls != 1 {
		t.Fatal("read-only source mutation reached adapter")
	}
}

type pausedNamespaceBucket struct {
	objstore.Bucket
	ready, release chan struct{}
	once           sync.Once
	mu             sync.Mutex
	keys           []string
}

func (b *pausedNamespaceBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.once.Do(func() { close(b.ready); <-b.release })
	b.mu.Lock()
	b.keys = append(b.keys, key)
	b.mu.Unlock()
	return b.Bucket.Upload(ctx, key, r, opts...)
}

func TestManagedNamespaceOldDelayedRequestAndRetryStayOld(t *testing.T) {
	_, oldPrefix, _ := ManagedNamespacePrefixes("root", "uid", 0, "assignment-old", "cluster")
	_, newPrefix, _ := ManagedNamespacePrefixes("root", "uid", 1, "assignment-new", "cluster")
	if oldPrefix == newPrefix || strings.HasPrefix(oldPrefix+"/", newPrefix+"/") {
		t.Fatal("generation namespaces overlap")
	}
	if _, _, err := ManagedNamespacePrefixes(oldPrefix, "uid", 1, "assignment-new", "cluster"); err == nil {
		t.Fatal("successor namespace nested inside old writable namespace")
	}
	base := &pausedNamespaceBucket{Bucket: objstore.NewInMemBucket(), ready: make(chan struct{}), release: make(chan struct{})}
	old, _ := BindNamespace(base, oldPrefix)
	successor, _ := BindNamespace(base, newPrefix)
	done := make(chan error, 1)
	go func() {
		done <- old.Upload(context.Background(), oldPrefix+"/archive/head.bin", strings.NewReader("delayed"))
	}()
	<-base.ready
	close(base.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := old.Upload(context.Background(), oldPrefix+"/archive/head.bin", strings.NewReader("retry")); err != nil {
		t.Fatal(err)
	}
	if err := old.Upload(context.Background(), newPrefix+"/archive/head.bin", strings.NewReader("forbidden")); err == nil {
		t.Fatal("old wrapper repointed to successor")
	}
	base.mu.Lock()
	defer base.mu.Unlock()
	for _, key := range base.keys {
		if !strings.HasPrefix(key, oldPrefix+"/") {
			t.Fatal("old request reached successor")
		}
	}
	if exists, err := successor.Exists(context.Background(), newPrefix+"/archive/head.bin"); err != nil || exists {
		t.Fatalf("successor affected: exists=%v err=%v", exists, err)
	}
}
