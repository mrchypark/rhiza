package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

type appendOnlyCountedReader struct {
	io.ReadCloser
	bytesRead *int
}

func (r appendOnlyCountedReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	*r.bytesRead += n
	return n, err
}

type appendOnlyReadCounter struct {
	objstore.Bucket
	key       string
	bytesRead int
}

func (b *appendOnlyReadCounter) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := b.Bucket.Get(ctx, key)
	if err != nil || key != b.key {
		return r, err
	}
	return appendOnlyCountedReader{ReadCloser: r, bytesRead: &b.bytesRead}, nil
}

type appendOnlyBucket struct {
	objstore.Bucket
	options   []objstore.ObjectUploadOptionType
	collision string
	mutations int
	reads     int
	keys      []string
}

func (b *appendOnlyBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.reads++
	return b.Bucket.Get(ctx, key)
}
func (b *appendOnlyBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	b.reads++
	return b.Bucket.GetRange(ctx, key, off, length)
}
func (b *appendOnlyBucket) Exists(ctx context.Context, key string) (bool, error) {
	b.reads++
	return b.Bucket.Exists(ctx, key)
}
func (b *appendOnlyBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	b.reads++
	return b.Bucket.Attributes(ctx, key)
}
func (b *appendOnlyBucket) Iter(ctx context.Context, dir string, f func(string) error, opts ...objstore.IterOption) error {
	b.reads++
	return b.Bucket.Iter(ctx, dir, f, opts...)
}

func (b *appendOnlyBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	if b.options != nil {
		return b.options
	}
	return b.Bucket.SupportedObjectUploadOptions()
}

func (b *appendOnlyBucket) Upload(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mutations++
	if b.collision != "" && strings.Contains(key, b.collision) {
		if exists, err := b.Bucket.Exists(ctx, key); err != nil {
			return err
		} else if !exists {
			if err := b.Bucket.Upload(ctx, key, strings.NewReader("corrupt")); err != nil {
				return err
			}
			b.keys = append(b.keys, key)
		}
	}
	return b.Bucket.Upload(ctx, key, r, opts...)
}

func (b *appendOnlyBucket) Delete(ctx context.Context, key string) error {
	b.mutations++
	return b.Bucket.Delete(ctx, key)
}

func TestHAAppendOnlyCheckpoint(t *testing.T) {
	ctx := context.Background()
	if _, err := NewAppendOnlyManager(nil, "ha", "", 1); err == nil {
		t.Fatal("nil bucket accepted")
	}
	for _, opts := range [][]objstore.ObjectUploadOptionType{{objstore.IfMatch}, {objstore.IfNotExists}} {
		b := &appendOnlyBucket{Bucket: objstore.NewInMemBucket(), options: opts}
		if _, err := NewAppendOnlyManager(b, "ha", "", 1); err == nil || b.mutations != 0 {
			t.Fatalf("missing capability: err=%v mutations=%d", err, b.mutations)
		}
	}
	for _, collision := range []string{"", "/checkpoint/roots/", "/checkpoint/blocks/"} {
		t.Run(collision, func(t *testing.T) {
			b := &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}
			m, err := NewAppendOnlyManager(b, "ha", "", 1)
			if err != nil {
				t.Fatal(err)
			}
			before, reads := b.mutations, b.reads
			if err := m.GarbageCollectFrom(ctx, nil, 1, 1, 0); err == nil || b.mutations != before || b.reads != reads {
				t.Fatalf("GC err=%v mutations=%d reads=%d", err, b.mutations-before, b.reads-reads)
			}
			claim, err := m.AcquirePublisherClaim(ctx, "test", 0, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer m.ReleasePublisherClaim(ctx, claim)
			b.collision = collision
			root, err := m.CreateFiles(ctx, claim, []Source{source(t, RoleSQLite, "payload")}, 1)
			if collision == "" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := m.DownloadAndVerifyRootFiles(ctx, root, t.TempDir()); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("corrupt collision accepted")
			}
			if len(b.keys) == 0 {
				t.Fatal("collision was not injected")
			}
			for _, key := range b.keys {
				r, err := b.Bucket.Get(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil || !bytes.Equal(data, []byte("corrupt")) {
					t.Fatalf("collision overwritten: %s %q %v", key, data, err)
				}
			}
		})
	}
}

func TestHAAppendOnlyIdenticalBlockReuse(t *testing.T) {
	ctx := context.Background()
	b := objstore.NewInMemBucket()
	m, err := NewAppendOnlyManager(b, "reuse", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := m.AcquirePublisherClaim(ctx, "test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer m.ReleasePublisherClaim(ctx, claim)
	s := source(t, RoleSQLite, "same-content")
	var generation uint64
	for index := uint64(1); index <= 3; index++ {
		root, err := m.CreateFiles(ctx, claim, []Source{s}, index)
		if err != nil {
			t.Fatalf("create index %d: %v", index, err)
		}
		if _, err := m.DownloadAndVerifyRootFiles(ctx, root, t.TempDir()); err != nil {
			t.Fatal(err)
		}
		if index == 2 {
			generation = root.Files[0].Blocks[0].Generation
			if generation == 0 {
				t.Fatal("second publication did not use fallback generation")
			}
		}
		if index == 3 && root.Files[0].Blocks[0].Generation != generation {
			t.Fatal("identical fallback was not reused")
		}
	}
}

func TestHAAppendOnlyLocalGCAliasRefused(t *testing.T) {
	ctx := context.Background()
	b := &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}
	m, err := NewAppendOnlyManager(b, "ha", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	beforeReads, beforeMutations := b.reads, b.mutations
	err = m.GarbageCollectLocal(ctx, map[[32]byte]struct{}{{1}: {}}, nil, 0)
	if err == nil || b.reads != beforeReads || b.mutations != beforeMutations {
		t.Fatalf("local GC alias: err=%v reads=%d mutations=%d", err, b.reads-beforeReads, b.mutations-beforeMutations)
	}
}

func TestHAAppendOnlyLocalMaintenanceRefused(t *testing.T) {
	b := &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}
	m, err := NewAppendOnlyManager(b, "ha", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	beforeReads, beforeMutations := b.reads, b.mutations
	called := false
	err = m.WithLocalMaintenanceContext(context.Background(), func(context.Context) error {
		called = true
		return nil
	})
	if err == nil || called || b.reads != beforeReads || b.mutations != beforeMutations {
		t.Fatalf("local maintenance: err=%v called=%t reads=%d mutations=%d", err, called, b.reads-beforeReads, b.mutations-beforeMutations)
	}
}

func TestHAAppendOnlyOversizedCollisionReadBound(t *testing.T) {
	ctx := context.Background()
	b := &appendOnlyReadCounter{Bucket: objstore.NewInMemBucket()}
	m, err := NewAppendOnlyManager(b, "ha", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := m.AcquirePublisherClaim(ctx, "test", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer m.ReleasePublisherClaim(ctx, claim)
	const content = "expected block"
	s := source(t, RoleSQLite, content)
	data, err := os.ReadFile(s.Path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	b.key = m.key(blockGenerationKey(hash, claim.Generation))
	for _, key := range []string{m.key(blockKey(hash)), b.key} {
		if err := b.Bucket.Upload(ctx, key, strings.NewReader(strings.Repeat("x", len(data)+100))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.CreateFiles(ctx, claim, []Source{s}, 1); err == nil {
		t.Fatal("oversized collision accepted")
	}
	if b.bytesRead != len(data)+1 {
		t.Fatalf("read %d bytes, want %d", b.bytesRead, len(data)+1)
	}
}

func TestLocalGCUnchanged(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "local", "", 1)
	root, err := manager.CreateLocalFiles(ctx, []Source{source(t, RoleSQLite, "retired")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		return manager.GarbageCollectLocal(ctx, map[[32]byte]struct{}{root.RootHash: {}}, nil, 0)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.OpenRoot(ctx, root.Index, root.RootHash); err == nil {
		t.Fatal("local GC retained a retired root")
	}
}
