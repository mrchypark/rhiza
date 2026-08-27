package checkpoint

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/thanos-io/objstore"
)

type getCountingBucket struct {
	objstore.Bucket
	gets atomic.Uint64
}

func (b *getCountingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
	return b.Bucket.Get(ctx, name)
}

func source(t *testing.T, role, value string) Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), role+".db")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return Source{Role: role, Path: path}
}

func TestFixedRoleCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	root, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "sql"), source(t, RoleGraphData, "graph")}, 11)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Files) != 2 {
		t.Fatalf("files=%d", len(root.Files))
	}
	loaded := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	files, err := loaded.DownloadRootFiles(ctx, 11, root.RootHash, dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		got, err := os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		want := "sql"
		if file.Role == RoleGraphData {
			want = "graph"
		}
		if string(got) != want {
			t.Fatalf("role %s=%q", file.Role, got)
		}
	}
}

func TestCorruptBlockIsRejected(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "node", t.TempDir())
	root, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "valid")}, 7)
	if err != nil {
		t.Fatal(err)
	}
	hash, _ := decodeHash(root.Files[0].Blocks[0].Hash)
	if err := bucket.Upload(ctx, manager.key(blockKey(hash)), bytes.NewReader([]byte("bad!!"))); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.DownloadRootFiles(ctx, 7, root.RootHash, t.TempDir()); err == nil {
		t.Fatal("corrupt block was accepted")
	}
}

func TestLoadUsesCurrentAndExactRoot(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	creator := NewManager(bucket, "cluster", t.TempDir(), 9)
	for index := uint64(1); index <= 3; index++ {
		if _, err := creator.CreateFiles(ctx, []Source{source(t, RoleSQLite, string(rune('0'+index)))}, index); err != nil {
			t.Fatal(err)
		}
	}
	bucket.gets.Store(0)
	loaded := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if gets := bucket.gets.Load(); gets != 2 {
		t.Fatalf("startup GETs=%d, want 2", gets)
	}
}

func TestGarbageCollectRetiresRootBeforeBlocks(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	old, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "old")}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "new")}, 11); err != nil {
		t.Fatal(err)
	}
	hash, _ := decodeHash(old.Files[0].Blocks[0].Hash)
	key := manager.key(blockKey(hash))
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, key); !exists {
		t.Fatal("retired root block deleted in same GC pass")
	}
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, key); exists {
		t.Fatal("orphaned block survived following GC pass")
	}
}
