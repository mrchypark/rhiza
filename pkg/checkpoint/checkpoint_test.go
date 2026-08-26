package checkpoint

import (
	"bytes"
	"context"
	"testing"

	"github.com/thanos-io/objstore"
)

func TestReadRejectsCorruptCheckpoint(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "node-1", t.TempDir())
	if err := manager.Create(ctx, []byte("valid"), 7); err != nil {
		t.Fatal(err)
	}
	cp := manager.Latest()
	key := manager.key(chunkKey(cp.RootHash, cp.Chunks[0].Hash))
	if err := bucket.Upload(ctx, key, bytes.NewReader([]byte("bad!!"))); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Read(ctx, 7); err == nil {
		t.Fatal("corrupt checkpoint was accepted")
	}
}

func TestCheckpointRejectsNumberedRoot(t *testing.T) {
	if err := decodePersistedJSON([]byte(`{"version":3}`), &Checkpoint{}); err == nil {
		t.Fatal("accepted numbered checkpoint root")
	}
}

func TestSharedRootStreamsAndVerifiesWithoutManifest(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	data := bytes.Repeat([]byte("x"), chunkSize+17)
	creator := NewManager(bucket, "cluster", t.TempDir(), 9)
	root, err := creator.CreateReader(ctx, bytes.NewReader(data), 11)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Chunks) != 2 {
		t.Fatalf("chunks=%d, want 2", len(root.Chunks))
	}
	reader := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := reader.Verify(ctx, 11, root.RootHash, root.Hash); err != nil {
		t.Fatal(err)
	}
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := reader.Read(ctx, 11)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read bytes=%d err=%v", len(got), err)
	}
}

func TestCandidateRootsRequireSealedHash(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	first, err := manager.CreateReader(ctx, bytes.NewReader([]byte("first")), 11)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.CreateReader(ctx, bytes.NewReader([]byte("second")), 11)
	if err != nil {
		t.Fatal(err)
	}
	loaded := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := loaded.Read(ctx, 11); err == nil {
		t.Fatal("ambiguous checkpoint index was accepted without a sealed root")
	}
	got, err := loaded.ReadRoot(ctx, 11, second.RootHash)
	if err != nil || string(got) != "second" {
		t.Fatalf("sealed root read = %q, err=%v", got, err)
	}
	if first.RootHash == second.RootHash {
		t.Fatal("test roots unexpectedly match")
	}
}

func TestGarbageCollectRetiresRootBeforeItsChunks(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	old, err := manager.CreateReader(ctx, bytes.NewReader([]byte("old")), 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateReader(ctx, bytes.NewReader([]byte("new")), 11); err != nil {
		t.Fatal(err)
	}
	oldChunk := manager.key(chunkKey(old.RootHash, old.Chunks[0].Hash))
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, oldChunk); !exists {
		t.Fatal("retired root chunks were deleted in the same GC pass")
	}
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, oldChunk); exists {
		t.Fatal("orphaned root chunks survived the following GC pass")
	}
}
