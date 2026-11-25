package checkpoint

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/thanos-io/objstore"
)

func TestLocalSnapshotRepeatHasStablePortableRoot(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "local", "", 1)
	src := source(t, RoleSQLite, "same bytes")
	first, err := manager.CreateLocalFiles(ctx, []Source{src}, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.CreateLocalFiles(ctx, []Source{src}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.RootHash != second.RootHash {
		t.Fatalf("same source changed portable root: first=%x second=%x first-files=%+v second-files=%+v", first.RootHash, second.RootHash, first.Files, second.Files)
	}
}

func TestLocalSnapshotRepeatRejectsCorruptExistingBlock(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "local", "", 1)
	src := source(t, RoleSQLite, "same bytes")
	first, err := manager.CreateLocalFiles(ctx, []Source{src}, 1)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := decodeHash(first.Files[0].Blocks[0].Hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, manager.key(blockKey(hash)), bytes.NewReader([]byte("corrupt"))); err != nil {
		t.Fatal(err)
	}
	_, err = manager.CreateLocalFiles(ctx, []Source{src}, 1)
	if err == nil || !strings.Contains(err.Error(), "existing checkpoint block does not match its hash") {
		t.Fatalf("expected corrupt block rejection, got %v", err)
	}
}
