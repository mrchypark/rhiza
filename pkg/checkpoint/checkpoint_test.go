package checkpoint

import (
	"bytes"
	"context"
	"fmt"
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
	key := manager.key("checkpoint/7-" + fmt.Sprintf("%x", cp.Hash[:8]))
	if err := bucket.Upload(ctx, key, bytes.NewReader([]byte("bad!!"))); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Read(ctx, 7); err == nil {
		t.Fatal("corrupt checkpoint was accepted")
	}
}
