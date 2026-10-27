package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
)

func TestPeerClosureStoreDoesNotOpenFilesystemDefault(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "must-not-be-created")
	bucket, err := openPeerClosureBucket(context.Background(), objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: dir})
	if err != nil || bucket != nil {
		t.Fatalf("filesystem peer-closure bucket = %v, %v; want nil", bucket, err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("filesystem default side effect: stat err=%v", err)
	}
}

func TestPeerClosureStoreRequiresUsableRemoteConfiguration(t *testing.T) {
	bucket, err := openPeerClosureBucket(context.Background(), objectstore.Config{Provider: objectstore.ProviderS3})
	if err == nil || bucket != nil {
		if closer, ok := bucket.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		t.Fatalf("incomplete remote config = bucket %v, err %v; want fail-closed error", bucket, err)
	}
}
