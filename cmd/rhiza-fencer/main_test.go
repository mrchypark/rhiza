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

func TestRecoveryAnchorBindingConfigRequiresExplicitPolicy(t *testing.T) {
	for _, test := range []struct {
		name, policy, storageID string
		store                   objectstore.Config
		wantErr                 bool
	}{
		{name: "derived", policy: "derived", store: objectstore.Config{Provider: objectstore.ProviderS3, Endpoint: "https://s3.example", Bucket: "archive", Prefix: "root"}},
		{name: "opaque", policy: "opaque", storageID: "external-store", store: objectstore.Config{Provider: objectstore.ProviderFilesystem}},
		{name: "missing policy", storageID: "external-store", wantErr: true},
		{name: "missing opaque ID", policy: "opaque", wantErr: true},
		{name: "derived and opaque ambiguous", policy: "derived", storageID: "external-store", store: objectstore.Config{Provider: objectstore.ProviderS3, Bucket: "archive"}, wantErr: true},
		{name: "derived incomplete store", policy: "derived", store: objectstore.Config{Provider: objectstore.ProviderS3}, wantErr: true},
		{name: "unknown policy", policy: "guess", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := recoveryAnchorBindingConfig(test.policy, test.storageID, test.store)
			if (err != nil) != test.wantErr {
				t.Fatalf("config=%+v err=%v wantErr=%t", cfg, err, test.wantErr)
			}
			if err == nil && cfg == nil {
				t.Fatal("valid explicit binding returned nil config")
			}
		})
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
