package types

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestManagedStorageBindingPreservesPreparedDigest(t *testing.T) {
	c := ExecutionConfig{ClusterID: "cluster", ObjStoreBucket: "bucket", ObjStoreEndpoint: "example:9000", ObjStorePrefix: "root/./data", ObjStoreRegion: "region", ObjStoreInsecure: true}
	want := fmt.Sprintf("%x", sha256.Sum256([]byte(`{"Provider":"s3","Endpoint":"example:9000","Bucket":"bucket","Prefix":"root/data","Account":"","FilesystemDir":"","Cluster":"cluster","Region":"region","Insecure":true}`)))
	got, err := ManagedStorageBinding(c)
	if err != nil || got != want {
		t.Fatalf("legacy digest changed: got=%s want=%s error=%v", got, want, err)
	}
	c.ObjStoreProvider = "s3"
	c.ObjStoreAccessKey = "private"
	c.ObjStoreSecretKey = "private"
	c.ObjStoreSessionToken = "private"
	got, err = ManagedStorageBinding(c)
	if err != nil || got != want {
		t.Fatal("credentials or default S3 spelling changed binding")
	}
	c.ObjStorePrefix = "other"
	got, err = ManagedStorageBinding(c)
	if err != nil || got == want {
		t.Fatal("physical prefix not bound")
	}
}

func TestManagedStorageBindingAzureDescriptorBoundary(t *testing.T) {
	c := ExecutionConfig{ObjStoreProvider: "azure", ObjStoreBucket: "container", ObjStoreAzureStorageAccount: "account", ObjStoreAzureConnectionString: "AccountName=account;AccountKey=secret;BlobEndpoint=https://account.example;"}
	got, err := ManagedStorageBinding(c)
	if err != nil {
		t.Fatal(err)
	}
	c.ObjStoreAzureConnectionString = "AccountName=account;AccountKey=changed;BlobEndpoint=https://account.example;"
	other, err := ManagedStorageBinding(c)
	if err != nil || other != got {
		t.Fatal("private account key included in physical binding")
	}
	c.ObjStoreAzureConnectionString = "AccountName=other;BlobEndpoint=https://account.example;"
	if _, err := ManagedStorageBinding(c); err == nil {
		t.Fatal("mismatched physical account accepted")
	}
}
