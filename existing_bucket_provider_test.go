package rhiza_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	localobj "github.com/mrchypark/rhiza/internal/objstore"
)

func TestEmbeddedObjectStorageExistingBucket(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider == "" || provider == "filesystem" {
		t.Skip("requires an explicit isolated S3, GCS, or Azure emulator")
	}
	cfg := embeddedStoreConfig(t)
	fixture := embeddedBucket(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const name = "existing-bucket/sentinel"
	if err := fixture.Upload(ctx, name, strings.NewReader("existing")); err != nil {
		t.Fatal(err)
	}
	before := embeddedRemoteImage(t, cfg)
	bucket, err := localobj.NewBucketWithoutProvisioning(ctx, embeddedBucketConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Error(err)
		}
	})
	r, err := bucket.Get(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if closeErr := r.Close(); closeErr != nil {
		t.Error(closeErr)
	}
	if err != nil || string(data) != "existing" {
		t.Fatalf("read existing object: %q, %v", data, err)
	}
	if after := embeddedRemoteImage(t, cfg); !reflect.DeepEqual(before, after) {
		t.Fatal("opening or reading changed bucket contents")
	}

	if provider != "azure" {
		return
	}
	client, err := azblob.NewClientFromConnectionString(cfg.ObjStoreAzureConnectionString, nil)
	if err != nil {
		t.Fatal(err)
	}
	missing := embeddedBucketConfig(cfg)
	missing.Bucket += "-missing"
	container := client.ServiceClient().NewContainerClient(missing.Bucket)
	checkMissing := func() {
		t.Helper()
		_, err := container.GetProperties(ctx, nil)
		var response *azcore.ResponseError
		if !errors.As(err, &response) || response.StatusCode != http.StatusNotFound {
			t.Fatalf("container is not confirmed missing: %v", err)
		}
	}
	checkMissing()
	missingBucket, err := localobj.NewBucketWithoutProvisioning(ctx, missing)
	if err != nil {
		t.Fatalf("construct missing-container adapter: %v", err)
	}
	if reader, err := missingBucket.Get(ctx, name); err == nil {
		_ = reader.Close()
		t.Fatal("read from missing container succeeded")
	} else if !missingBucket.IsObjNotFoundErr(err) {
		t.Fatalf("missing container error is not classified as not found: %v", err)
	}
	if err := missingBucket.Close(); err != nil {
		t.Fatal(err)
	}
	checkMissing()
}
