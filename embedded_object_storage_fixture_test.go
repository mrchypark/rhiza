package rhiza_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/rhiza"
	localobj "github.com/mrchypark/rhiza/internal/objstore"
	thanosobj "github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

const embeddedTestAccount = "rhizatest"
const embeddedTestAccess = "rhiza-local-test"

func embeddedEndpoint(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("RHIZA_EMBEDDED_ENDPOINT")
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		t.Fatalf("explicit HTTP emulator origin required, got %q", raw)
	}
	host := u.Hostname()
	// The Kubernetes fixture has one fixed namespace-local service name.
	if host != "localhost" && host != "floci" && (net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback()) {
		t.Fatalf("nonlocal emulator endpoint rejected: %q", raw)
	}
	if host == "floci" && os.Getenv("RHIZA_EMBEDDED_K8S_STAGE") == "" && os.Getenv("RHIZA_EMBEDDED_CONTAINER") != "1" {
		t.Fatal("service endpoint requires an owned Docker or Kubernetes fixture")
	}
	return raw
}

func embeddedStoreConfig(t *testing.T) rhiza.Config {
	t.Helper()
	cfg := rhiza.Config{Local: true, NodeID: "embedded-backup", DataDir: filepath.Join(t.TempDir(), "local"), ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck, ObjStoreSyncInterval: 10 * time.Millisecond}
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider == "" || provider == "filesystem" {
		cfg.ObjStoreProvider = rhiza.ObjectStoreProviderFilesystem
		cfg.ObjStoreDir = t.TempDir()
		return cfg
	}
	endpoint := embeddedEndpoint(t)
	bucket := fmt.Sprintf("rhiza-embedded-%d", time.Now().UnixNano())
	if fixed := os.Getenv("RHIZA_EMBEDDED_BUCKET"); fixed != "" {
		if os.Getenv("RHIZA_EMBEDDED_K8S_STAGE") == "" || !strings.HasPrefix(fixed, "rhiza-embedded-") {
			t.Fatal("fixed buckets are reserved for owned Kubernetes fixtures")
		}
		bucket = fixed
	}
	cfg.ObjStoreProvider, cfg.ObjStoreBucket = provider, bucket
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch provider {
	case "s3":
		cfg.ObjStoreEndpoint, cfg.ObjStoreInsecure = strings.TrimPrefix(endpoint, "http://"), true
		cfg.ObjStoreAccessKey, cfg.ObjStoreSecretKey, cfg.ObjStoreRegion = embeddedTestAccess, embeddedTestAccess, "us-east-1"
		client, err := minio.New(cfg.ObjStoreEndpoint, &minio.Options{Creds: credentials.NewStaticV4(embeddedTestAccess, embeddedTestAccess, ""), Secure: false, Region: cfg.ObjStoreRegion, BucketLookup: minio.BucketLookupPath})
		if err != nil {
			t.Fatal(err)
		}
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: cfg.ObjStoreRegion}); err != nil {
			exists, checkErr := client.BucketExists(ctx, bucket)
			if checkErr != nil || !exists {
				t.Fatalf("create S3 bucket: %v; exists=%v: %v", err, exists, checkErr)
			}
		}
		if os.Getenv("RHIZA_EMBEDDED_K8S_STAGE") == "" {
			t.Cleanup(func() {
				embeddedDeleteBucketObjects(t, cfg)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if err := client.RemoveBucket(ctx, bucket); err != nil {
					t.Errorf("remove S3 bucket: %v", err)
				}
			})
		}
	case "gcs":
		// The production adapter delegates emulator routing to the Go storage SDK.
		t.Setenv("STORAGE_EMULATOR_HOST", endpoint)
		// Thanos constructs its auth transport before storage.NewClient sees the
		// emulator setting. Use disposable credentials whose token URI is local.
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		adc, err := json.Marshal(map[string]string{
			"type": "service_account", "project_id": "rhiza-local-test",
			"private_key_id": "rhiza-local-test", "client_email": "rhiza@rhiza-local-test.iam.gserviceaccount.com",
			"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
			"token_uri":   endpoint + "/token",
		})
		if err != nil {
			t.Fatal(err)
		}
		credentialPath := filepath.Join(t.TempDir(), "emulator-adc.json")
		if err := os.WriteFile(credentialPath, adc, 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialPath)
		client, err := storage.NewClient(ctx, option.WithoutAuthentication())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if err := client.Bucket(bucket).Create(ctx, "rhiza-local-test", nil); err != nil {
			t.Fatalf("create GCS bucket: %v", err)
		}
		t.Cleanup(func() {
			embeddedDeleteBucketObjects(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := client.Bucket(bucket).Delete(ctx); err != nil {
				t.Errorf("remove GCS bucket: %v", err)
			}
		})
	case "azure":
		key := base64.StdEncoding.EncodeToString([]byte("rhiza-local-test-key-only-32bytes!"))
		cfg.ObjStoreAzureStorageAccount = embeddedTestAccount
		cfg.ObjStoreAzureConnectionString = fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;", embeddedTestAccount, key, endpoint, embeddedTestAccount)
		client, err := azblob.NewClientFromConnectionString(cfg.ObjStoreAzureConnectionString, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.CreateContainer(ctx, bucket, nil); err != nil {
			t.Fatalf("create Azure container: %v", err)
		}
		t.Cleanup(func() {
			embeddedDeleteBucketObjects(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if _, err := client.DeleteContainer(ctx, bucket, nil); err != nil {
				t.Errorf("remove Azure container: %v", err)
			}
		})
	default:
		t.Fatalf("unsupported explicit test provider %q", provider)
	}
	return cfg
}

func embeddedBucket(t *testing.T, cfg rhiza.Config) *localobj.MeteredBucket {
	t.Helper()
	b, err := localobj.NewBucket(localobj.Config{Provider: localobj.Provider(cfg.ObjStoreProvider), FilesystemDir: cfg.ObjStoreDir, Endpoint: cfg.ObjStoreEndpoint, Bucket: cfg.ObjStoreBucket, Region: cfg.ObjStoreRegion, Insecure: cfg.ObjStoreInsecure, AccessKey: cfg.ObjStoreAccessKey, SecretKey: cfg.ObjStoreSecretKey, AzureStorageAccount: cfg.ObjStoreAzureStorageAccount, AzureConnectionString: cfg.ObjStoreAzureConnectionString})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("close fixture bucket: %v", err)
		}
	})
	return b
}

func embeddedRemoteImage(t *testing.T, cfg rhiza.Config) map[string][]byte {
	t.Helper()
	return embeddedRemoteImageState(t, cfg, false)
}

func embeddedRemoteImageState(t *testing.T, cfg rhiza.Config, allowHeld bool) map[string][]byte {
	t.Helper()
	b := embeddedBucket(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		image := make(map[string][]byte)
		busy := false
		err := b.Iter(ctx, "", func(name string) error {
			r, err := b.Get(ctx, name)
			if err != nil {
				return err
			}
			data, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if readErr != nil {
				return readErr
			}
			if closeErr != nil {
				return closeErr
			}
			image[name] = data
			if !allowHeld && strings.HasSuffix("/"+name, "/local/v1/local-gc-control") {
				var gate struct {
					Version uint   `json:"version"`
					Token   string `json:"token"`
				}
				if err := json.Unmarshal(data, &gate); err != nil || gate.Version != 1 {
					return fmt.Errorf("invalid maintenance authority at %s: %v", name, err)
				}
				busy = busy || gate.Token != ""
			}
			return nil
		}, thanosobj.WithRecursiveIter())
		if busy {
			t.Fatal("full namespace capture requires a stopped/drained owner and free gate; refusing to copy held authority")
		}
		if err == nil && !busy {
			return image
		}
		if err != nil && !b.IsObjNotFoundErr(err) && !os.IsNotExist(err) {
			t.Fatalf("remote inventory: %v", err)
		}
		// List and Get are not atomic with GC. Restart the complete image rather
		// than accepting a partial map that hides a real object deletion.
		select {
		case <-ctx.Done():
			t.Fatalf("remote inventory never stabilized: %v (last error: %v, gate busy: %v)", ctx.Err(), err, busy)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func embeddedReplaceRemote(t *testing.T, cfg rhiza.Config, image map[string][]byte) {
	t.Helper()
	for name, data := range image {
		if !strings.HasSuffix("/"+name, "/local/v1/local-gc-control") {
			continue
		}
		var gate struct {
			Version uint   `json:"version"`
			Token   string `json:"token"`
		}
		if err := json.Unmarshal(data, &gate); err != nil || gate.Version != 1 || gate.Token != "" {
			t.Fatalf("refusing historical copy with invalid/held maintenance authority: %s", name)
		}
	}
	current := embeddedRemoteImage(t, cfg)
	b := embeddedBucket(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for name := range current {
		if err := b.Delete(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range image {
		if err := b.Upload(ctx, name, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
}

func embeddedDeleteBucketObjects(t *testing.T, cfg rhiza.Config) {
	// Cleanup must report failures without Fatal, which would skip bucket removal.
	b, err := localobj.NewBucket(localobj.Config{Provider: localobj.Provider(cfg.ObjStoreProvider), Endpoint: cfg.ObjStoreEndpoint, Bucket: cfg.ObjStoreBucket, Region: cfg.ObjStoreRegion, Insecure: cfg.ObjStoreInsecure, AccessKey: cfg.ObjStoreAccessKey, SecretKey: cfg.ObjStoreSecretKey, AzureStorageAccount: cfg.ObjStoreAzureStorageAccount, AzureConnectionString: cfg.ObjStoreAzureConnectionString})
	if err != nil {
		t.Errorf("cleanup bucket client: %v", err)
		return
	}
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := b.Iter(ctx, "", func(name string) error { return b.Delete(ctx, name) }, thanosobj.WithRecursiveIter()); err != nil {
		t.Errorf("cleanup objects: %v", err)
	}
}

func embeddedLocalImage(t *testing.T, root string) map[string][]byte {
	t.Helper()
	image := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root {
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				image[rel+"/"] = nil
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected nonregular file %s", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		image[rel] = data
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func embeddedReplaceLocal(t *testing.T, root string, image map[string][]byte) {
	t.Helper()
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	for name, data := range image {
		path := filepath.Join(root, name)
		if strings.HasSuffix(name, "/") {
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEmbeddedObjectStorageProviderContract(t *testing.T) {
	cfg := embeddedStoreConfig(t)
	b := embeddedBucket(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const name = "fixture/conditional-object"
	if _, err := b.Get(ctx, name); err == nil || !b.IsObjNotFoundErr(err) {
		t.Fatalf("missing object classification: %v", err)
	}
	if err := b.Upload(ctx, name, strings.NewReader("first"), thanosobj.WithIfNotExists()); err != nil {
		t.Fatalf("initial conditional upload: %v", err)
	}
	if err := b.Upload(ctx, name, strings.NewReader("wrong"), thanosobj.WithIfNotExists()); err == nil || !b.IsConditionNotMetErr(err) {
		t.Fatalf("conditional create conflict classification: %v", err)
	}
	image := embeddedRemoteImage(t, cfg)
	if string(image[name]) != "first" || len(image) != 1 {
		t.Fatalf("list/get after conditional conflict: %q", image)
	}
	attrs, err := b.Attributes(ctx, name)
	if err != nil {
		t.Fatalf("attributes: %v", err)
	}
	if attrs.Size != 5 {
		t.Fatalf("size=%d want=5", attrs.Size)
	}
	if err := b.Delete(ctx, name); err != nil {
		t.Fatal(err)
	}
	if exists, err := b.Exists(ctx, name); err != nil || exists {
		t.Fatalf("exists after delete=%v err=%v", exists, err)
	}
}
