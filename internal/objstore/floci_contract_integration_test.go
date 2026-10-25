package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	thanosobjstore "github.com/thanos-io/objstore"
)

const (
	flociS3Endpoint  = "http://floci-s3:4566"
	flociGCSEndpoint = "http://floci-gcs:4588"
	flociAzureURL    = "http://floci-az:4577/flocitest"
	flociGCSProject  = "floci-local"
	flociAzureKey    = "ZmxvY2ktdGVzdC1rZXktbm90LWEtc2VjcmV0"
)

// TestFlociProviderContracts exercises Rhiza's real provider factory against
// the pinned Floci service containers. It is deliberately Actions-only: the
// launcher requires CI=true and this test rejects every endpoint except the
// fixed service aliases below. It is not cloud or provider qualification.
func TestFlociProviderContracts(t *testing.T) {
	if os.Getenv("RHIZA_FLOCI_INTEGRATION") != "1" || os.Getenv("CI") != "true" {
		t.Skip("run scripts/test-floci-objstore.sh inside the pinned Floci Actions job")
	}
	if os.Getenv("RHIZA_FLOCI_S3_ENDPOINT") != flociS3Endpoint ||
		os.Getenv("STORAGE_EMULATOR_HOST") != flociGCSEndpoint ||
		os.Getenv("RHIZA_FLOCI_AZURE_BLOB_ENDPOINT") != flociAzureURL {
		t.Fatal("Floci endpoint mismatch; refusing to use a non-service endpoint")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		t.Fatal("GOOGLE_APPLICATION_CREDENTIALS must be unset for the emulator test")
	}
	if os.Getenv("AWS_EC2_METADATA_DISABLED") != "true" {
		t.Fatal("AWS metadata credentials must be disabled for the emulator test")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") != "floci-test" || os.Getenv("AWS_SECRET_ACCESS_KEY") != "floci-test-secret" {
		t.Fatal("explicit throwaway S3 credentials are required")
	}

	for _, provider := range []Provider{ProviderS3, ProviderGCS, ProviderAzure} {
		t.Run(string(provider), func(t *testing.T) {
			runFlociProviderContract(t, provider)
		})
	}
}

func runFlociProviderContract(t *testing.T, provider Provider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var suffixBytes [10]byte
	if _, err := rand.Read(suffixBytes[:]); err != nil {
		t.Fatal("create test resource suffix: random source unavailable")
	}
	suffix := hex.EncodeToString(suffixBytes[:])
	bucketName := "rhiza-floci-" + suffix
	key := "contract/" + suffix + "/object.bin"
	config, cleanup := flociBucketConfig(t, ctx, provider, bucketName)
	t.Cleanup(cleanup)

	if provider == ProviderGCS {
		config.ServiceAccount = flociGCSLocalCredentials(t)
	}
	bucket, err := NewBucket(config)
	if err != nil {
		t.Fatalf("create Rhiza %s bucket client: %v", provider, err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Errorf("close Rhiza bucket client: %v", err)
		}
	})
	t.Cleanup(func() {
		if exists, err := bucket.Exists(context.Background(), key); err != nil {
			t.Errorf("check task-owned object before cleanup: %v", err)
		} else if exists {
			if provider == ProviderGCS {
				clearGCSHoldForCleanup(t, bucketName, key)
			}
			if err := bucket.Delete(context.Background(), key); err != nil {
				t.Errorf("remove task-owned object: %v", err)
			}
		}
	})

	for _, option := range []thanosobjstore.ObjectUploadOptionType{thanosobjstore.IfNotExists, thanosobjstore.IfMatch} {
		if !containsUploadOption(bucket.SupportedObjectUploadOptions(), option) {
			t.Fatalf("%s adapter does not advertise required conditional option %v", provider, option)
		}
	}

	original := []byte("floci-provider-contract-v1")
	if err := bucket.Upload(ctx, key, bytes.NewReader(original), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatalf("conditional create: %v", err)
	}
	conflict := bucket.Upload(ctx, key, bytes.NewReader([]byte("must-not-replace")), thanosobjstore.WithIfNotExists())
	if conflict == nil || !bucket.IsConditionNotMetErr(conflict) {
		t.Fatalf("duplicate conditional create error=%v, want provider condition conflict", conflict)
	}

	reader, err := bucket.Get(ctx, key)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(body, original) {
		t.Fatalf("object readback mismatch: read_error=%v close_error=%v", readErr, closeErr)
	}

	rangeReader, err := bucket.GetRange(ctx, key, 7, 8)
	if err != nil {
		t.Fatalf("get range: %v", err)
	}
	rangeBody, rangeReadErr := io.ReadAll(rangeReader)
	rangeCloseErr := rangeReader.Close()
	if rangeReadErr != nil || rangeCloseErr != nil || !bytes.Equal(rangeBody, original[7:15]) {
		t.Fatalf("range readback mismatch: read_error=%v close_error=%v", rangeReadErr, rangeCloseErr)
	}

	attrs, err := bucket.Attributes(ctx, key)
	if err != nil {
		t.Fatalf("object attributes: %v", err)
	}
	if attrs.Size != int64(len(original)) || attrs.Version == nil || attrs.Version.Value == "" {
		t.Fatalf("attributes size=%d version_present=%t; want size and provider version", attrs.Size, attrs.Version != nil)
	}
	var listed bool
	if err := bucket.Iter(ctx, "contract/"+suffix, func(name string) error {
		if name == key {
			listed = true
		}
		return nil
	}); err != nil {
		t.Fatalf("list objects: %v", err)
	}
	if !listed {
		t.Fatal("list did not return the test-owned object")
	}

	updated := []byte("floci-provider-contract-v2")
	if err := bucket.Upload(ctx, key, bytes.NewReader(updated), thanosobjstore.WithIfMatch(attrs.Version)); err != nil {
		t.Fatalf("conditional update with current provider version: %v", err)
	}
	stale := bucket.Upload(ctx, key, bytes.NewReader([]byte("stale-write")), thanosobjstore.WithIfMatch(attrs.Version))
	if stale == nil || !bucket.IsConditionNotMetErr(stale) {
		t.Fatalf("stale conditional update error=%v, want provider condition conflict", stale)
	}

	if provider == ProviderGCS {
		checkGCSMetadataAndHold(t, ctx, bucketName, key, updated)
	}

	if provider == ProviderGCS {
		current, err := bucket.Attributes(ctx, key)
		if err != nil || current.Version == nil {
			t.Fatalf("read current GCS generation: version_present=%t error=%v", current.Version != nil, err)
		}
		if err := DeleteVersion(ctx, bucket, key, *current.Version); err != nil {
			t.Fatalf("delete exact GCS generation: %v", err)
		}
	} else if err := bucket.Delete(ctx, key); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if exists, err := bucket.Exists(ctx, key); err != nil || exists {
		t.Fatalf("object after delete exists=%t error=%v", exists, err)
	}
	if _, err := bucket.Get(ctx, key); err == nil || !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("get after delete error=%v, want provider not-found", err)
	}
	if _, err := bucket.Attributes(ctx, key); err == nil || !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("attributes after delete error=%v, want provider not-found", err)
	}
}

// flociGCSLocalCredentials pins OAuth refreshes to a loopback stub because the
// Thanos adapter does not apply the Storage SDK emulator's automatic no-auth mode.
func flociGCSLocalCredentials(t *testing.T) string {
	t.Helper()
	const (
		clientID     = "floci-test-client"
		clientSecret = "floci-test-client-secret"
		refreshToken = "floci-test-refresh-token"
	)
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/token" {
			http.Error(w, "unexpected local token request", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "refresh_token" ||
			r.Form.Get("client_id") != clientID || r.Form.Get("client_secret") != clientSecret ||
			r.Form.Get("refresh_token") != refreshToken {
			http.Error(w, "unexpected local refresh grant", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"floci-test-access-token","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(tokenServer.Close)
	credentialJSON, err := json.Marshal(struct {
		Type         string `json:"type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		RefreshToken string `json:"refresh_token"`
		TokenURI     string `json:"token_uri"`
	}{
		Type: "authorized_user", ClientID: clientID, ClientSecret: clientSecret,
		RefreshToken: refreshToken, TokenURI: tokenServer.URL + "/token",
	})
	if err != nil {
		t.Fatalf("marshal local Floci credentials: %v", err)
	}
	return string(credentialJSON)
}

func flociBucketConfig(t *testing.T, ctx context.Context, provider Provider, bucketName string) (Config, func()) {
	t.Helper()
	switch provider {
	case ProviderS3:
		endpoint, err := url.Parse(flociS3Endpoint)
		if err != nil || endpoint.Host != "floci-s3:4566" || endpoint.Scheme != "http" {
			t.Fatal("invalid fixed S3 emulator endpoint")
		}
		client, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4("floci-test", "floci-test-secret", ""), Secure: false, Region: "us-east-1"})
		if err != nil {
			t.Fatalf("create S3 setup client: %v", err)
		}
		if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
			t.Fatalf("create task-owned S3 bucket: %v", err)
		}
		return Config{Provider: ProviderS3, Endpoint: endpoint.Host, Bucket: bucketName, Region: "us-east-1", Insecure: true, AccessKey: "floci-test", SecretKey: "floci-test-secret"}, func() {
			if err := client.RemoveBucket(context.Background(), bucketName); err != nil {
				t.Errorf("remove task-owned S3 bucket: %v", err)
			}
		}
	case ProviderGCS:
		t.Setenv("GOOGLE_CLOUD_PROJECT", flociGCSProject)
		client, err := storage.NewClient(ctx)
		if err != nil {
			t.Fatalf("create GCS setup client: %v", err)
		}
		if err := client.Bucket(bucketName).Create(ctx, flociGCSProject, nil); err != nil {
			_ = client.Close()
			t.Fatalf("create task-owned GCS bucket: %v", err)
		}
		return Config{Provider: ProviderGCS, Bucket: bucketName}, func() {
			if err := client.Bucket(bucketName).Delete(context.Background()); err != nil {
				t.Errorf("remove task-owned GCS bucket: %v", err)
			}
			if err := client.Close(); err != nil {
				t.Errorf("close GCS setup client: %v", err)
			}
		}
	case ProviderAzure:
		connectionString := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=flocitest;AccountKey=%s;BlobEndpoint=%s;", flociAzureKey, flociAzureURL)
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			t.Fatalf("create Azure setup client: %v", err)
		}
		container := client.ServiceClient().NewContainerClient(bucketName)
		if _, err := container.Create(ctx, nil); err != nil {
			t.Fatalf("create task-owned Azure container: %v", err)
		}
		return Config{Provider: ProviderAzure, Bucket: bucketName, AzureStorageAccount: "flocitest", AzureConnectionString: connectionString}, func() {
			if _, err := container.Delete(context.Background(), nil); err != nil {
				t.Errorf("remove task-owned Azure container: %v", err)
			}
		}
	default:
		t.Fatalf("unsupported Floci provider %q", provider)
		return Config{}, func() {}
	}
}

func checkGCSMetadataAndHold(t *testing.T, ctx context.Context, bucketName, key string, expected []byte) {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST") != flociGCSEndpoint {
		t.Fatal("GCS emulator endpoint changed during test")
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("create GCS metadata client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close GCS metadata client: %v", err)
		}
	}()
	bucketAttrs, err := client.Bucket(bucketName).Attrs(ctx)
	if err != nil {
		t.Fatalf("read GCS bucket attributes: %v", err)
	}
	if bucketAttrs == nil || bucketAttrs.MetaGeneration < 1 {
		t.Fatalf("GCS bucket attributes missing or invalid: %#v", bucketAttrs)
	}
	object := client.Bucket(bucketName).Object(key)
	objectAttrs, err := object.Attrs(ctx)
	if err != nil {
		t.Fatalf("read GCS object attributes: %v", err)
	}
	if objectAttrs == nil || objectAttrs.Generation <= 0 || objectAttrs.Metageneration <= 0 {
		t.Fatalf("GCS object attributes missing or invalid: %#v", objectAttrs)
	}
	if _, err := object.If(storage.Conditions{GenerationMatch: objectAttrs.Generation, MetagenerationMatch: objectAttrs.Metageneration}).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: true}); err != nil {
		t.Fatalf("set GCS temporary hold with exact generation/metageneration: %v", err)
	}
	held, err := object.Attrs(ctx)
	if err != nil {
		t.Fatalf("read GCS held-object attributes: %v", err)
	}
	if held == nil || !held.TemporaryHold || held.Generation != objectAttrs.Generation || held.Metageneration <= objectAttrs.Metageneration {
		t.Fatalf("GCS hold readback missing or invalid: %#v", held)
	}
	if _, err := object.If(storage.Conditions{GenerationMatch: objectAttrs.Generation, MetagenerationMatch: objectAttrs.Metageneration}).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: false}); err == nil {
		t.Fatal("GCS accepted a stale metageneration condition")
	}
	if _, err := object.If(storage.Conditions{GenerationMatch: held.Generation, MetagenerationMatch: held.Metageneration}).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: false}); err != nil {
		t.Fatalf("clear GCS temporary hold with exact generation/metageneration: %v", err)
	}
	reader, err := object.NewReader(ctx)
	if err != nil {
		t.Fatalf("read GCS object after hold update: %v", err)
	}
	readback, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(readback, expected) {
		t.Fatalf("GCS held-object content changed: read_error=%v close_error=%v", readErr, closeErr)
	}
}

func clearGCSHoldForCleanup(t *testing.T, bucketName, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Errorf("create GCS cleanup client: %v", err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Errorf("close GCS cleanup client: %v", err)
		}
	}()
	object := client.Bucket(bucketName).Object(key)
	attrs, err := object.Attrs(ctx)
	if err != nil {
		t.Errorf("read GCS cleanup attributes: %v", err)
		return
	}
	if attrs.TemporaryHold {
		if _, err := object.If(storage.Conditions{GenerationMatch: attrs.Generation, MetagenerationMatch: attrs.Metageneration}).Update(ctx, storage.ObjectAttrsToUpdate{TemporaryHold: false}); err != nil {
			t.Errorf("clear task-owned GCS hold for cleanup: %v", err)
		}
	}
}

func containsUploadOption(options []thanosobjstore.ObjectUploadOptionType, want thanosobjstore.ObjectUploadOptionType) bool {
	for _, option := range options {
		if option == want {
			return true
		}
	}
	return false
}
