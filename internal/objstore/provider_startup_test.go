package objstore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/thanos-io/objstore/providers/azure"
)

func TestAzureSupervisedConstructorDoesNotProvisionBeforeWALBind(t *testing.T) {
	var properties, creates atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			properties.Add(1)
			w.Header().Set("x-ms-error-code", "ContainerNotFound")
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>ContainerNotFound</Code><Message>not found</Message></Error>`))
		case http.MethodPut:
			creates.Add(1)
			// The real SDK sent the mutation; lose its response rather than
			// returning a rejection that would resolve the outcome.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		default:
			http.Error(w, "unexpected method", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	previous := azure.DefaultConfig
	azure.DefaultConfig.HTTPConfig.Transport = server.Client().Transport
	azure.DefaultConfig.PipelineConfig.MaxTries = 1
	azure.DefaultConfig.PipelineConfig.RetryDelay = 1
	azure.DefaultConfig.PipelineConfig.MaxRetryDelay = 1
	azure.DefaultConfig.PipelineConfig.TryTimeout = 1_000_000_000
	t.Cleanup(func() { azure.DefaultConfig = previous })
	config := Config{Provider: ProviderAzure, Bucket: "container", AzureStorageAccount: "account",
		AzureConnectionString: fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=account;AccountKey=dGVzdA==;BlobEndpoint=%s", server.URL)}
	// Backward-compatible ordinary startup still takes the actual pinned
	// provider's GetProperties -> Create path and exposes the lost response.
	legacy, err := NewBucketWithContext(context.Background(), config)
	if err == nil || legacy != nil || properties.Load() == 0 || creates.Load() == 0 {
		t.Fatalf("legacy control: bucket=%v error=%v properties=%d creates=%d", legacy != nil, err, properties.Load(), creates.Load())
	}
	t.Logf("legacy constructor lost-response control: properties=%d create attempts=%d", properties.Load(), creates.Load())
	for _, constructor := range []struct {
		name string
		open func(context.Context, Config) (*MeteredBucket, error)
	}{{"without provisioning", NewBucketWithoutProvisioning}, {"supervised", NewBucketForSupervisedStartup}} {
		t.Run(constructor.name, func(t *testing.T) {
			properties.Store(0)
			creates.Store(0)
			bucket, err := constructor.open(WithReplayObservation(context.Background()), config)
			if err != nil {
				t.Fatalf("constructor: %v", err)
			}
			defer bucket.Close()
			if properties.Load() != 0 || creates.Load() != 0 {
				t.Fatalf("constructor requests: properties=%d creates=%d", properties.Load(), creates.Load())
			}
			if !bucket.Stats().ReplayGroupingEnabled {
				t.Fatal("constructor dropped the context observer")
			}
		})
	}
}

func TestNonProvisioningConstructorsRejectInvalidConfig(t *testing.T) {
	for _, open := range []func(context.Context, Config) (*MeteredBucket, error){NewBucketWithoutProvisioning, NewBucketForSupervisedStartup} {
		for _, cfg := range []Config{{}, {Provider: "unknown"}, {Provider: ProviderAzure}} {
			bucket, err := open(nil, cfg)
			if err == nil || bucket != nil {
				t.Fatalf("config %+v: bucket=%v error=%v", cfg, bucket != nil, err)
			}
		}
	}
}

func TestNonProvisioningConstructorsAcceptNilContext(t *testing.T) {
	for _, open := range []func(context.Context, Config) (*MeteredBucket, error){NewBucketWithoutProvisioning, NewBucketForSupervisedStartup} {
		bucket, err := open(nil, Config{Provider: ProviderFilesystem, FilesystemDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		if err := bucket.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
