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
		case http.MethodHead:
			properties.Add(1)
			w.Header().Set("x-ms-error-code", "ContainerNotFound")
			w.WriteHeader(http.StatusNotFound)
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
	properties.Store(0)
	creates.Store(0)
	bucket, err := NewBucketForSupervisedStartup(WithReplayObservation(context.Background()), config)
	if err != nil {
		t.Fatalf("supervised constructor: %v", err)
	}
	defer bucket.Close()
	if properties.Load() != 0 || creates.Load() != 0 {
		t.Fatalf("supervised pre-bind requests: properties=%d creates=%d", properties.Load(), creates.Load())
	}
	if !bucket.Stats().ReplayGroupingEnabled {
		t.Fatal("supervised constructor dropped the context observer")
	}
}
