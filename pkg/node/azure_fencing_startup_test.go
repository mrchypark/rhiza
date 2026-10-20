package node

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore/providers/azure"
)

func TestOpenSupervisedAzureDoesNotCreateContainerBeforeRejectedBind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reads, creates, binds atomic.Int32
	store := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			creates.Add(1)
			http.Error(w, "pre-bind mutation forbidden", http.StatusForbidden)
			return
		}
		reads.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("comp") == "list" {
			_, _ = w.Write([]byte(`<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`))
			return
		}
		code := "BlobNotFound"
		if r.Method == http.MethodHead {
			code = "ContainerNotFound"
		}
		w.Header().Set("x-ms-error-code", code)
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>not found</Message></Error>`, code)
	}))
	defer store.Close()
	previous := azure.DefaultConfig
	azure.DefaultConfig.HTTPConfig.Transport = store.Client().Transport
	t.Cleanup(func() { azure.DefaultConfig = previous })
	socket := filepath.Join(fenceSocketDir(t), "startup.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	supervisor := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/startup":
			_ = json.NewEncoder(w).Encode(fencing.Identity{SourceClusterID: "cluster", NodeID: "a", PodUID: "pod", RunID: "run"})
		case "/v1/bind":
			binds.Add(1)
			w.WriteHeader(http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = supervisor.Serve(listener) }()
	t.Cleanup(func() { _ = supervisor.Close(); _ = listener.Close() })
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")
	config := voterTestConfig(t)
	config.PeerToken = "a-token"
	config.ObjStoreProvider, config.ObjStoreBucket = "azure", "container"
	config.ObjStoreAzureStorageAccount = "account"
	config.ObjStoreAzureConnectionString = fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=account;AccountKey=dGVzdA==;BlobEndpoint=%s", store.URL)
	n := New(config)
	err = n.Open(objmetrics.WithReplayObservation(ctx))
	if err == nil || !strings.Contains(err.Error(), "403") || binds.Load() != 1 {
		t.Fatalf("did not reach the actual rejected WAL bind: error=%v binds=%d reads=%d creates=%d", err, binds.Load(), reads.Load(), creates.Load())
	}
	if n.fenceSession == nil || n.fenceSession.Identity().WALIdentity != "" {
		t.Fatal("rejected bind unexpectedly admitted a WAL")
	}
	if reads.Load() == 0 || creates.Load() != 0 {
		t.Fatalf("actual supervised Node.Open pre-bind requests: reads=%d mutations=%d", reads.Load(), creates.Load())
	}
}
