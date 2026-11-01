package rhiza_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	localobj "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/thanos-io/objstore"
)

type managedProviderCounter struct {
	objstore.Bucket
	calls atomic.Int64
}

func (b *managedProviderCounter) Upload(ctx context.Context, key string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.calls.Add(1)
	return b.Bucket.Upload(ctx, key, r, opts...)
}

func (b *managedProviderCounter) Delete(ctx context.Context, key string) error {
	b.calls.Add(1)
	return b.Bucket.Delete(ctx, key)
}

func (b *managedProviderCounter) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.calls.Add(1)
	return b.Bucket.Get(ctx, key)
}

// Actual provider SDKs talk only to the explicitly selected isolated emulator.
// The birth record is an exact assignment record, not an atomic prefix claim.
func TestEmbeddedObjectStorageManagedNamespace(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider != "s3" && provider != "gcs" && provider != "azure" {
		t.Skip("requires an explicit isolated S3, GCS, or Azure emulator")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	upstream, err := url.Parse(embeddedEndpoint(t))
	if err != nil {
		t.Fatal(err)
	}
	_, oldPrefix, err := localobj.ManagedNamespacePrefixes("managed-provider", "full-source-ref-uid", 17, "immutable-old-assignment", "old-cluster")
	if err != nil {
		t.Fatal(err)
	}
	_, nextPrefix, err := localobj.ManagedNamespacePrefixes("managed-provider", "full-source-ref-uid", 18, "immutable-successor-assignment", "next-cluster")
	if err != nil {
		t.Fatal(err)
	}
	delayedKey := oldPrefix + "/archive/delayed"
	arrived, release := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	var attempts, requests atomic.Int64
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if armed.Load() && (r.Method == http.MethodPut || r.Method == http.MethodPost) && (strings.HasSuffix(r.URL.Path, "/"+delayedKey) || r.URL.Query().Get("name") == delayedKey) {
			if attempts.Add(1) == 1 {
				close(arrived)
				select {
				case <-release:
				case <-ctx.Done():
					return
				}
				http.Error(w, "injected retryable old-scope failure", http.StatusServiceUnavailable)
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	t.Setenv("RHIZA_EMBEDDED_ENDPOINT", server.URL)
	cfg := embeddedStoreConfig(t)
	base, err := localobj.NewBucketForNamespace(ctx, embeddedBucketConfig(cfg), oldPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := base.Close(); err != nil {
			t.Error(err)
		}
	})
	counter := &managedProviderCounter{Bucket: base}
	old, err := localobj.BindNamespace(counter, oldPrefix)
	if err != nil {
		t.Fatal(err)
	}
	read := func(b objstore.Bucket, key, want string) {
		t.Helper()
		r, err := b.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		closeErr := r.Close()
		if err != nil || closeErr != nil || string(data) != want {
			t.Fatalf("exact read %s: %q, %v, %v", key, data, err, closeErr)
		}
	}
	birth := oldPrefix + "/voters/managed-sql-initialization.json"
	const assignment = `{"assignment":"immutable-old-assignment","generation":17}`
	if err := old.Upload(ctx, birth, strings.NewReader(assignment), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{assignment, `{"assignment":"conflicting-assignment"}`} {
		if err := old.Upload(ctx, birth, strings.NewReader(body), objstore.WithIfNotExists()); err == nil || !old.IsConditionNotMetErr(err) {
			t.Fatalf("conditional birth collision: %v", err)
		}
		read(old, birth, assignment)
	}
	start := oldPrefix + "/voters/managed-sql-application-start.json"
	if err := old.Upload(ctx, start, strings.NewReader(assignment), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	read(old, start, assignment)
	for _, key := range []string{nextPrefix + "/foreign", oldPrefix + "-similar/object", oldPrefix + "/../escape", oldPrefix + "/%2e%2e/escape"} {
		before, wireBefore := counter.calls.Load(), requests.Load()
		if old.Upload(ctx, key, strings.NewReader("forbidden")) == nil {
			t.Fatal("foreign upload accepted")
		}
		if old.Delete(ctx, key) == nil {
			t.Fatal("foreign delete accepted")
		}
		if r, err := old.Get(ctx, key); err == nil {
			_ = r.Close()
			t.Fatal("foreign read accepted")
		}
		if counter.calls.Load() != before || requests.Load() != wireBefore {
			t.Fatal("invalid key reached adapter or SDK")
		}
	}
	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- old.Upload(ctx, delayedKey, strings.NewReader("old-only"), objstore.WithIfNotExists()) }()
	// Always release and join our request before emulator/server cleanup.
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
			cancel()
			<-done
		}
	})
	select {
	case <-arrived:
	case <-ctx.Done():
		t.Fatal("old SDK PUT not observed")
	}
	next, err := localobj.NewBucketForNamespace(ctx, embeddedBucketConfig(cfg), nextPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := next.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := next.Upload(ctx, nextPrefix+"/voters/managed-sql-initialization.json", strings.NewReader("successor"), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	// Mutating the constructor input cannot redirect an already-created SDK.
	cfg.ObjStoreBucket, cfg.ObjStoreEndpoint = "must-not-exist", "127.0.0.1:1"
	close(release)
	released = true
	if err := <-done; err != nil {
		t.Fatalf("SDK old-scope retry: %v", err)
	}
	if attempts.Load() < 2 {
		t.Fatal("SDK retry was not observed")
	}
	read(old, delayedKey, "old-only")
	if exists, err := next.Exists(ctx, nextPrefix+"/archive/delayed"); err != nil || exists {
		t.Fatalf("old request leaked to successor: exists=%v err=%v", exists, err)
	}
	read(next, nextPrefix+"/voters/managed-sql-initialization.json", "successor")
	t.Logf("provider=%s conditional birth/start verified; old SDK attempts=%d; invalid keys rejected before adapter", provider, attempts.Load())
}
