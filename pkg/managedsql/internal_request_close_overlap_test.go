package managedsql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
)

// An independent client cancellation can produce precisely the same server
// EOF while Close is in progress. The read error must remain observable.
func TestInternalRequestIndependentPUTCancelOverlapsClose(t *testing.T) {
	var outage, enabled atomic.Bool
	var attempts atomic.Int64
	ready := make(chan string, 1)
	release := make(chan struct{})
	readErrors := make(chan error, 1)
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	configs, hosts, fixture := openInternalRequestHosts(t, &outage, &attempts, func(c *sqlWireControl) {
		c.putEnabled, c.putReady, c.putRelease = &enabled, ready, release
		c.putReadErrors = readErrors
		c.putReadGateBytes = 1
		c.putPrefix, c.putSuffix = "/checkpoint/blocks/", ".block"
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := configs[0]
	bucket, err := objectstore.NewBucketForNamespace(ctx, objectstore.Config{
		Provider: objectstore.ProviderS3, Endpoint: cfg.Runtime.ObjStoreEndpoint,
		Bucket: cfg.Runtime.ObjStoreBucket, Region: cfg.Runtime.ObjStoreRegion,
		Insecure: true, AccessKey: "fixture-host-0", SecretKey: cfg.Runtime.ObjStoreSecretKey,
	}, fixture.source.finalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	key := fixture.source.finalPrefix + "/checkpoint/blocks/independent-cancel.block"
	reader := newObservedSDKUploadReader(bytes.Repeat([]byte("x"), 4<<20))
	uploadCtx, cancelUpload := context.WithCancel(ctx)
	defer cancelUpload()
	result := make(chan error, 1)
	enabled.Store(true)
	go func() { result <- bucket.Upload(uploadCtx, key, reader) }()
	select {
	case path := <-ready:
		if path != "/bucket/"+key || reader.remaining.Load() == 0 {
			t.Fatal("PUT not incomplete at gate")
		}
	case err := <-result:
		t.Fatalf("upload ended early: %v", err)
	case <-ctx.Done():
		t.Fatal("PUT did not reach gate", ctx.Err())
	}
	// Keep Close in progress while the server observes the separately canceled
	// request. Close cancels admission before acquiring this mutex.
	host := hosts[0]
	host.mu.Lock()
	locked := true
	defer func() {
		if locked {
			host.mu.Unlock()
		}
	}()
	cancelUpload()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("independent cancel: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("independent upload did not cancel", ctx.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- host.Close() }()
	select {
	case <-host.admissionLifetime.Done():
	case <-ctx.Done():
		t.Fatal("Close did not cancel admission", ctx.Err())
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-readErrors:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("read error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("server did not report independent cancellation", ctx.Err())
	}
	if exists, err := bucket.Exists(ctx, key); err != nil || exists {
		t.Fatalf("partial object exists=%t error=%v", exists, err)
	}
	host.mu.Unlock()
	locked = false
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not finish", ctx.Err())
	}
}
