package managedsql

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
)

func TestManagedSQLWireCloseCancelsHostLifetimeSDKUpload(t *testing.T) {
	configs, fixture := sqlCluster(t)
	lifecycle := &sqlWireCloseLifecycle{acceptedPUT: make(chan string, 4)}
	lifecycle.observePUTFailures(t, fixture)
	control := fixture.source
	control.putReady = make(chan string, 1)
	control.putRelease = make(chan struct{})
	control.putReadGateBytes = 1
	control.putPrefix = "/archive/close-lifetime/"
	control.putSuffix = ".block"
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(control.putRelease) }) }
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	cfg := configs[0]
	host, err := fixture.open(ctx, cfg)
	if host != nil {
		t.Cleanup(func() {
			if err := fixture.closeHost(host); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if lifecycle.closing.Load() || !fixture.closeObserverAtOpen.Load() {
		t.Fatal("expected the observer before any owned Close")
	}
	bucket, err := objectstore.NewBucketForNamespace(ctx, objectstore.Config{
		Provider: objectstore.ProviderS3, Endpoint: cfg.Runtime.ObjStoreEndpoint,
		Bucket: cfg.Runtime.ObjStoreBucket, Region: cfg.Runtime.ObjStoreRegion,
		Insecure: true, AccessKey: cfg.Runtime.ObjStoreAccessKey, SecretKey: cfg.Runtime.ObjStoreSecretKey,
	}, control.finalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	key := control.finalPrefix + "/archive/close-lifetime/partial.block"
	body := newObservedSDKUploadReader(bytes.Repeat([]byte("p"), 4<<20))
	uploadCtx, stopUpload := context.WithCancel(host.admissionLifetime)
	defer stopUpload()
	result := make(chan error, 1)
	uploadDone := make(chan struct{})
	go func() {
		defer close(uploadDone)
		result <- bucket.Upload(uploadCtx, key, body)
	}()
	t.Cleanup(func() { <-uploadDone })

	wantPath := "/bucket/" + key
	select {
	case path := <-control.putReady:
		if path != wantPath || body.remaining.Load() == 0 {
			t.Fatal("owned SDK PUT was not demonstrably incomplete at the body gate")
		}
	case err := <-result:
		t.Fatalf("owned SDK PUT ended before the body gate: %v", err)
	case <-ctx.Done():
		t.Fatal("owned SDK PUT did not reach the body gate", ctx.Err())
	}
	// This request uses the real Host lifetime. No raw TCP disconnect or
	// injected read error stands in for cancellation by Close.
	if err := fixture.closeHost(host); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SDK PUT returned %v, want Host cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("SDK PUT did not observe Host cancellation", ctx.Err())
	}
	release()
	select {
	case path := <-lifecycle.acceptedPUT:
		if path != wantPath {
			t.Fatal("the close observer accepted a different PUT")
		}
	case <-ctx.Done():
		t.Fatal("no actual canceled PUT body EOF reached the strict observer", ctx.Err())
	}
	if exists, err := bucket.Exists(ctx, key); err != nil || exists {
		t.Fatalf("canceled PUT left a partial object: exists=%t err=%v", exists, err)
	}
}
