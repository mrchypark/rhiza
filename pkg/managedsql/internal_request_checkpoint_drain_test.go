package managedsql

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
)

func TestInternalRequestAllAdmittedPublishersParkBeforeClose(t *testing.T) {
	var outage atomic.Bool
	var attempts atomic.Int64
	gate := newInternalPublisherGate()
	_, hosts, _ := openInternalRequestHostsObserved(t, &outage, &attempts, gate.start, gate.before, gate.after)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for _, host := range hosts {
		awaitPublicInternalReady(t, ctx, host)
	}
	for i := range hosts {
		if !gate.wasStarted(i) {
			t.Fatalf("host %d Ready without started publisher", i)
		}
	}
	if err := gate.quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	for i, host := range hosts {
		if host.admissionLifetime.Err() != nil {
			t.Fatalf("host %d closed before all publishers parked", i)
		}
	}
}

func TestInternalRequestLateAdmittedPublisherParksBeforeAttempt(t *testing.T) {
	var outage atomic.Bool
	var attempts atomic.Int64
	gate := newInternalPublisherGate()
	_, hosts, _ := openInternalRequestHostsObserved(t, &outage, &attempts, gate.start, gate.before, gate.after)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	awaitPublicInternalReady(t, ctx, hosts[0])
	if !gate.wasStarted(0) || gate.wasStarted(1) {
		t.Fatal("unexpected pre-quiescence admission")
	}
	if err := gate.quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	awaitPublicInternalReady(t, ctx, hosts[1])
	if !gate.wasStarted(1) {
		t.Fatal("late Host did not register its real publisher")
	}
	select {
	case <-gate.parked[1]:
	case <-ctx.Done():
		t.Fatal("late publisher did not park before attempting work", ctx.Err())
	}
	select {
	case err := <-gate.settled[1]:
		t.Fatalf("late publisher entered an attempt: %v", err)
	default:
	}
	for i, host := range hosts {
		if host.admissionLifetime.Err() != nil {
			t.Fatalf("host %d closed before late publisher parked", i)
		}
	}
}

func TestInternalRequestCheckpointAttemptSettlesBeforeClose(t *testing.T) {
	var outage, enabled atomic.Bool
	var attempts atomic.Int64
	ready := make(chan string, 1)
	readyOwner := make(chan int, 1)
	release := make(chan struct{})
	startPermit := make(chan struct{})
	started0 := make(chan struct{}, 1)
	gate := newInternalPublisherGate()
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	configs, hosts, fixture := openInternalRequestHostsObserved(t, &outage, &attempts, gate.start, func(i int, ctx context.Context) {
		gate.before(i, ctx)
		if i == 0 {
			started0 <- struct{}{}
			select {
			case <-startPermit:
			case <-ctx.Done():
			}
		}
	}, gate.after, func(c *sqlWireControl) {
		c.putEnabled, c.putReady, c.putRelease = &enabled, ready, release
		c.putReadyOwner = readyOwner
		c.putReadGateBytes = 1
		c.putPrefix, c.putSuffix = "/checkpoint/blocks/", ".block"
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	enabled.Store(true)
	writeDone := make(chan error, 1)
	go func() {
		_, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "checkpoint-drain", SQL: "CREATE TABLE checkpoint_drain(id INTEGER)"})
		writeDone <- err
	}()
	select {
	case <-started0:
	case <-ctx.Done():
		t.Fatal("publisher did not start", ctx.Err())
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("write did not complete before checkpoint attempt", ctx.Err())
	}
	close(startPermit)
	var putPath string
	select {
	case putPath = <-ready:
	case <-ctx.Done():
		t.Fatal("no checkpoint block PUT", ctx.Err())
	}
	select {
	case owner := <-readyOwner:
		if owner != 0 {
			t.Fatalf("gated PUT belongs to Host %d, want Host 0", owner)
		}
	case <-ctx.Done():
		t.Fatal("missing gated PUT owner", ctx.Err())
	}
	select {
	case err := <-gate.settled[0]:
		t.Fatalf("attempt settled before full PUT: %v", err)
	default:
	}
	if hosts[0].admissionLifetime.Err() != nil {
		t.Fatal("Host closed before attempt settled")
	}
	enabled.Store(false)
	releaseOnce.Do(func() { close(release) })
	if err := gate.quiesce(ctx); err != nil {
		t.Fatal(err)
	}
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
	key := strings.TrimPrefix(putPath, "/bucket/")
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if got, want := path.Base(key), fmt.Sprintf("%x.block", sum); got != want {
		t.Fatalf("checkpoint block bytes do not match key: got %s want %s", got, want)
	}
}
