package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

var (
	errGCIteration = errors.New("injected archive GC iteration failure")
	errGCDelete    = errors.New("injected archive GC object delete failure")
)

// gcDeleteBoundary blocks only eligible object deletes. It is an external
// bucket boundary: the manager must wait for all calls that crossed it.
type gcDeleteBoundary struct {
	objstore.Bucket
	eligible map[string]struct{}
	started  chan string
	release  chan struct{}
	failGate chan struct{}
	failName string
	iterErr  error
	expected int
	allStart chan struct{}
	canceled chan struct{}

	mu           sync.Mutex
	inflight     int
	maximum      int
	startedN     int
	events       []string
	cancelOnce   sync.Once
	releaseOnce  sync.Once
	failGateOnce sync.Once
}

func (b *gcDeleteBoundary) Delete(ctx context.Context, name string) error {
	if _, ok := b.eligible[name]; !ok {
		b.mu.Lock()
		b.events = append(b.events, "marker:"+name)
		b.mu.Unlock()
		return b.Bucket.Delete(ctx, name)
	}
	b.mu.Lock()
	b.inflight++
	if b.inflight > b.maximum {
		b.maximum = b.inflight
	}
	b.events = append(b.events, "object-start:"+name)
	b.startedN++
	if b.allStart != nil && b.startedN == b.expected {
		close(b.allStart)
	}
	b.mu.Unlock()
	b.started <- name
	if b.canceled != nil {
		go func() {
			<-ctx.Done()
			b.cancelOnce.Do(func() { close(b.canceled) })
		}()
	}
	if name == b.failName {
		<-b.failGate
		b.finish(name)
		return errGCDelete
	}
	<-b.release
	err := b.Bucket.Delete(ctx, name)
	b.finish(name)
	return err
}

func (b *gcDeleteBoundary) unblock() {
	b.releaseOnce.Do(func() { close(b.release) })
	if b.failGate != nil {
		b.failGateOnce.Do(func() { close(b.failGate) })
	}
}

func (b *gcDeleteBoundary) finish(name string) {
	b.mu.Lock()
	b.inflight--
	b.events = append(b.events, "object-done:"+name)
	b.mu.Unlock()
}

func (b *gcDeleteBoundary) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	err := b.Bucket.Iter(ctx, dir, f, options...)
	if err == nil && b.iterErr != nil && dir == "cluster/archive/blocks" {
		<-b.allStart
		return b.iterErr
	}
	return err
}

func (b *gcDeleteBoundary) snapshot() (maximum int, events []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maximum, append([]string(nil), b.events...)
}

func waitGC[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		var zero T
		t.Fatalf("timed out waiting for %s", what)
		return zero
	}
}

func seedEligibleArchiveGC(t *testing.T, count int) (*objstore.InMemBucket, []string) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	value := []byte("live")
	hash := sha256.Sum256(value)
	source := archiveBenchmarkSource{
		decisions: []quepaxa.DecidedValue{{Slot: 1, Hash: hash, Value: value, Certificate: []byte("certificate")}},
		prefixes:  [][32]byte{{}, quepaxa.AdvancePrefixHash([32]byte{}, 1, hash)},
	}
	writer := NewManager(bucket, "cluster", 1)
	if err := writer.syncNow(ctx, source, 1); err != nil {
		t.Fatal(err)
	}
	writer.Close()

	names := make([]string, 0, count)
	for i := range count {
		data := []byte(fmt.Sprintf("orphan-%d", i))
		name := "cluster/" + extentObjectKey(sha256.Sum256(data), 1)
		if err := bucket.Upload(ctx, name, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		marker := "cluster/" + fmt.Sprintf("archive/gc-candidates/%x", sha256.Sum256([]byte(name)))
		if err := bucket.Upload(ctx, marker, bytes.NewReader([]byte(name))); err != nil {
			t.Fatal(err)
		}
		if err := bucket.ChangeLastModified(marker, time.Unix(0, 0)); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return bucket, names
}

func TestArchiveGCParallelDeletesAreBoundedAndOrdered(t *testing.T) {
	bucket, names := seedEligibleArchiveGC(t, 5)
	eligible := make(map[string]struct{}, len(names))
	for _, name := range names {
		eligible[name] = struct{}{}
	}
	boundary := &gcDeleteBoundary{Bucket: bucket, eligible: eligible, started: make(chan string, len(names)), release: make(chan struct{})}
	manager := NewManager(boundary, "cluster", 1)
	defer manager.Close()
	defer boundary.unblock()
	done := make(chan error, 1)
	go func() { done <- manager.Cleanup(context.Background(), 0) }()
	for range 4 {
		waitGC(t, boundary.started, "four concurrent object deletes")
	}
	if maximum, _ := boundary.snapshot(); maximum != 4 {
		t.Fatalf("concurrent object deletes=%d, want 4", maximum)
	}
	boundary.unblock()
	if err := waitGC(t, done, "parallel GC cleanup"); err != nil {
		t.Fatal(err)
	}
	maximum, events := boundary.snapshot()
	if maximum > 4 {
		t.Fatalf("concurrent object deletes=%d, limit 4", maximum)
	}
	positions := make(map[string]int, len(events))
	for i, event := range events {
		positions[event] = i
	}
	for _, name := range names {
		marker := manager.gcMarkerKey(name)
		if positions["object-done:"+name] >= positions["marker:"+marker] {
			t.Fatalf("marker deleted before object: %q events=%v", name, events)
		}
		if exists, err := bucket.Exists(context.Background(), name); err != nil || exists {
			t.Fatalf("object %q exists=%t err=%v", name, exists, err)
		}
		if exists, err := bucket.Exists(context.Background(), marker); err != nil || exists {
			t.Fatalf("marker %q exists=%t err=%v", marker, exists, err)
		}
	}
}

func TestArchiveGCJoinsOutstandingDeletesBeforeReleasingLease(t *testing.T) {
	bucket, names := seedEligibleArchiveGC(t, 3)
	eligible := make(map[string]struct{}, len(names))
	for _, name := range names {
		eligible[name] = struct{}{}
	}
	boundary := &gcDeleteBoundary{
		Bucket: bucket, eligible: eligible, started: make(chan string, len(names)), release: make(chan struct{}),
		failGate: make(chan struct{}), failName: names[0], iterErr: errGCIteration, expected: len(names), allStart: make(chan struct{}), canceled: make(chan struct{}),
	}
	manager := NewManager(boundary, "cluster", 1)
	defer manager.Close()
	defer boundary.unblock()
	done := make(chan error, 1)
	go func() { done <- manager.Cleanup(context.Background(), 0) }()
	waitGC(t, boundary.allStart, "all delete calls to cross the boundary")
	// This waits until cleanup has observed the injected Iter error and canceled
	// the errgroup context, rather than merely until Iter has scheduled its return.
	waitGC(t, boundary.canceled, "iteration-error cancellation")
	boundary.failGateOnce.Do(func() { close(boundary.failGate) })
	select {
	case err := <-done:
		t.Fatalf("Cleanup returned before outstanding deletes completed: %v", err)
	default:
	}
	contender := NewManager(bucket, "cluster", 1)
	defer contender.Close()
	if err := contender.Cleanup(context.Background(), 0); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("concurrent Cleanup error=%v, want ErrArchiveBusy while deletes run", err)
	}
	boundary.unblock()
	err := waitGC(t, done, "erroring GC cleanup")
	if !errors.Is(err, errGCIteration) || !errors.Is(err, errGCDelete) {
		t.Fatalf("Cleanup error=%v, want joined iteration and delete failures", err)
	}
	marker := manager.gcMarkerKey(names[0])
	if exists, existsErr := bucket.Exists(context.Background(), names[0]); existsErr != nil || !exists {
		t.Fatalf("failed object exists=%t err=%v", exists, existsErr)
	}
	if exists, existsErr := bucket.Exists(context.Background(), marker); existsErr != nil || !exists {
		t.Fatalf("failed object marker exists=%t err=%v", exists, existsErr)
	}
}

func TestArchiveGCCanceledQueuedDeleteNeverReachesBucket(t *testing.T) {
	bucket, names := seedEligibleArchiveGC(t, 5)
	eligible := make(map[string]struct{}, len(names))
	for _, name := range names {
		eligible[name] = struct{}{}
	}
	boundary := &gcDeleteBoundary{
		Bucket: bucket, eligible: eligible, started: make(chan string, len(names)), release: make(chan struct{}), canceled: make(chan struct{}),
	}
	manager := NewManager(boundary, "cluster", 1)
	defer manager.Close()
	defer boundary.unblock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Cleanup(ctx, 0) }()
	for range 4 {
		waitGC(t, boundary.started, "four concurrent object deletes")
	}
	cancel()
	waitGC(t, boundary.canceled, "parent cancellation at delete boundary")
	boundary.unblock()
	if err := waitGC(t, done, "canceled GC cleanup"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Cleanup error=%v, want context.Canceled", err)
	}
	_, events := boundary.snapshot()
	starts := 0
	for _, event := range events {
		if strings.HasPrefix(event, "object-start:") {
			starts++
		}
	}
	if starts != 4 {
		t.Fatalf("object deletes that reached bucket=%d, want 4; events=%v", starts, events)
	}
	select {
	case name := <-boundary.started:
		t.Fatalf("queued delete reached bucket after cancellation: %q", name)
	default:
	}
}
