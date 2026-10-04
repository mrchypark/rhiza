//go:build rhiza_local_testhooks

package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// gcRetryBucket observes only the cleanup manager. The racing writer uses the
// underlying bucket, so its reads cannot inflate the old-prefix GET counts.
type gcRetryBucket struct {
	objstore.Bucket
	oldKeys       map[string]int
	appendTip     func() error
	headAttempts  int
	conflict      error
	compacting    bool
	afterConflict func()
	uploadPrefix  string
	prefixUploads int
}

// gcPublisherAdmissionBucket observes a separate Manager's publication-lease
// read and real HEAD upload. The paused Cleanup uses the underlying bucket.
type gcPublisherAdmissionBucket struct {
	objstore.Bucket
	lockRead    chan struct{}
	releaseRead <-chan struct{}
	headUpload  chan struct{}
	headCount   atomic.Int32
}

type cancelPublicationReadBucket struct {
	objstore.Bucket
	cancel context.CancelFunc
	once   sync.Once
}

func (b *cancelPublicationReadBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if strings.HasSuffix(name, "/archive/PUBLISH_LOCK") {
		b.once.Do(b.cancel)
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *gcPublisherAdmissionBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if strings.HasSuffix(name, "/archive/PUBLISH_LOCK") {
		select {
		case b.lockRead <- struct{}{}:
		default:
		}
		select {
		case <-b.releaseRead:
		case <-ctx.Done():
			return objstore.ObjectAttributes{}, ctx.Err()
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *gcPublisherAdmissionBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/archive/head.bin") {
		b.headCount.Add(1)
		select {
		case b.headUpload <- struct{}{}:
		default:
		}
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func testArchiveIndependentPublisherWaitsForGCPublicationLease(t *testing.T) {
	baseCtx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	if _, _, err := core.Propose(baseCtx, []byte("second extent")); err != nil {
		t.Fatal(err)
	}
	if err := seed.SyncThrough(baseCtx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(baseCtx, 15*time.Second)
	defer cancel()
	gc := NewManager(base, "cluster", 1)
	defer gc.Close()
	releaseRead := make(chan struct{})
	probe := &gcPublisherAdmissionBucket{
		Bucket: base, lockRead: make(chan struct{}, 1),
		releaseRead: releaseRead, headUpload: make(chan struct{}, 1),
	}
	publisher := NewManager(probe, "cluster", 1)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("independent publisher tip")); err != nil {
		t.Fatal(err)
	}
	wantTip := core.Tip()

	gcAtPublication := make(chan struct{})
	releaseGC := make(chan struct{})
	var publicationOnce, releaseGCOnce, releaseReadOnce sync.Once
	defer releaseGCOnce.Do(func() { close(releaseGC) })
	defer releaseReadOnce.Do(func() { close(releaseRead) })
	gcCtx := localtesthooks.WithArchiveGCPhaseTrace(ctx, func(event string) {
		if event == "archive-gc:publication:begin" {
			publicationOnce.Do(func() {
				close(gcAtPublication)
				select {
				case <-releaseGC:
				case <-ctx.Done():
				}
			})
		}
	})
	gcDone := make(chan error, 1)
	go func() { gcDone <- gc.Cleanup(gcCtx, time.Hour) }()
	select {
	case <-gcAtPublication:
	case err := <-gcDone:
		t.Fatalf("Cleanup never reached publication while holding GC lease: %v", err)
	case <-ctx.Done():
		t.Fatalf("Cleanup publication barrier: %v", ctx.Err())
	}
	lock, err := gc.readGCLock(ctx)
	if err != nil || lock == nil || lock.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatalf("Cleanup remote lease not live: lock=%v err=%v", lock, err)
	}
	admission, err := gc.readArchiveLock(ctx, gc.publicationLockKey())
	if err != nil || admission == nil || admission.OwnerID != "archive-gc-publish" ||
		admission.Generation == 0 || admission.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatalf("Cleanup publication lease not confirmed live: lock=%v err=%v", admission, err)
	}
	publisherDone := make(chan error, 1)
	go func() { publisherDone <- publisher.SyncThrough(ctx, core, wantTip) }()

	select {
	case <-probe.headUpload:
		releaseGCOnce.Do(func() { close(releaseGC) })
		gcErr := <-gcDone
		publisherErr := <-publisherDone
		t.Fatalf("independent publisher attempted real HEAD upload during GC remote lease: publisher=%v cleanup=%v", publisherErr, gcErr)
	case <-probe.lockRead:
		select {
		case err := <-publisherDone:
			t.Fatalf("publisher acknowledged before Cleanup released admission: %v", err)
		default:
		}
		// Release the actual GC operation before letting the publisher finish
		// its publication-lease read. No scheduler delay is needed for ordering.
		releaseGCOnce.Do(func() { close(releaseGC) })
		if err := <-gcDone; err != nil {
			t.Fatalf("Cleanup after release: %v", err)
		}
		releaseReadOnce.Do(func() { close(releaseRead) })
		if err := <-publisherDone; err != nil {
			t.Fatalf("independent publisher after GC release: %v", err)
		}
		if got := probe.headCount.Load(); got != 1 {
			t.Fatalf("publisher HEAD uploads after GC release=%d, want exactly one fresh-head CAS", got)
		}
	case <-ctx.Done():
		t.Fatalf("publisher admission event: %v", ctx.Err())
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := final.DecisionsFrom(ctx, 1, int(wantTip))
	if err != nil || tip != wantTip || len(values) != int(wantTip) ||
		!bytes.Equal(values[len(values)-1].Value, []byte("independent publisher tip")) {
		t.Fatalf("independent final chain tip=%d want=%d decisions=%d err=%v", tip, wantTip, len(values), err)
	}
}

func testArchiveLateSharedBatchTargetUsesOnePublicationAdmission(t *testing.T) {
	baseCtx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	ctx, cancel := context.WithTimeout(baseCtx, 15*time.Second)
	defer cancel()
	releaseRead := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseRead) })
	probe := &gcPublisherAdmissionBucket{
		Bucket: base, lockRead: make(chan struct{}, 1),
		releaseRead: releaseRead, headUpload: make(chan struct{}, 1),
	}
	publisher := NewManager(probe, "cluster", 1)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("early batch decision")); err != nil {
		t.Fatal(err)
	}
	firstTarget := core.Tip()
	firstDone := make(chan error, 1)
	go func() { firstDone <- publisher.SyncThrough(ctx, core, firstTarget) }()
	select {
	case <-probe.lockRead:
		// This real remote-lease read happens after flushBatch captured its
		// original target at the end of the group delay.
	case err := <-firstDone:
		t.Fatalf("first batch completed before admission was held: %v", err)
	case <-ctx.Done():
		t.Fatalf("first batch did not reach publication admission: %v", ctx.Err())
	}
	for i := 0; i < 16; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	lateTarget := core.Tip()
	waitTarget := func(want quepaxa.Slot) {
		t.Helper()
		for {
			publisher.batchMu.Lock()
			batch := publisher.batch
			joined := batch != nil && batch.target >= want
			publisher.batchMu.Unlock()
			if joined {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("late waiter did not join existing batch through %d: %v", want, ctx.Err())
			default:
				runtime.Gosched()
			}
		}
	}
	canceledCtx, cancelWaiter := context.WithCancel(ctx)
	canceledDone := make(chan error, 1)
	go func() { canceledDone <- publisher.SyncThrough(canceledCtx, core, firstTarget+1) }()
	waitTarget(firstTarget + 1)
	cancelWaiter()
	if err := <-canceledDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter result=%v, want context canceled", err)
	}
	lateDone := make(chan error, 1)
	go func() { lateDone <- publisher.SyncThrough(ctx, core, lateTarget) }()
	waitTarget(lateTarget)
	if got := probe.headCount.Load(); got != 0 {
		t.Fatalf("HEAD uploads before admission release=%d, want 0", got)
	}
	select {
	case err := <-firstDone:
		t.Fatalf("early waiter acknowledged before admission release: %v", err)
	case err := <-lateDone:
		t.Fatalf("late waiter acknowledged before admission release: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(releaseRead) })
	for name, done := range map[string]<-chan error{"early": firstDone, "late": lateDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s shared-batch waiter: %v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("%s shared-batch waiter after admission release: %v", name, ctx.Err())
		}
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := final.DecisionsFrom(ctx, 1, int(lateTarget))
	if err != nil || tip != lateTarget || len(values) != int(lateTarget) {
		t.Fatalf("independent shared-batch chain tip=%d want=%d values=%d err=%v", tip, lateTarget, len(values), err)
	}
	for i, got := range values {
		want, ok := core.CertifiedValue(quepaxa.Slot(i + 1))
		if !ok || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("independent shared-batch decision %d mismatched", i+1)
		}
	}
	if got := probe.headCount.Load(); got != 1 {
		t.Fatalf("actual HEAD uploads for late shared-batch target=%d, want exactly one", got)
	}
}

func testArchiveDirectSyncKeepsFixedTargetAndNoopFastPath(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	for _, value := range []string{"direct target", "not requested"} {
		if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	bucket := &countingBucket{Bucket: base}
	publisher := NewManager(bucket, "cluster", 1)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := publisher.syncNow(ctx, core, 2); err != nil {
		t.Fatal(err)
	}
	if tip := publisher.Tip(); tip != 2 {
		t.Fatalf("direct sync tip=%d, want fixed target 2 despite certified core tip %d", tip, core.Tip())
	}
	reads, uploads := bucket.heads.Load()+bucket.gets.Load(), bucket.puts.Load()
	if err := publisher.syncNow(ctx, core, 2); err != nil {
		t.Fatal(err)
	}
	if gotReads, gotUploads := bucket.heads.Load()+bucket.gets.Load(), bucket.puts.Load(); gotReads != reads || gotUploads != uploads {
		t.Fatalf("already-durable direct sync issued I/O: reads %d->%d uploads %d->%d", reads, gotReads, uploads, gotUploads)
	}
	independent := NewManager(base, "cluster", 1)
	defer independent.Close()
	if err := independent.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := independent.DecisionsFrom(ctx, 1, 2)
	if err != nil || tip != 2 || len(values) != 2 || !bytes.Equal(values[1].Value, []byte("direct target")) {
		t.Fatalf("fixed-target independent chain tip=%d values=%d err=%v", tip, len(values), err)
	}
}

func testArchivePublisherDuringGCDeleteScanKeepsReachableChain(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	if _, _, err := core.Propose(ctx, []byte("before compaction")); err != nil {
		t.Fatal(err)
	}
	if err := seed.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	publisher := NewManager(base, "cluster", 1)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("during candidate scan")); err != nil {
		t.Fatal(err)
	}
	wantTip := core.Tip()
	gc := NewManager(base, "cluster", 1)
	defer gc.Close()
	published := false
	gcCtx := localtesthooks.WithArchiveGCPhaseTrace(ctx, func(event string) {
		if event != "archive-gc:candidate-scan:begin" || published {
			return
		}
		published = true
		writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := publisher.SyncThrough(writeCtx, core, wantTip); err != nil {
			t.Errorf("publisher blocked behind post-publication deletion: %v", err)
		}
	})
	if err := gc.Cleanup(gcCtx, 0); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("candidate scan boundary not exercised")
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := final.DecisionsFrom(ctx, 1, int(wantTip))
	if err != nil || tip != wantTip || len(values) != int(wantTip) ||
		!bytes.Equal(values[len(values)-1].Value, []byte("during candidate scan")) {
		t.Fatalf("post-scan chain tip=%d want=%d decisions=%d err=%v", tip, wantTip, len(values), err)
	}
	for _, ref := range final.extents {
		if ok, err := base.Exists(ctx, final.key(extentObjectKey(ref.hash, ref.object))); err != nil || !ok {
			t.Fatalf("reachable extent lost during scan: exists=%t err=%v", ok, err)
		}
	}
}

func testArchiveNoCompactionKeepsConcurrentPublishedExtent(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	publisher := NewManager(base, "cluster", 1)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("published during no-op scan")); err != nil {
		t.Fatal(err)
	}
	wantTip := core.Tip()
	gc := NewManager(base, "cluster", 1)
	defer gc.Close()
	published := false
	publicationBegins := 0
	gcCtx := localtesthooks.WithArchiveGCPhaseTrace(ctx, func(event string) {
		if event == "archive-gc:publication:begin" {
			publicationBegins++
		}
		if event != "archive-gc:candidate-scan:begin" || published {
			return
		}
		published = true
		writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := publisher.SyncThrough(writeCtx, core, wantTip); err != nil {
			t.Errorf("publisher during no-compaction scan: %v", err)
		}
	})
	if err := gc.Cleanup(gcCtx, 0); err != nil {
		t.Fatal(err)
	}
	if !published || publicationBegins != 0 {
		t.Fatalf("no-compaction scan published=%t GC publication events=%d, want true/0", published, publicationBegins)
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := final.DecisionsFrom(ctx, 1, int(wantTip))
	if err != nil || tip != wantTip || len(values) != int(wantTip) ||
		!bytes.Equal(values[len(values)-1].Value, []byte("published during no-op scan")) {
		t.Fatalf("no-compaction final chain tip=%d want=%d decisions=%d err=%v", tip, wantTip, len(values), err)
	}
	for _, ref := range final.extents {
		if ok, err := base.Exists(ctx, final.key(extentObjectKey(ref.hash, ref.object))); err != nil || !ok {
			t.Fatalf("reachable no-compaction extent lost: exists=%t err=%v", ok, err)
		}
	}
}

func testArchivePublicationLeaseCancellationAndStaleOwner(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	key := holder.publicationLockKey()
	lease, err := holder.acquireArchiveLock(ctx, key, "test-holder", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.releaseArchiveLock(context.Background(), key, lease) }()

	waiter := NewManager(base, "cluster", 1)
	defer waiter.Close()
	entered := false
	if err := waiter.withPublicationLock(ctx, "test-waiter", func(context.Context) error {
		entered = true
		return nil
	}); !errors.Is(err, ErrArchiveBusy) || entered {
		t.Fatalf("deadline-less wait error=%v entered=%t, want immediate Busy", err, entered)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := waiter.withPublicationLock(cancelCtx, "test-waiter", func(context.Context) error {
		entered = true
		return nil
	}); !errors.Is(err, context.Canceled) || entered {
		t.Fatalf("pre-canceled wait error=%v entered=%t", err, entered)
	}
	cancelCtx, cancel = context.WithTimeout(ctx, time.Second)
	cancelBucket := &cancelPublicationReadBucket{Bucket: base, cancel: cancel}
	cancelWaiter := NewManager(cancelBucket, "cluster", 1)
	defer cancelWaiter.Close()
	if err := cancelWaiter.withPublicationLock(cancelCtx, "test-waiter", func(context.Context) error {
		entered = true
		return nil
	}); !errors.Is(err, context.Canceled) || entered {
		t.Fatalf("mid-admission cancellation error=%v entered=%t", err, entered)
	}
	cancel()

	if err := holder.releaseArchiveLock(ctx, key, lease); err != nil {
		t.Fatal(err)
	}
	if err := waiter.Load(ctx); err != nil {
		t.Fatal(err)
	}
	waiter.mu.Lock()
	head, version := waiter.head, waiter.headCAS
	waiter.mu.Unlock()
	var successor *archiveGCLock
	err = waiter.withPublicationLock(ctx, "old-owner", func(workCtx context.Context) error {
		current, err := waiter.readArchiveLock(ctx, key)
		if err != nil {
			return err
		}
		current.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
		if err := waiter.writeArchiveLock(ctx, key, *current, objstore.WithIfMatch(current.version)); err != nil {
			return err
		}
		successor, err = holder.acquireArchiveLock(ctx, key, "successor", archivePinLease)
		if err != nil {
			return err
		}
		return waiter.publishHead(workCtx, head, version)
	})
	if !errors.Is(err, ErrArchiveBusy) || successor == nil {
		t.Fatalf("stale owner publication error=%v successor=%v", err, successor)
	}
	if err := holder.releaseArchiveLock(ctx, key, successor); err != nil {
		t.Fatal(err)
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil || final.head.Generation != head.Generation || final.Tip() != head.Tip {
		t.Fatalf("stale owner changed HEAD: generation=%d tip=%d err=%v", final.head.Generation, final.Tip(), err)
	}
}

func testArchiveSharedBatchSurvivesOneCanceledWaiter(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	if _, _, err := core.Propose(ctx, []byte("shared batch tip")); err != nil {
		t.Fatal(err)
	}
	wantTip := core.Tip()
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	key := holder.publicationLockKey()
	lease, err := holder.acquireArchiveLock(ctx, key, "test-holder", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.releaseArchiveLock(ctx, key, lease) }()
	releaseRead := make(chan struct{})
	probe := &gcPublisherAdmissionBucket{
		Bucket: base, lockRead: make(chan struct{}, 1),
		releaseRead: releaseRead, headUpload: make(chan struct{}, 1),
	}
	publisher := NewManager(probe, "cluster", 1)
	defer publisher.Close()
	if err := publisher.Load(ctx); err != nil {
		t.Fatal(err)
	}
	firstCtx, cancelFirst := context.WithCancel(ctx)
	secondCtx, cancelSecond := context.WithTimeout(ctx, 2*time.Second)
	defer cancelSecond()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- publisher.SyncThrough(firstCtx, core, wantTip) }()
	select {
	case <-probe.lockRead:
	case err := <-firstDone:
		t.Fatalf("first waiter completed before admission: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("publisher did not reach publication admission")
	}
	go func() { secondDone <- publisher.SyncThrough(secondCtx, core, wantTip) }()
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error=%v", err)
	}
	select {
	case err := <-secondDone:
		t.Fatalf("second waiter acknowledged before durable publication: %v", err)
	default:
	}
	close(releaseRead)
	if err := holder.releaseArchiveLock(ctx, key, lease); err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("remaining waiter did not complete shared batch: %v", err)
	}
	if got := probe.headCount.Load(); got != 1 {
		t.Fatalf("shared batch HEAD uploads=%d, want one", got)
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil || final.Tip() != wantTip {
		t.Fatalf("shared batch durable tip=%d want=%d err=%v", final.Tip(), wantTip, err)
	}
}

func testArchiveCleanupWaitsForBoundedPublicationAdmission(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	if _, _, err := core.Propose(ctx, []byte("second extent")); err != nil {
		t.Fatal(err)
	}
	if err := seed.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	key := holder.publicationLockKey()
	lease, err := holder.acquireArchiveLock(ctx, key, "test-publisher", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.releaseArchiveLock(ctx, key, lease) }()
	releaseRead := make(chan struct{})
	probe := &gcPublisherAdmissionBucket{
		Bucket: base, lockRead: make(chan struct{}, 1),
		releaseRead: releaseRead, headUpload: make(chan struct{}, 1),
	}
	gc := NewManager(probe, "cluster", 1)
	defer gc.Close()
	gcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gc.Cleanup(gcCtx, time.Hour) }()
	select {
	case <-probe.lockRead:
	case err := <-done:
		t.Fatalf("Cleanup never reached publication admission: %v", err)
	case <-gcCtx.Done():
		t.Fatal(gcCtx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("Cleanup completed before publisher released admission: %v", err)
	default:
	}
	close(releaseRead)
	if err := holder.releaseArchiveLock(ctx, key, lease); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("bounded Cleanup admission: %v", err)
	}
	if got := probe.headCount.Load(); got != 1 {
		t.Fatalf("Cleanup HEAD attempts after admission=%d, want one", got)
	}
}

func (b *gcRetryBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if _, ok := b.oldKeys[name]; ok && b.compacting {
		b.oldKeys[name]++
	}
	return b.Bucket.Get(ctx, name)
}

func (b *gcRetryBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if b.uploadPrefix != "" && strings.HasPrefix(name, b.uploadPrefix) {
		b.prefixUploads++
	}
	if strings.HasSuffix(name, "/archive/head.bin") {
		b.headAttempts++
		if b.headAttempts == 1 {
			if err := b.appendTip(); err != nil {
				return err
			}
			err := b.Bucket.Upload(ctx, name, r, options...)
			b.conflict = err
			if b.afterConflict != nil {
				b.afterConflict()
			}
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func testArchiveCleanupReusesAcknowledgedCompleteUploadedPrefixAfterRealConflict(t *testing.T) {
	ctx, base, core, writer := newSealableArchive(t)
	defer writer.Close()
	if !writer.CASSupported() {
		t.Fatal("in-memory archive must support conditional HEAD publication")
	}
	// More than maxExtentItems decisions force a complete packed output group
	// before the mutable tail. Periodic publication leaves multiple input refs.
	for i := 2; i <= maxExtentItems+1; i++ {
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
		if i%256 == 0 || i == maxExtentItems+1 {
			if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
				t.Fatal(err)
			}
		}
	}
	probe := NewManager(base, "cluster", 1)
	defer probe.Close()
	if err := probe.Load(ctx); err != nil {
		t.Fatal(err)
	}
	packed, err := probe.compactExtents(ctx, probe.extents, probe.head.BasePrefix)
	if err != nil || len(packed) < 2 {
		t.Fatalf("complete packed prefix unavailable: groups=%d err=%v", len(packed), err)
	}
	firstData, err := encodeExtent(packed[0])
	if err != nil {
		t.Fatal(err)
	}
	firstHash := sha256.Sum256(firstData)
	if _, _, err := core.Propose(ctx, []byte("racing-tail")); err != nil {
		t.Fatal(err)
	}
	wantTip := core.Tip()
	bucket := &gcRetryBucket{
		Bucket:       base,
		uploadPrefix: probe.key(fmt.Sprintf("archive/blocks/%x_", firstHash)),
	}
	bucket.appendTip = func() error { return publishLegacyArchiveTip(ctx, base, writer, core, wantTip) }
	gc := NewManager(bucket, "cluster", 1)
	defer gc.Close()
	if err := gc.Cleanup(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if bucket.headAttempts != 2 || !base.IsConditionNotMetErr(bucket.conflict) {
		t.Fatalf("real conditional HEAD conflict absent: attempts=%d conflict=%v", bucket.headAttempts, bucket.conflict)
	}
	if bucket.prefixUploads != 1 {
		t.Fatalf("complete immutable prefix uploaded %d times across one real retry, want once", bucket.prefixUploads)
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if len(final.extents) < 2 || final.extents[0].hash != firstHash || final.extents[0].object != probe.head.Generation+1 {
		t.Fatalf("independently loaded complete prefix lost acknowledged identity: refs=%d", len(final.extents))
	}
	if final.extents[1].PreviousHash != firstHash || final.extents[1].PreviousObject != final.extents[0].object ||
		final.extents[1].object != final.head.Generation {
		t.Fatal("fresh tail does not link to retained (hash, object) with fresh generation")
	}
	values, tip, err := final.DecisionsFrom(ctx, 1, int(wantTip))
	if err != nil || tip != wantTip || len(values) != int(wantTip) || !bytes.Equal(values[len(values)-1].Value, []byte("racing-tail")) {
		t.Fatalf("final chain tip=%d want=%d decisions=%d err=%v", tip, wantTip, len(values), err)
	}
	for _, ref := range probe.extents {
		name := probe.key(extentObjectKey(ref.hash, ref.object))
		if exists, err := base.Exists(ctx, name); err != nil || !exists {
			t.Errorf("preexisting immutable block lost: exists=%t err=%v", exists, err)
		}
	}
}

type gcAmbiguousHeadBucket struct {
	objstore.Bucket
	headUploads int
	failedOnce  bool
}

type gcCanceledHeadBucket struct {
	objstore.Bucket
	cancel  context.CancelFunc
	uploads int
}

type gcUnacknowledgedBlockBucket struct {
	objstore.Bucket
	failed      bool
	headUploads int
}

func (b *gcUnacknowledgedBlockBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.Contains(name, "/archive/blocks/") && !b.failed {
		b.failed = true
		return syscall.ECONNRESET
	}
	if strings.HasSuffix(name, "/archive/head.bin") {
		b.headUploads++
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func (b *gcCanceledHeadBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/archive/head.bin") {
		b.uploads++
		b.cancel()
		return ctx.Err()
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func (b *gcAmbiguousHeadBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	err := b.Bucket.Upload(ctx, name, r, options...)
	if strings.HasSuffix(name, "/archive/head.bin") {
		b.headUploads++
		if err == nil && !b.failedOnce {
			b.failedOnce = true
			return errors.New("synthetic lost HEAD upload acknowledgment")
		}
	}
	return err
}

func TestArchiveCleanupReusesUnchangedPrefixAfterRealHeadConflict(t *testing.T) {
	t.Run("independent_publisher_waits_for_gc_publication_lease", testArchiveIndependentPublisherWaitsForGCPublicationLease)
	t.Run("late_shared_batch_target_uses_one_publication_admission", testArchiveLateSharedBatchTargetUsesOnePublicationAdmission)
	t.Run("direct_sync_keeps_fixed_target_and_noop_fast_path", testArchiveDirectSyncKeepsFixedTargetAndNoopFastPath)
	t.Run("publisher_during_gc_delete_scan_keeps_chain", testArchivePublisherDuringGCDeleteScanKeepsReachableChain)
	t.Run("no_compaction_keeps_concurrent_published_extent", testArchiveNoCompactionKeepsConcurrentPublishedExtent)
	t.Run("publication_lease_cancel_and_stale_owner", testArchivePublicationLeaseCancellationAndStaleOwner)
	t.Run("shared_batch_survives_canceled_waiter", testArchiveSharedBatchSurvivesOneCanceledWaiter)
	t.Run("cleanup_waits_for_bounded_publication_admission", testArchiveCleanupWaitsForBoundedPublicationAdmission)
	t.Run("acknowledged_uploaded_prefix", testArchiveCleanupReusesAcknowledgedCompleteUploadedPrefixAfterRealConflict)
	t.Run("upload_identity_fallback", testArchiveCleanupUploadIdentityFallback)
	t.Run("unacknowledged_upload", testArchiveCleanupUnacknowledgedUploadDoesNotPublish)
	ctx, base, core, writer := newSealableArchive(t)
	defer writer.Close()
	if !writer.CASSupported() {
		t.Fatal("in-memory archive must support conditional HEAD publication")
	}
	// Separate SyncThrough calls produce more immutable input blocks than the
	// manager cache can retain. The sixth decision is held for the racing writer.
	for i := 2; i <= maxCachedExtents+4; i++ {
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	initialTip := core.Tip()
	reader := NewManager(base, "cluster", 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if len(reader.extents) <= maxCachedExtents {
		t.Fatalf("initial refs=%d, cache limit=%d", len(reader.extents), maxCachedExtents)
	}
	oldKeys := make(map[string]int, len(reader.extents))
	for _, ref := range reader.extents {
		oldKeys[reader.key(extentObjectKey(ref.hash, ref.object))] = 0
	}
	initialGeneration := reader.head.Generation
	if _, _, err := core.Propose(ctx, []byte("racing-tip")); err != nil {
		t.Fatal(err)
	}
	wantTip := core.Tip()
	if wantTip != initialTip+1 {
		t.Fatalf("racing tip=%d, initial=%d", wantTip, initialTip)
	}

	bucket := &gcRetryBucket{Bucket: base, oldKeys: oldKeys}
	bucket.appendTip = func() error { return publishLegacyArchiveTip(ctx, base, writer, core, wantTip) }
	gc := NewManager(bucket, "cluster", 1)
	defer gc.Close()
	events := make(map[string]int)
	tracedCtx := localtesthooks.WithArchiveGCPhaseTrace(ctx, func(event string) {
		events[event]++
		switch event {
		case "archive-gc:compaction:begin":
			bucket.compacting = true
		case "archive-gc:compaction:success", "archive-gc:compaction:error":
			bucket.compacting = false
		}
	})
	if err := gc.Cleanup(tracedCtx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if bucket.headAttempts < 2 || !base.IsConditionNotMetErr(bucket.conflict) {
		t.Fatalf("real conditional race not observed: attempts=%d conflict=%v", bucket.headAttempts, bucket.conflict)
	}
	if events["archive-gc:publication-result:typed_condition"] != 1 || events["archive-gc:publication-result:success"] != 1 ||
		events["archive-gc:compaction-choice:full"] != 1 || events["archive-gc:compaction-choice:reuse"] != 2 {
		t.Fatalf("actual conflict/reuse classification=%v", events)
	}
	for key, gets := range oldKeys {
		if gets > 1 {
			t.Errorf("unchanged immutable block %q fetched %d times during compaction across GC retry, want at most once", key, gets)
		}
	}

	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if final.head.Generation <= initialGeneration || final.Tip() != wantTip {
		t.Fatalf("final generation=%d (initial %d), tip=%d (want %d)", final.head.Generation, initialGeneration, final.Tip(), wantTip)
	}
	values, tip, err := final.DecisionsFrom(ctx, 1, int(wantTip))
	if err != nil || tip != wantTip || len(values) != int(wantTip) {
		t.Fatalf("final chain tip=%d, decisions=%d, err=%v", tip, len(values), err)
	}
	if !bytes.Equal(values[len(values)-1].Value, []byte("racing-tip")) {
		t.Fatalf("final tip value=%q", values[len(values)-1].Value)
	}
	for _, ref := range reader.extents {
		name := reader.key(extentObjectKey(ref.hash, ref.object))
		if exists, err := base.Exists(ctx, name); err != nil || !exists {
			t.Errorf("preexisting immutable block lost: exists=%t err=%v", exists, err)
		}
	}
	if prefix, ok := core.PrefixHash(quepaxa.Slot(wantTip)); !ok || final.extents[len(final.extents)-1].EndPrefix != prefix {
		t.Fatalf("final chain prefix differs from source: available=%t", ok)
	}
}

func testArchiveCleanupUploadIdentityFallback(t *testing.T) {
	value := []byte("one")
	valueHash := sha256.Sum256(value)
	candidate := Extent{
		ConfigID: 1, Start: 1, End: 1,
		EndPrefix: quepaxa.AdvancePrefixHash([32]byte{}, 1, valueHash),
		Decisions: []quepaxa.DecidedValue{{Slot: 1, Value: value, Hash: valueHash, Certificate: []byte{1}}},
	}
	data, err := encodeExtent(candidate)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	saved := candidate
	saved.hash, saved.object = hash, 7
	if !canReuseCleanupUpload(hash, saved, 7) || !canReuseCleanupUpload(hash, saved, 8) {
		t.Fatal("acknowledged same-byte object was not eligible at its generation or later")
	}
	if canReuseCleanupUpload(hash, saved, 6) {
		t.Fatal("future-generation object reused against older HEAD")
	}
	saved.object = 0
	if canReuseCleanupUpload(hash, saved, 8) {
		t.Fatal("missing object identity reused")
	}
	saved.object = 7
	candidate.PreviousHash, candidate.PreviousObject = [32]byte{1}, 6
	data, err = encodeExtent(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if canReuseCleanupUpload(sha256.Sum256(data), saved, 8) {
		t.Fatal("different actual predecessor reused prior encoded object")
	}
}

func testArchiveCleanupUnacknowledgedUploadDoesNotPublish(t *testing.T) {
	ctx, base, core, writer := newSealableArchive(t)
	defer writer.Close()
	if _, _, err := core.Propose(ctx, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	bucket := &gcUnacknowledgedBlockBucket{Bucket: base}
	gc := NewManager(bucket, "cluster", 1)
	defer gc.Close()
	if err := gc.Cleanup(ctx, time.Hour); !errors.Is(err, syscall.ECONNRESET) || !bucket.failed || bucket.headUploads != 0 {
		t.Fatalf("unacknowledged upload err=%v failed=%t HEAD uploads=%d", err, bucket.failed, bucket.headUploads)
	}
	remote := NewManager(base, "cluster", 1)
	defer remote.Close()
	if err := remote.Load(ctx); err != nil || remote.Tip() != core.Tip() {
		t.Fatalf("unacknowledged cleanup changed reachable archive: tip=%d err=%v", remote.Tip(), err)
	}
}

func TestArchiveCleanupReuseRequiresExactBaseAndRefPrefix(t *testing.T) {
	base := archiveHead{
		ConfigID: 1, Base: 1, BasePrefix: [32]byte{1},
		BaseSeal:      &quepaxa.CheckpointSeal{Index: 1, PrefixHash: [32]byte{1}},
		BaseDecision:  &quepaxa.DecidedValue{Slot: 1, Value: []byte{1}},
		BaseAnchor:    &archiveAnchorRef{Hash: [32]byte{2}},
		LineageAnchor: &archiveAnchorRef{Hash: [32]byte{3}},
	}
	old := []Extent{{ConfigID: 1, Start: 2, End: 2, StartPrefix: [32]byte{4}, EndPrefix: [32]byte{5}, PreviousHash: [32]byte{6}, PreviousObject: 7, hash: [32]byte{8}, object: 9}}
	packed := []Extent{{Decisions: []quepaxa.DecidedValue{{Slot: 2}}}}
	if !canReuseCleanupCompaction(context.Background(), 1, base, base, old, old, packed) {
		t.Fatal("unchanged validated base and ref prefix was not reusable")
	}
	for _, tc := range []struct {
		name   string
		change func(*archiveHead, *[]Extent)
	}{
		{"config", func(h *archiveHead, _ *[]Extent) { h.ConfigID++ }},
		{"base", func(h *archiveHead, _ *[]Extent) { h.Base++ }},
		{"base_prefix", func(h *archiveHead, _ *[]Extent) { h.BasePrefix[0]++ }},
		{"base_seal", func(h *archiveHead, _ *[]Extent) { h.BaseSeal = nil }},
		{"base_decision", func(h *archiveHead, _ *[]Extent) { h.BaseDecision = nil }},
		{"base_anchor", func(h *archiveHead, _ *[]Extent) { h.BaseAnchor = nil }},
		{"lineage_anchor", func(h *archiveHead, _ *[]Extent) { h.LineageAnchor = nil }},
		{"sealed", func(h *archiveHead, _ *[]Extent) { h.Sealed = true }},
		{"removed_ref", func(_ *archiveHead, refs *[]Extent) { *refs = nil }},
		{"ref_config", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].ConfigID++ }},
		{"ref_start", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].Start++ }},
		{"ref_end", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].End++ }},
		{"ref_start_prefix", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].StartPrefix[0]++ }},
		{"ref_end_prefix", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].EndPrefix[0]++ }},
		{"ref_previous_hash", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].PreviousHash[0]++ }},
		{"ref_previous_object", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].PreviousObject++ }},
		{"ref_hash", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].hash[0]++ }},
		{"ref_object", func(_ *archiveHead, refs *[]Extent) { (*refs)[0].object++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			head, refs := base, append([]Extent(nil), old...)
			tc.change(&head, &refs)
			if canReuseCleanupCompaction(context.Background(), 1, head, base, refs, old, packed) {
				t.Fatal("changed recovery base or immutable ref reused prior packing")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if canReuseCleanupCompaction(ctx, 1, base, base, old, old, packed) ||
		canReuseCleanupCompaction(context.Background(), 1, base, base, old, old, nil) {
		t.Fatal("canceled or absent prior compaction was reused")
	}
}

func TestArchiveCleanupIncrementalPackingMatchesFullAtBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		n, size int
	}{
		{name: "append_to_tail", n: 1023, size: 1},
		{name: "count_boundary", n: 1024, size: 1},
		{name: "byte_boundary", n: 1024, size: 8 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
			defer manager.Close()
			var refs []Extent
			var prefix [32]byte
			starts := []int{0, tc.n / 2, tc.n, tc.n + 1}
			for group := 0; group < 3; group++ {
				extent := Extent{ConfigID: 1, Start: quepaxa.Slot(starts[group] + 1), StartPrefix: prefix}
				for i := starts[group]; i < starts[group+1]; i++ {
					value := bytes.Repeat([]byte{byte(i%251 + 1)}, tc.size)
					decision := quepaxa.DecidedValue{Slot: quepaxa.Slot(i + 1), Value: value, Hash: sha256.Sum256(value), Certificate: []byte{1}}
					extent.Decisions = append(extent.Decisions, decision)
					prefix = quepaxa.AdvancePrefixHash(prefix, decision.Slot, decision.Hash)
				}
				extent.End, extent.EndPrefix = quepaxa.Slot(starts[group+1]), prefix
				extent.hash, extent.object = sha256.Sum256([]byte{byte(group + 1)}), uint64(group+1)
				ref := extent
				ref.Decisions = nil
				refs = append(refs, ref)
				manager.cache[extentObject{hash: extent.hash, id: extent.object}] = extent
			}
			prior, err := manager.compactExtents(context.Background(), refs[:2], [32]byte{})
			if err != nil {
				t.Fatal(err)
			}
			incremental, err := manager.compactExtentsFrom(context.Background(), refs[2:], [32]byte{}, prior)
			if err != nil {
				t.Fatal(err)
			}
			full, err := manager.compactExtents(context.Background(), refs, [32]byte{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(incremental, full) {
				t.Fatalf("incremental packing differs from full: groups=%d want=%d", len(incremental), len(full))
			}
			for _, extent := range incremental {
				encoded, err := encodeExtent(extent)
				if err != nil || len(encoded) > maxExtentSize {
					t.Fatalf("invalid packed extent bytes=%d err=%v", len(encoded), err)
				}
			}
		})
	}
}

func TestArchiveCleanupAmbiguousPublicationReloadsCommittedHead(t *testing.T) {
	ctx, base, core, writer := newSealableArchive(t)
	defer writer.Close()
	for i := 0; i < 2; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	bucket := &gcAmbiguousHeadBucket{Bucket: base}
	gc := NewManager(bucket, "cluster", 1)
	defer gc.Close()
	events := make(map[string]int)
	tracedCtx := localtesthooks.WithArchiveGCPhaseTrace(ctx, func(event string) { events[event]++ })
	if err := gc.Cleanup(tracedCtx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if !bucket.failedOnce || bucket.headUploads != 1 {
		t.Fatalf("ambiguous publication=%t head uploads=%d, want one committed upload followed by reload", bucket.failedOnce, bucket.headUploads)
	}
	if events["archive-gc:publication-result:other"] != 1 || events["archive-gc:compaction-choice:full"] != 2 || events["archive-gc:compaction-choice:reuse"] != 1 {
		t.Fatalf("ambiguous committed publication classification=%v", events)
	}
	remote := NewManager(base, "cluster", 1)
	defer remote.Close()
	if err := remote.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := remote.DecisionsFrom(ctx, 1, int(core.Tip()))
	if err != nil || tip != core.Tip() || len(values) != int(tip) {
		t.Fatalf("ambiguous publication lost committed chain: tip=%d values=%d err=%v", tip, len(values), err)
	}
}

func TestArchiveCleanupCanceledAfterHeadConflictDoesNotRepublish(t *testing.T) {
	ctx, base, core, writer := newSealableArchive(t)
	defer writer.Close()
	for i := 0; i < 2; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := core.Propose(ctx, []byte("racing-tip")); err != nil {
		t.Fatal(err)
	}
	cleanupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	bucket := &gcRetryBucket{Bucket: base, oldKeys: make(map[string]int)}
	bucket.appendTip = func() error { return publishLegacyArchiveTip(ctx, base, writer, core, core.Tip()) }
	bucket.afterConflict = cancel
	gc := NewManager(bucket, "cluster", 1)
	defer gc.Close()
	events := make(map[string]int)
	tracedCtx := localtesthooks.WithArchiveGCPhaseTrace(cleanupCtx, func(event string) { events[event]++ })
	err := gc.Cleanup(tracedCtx, time.Hour)
	if !errors.Is(err, context.Canceled) || !base.IsConditionNotMetErr(bucket.conflict) || bucket.headAttempts != 1 {
		t.Fatalf("canceled cleanup error=%v condition=%v head uploads=%d", err, bucket.conflict, bucket.headAttempts)
	}
	if events["archive-gc:publication-result:typed_condition"] != 1 || events["archive-gc:publication-result:context_done"] != 0 {
		t.Fatalf("post-return cancellation reclassified typed condition: %v", events)
	}
	remote := NewManager(base, "cluster", 1)
	defer remote.Close()
	if err := remote.Load(ctx); err != nil || remote.Tip() != core.Tip() {
		t.Fatalf("racing writer tip after canceled GC=%d want=%d err=%v", remote.Tip(), core.Tip(), err)
	}
	t.Run("returned_context_cause", testArchiveCleanupPublicationReturningContextCauseIsDistinct)
}

func testArchiveCleanupPublicationReturningContextCauseIsDistinct(t *testing.T) {
	ctx, base, core, writer := newSealableArchive(t)
	defer writer.Close()
	for i := 0; i < 2; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	cleanupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	bucket := &gcCanceledHeadBucket{Bucket: base, cancel: cancel}
	gc := NewManager(bucket, "cluster", 1)
	defer gc.Close()
	events := make(map[string]int)
	tracedCtx := localtesthooks.WithArchiveGCPhaseTrace(cleanupCtx, func(event string) { events[event]++ })
	err := gc.Cleanup(tracedCtx, time.Hour)
	if !errors.Is(err, context.Canceled) || bucket.uploads != 1 {
		t.Fatalf("publication cancellation error=%v head uploads=%d", err, bucket.uploads)
	}
	if events["archive-gc:publication-result:context_done"] != 1 || events["archive-gc:publication-result:typed_condition"] != 0 ||
		events["archive-gc:compaction-choice:full"] != 1 {
		t.Fatalf("returned context cause classification=%v", events)
	}
}
