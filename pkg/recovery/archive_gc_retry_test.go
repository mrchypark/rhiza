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

// conditionalArchiveLockBucket makes an independent, valid lock generation
// win each of the caller's conditional writes. The bucket still returns the
// real conditional error; no Busy error is fabricated by this wrapper.
type conditionalArchiveLockBucket struct {
	objstore.Bucket
	key    string
	before func(context.Context) error
	count  int
}

func (b *conditionalArchiveLockBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if name == b.key {
		if err := b.before(ctx); err != nil {
			return err
		}
		b.count++
	}
	return b.Bucket.Upload(ctx, name, r, options...)
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
	var busyEvents []localtesthooks.ArchiveBusyEvent
	trace := func(event localtesthooks.ArchiveBusyEvent) { busyEvents = append(busyEvents, event) }
	tracedCtx := localtesthooks.WithArchiveBusyTrace(ctx, trace)
	if err := waiter.withPublicationLock(tracedCtx, "test-waiter", func(context.Context) error {
		entered = true
		return nil
	}); !errors.Is(err, ErrArchiveBusy) || entered {
		t.Fatalf("deadline-less wait error=%v entered=%t, want immediate Busy", err, entered)
	}
	if len(busyEvents) != 2 || busyEvents[0].Resource != "publication_lock" || busyEvents[0].Branch != "active_live_lease" || busyEvents[0].Entered ||
		busyEvents[1].Stage != "terminal" || busyEvents[1].Entered {
		t.Fatalf("deadline-less exact Busy boundaries=%+v", busyEvents)
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
	busyEvents = nil
	cancelCtx = localtesthooks.WithArchiveBusyTrace(cancelCtx, trace)
	cancelBucket := &cancelPublicationReadBucket{Bucket: base, cancel: cancel}
	cancelWaiter := NewManager(cancelBucket, "cluster", 1)
	defer cancelWaiter.Close()
	if err := cancelWaiter.withPublicationLock(cancelCtx, "test-waiter", func(context.Context) error {
		entered = true
		return nil
	}); !errors.Is(err, context.Canceled) || entered {
		t.Fatalf("mid-admission cancellation error=%v entered=%t", err, entered)
	}
	for _, event := range busyEvents {
		if event.Stage == "terminal" {
			t.Fatalf("canceled deadline-bound admission falsely recorded returned Busy: %+v", busyEvents)
		}
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
	busyEvents = nil
	err = waiter.withPublicationLock(tracedCtx, "old-owner", func(workCtx context.Context) error {
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
	var sawPostEntry, sawTerminal bool
	for _, event := range busyEvents {
		if event.Branch == "confirm_mismatch_or_expired" && event.Entered {
			sawPostEntry = true
		}
		if event.Stage == "terminal" && event.Entered {
			sawTerminal = true
		}
	}
	if !sawPostEntry || !sawTerminal {
		t.Fatalf("post-entry stale-owner Busy boundaries=%+v", busyEvents)
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

func testArchiveBusyTraceRealGCLockAndNonBusy(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	lease, err := holder.acquireGCLock(ctx, "test-holder", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.releaseGCLock(context.Background(), lease) }()
	waiter := NewManager(base, "cluster", 1)
	defer waiter.Close()
	var events []localtesthooks.ArchiveBusyEvent
	traced := localtesthooks.WithArchiveBusyTrace(ctx, func(event localtesthooks.ArchiveBusyEvent) {
		events = append(events, event)
	})
	if err := waiter.Cleanup(traced, 24*time.Hour); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("real live GC lease: error=%v, want Busy", err)
	}
	if len(events) != 2 || events[0].Resource != "gc_lock" || events[0].Branch != "active_live_lease" || events[0].Entered ||
		events[1].Stage != "terminal" || events[1].Entered {
		t.Fatalf("real GC-lock boundaries=%+v", events)
	}
	if err := holder.releaseGCLock(ctx, lease); err != nil {
		t.Fatal(err)
	}
	events = nil
	if err := waiter.Cleanup(traced, 24*time.Hour); err != nil {
		t.Fatalf("cleanup after confirmed release: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("successful cleanup emitted Busy: %+v", events)
	}
	final := NewManager(base, "cluster", 1)
	defer final.Close()
	if err := final.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := final.DecisionsFrom(ctx, 1, int(core.Tip()))
	if err != nil || tip != core.Tip() || len(values) != int(core.Tip()) {
		t.Fatalf("independent archive read after GC Busy: tip=%d want=%d values=%d err=%v", tip, core.Tip(), len(values), err)
	}
	if err := waiter.withPublicationLock(traced, "test-negative", func(context.Context) error {
		return fmt.Errorf("ordinary negative control")
	}); err == nil || errors.Is(err, ErrArchiveBusy) || len(events) != 0 {
		t.Fatalf("non-Busy return rewritten or observed: err=%v events=%+v", err, events)
	}
}

func testArchiveActiveGCLockSubtypeTrim(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	sealed, decision := realTrimFixture(t, ctx, core, seed)
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	lease, err := holder.acquireGCLock(ctx, "test-holder", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.releaseGCLock(context.Background(), lease) }()
	trimmer := NewManager(base, "cluster", 1)
	defer trimmer.Close()
	if err := trimmer.TrimThrough(ctx, sealed, decision); err != ErrActiveGCLock || !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("real pre-entry live GC lease: error=%v, want exact subtype and Busy compatibility", err)
	}
	independent := NewManager(base, "cluster", 1)
	defer independent.Close()
	if err := independent.Load(ctx); err != nil || independent.head.Base != 0 {
		t.Fatalf("trim changed HEAD while GC lease held: base=%d err=%v", independent.head.Base, err)
	}
	if err := holder.releaseGCLock(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err := trimmer.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatalf("real trim after confirmed GC release: %v", err)
	}
	if err := independent.Load(ctx); err != nil || independent.head.Base != sealed.Index {
		t.Fatalf("trim did not advance after release: base=%d want=%d err=%v", independent.head.Base, sealed.Index, err)
	}
}

func testArchiveBusyTraceRealConditionalExhaustion(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	lease, err := holder.acquireGCLock(ctx, "initial", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.releaseGCLock(ctx, lease); err != nil {
		t.Fatal(err)
	}
	key := holder.gcLockKey()
	bucket := &conditionalArchiveLockBucket{Bucket: base, key: key}
	bucket.before = func(ctx context.Context) error {
		current, err := holder.readArchiveLock(ctx, key)
		if err != nil {
			return err
		}
		current.Generation++
		current.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
		return holder.writeArchiveLock(ctx, key, *current, objstore.WithIfMatch(current.version))
	}
	waiter := NewManager(bucket, "cluster", 1)
	defer waiter.Close()
	var events []localtesthooks.ArchiveBusyEvent
	traced := localtesthooks.WithArchiveBusyTrace(ctx, func(event localtesthooks.ArchiveBusyEvent) {
		events = append(events, event)
	})
	if _, err := waiter.acquireGCLock(traced, "loser", archivePinLease); !errors.Is(err, ErrArchiveBusy) || err == ErrActiveGCLock {
		t.Fatalf("actual conditional exhaustion error=%v, want terminal generic Busy", err)
	}
	if bucket.count != maxPublishRetries || len(events) != 1 || events[0].Resource != "gc_lock" ||
		events[0].Branch != "conditional_exhausted" || events[0].Entered {
		t.Fatalf("conditional conflicts=%d events=%+v", bucket.count, events)
	}
	independent := NewManager(base, "cluster", 1)
	defer independent.Close()
	if err := independent.Load(ctx); err != nil || independent.head.Generation != seed.head.Generation {
		t.Fatalf("independent HEAD after lock conflicts: generation=%d want=%d err=%v", independent.head.Generation, seed.head.Generation, err)
	}
}

// realTrimFixture proposes a real certified checkpoint and syncs it so the
// returned seal and decision drive the real TrimThrough payload validation.
func realTrimFixture(t *testing.T, ctx context.Context, core *quepaxa.Core, seed *Manager) (quepaxa.SealedCheckpoint, quepaxa.DecidedValue) {
	t.Helper()
	for i := 0; i < 2; i++ {
		if _, _, err := core.Propose(ctx, []byte{byte(i + 2)}); err != nil {
			t.Fatal(err)
		}
	}
	prefix, ok := core.PrefixHash(2)
	if !ok {
		t.Fatal("missing checkpoint prefix")
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: 2, RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: prefix,
		NextLeaderOrder: []quepaxa.NodeID{"n1"},
	}
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing certified checkpoint decision")
	}
	if err := seed.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	return quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: slot}, decision
}

// A real live recovery pin must stop TrimThrough before any archive work and
// leave HEAD unchanged. Releasing the pin must let the same real trim proceed.
func testArchiveBusyTraceRealRecoveryPinAndRelease(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	pinOwner := "test-recovery-pin"
	pinKey := seed.recoveryPinKey(pinOwner)
	// A pin with Tip past Base must carry a non-zero tail hash/object pair.
	pin := archiveRecoveryPin{OwnerID: pinOwner, Token: "token", Base: 0, Tip: 1, TailHash: [32]byte{3}, TailObject: 1, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
	if err := seed.writeRecoveryPin(ctx, pinKey, pin); err != nil {
		t.Fatal(err)
	}
	sealed, decision := realTrimFixture(t, ctx, core, seed)
	waiter := NewManager(base, "cluster", 1)
	defer waiter.Close()
	var events []localtesthooks.ArchiveBusyEvent
	traced := localtesthooks.WithArchiveBusyTrace(ctx, func(event localtesthooks.ArchiveBusyEvent) {
		events = append(events, event)
	})
	if err := waiter.TrimThrough(traced, sealed, decision); !errors.Is(err, ErrArchiveBusy) || err == ErrArchiveBusy {
		t.Fatalf("live recovery pin: error=%v, want a distinct active-pin Busy preserving ErrArchiveBusy", err)
	}
	var sawPin bool
	for _, event := range events {
		if event.Resource == "recovery_pin" && event.Branch == "active_recovery_pin" && event.Entered {
			sawPin = true
		}
	}
	if !sawPin {
		t.Fatalf("live recovery pin boundaries=%+v", events)
	}
	independent := NewManager(base, "cluster", 1)
	defer independent.Close()
	if err := independent.Load(ctx); err != nil || independent.head.Generation != seed.head.Generation {
		t.Fatalf("HEAD changed under live recovery pin: generation=%d want=%d err=%v", independent.head.Generation, seed.head.Generation, err)
	}
	events = nil
	current, err := seed.readRecoveryPin(ctx, pinKey)
	if err != nil {
		t.Fatal(err)
	}
	current.LeaseUntilMS = 0
	if err := seed.writeRecoveryPin(ctx, pinKey, *current, objstore.WithIfMatch(current.version)); err != nil {
		t.Fatal(err)
	}
	if err := waiter.TrimThrough(traced, sealed, decision); err != nil {
		t.Fatalf("trim after confirmed pin release: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("successful trim emitted Busy: %+v", events)
	}
}

func testArchiveActivePinWithReleaseFailureIsTerminal(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	pinKey := seed.recoveryPinKey("release-conflict-reader")
	pin := archiveRecoveryPin{OwnerID: "release-conflict-reader", Token: "token", Base: 0, Tip: 1, TailHash: [32]byte{3}, TailObject: 1, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
	if err := seed.writeRecoveryPin(ctx, pinKey, pin); err != nil {
		t.Fatal(err)
	}
	sealed, decision := realTrimFixture(t, ctx, core, seed)
	waiter := NewManager(base, "cluster", 1)
	defer waiter.Close()
	var injectionErr error
	traced := localtesthooks.WithArchiveBusyTrace(ctx, func(event localtesthooks.ArchiveBusyEvent) {
		if event.Resource != "recovery_pin" || event.Branch != "active_recovery_pin" {
			return
		}
		key := waiter.gcLockKey()
		lease, err := waiter.readArchiveLock(ctx, key)
		if err != nil {
			injectionErr = err
			return
		}
		lease.OwnerID = "successor"
		lease.Generation++
		injectionErr = waiter.writeArchiveLock(ctx, key, *lease, objstore.WithIfMatch(lease.version))
	})
	err := waiter.TrimThrough(traced, sealed, decision)
	if injectionErr != nil || err == ErrActiveRecoveryPin || !errors.Is(err, ErrActiveRecoveryPin) || !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("active pin plus failed GC release must be terminal: error=%v injection=%v", err, injectionErr)
	}
}

func testArchiveActivePinWithCancellationIsTerminal(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	pinKey := seed.recoveryPinKey("canceled-reader")
	pin := archiveRecoveryPin{OwnerID: "canceled-reader", Token: "token", Base: 0, Tip: 1, TailHash: [32]byte{3}, TailObject: 1, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
	if err := seed.writeRecoveryPin(ctx, pinKey, pin); err != nil {
		t.Fatal(err)
	}
	sealed, decision := realTrimFixture(t, ctx, core, seed)
	waiter := NewManager(base, "cluster", 1)
	defer waiter.Close()
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	traced := localtesthooks.WithArchiveBusyTrace(operationCtx, func(event localtesthooks.ArchiveBusyEvent) {
		if event.Resource == "recovery_pin" && event.Branch == "active_recovery_pin" {
			cancel()
		}
	})
	err := waiter.TrimThrough(traced, sealed, decision)
	if err == ErrActiveRecoveryPin || !errors.Is(err, ErrActiveRecoveryPin) || !errors.Is(err, context.Canceled) {
		t.Fatalf("active pin plus canceled operation must be terminal: %v", err)
	}
}

// A release whose stored owner/generation no longer matches is a terminal
// post-entry Busy and must be reported, never replayed.
func testArchiveBusyTraceRealReleaseConflict(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	holder := NewManager(base, "cluster", 1)
	defer holder.Close()
	lease, err := holder.acquireGCLock(ctx, "test-holder", archivePinLease)
	if err != nil {
		t.Fatal(err)
	}
	key := holder.gcLockKey()
	stolen, err := holder.readArchiveLock(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	stolen.OwnerID = "successor"
	stolen.Generation = lease.Generation + 1
	if err := holder.writeArchiveLock(ctx, key, *stolen, objstore.WithIfMatch(stolen.version)); err != nil {
		t.Fatal(err)
	}
	var events []localtesthooks.ArchiveBusyEvent
	traced := localtesthooks.WithArchiveBusyTrace(ctx, func(event localtesthooks.ArchiveBusyEvent) {
		events = append(events, event)
	})
	if err := holder.releaseGCLock(traced, lease); !errors.Is(err, ErrArchiveBusy) || err == ErrActiveGCLock {
		t.Fatalf("stale-owner release error=%v, want terminal generic Busy", err)
	}
	var sawTerminal bool
	for _, event := range events {
		if event.Resource == "gc_lock" && event.Branch == "release_mismatch_or_conflict" && event.Entered {
			sawTerminal = true
		}
	}
	if !sawTerminal {
		t.Fatalf("stale-owner release boundaries=%+v", events)
	}
}

// An absent observer must leave behavior identical and record nothing.
func testArchiveBusyTraceAbsentObserver(t *testing.T) {
	ctx, _, _, seed := newSealableArchive(t)
	defer seed.Close()
	if err := seed.Cleanup(ctx, 24*time.Hour); err != nil {
		t.Fatalf("cleanup without an observer: %v", err)
	}
	if err := seed.withPublicationLock(ctx, "test-absent", func(context.Context) error {
		return fmt.Errorf("ordinary negative control")
	}); err == nil || errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("absent-observer publication error=%v", err)
	}
}

// The publication (non-GC) release runs on a fresh 5s background context. A
// real owner/generation steal during work must surface the release-side Busy on
// that fresh context, reach the caller, never replay work, and leave HEAD intact.
func testArchiveBusyTracePublicationReleaseConflict(t *testing.T) {
	ctx, base, core, seed := newSealableArchive(t)
	defer seed.Close()
	waiter := NewManager(base, "cluster", 1)
	defer waiter.Close()
	key := waiter.publicationLockKey()
	var events []localtesthooks.ArchiveBusyEvent
	traced := localtesthooks.WithArchiveBusyTrace(ctx, func(event localtesthooks.ArchiveBusyEvent) {
		events = append(events, event)
	})
	workCalls := 0
	err := waiter.withArchiveLock(traced, key, "test-publisher", false, func(context.Context) error {
		workCalls++
		// A real independent generation replaces the stored lock while the work
		// runs, so the later release owner/generation check genuinely conflicts.
		thief := NewManager(base, "cluster", 1)
		defer thief.Close()
		stolen, err := thief.readArchiveLock(ctx, key)
		if err != nil {
			return err
		}
		stolen.OwnerID = "thief"
		stolen.Generation = stolen.Generation + 1
		return thief.writeArchiveLock(ctx, key, *stolen, objstore.WithIfMatch(stolen.version))
	})
	if !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("publication release conflict error=%v, want Busy", err)
	}
	if workCalls != 1 {
		t.Fatalf("work replayed %d times, want exactly 1", workCalls)
	}
	var sawTerminal bool
	for _, event := range events {
		if event.Resource == "publication_lock" && event.Branch == "release_mismatch_or_conflict" &&
			event.Stage == "terminal" && event.Entered {
			sawTerminal = true
		}
	}
	if !sawTerminal {
		t.Fatalf("publication release boundaries=%+v", events)
	}
	independent := NewManager(base, "cluster", 1)
	defer independent.Close()
	if err := independent.Load(ctx); err != nil || independent.head.Generation != seed.head.Generation {
		t.Fatalf("HEAD changed after release conflict: generation=%d want=%d err=%v", independent.head.Generation, seed.head.Generation, err)
	}
	if _, _, err := seed.DecisionsFrom(ctx, 1, int(core.Tip())); err != nil {
		t.Fatalf("archive unreadable after release conflict: %v", err)
	}
}

// Carrying the observer onto a fresh release context must not import the source
// context's deadline or cancellation.
func testArchiveBusyTraceObserverCopyKeepsReleaseDeadline(t *testing.T) {
	src, cancelSrc := context.WithCancel(context.Background())
	defer cancelSrc()
	var observed int
	src = localtesthooks.WithArchiveBusyTrace(src, func(localtesthooks.ArchiveBusyEvent) { observed++ })
	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRelease()
	carried := localtesthooks.CarryArchiveBusyObserver(releaseCtx, src)
	if _, ok := carried.Deadline(); !ok {
		t.Fatal("carried context lost the release deadline")
	}
	if deadline, _ := carried.Deadline(); time.Until(deadline) > 5*time.Second {
		t.Fatalf("carried deadline=%v, want the fresh 5s release deadline", time.Until(deadline))
	}
	cancelSrc()
	if err := carried.Err(); err != nil {
		t.Fatalf("source cancellation leaked into the release context: %v", err)
	}
	localtesthooks.HitArchiveBusy(carried, localtesthooks.ArchiveBusyEvent{Operation: "archive-sync", Resource: "publication_lock"})
	if observed != 1 {
		t.Fatalf("observer calls=%d, want 1", observed)
	}
	bare := localtesthooks.CarryArchiveBusyObserver(releaseCtx, context.Background())
	localtesthooks.HitArchiveBusy(bare, localtesthooks.ArchiveBusyEvent{Operation: "archive-sync"})
	if observed != 1 {
		t.Fatalf("absent source observer was fabricated: calls=%d", observed)
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

// recoveryPinBackendFailureBucket fails one recovery-pin read or one
// conditional pin write with a real backend error. No Busy error is fabricated;
// the underlying bucket returns its own error value.
type recoveryPinBackendFailureBucket struct {
	objstore.Bucket
	key        string
	failRead   error
	failUpload error
	reads      int
	uploads    int
}

func (b *recoveryPinBackendFailureBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if name == b.key && b.failRead != nil {
		b.reads++
		return objstore.ObjectAttributes{}, b.failRead
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *recoveryPinBackendFailureBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.key && b.failRead != nil {
		b.reads++
		return nil, b.failRead
	}
	return b.Bucket.Get(ctx, name)
}

func (b *recoveryPinBackendFailureBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if name == b.key && b.failUpload != nil {
		b.uploads++
		return b.failUpload
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

// recoveryPinTrace records only the finite lifecycle classifications that the
// bounded diagnostic is allowed to expose. It stores no owner string, token,
// or full object key.
type recoveryPinTrace struct {
	mu     sync.Mutex
	events []localtesthooks.RecoveryPinEvent
}

func (tr *recoveryPinTrace) observe(event localtesthooks.RecoveryPinEvent) {
	tr.mu.Lock()
	tr.events = append(tr.events, event)
	tr.mu.Unlock()
}

func (tr *recoveryPinTrace) phases(phase string) []localtesthooks.RecoveryPinEvent {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var matched []localtesthooks.RecoveryPinEvent
	for _, event := range tr.events {
		if event.Phase == phase {
			matched = append(matched, event)
		}
	}
	return matched
}

// Node runs the recovery Manager under a non-empty cluster prefix, so the pin
// object key carries that prefix. The bounded hash suffix must be extracted from
// the exact Manager namespace; otherwise every natural key collapses to
// "unknown". A foreign namespace, a malformed suffix, or a key belonging to a
// different Manager must stay "unknown" rather than leak a bounded value.
func testRecoveryPinLifecycleKeyHashUsesManagerNamespace(t *testing.T) {
	manager := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	defer manager.Close()

	first := manager.recoveryPinKey("pin-namespace-a")
	second := manager.recoveryPinKey("pin-namespace-b")
	if first == second {
		t.Fatal("two distinct owners must not share a pin object key")
	}
	if !strings.HasPrefix(first, "cluster/archive/recovery-pins/") {
		t.Fatalf("fixture assumption broken: pin key %q lacks the cluster prefix", first)
	}

	firstHash := manager.recoveryPinKeyHash(first)
	if firstHash == "unknown" || firstHash == "" {
		t.Fatalf("prefixed manager key must yield a bounded hash suffix, got %q", firstHash)
	}
	if len(firstHash) > 16 {
		t.Fatalf("key hash %q exceeds the bounded 16-hex suffix", firstHash)
	}
	if !strings.HasPrefix(strings.TrimPrefix(first, manager.key("archive/recovery-pins/")), firstHash) || strings.Contains(firstHash, "/") {
		t.Fatalf("key hash %q must be only the bounded suffix of the pin key", firstHash)
	}
	if secondHash := manager.recoveryPinKeyHash(second); secondHash == firstHash {
		t.Fatalf("distinct owners must stay distinct under the manager prefix: both %q", firstHash)
	}

	// An unprefixed Manager accepts the unprefixed namespace exactly, because
	// that is what m.key produces when the prefix is empty.
	unprefixed := NewManager(objstore.NewInMemBucket(), "", 1)
	defer unprefixed.Close()
	unprefixedKey := unprefixed.recoveryPinKey("pin-unprefixed")
	if got := unprefixed.recoveryPinKeyHash(unprefixedKey); got == "unknown" || got == "" {
		t.Fatalf("empty-prefix manager key must yield a bounded hash suffix, got %q", got)
	}

	for name, key := range map[string]string{
		"foreign_cluster_prefix":   "other-cluster/archive/recovery-pins/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"foreign_namespace":        "cluster/archive/other/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"nested_suffix":            "cluster/archive/recovery-pins/ab/cd0123456789abcdef",
		"empty_suffix":             "cluster/archive/recovery-pins/",
		"unrelated_object":         "cluster/archive/PUBLISH_LOCK",
		"empty":                    "",
		"prefix_only_no_slash":     "cluster/archive/recovery-pins",
		"prefix_without_namespace": "clusterarchive/recovery-pins/0123456789abcdef",
		// The suffix must be an exact 64-char lowercase owner digest. A key that
		// is not one must never contribute any of its own plaintext characters.
		"plaintext_64_chars": "cluster/archive/recovery-pins/this-is-not-a-hash-but-exactly-sixty-four-characters-long-abcdef",
		"short_suffix":       "cluster/archive/recovery-pins/0123456789abcdef",
		"long_suffix":        "cluster/archive/recovery-pins/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"uppercase_suffix":   "cluster/archive/recovery-pins/0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
		"non_hex_suffix":     "cluster/archive/recovery-pins/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
	} {
		if got := manager.recoveryPinKeyHash(key); got != "unknown" {
			t.Errorf("%s: key hash = %q, want unknown for key %q", name, got, key)
		}
	}

	// A valid 64-char digest still yields the bounded 16-hex suffix.
	const validDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if got := manager.recoveryPinKeyHash("cluster/archive/recovery-pins/" + validDigest); got != validDigest[:16] {
		t.Errorf("valid digest key hash = %q, want %q", got, validDigest[:16])
	}

	// A key from a different Manager namespace is foreign to this one.
	if got := unprefixed.recoveryPinKeyHash(first); got != "unknown" {
		t.Fatalf("cluster-prefixed key must stay unknown to an unprefixed manager, got %q", got)
	}
}

// A created pin is confirmed by the existing confirm helper, which re-reads and
// compares the stored record. Renewal and close only receive an upload ACK on
// their own paths, so neither may claim a verified readback.
func testRecoveryPinLifecycleCreateReadbackAndCloseACK(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	trace := &recoveryPinTrace{}
	traced := localtesthooks.WithRecoveryPinTrace(ctx, trace.observe)
	traced = localtesthooks.WithRecoveryPinCategory(traced, localtesthooks.RecoveryPinOwnerStartup)

	snapshot, err := seed.BeginRecoverySnapshot(traced, "pin-lifecycle", time.Minute)
	if err != nil {
		t.Fatalf("begin recovery snapshot: %v", err)
	}
	created := trace.phases(localtesthooks.RecoveryPinCreateConfirmed)
	if len(created) != 1 || created[0].ReadbackStatus != localtesthooks.RecoveryPinReadbackDone {
		t.Fatalf("create_confirmed=%+v, want exactly one verified readback", created)
	}
	if attempts := trace.phases(localtesthooks.RecoveryPinCreateAttempt); len(attempts) != 1 {
		t.Fatalf("create_attempt=%+v, want exactly one pre-upload boundary", attempts)
	}
	key := seed.recoveryPinKey("pin-lifecycle")
	if created[0].KeyHash == key || created[0].KeyHash == "" {
		t.Fatalf("key hash must stay bounded and never the full object key: hash=%q", created[0].KeyHash)
	}
	if created[0].OwnerCategory != localtesthooks.RecoveryPinOwnerStartup {
		t.Fatalf("owner category=%q, want %q", created[0].OwnerCategory, localtesthooks.RecoveryPinOwnerStartup)
	}

	if err := snapshot.Renew(traced, 2*time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}
	renewed := trace.phases(localtesthooks.RecoveryPinRenewConfirmed)
	if len(renewed) != 1 || renewed[0].ReadbackStatus != localtesthooks.RecoveryPinReadbackNone {
		t.Fatalf("renew_confirmed=%+v, want an upload ACK with no readback", renewed)
	}
	if err := snapshot.Close(traced); err != nil {
		t.Fatalf("close: %v", err)
	}
	closed := trace.phases(localtesthooks.RecoveryPinCloseConfirmed)
	if len(closed) != 1 || closed[0].ReadbackStatus != localtesthooks.RecoveryPinReadbackNone {
		t.Fatalf("close_confirmed=%+v, want an upload ACK with no readback", closed)
	}
	// The stored record itself remains the only readback proof.
	stored, err := seed.readRecoveryPin(ctx, key)
	if err != nil {
		t.Fatalf("read stored pin after close: %v", err)
	}
	if stored.LeaseUntilMS > time.Now().UnixMilli() {
		t.Fatalf("close did not expire the stored lease: lease_until_ms=%d", stored.LeaseUntilMS)
	}
	_ = base
}

// The guard reports the exact stored pin that made it busy, and it cannot know
// which caller created it, so the owner category must stay guard_unknown.
func testRecoveryPinLifecycleGuardReadsActiveLegitimatePin(t *testing.T) {
	ctx, _, _, seed := newSealableArchive(t)
	defer seed.Close()
	pinOwner := "pin-guard-legitimate"
	pinKey := seed.recoveryPinKey(pinOwner)
	pin := archiveRecoveryPin{OwnerID: pinOwner, Token: "token", Base: 0, Tip: 0, TailHash: [32]byte{1}, TailObject: 1, LeaseUntilMS: time.Now().Add(time.Hour).UnixMilli()}
	if err := seed.writeRecoveryPin(ctx, pinKey, pin); err != nil {
		t.Fatal(err)
	}
	trace := &recoveryPinTrace{}
	traced := localtesthooks.WithRecoveryPinTrace(ctx, trace.observe)
	guard := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	guard.bucket = seed.bucket
	defer guard.Close()
	active, err := guard.hasActiveRecoveryPins(traced)
	if err != nil || !active {
		t.Fatalf("active recovery pin: active=%t err=%v", active, err)
	}
	reads := trace.phases(localtesthooks.RecoveryPinGuardRead)
	if len(reads) == 0 {
		t.Fatalf("guard emitted no recovery pin read boundary: %+v", trace.events)
	}
	var sawActive bool
	for _, event := range reads {
		if event.OwnerCategory != localtesthooks.RecoveryPinOwnerGuardUnknown {
			t.Fatalf("guard category=%q, want %q", event.OwnerCategory, localtesthooks.RecoveryPinOwnerGuardUnknown)
		}
		if event.LeaseState == localtesthooks.RecoveryPinLeaseActive && event.LeaseDeltaMS > 0 {
			sawActive = true
		}
	}
	if !sawActive {
		t.Fatalf("guard never reported the active stored lease: %+v", reads)
	}
}

// A close whose stored record no longer matches the snapshot is a finite
// conflict, not an unknown. This is the boundary the reachable startup and
// catch-up callers currently discard, so it must stay observable.
func testRecoveryPinLifecycleCloseConflictIsFinite(t *testing.T) {
	ctx, _, _, seed := newSealableArchive(t)
	defer seed.Close()
	trace := &recoveryPinTrace{}
	traced := localtesthooks.WithRecoveryPinTrace(ctx, trace.observe)
	traced = localtesthooks.WithRecoveryPinCategory(traced, localtesthooks.RecoveryPinOwnerNodeCatchup)
	snapshot, err := seed.BeginRecoverySnapshot(traced, "pin-close-conflict", time.Minute)
	if err != nil {
		t.Fatalf("begin recovery snapshot: %v", err)
	}
	// Another generation takes the stored record over behind this snapshot.
	stored, err := seed.readRecoveryPin(ctx, snapshot.pinKey)
	if err != nil {
		t.Fatalf("read stored pin: %v", err)
	}
	stored.Token = "other-token"
	stored.LeaseUntilMS = time.Now().Add(time.Hour).UnixMilli()
	if err := seed.writeRecoveryPin(ctx, snapshot.pinKey, *stored, objstore.WithIfMatch(stored.version)); err != nil {
		t.Fatalf("write replaced pin: %v", err)
	}
	if err := snapshot.Close(traced); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("close after takeover: err=%v, want ErrArchiveBusy", err)
	}
	conflicts := trace.phases(localtesthooks.RecoveryPinCloseConflict)
	if len(conflicts) != 1 || conflicts[0].WriteStatus != localtesthooks.RecoveryPinWriteNotAttempted {
		t.Fatalf("close_conflict=%+v, want one not_attempted classification", conflicts)
	}
	if conflicts[0].ReadStatus != localtesthooks.RecoveryPinReadIdentityMismatch {
		t.Fatalf("pre-write mismatch read status=%q, want %q", conflicts[0].ReadStatus, localtesthooks.RecoveryPinReadIdentityMismatch)
	}
	if conflicts[0].WriteStatus == localtesthooks.RecoveryPinWriteCondMet {
		t.Fatalf("a pre-write mismatch must never claim a conditional upload conflict")
	}
	if len(trace.phases(localtesthooks.RecoveryPinCloseConfirmed)) != 0 {
		t.Fatalf("a conflicting close must not report confirmation: %+v", trace.events)
	}
	if conflicts[0].OwnerCategory != localtesthooks.RecoveryPinOwnerNodeCatchup {
		t.Fatalf("owner category=%q, want %q", conflicts[0].OwnerCategory, localtesthooks.RecoveryPinOwnerNodeCatchup)
	}
}

// Without a scoped observer the lifecycle paths must behave exactly as before
// and retain nothing; the diagnostic cannot change behavior when it is absent.
func testRecoveryPinLifecycleDisabledObserverIsNoop(t *testing.T) {
	ctx, _, _, seed := newSealableArchive(t)
	defer seed.Close()
	snapshot, err := seed.BeginRecoverySnapshot(ctx, "pin-absent-observer", time.Minute)
	if err != nil {
		t.Fatalf("begin recovery snapshot: %v", err)
	}
	if err := snapshot.Renew(ctx, 2*time.Minute); err != nil {
		t.Fatalf("renew without observer: %v", err)
	}
	if err := snapshot.Close(ctx); err != nil {
		t.Fatalf("close without observer: %v", err)
	}
	if localtesthooks.RecoveryPinCategory(ctx) != localtesthooks.RecoveryPinOwnerGuardUnknown {
		t.Fatalf("unstamped context must report guard_unknown")
	}
	carried := localtesthooks.CarryRecoveryPinObserver(ctx, ctx)
	if carried != ctx {
		t.Fatalf("carrying an absent observer must be an identity, not a new context")
	}
}

// A real backend read failure during close must be reported as a finite
// close_error with an invalid read, never as a conditional conflict.
func testRecoveryPinLifecycleBackendReadFailureIsCloseError(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	snapshot, err := seed.BeginRecoverySnapshot(ctx, "pin-read-failure", time.Minute)
	if err != nil {
		t.Fatalf("begin recovery snapshot: %v", err)
	}
	trace := &recoveryPinTrace{}
	traced := localtesthooks.WithRecoveryPinTrace(ctx, trace.observe)
	// The existing store wrapper fails the pin read with a real backend error;
	// no Busy error is fabricated by the wrapper.
	probe := &recoveryPinBackendFailureBucket{Bucket: base, key: snapshot.pinKey, failRead: syscall.ECONNRESET}
	failing := NewManager(probe, "cluster", 1)
	defer failing.Close()
	failingSnapshot := &RecoverySnapshot{manager: failing, pinKey: snapshot.pinKey, pin: snapshot.pin}
	if err := failingSnapshot.Close(traced); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("close with a failing backend read: err=%v, want the backend error", err)
	}
	if probe.reads == 0 {
		t.Fatalf("backend wrapper never saw the pin read")
	}
	errors := trace.phases(localtesthooks.RecoveryPinCloseError)
	if len(errors) != 1 || errors[0].ReadStatus != localtesthooks.RecoveryPinReadInvalid {
		t.Fatalf("close_error=%+v, want one invalid-read classification", errors)
	}
	if errors[0].WriteStatus != localtesthooks.RecoveryPinWriteNotAttempted {
		t.Fatalf("a failed read must never claim a write outcome: %+v", errors[0])
	}
	if len(trace.phases(localtesthooks.RecoveryPinCloseConflict)) != 0 {
		t.Fatalf("a backend read failure must not be reported as a conflict: %+v", trace.events)
	}
	if err := snapshot.Close(ctx); err != nil {
		t.Fatalf("the healthy pin must still close: %v", err)
	}
}

// A genuine conditional-upload conflict, returned by the store itself on the
// write, is the only path that may report condition_not_met.
func testRecoveryPinLifecycleConditionalWriteConflict(t *testing.T) {
	ctx, base, _, seed := newSealableArchive(t)
	defer seed.Close()
	snapshot, err := seed.BeginRecoverySnapshot(ctx, "pin-conditional-conflict", time.Minute)
	if err != nil {
		t.Fatalf("begin recovery snapshot: %v", err)
	}
	trace := &recoveryPinTrace{}
	traced := localtesthooks.WithRecoveryPinTrace(ctx, trace.observe)
	// An independent, valid generation moves the stored record between this
	// snapshot's read and its conditional write, so the real store answers the
	// conditional upload with its own condition-not-met error.
	racing := &conditionalArchiveLockBucket{Bucket: base, key: snapshot.pinKey}
	racing.before = func(context.Context) error {
		current, err := seed.readRecoveryPin(ctx, snapshot.pinKey)
		if err != nil {
			return err
		}
		current.LeaseUntilMS = time.Now().Add(time.Hour).UnixMilli()
		return seed.writeRecoveryPin(ctx, snapshot.pinKey, *current, objstore.WithIfMatch(current.version))
	}
	probe := NewManager(racing, "cluster", 1)
	defer probe.Close()
	conflicting := &RecoverySnapshot{manager: probe, pinKey: snapshot.pinKey, pin: snapshot.pin}
	if err := conflicting.Close(traced); !errors.Is(err, ErrArchiveBusy) {
		t.Fatalf("conditional close conflict: err=%v, want ErrArchiveBusy", err)
	}
	if racing.count == 0 {
		t.Fatalf("the conditional pin write was never attempted")
	}
	conflicts := trace.phases(localtesthooks.RecoveryPinCloseConflict)
	if len(conflicts) != 1 || conflicts[0].WriteStatus != localtesthooks.RecoveryPinWriteCondMet {
		t.Fatalf("close_conflict=%+v, want one condition_not_met classification from the real write", conflicts)
	}
	if len(trace.phases(localtesthooks.RecoveryPinCloseError)) != 0 {
		t.Fatalf("a conditional conflict must not be reported as a backend error: %+v", trace.events)
	}
	if err := snapshot.Close(ctx); err != nil {
		t.Fatalf("the original snapshot must still be closable: %v", err)
	}
}

// An expired stored pin is reclaimed by the guard's tombstone write. The lease
// state is forced by writing an already-expired expiry, so no long sleep and no
// wall-clock wait is required.
func testRecoveryPinLifecycleExpiredTombstoneGuard(t *testing.T) {
	ctx, _, _, seed := newSealableArchive(t)
	defer seed.Close()
	pinOwner := "pin-expired-tombstone"
	pinKey := seed.recoveryPinKey(pinOwner)
	expired := archiveRecoveryPin{OwnerID: pinOwner, Token: "token", Base: 0, Tip: 0, TailHash: [32]byte{2}, TailObject: 1, LeaseUntilMS: time.Now().Add(-time.Minute).UnixMilli()}
	if err := seed.writeRecoveryPin(ctx, pinKey, expired); err != nil {
		t.Fatal(err)
	}
	trace := &recoveryPinTrace{}
	traced := localtesthooks.WithRecoveryPinTrace(ctx, trace.observe)
	guard := NewManager(objstore.NewInMemBucket(), "cluster", 1)
	guard.bucket = seed.bucket
	defer guard.Close()
	active, err := guard.hasActiveRecoveryPins(traced)
	if err != nil || active {
		t.Fatalf("expired pin must not report active: active=%t err=%v", active, err)
	}
	reads := trace.phases(localtesthooks.RecoveryPinGuardRead)
	if len(reads) == 0 {
		t.Fatalf("guard emitted no boundary for an expired pin: %+v", trace.events)
	}
	var sawExpiryWrite, sawExpiredLease bool
	for _, event := range reads {
		if event.WriteStatus == localtesthooks.RecoveryPinWriteOK {
			sawExpiryWrite = true
		}
		if event.LeaseState == localtesthooks.RecoveryPinLeaseExpired || event.LeaseState == localtesthooks.RecoveryPinLeaseZero {
			sawExpiredLease = true
		}
	}
	if !sawExpiryWrite {
		t.Fatalf("guard never reported the tombstone write status: %+v", reads)
	}
	if !sawExpiredLease {
		t.Fatalf("guard never classified the stored lease as expired: %+v", reads)
	}
}

func TestArchiveCleanupReusesUnchangedPrefixAfterRealHeadConflict(t *testing.T) {
	t.Run("recovery_pin_lifecycle_key_hash_uses_manager_namespace", testRecoveryPinLifecycleKeyHashUsesManagerNamespace)
	t.Run("recovery_pin_lifecycle_backend_read_failure_is_close_error", testRecoveryPinLifecycleBackendReadFailureIsCloseError)
	t.Run("recovery_pin_lifecycle_conditional_write_conflict", testRecoveryPinLifecycleConditionalWriteConflict)
	t.Run("recovery_pin_lifecycle_expired_tombstone_guard", testRecoveryPinLifecycleExpiredTombstoneGuard)
	t.Run("recovery_pin_lifecycle_create_readback_and_close_ack", testRecoveryPinLifecycleCreateReadbackAndCloseACK)
	t.Run("recovery_pin_lifecycle_guard_reads_active_legitimate_pin", testRecoveryPinLifecycleGuardReadsActiveLegitimatePin)
	t.Run("recovery_pin_lifecycle_close_conflict_is_finite", testRecoveryPinLifecycleCloseConflictIsFinite)
	t.Run("recovery_pin_lifecycle_disabled_observer_is_noop", testRecoveryPinLifecycleDisabledObserverIsNoop)
	t.Run("archive_busy_real_gc_lock_and_non_busy", testArchiveBusyTraceRealGCLockAndNonBusy)
	t.Run("archive_active_gc_lock_subtype_trim", testArchiveActiveGCLockSubtypeTrim)
	t.Run("archive_busy_real_conditional_exhaustion", testArchiveBusyTraceRealConditionalExhaustion)
	t.Run("archive_busy_real_recovery_pin_and_release", testArchiveBusyTraceRealRecoveryPinAndRelease)
	t.Run("archive_busy_active_pin_release_failure_terminal", testArchiveActivePinWithReleaseFailureIsTerminal)
	t.Run("archive_busy_active_pin_cancellation_terminal", testArchiveActivePinWithCancellationIsTerminal)
	t.Run("archive_busy_real_release_conflict", testArchiveBusyTraceRealReleaseConflict)
	t.Run("archive_busy_absent_observer", testArchiveBusyTraceAbsentObserver)
	t.Run("archive_busy_publication_release_conflict", testArchiveBusyTracePublicationReleaseConflict)
	t.Run("archive_busy_observer_copy_keeps_release_deadline", testArchiveBusyTraceObserverCopyKeepsReleaseDeadline)
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
