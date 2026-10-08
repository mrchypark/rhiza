package recovery

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type archiveStatsGate struct {
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	relOnce   sync.Once
}

func newArchiveStatsGate() *archiveStatsGate {
	return &archiveStatsGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func newArchivePreflightManagers(t *testing.T) (context.Context, objstore.Bucket, *quepaxa.Core, *Manager, *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	stale, writer := NewManager(bucket, "cluster", 1), NewManager(bucket, "cluster", 1)
	t.Cleanup(stale.Close)
	t.Cleanup(writer.Close)
	if err := stale.Load(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, bucket, core, stale, writer
}

func (g *archiveStatsGate) wait(ctx context.Context) error {
	g.enterOnce.Do(func() { close(g.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.release:
		return nil
	}
}

func (g *archiveStatsGate) open() {
	g.relOnce.Do(func() { close(g.release) })
}

type archiveStatsBucket struct {
	objstore.Bucket
	syncStarted    atomic.Bool
	headWritten    atomic.Bool
	lockReads      atomic.Uint32
	loadReads      atomic.Uint32
	failPreflight  atomic.Bool
	lockRead       *archiveStatsGate
	admission      *archiveStatsGate
	admissionRetry *archiveStatsGate
	preflightLoad  *archiveStatsGate
	load           *archiveStatsGate
	extent         *archiveStatsGate
	head           *archiveStatsGate
	release        *archiveStatsGate
}

func (b *archiveStatsBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.syncStarted.Load() && strings.HasSuffix(name, "/archive/PUBLISH_LOCK") {
		if b.lockRead != nil {
			if err := b.lockRead.wait(ctx); err != nil {
				return nil, err
			}
		}
		switch b.lockReads.Add(1) {
		case 1:
			reader, err := b.Bucket.Get(ctx, name)
			if err != nil {
				return nil, err
			}
			if err := b.admission.wait(ctx); err != nil {
				_ = reader.Close()
				return nil, err
			}
			return reader, nil
		case 2:
			if err := b.admissionRetry.wait(ctx); err != nil {
				return nil, err
			}
		}
	}
	return b.Bucket.Get(ctx, name)
}

func (b *archiveStatsBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.syncStarted.Load() && strings.HasSuffix(name, "/archive/PUBLISH_LOCK") && b.lockRead != nil {
		if err := b.lockRead.wait(ctx); err != nil {
			return objstore.ObjectAttributes{}, err
		}
	}
	if b.syncStarted.Load() && strings.HasSuffix(name, "/archive/head.bin") {
		switch b.loadReads.Add(1) {
		case 1:
			if b.preflightLoad != nil {
				if err := b.preflightLoad.wait(ctx); err != nil {
					return objstore.ObjectAttributes{}, err
				}
			}
			if b.failPreflight.CompareAndSwap(true, false) {
				return objstore.ObjectAttributes{}, errors.New("injected preflight head read failure")
			}
		case 2:
			if b.load != nil {
				if err := b.load.wait(ctx); err != nil {
					return objstore.ObjectAttributes{}, err
				}
			}
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *archiveStatsBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	switch {
	case b.syncStarted.Load() && strings.Contains(name, "/archive/blocks/"):
		if err := b.extent.wait(ctx); err != nil {
			return err
		}
	case b.syncStarted.Load() && strings.HasSuffix(name, "/archive/head.bin"):
		if err := b.head.wait(ctx); err != nil {
			return err
		}
	case b.headWritten.Load() && strings.HasSuffix(name, "/archive/PUBLISH_LOCK"):
		if err := b.release.wait(ctx); err != nil {
			return err
		}
	}
	err := b.Bucket.Upload(ctx, name, r, options...)
	if b.syncStarted.Load() && strings.HasSuffix(name, "/archive/head.bin") && err == nil {
		b.headWritten.Store(true)
	}
	return err
}

func TestArchiveStatsMeasureSyncThroughBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, bucket, core, manager := newSealableArchive(t)
	defer manager.Close()
	gates := &archiveStatsBucket{
		Bucket: bucket, admission: newArchiveStatsGate(), admissionRetry: newArchiveStatsGate(),
		preflightLoad: newArchiveStatsGate(), load: newArchiveStatsGate(),
		extent: newArchiveStatsGate(), head: newArchiveStatsGate(), release: newArchiveStatsGate(),
	}
	manager.bucket = gates
	t.Cleanup(func() {
		gates.admission.open()
		gates.admissionRetry.open()
		gates.preflightLoad.open()
		gates.load.open()
		gates.extent.open()
		gates.head.open()
		gates.release.open()
	})

	holder := NewManager(bucket, "cluster", 1)
	defer holder.Close()
	held, releaseHolder := make(chan struct{}), newArchiveStatsGate()
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- holder.withPublicationLock(ctx, "stats-test-holder", func(context.Context) error {
			close(held)
			<-releaseHolder.release
			return nil
		})
	}()
	t.Cleanup(releaseHolder.open)
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("publication lease was not acquired")
	}

	_, _, err := core.Propose(ctx, []byte("archive stats sample"))
	if err != nil {
		t.Fatal(err)
	}
	before := manager.ArchiveStats()
	syncDone := make(chan error, 1)
	gates.syncStarted.Store(true)
	go func() { syncDone <- manager.SyncThrough(ctx, core, core.Tip()) }()

	waitArchiveStatsGate(t, ctx, gates.preflightLoad)
	assertArchiveStageCount(t, manager.ArchiveStats().Stages.PublicationAdmission, stageCount(t, before.Stages.PublicationAdmission))
	gates.preflightLoad.open()
	waitArchiveStatsGate(t, ctx, gates.admission)
	gates.admission.open()
	// A busy retry now validates the head before attempting the lock again.
	// This request remains uncovered, so it must continue into fenced work.
	waitArchiveStatsGate(t, ctx, gates.load)
	gates.load.open()
	waitArchiveStatsGate(t, ctx, gates.admissionRetry)
	releaseHolder.open()
	select {
	case err := <-holderDone:
		if err != nil {
			t.Fatalf("release held publication lease: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("held publication lease did not release")
	}
	gates.admissionRetry.open()
	waitArchiveStatsGate(t, ctx, gates.extent)
	afterAdmission := manager.ArchiveStats()
	assertArchiveStageCount(t, afterAdmission.Stages.PublicationAdmission, stageCount(t, before.Stages.PublicationAdmission)+1)
	if got := *afterAdmission.Stages.PublicationAdmission.DurationNSSum - *before.Stages.PublicationAdmission.DurationNSSum; got < uint64(50*time.Millisecond) {
		t.Fatalf("admission duration=%dns, want at least one bounded retry interval", got)
	}
	assertArchiveStageCount(t, afterAdmission.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+3)
	assertArchiveStageCount(t, afterAdmission.Stages.ExtentBuildUpload, stageCount(t, before.Stages.ExtentBuildUpload))
	assertArchiveStageCount(t, afterAdmission.Stages.HeadPublish, stageCount(t, before.Stages.HeadPublish))
	assertArchiveStageCount(t, afterAdmission.Stages.PublicationRelease, stageCount(t, before.Stages.PublicationRelease))

	afterLoad := manager.ArchiveStats()
	assertArchiveStageCount(t, afterLoad.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+3)
	assertArchiveStageCount(t, afterLoad.Stages.ExtentBuildUpload, stageCount(t, before.Stages.ExtentBuildUpload))

	gates.extent.open()
	waitArchiveStatsGate(t, ctx, gates.head)
	afterExtent := manager.ArchiveStats()
	assertArchiveStageCount(t, afterExtent.Stages.ExtentBuildUpload, stageCount(t, before.Stages.ExtentBuildUpload)+1)
	assertArchiveStageCount(t, afterExtent.Stages.HeadPublish, stageCount(t, before.Stages.HeadPublish))

	gates.head.open()
	waitArchiveStatsGate(t, ctx, gates.release)
	afterHead := manager.ArchiveStats()
	assertArchiveStageCount(t, afterHead.Stages.HeadPublish, stageCount(t, before.Stages.HeadPublish)+1)
	assertArchiveStageCount(t, afterHead.Stages.PublicationRelease, stageCount(t, before.Stages.PublicationRelease))

	gates.release.open()
	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncThrough: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("SyncThrough did not finish")
	}
	after := manager.ArchiveStats()
	assertArchiveStageCount(t, after.Stages.PublicationAdmission, stageCount(t, before.Stages.PublicationAdmission)+1)
	assertArchiveStageCount(t, after.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+3)
	assertArchiveStageCount(t, after.Stages.ExtentBuildUpload, stageCount(t, before.Stages.ExtentBuildUpload)+1)
	assertArchiveStageCount(t, after.Stages.HeadPublish, stageCount(t, before.Stages.HeadPublish)+1)
	assertArchiveStageCount(t, after.Stages.PublicationRelease, stageCount(t, before.Stages.PublicationRelease)+1)
}

func TestArchiveSyncThroughSkipsLeaseForDurableTip(t *testing.T) {
	ctx, base, core, stale, writer := newArchivePreflightManagers(t)
	if _, _, err := core.Propose(ctx, []byte("durable before stale sync")); err != nil {
		t.Fatal(err)
	}
	through := core.Tip()
	if err := writer.SyncThrough(ctx, core, through); err != nil {
		t.Fatalf("publish durable target: %v", err)
	}
	for range 2 {
		if _, _, err := core.Propose(ctx, []byte("later unpublished suffix")); err != nil {
			t.Fatal(err)
		}
	}
	fullTip := core.Tip()
	if fullTip <= through {
		t.Fatalf("test setup core tip=%d must exceed durable target=%d", fullTip, through)
	}
	gates := &archiveStatsBucket{
		Bucket: base, admission: newArchiveStatsGate(), admissionRetry: newArchiveStatsGate(),
		extent: newArchiveStatsGate(), head: newArchiveStatsGate(), release: newArchiveStatsGate(),
	}
	for _, gate := range []*archiveStatsGate{gates.admissionRetry, gates.extent, gates.head, gates.release} {
		gate.open()
	}
	stale.bucket = gates
	gates.syncStarted.Store(true)
	t.Cleanup(gates.admission.open)

	holder := NewManager(base, "cluster", 1)
	t.Cleanup(holder.Close)
	lease, err := holder.acquireArchiveLock(ctx, holder.publicationLockKey(), "preflight-test-holder", archivePinLease)
	if err != nil {
		t.Fatalf("hold publication lease: %v", err)
	}
	leaseReleased := false
	t.Cleanup(func() {
		if !leaseReleased {
			if err := holder.releaseArchiveLock(ctx, holder.publicationLockKey(), lease); err != nil {
				t.Errorf("release publication lease: %v", err)
			}
		}
	})

	before := stale.ArchiveStats()
	done := make(chan error, 1)
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { done <- stale.SyncThrough(callCtx, core, through) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("already durable target: %v", err)
		}
	case <-gates.admission.entered:
		cancel()
		<-done
		t.Fatal("already durable target attempted remote publication admission")
	case <-time.After(5 * time.Second):
		t.Fatal("already durable target did not return")
	}
	assertArchiveStageCount(t, stale.ArchiveStats().Stages.PublicationAdmission, stageCount(t, before.Stages.PublicationAdmission))
	assertArchiveStageCount(t, stale.ArchiveStats().Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+1)
	if stale.Tip() != through {
		t.Fatalf("loaded durable tip=%d, want %d", stale.Tip(), through)
	}
	reader := NewManager(base, "cluster", 1)
	t.Cleanup(reader.Close)
	if err := reader.Load(ctx); err != nil || reader.Tip() != through {
		t.Fatalf("independent durable tip=%d want=%d err=%v", reader.Tip(), through, err)
	}
	if err := holder.releaseArchiveLock(ctx, holder.publicationLockKey(), lease); err != nil {
		t.Fatalf("release held publication lease: %v", err)
	}
	leaseReleased = true
	gates.admission.open()
	if err := stale.SyncThrough(ctx, core, fullTip); err != nil {
		t.Fatalf("publish later suffix on explicit higher request: %v", err)
	}
	if err := reader.Load(ctx); err != nil || reader.Tip() != fullTip {
		t.Fatalf("independent suffix tip=%d want=%d err=%v", reader.Tip(), fullTip, err)
	}
}

func TestArchiveSyncThroughRechecksPublishedTargetWhileLeaseHeld(t *testing.T) {
	ctx, base, core, waiter, writer := newArchivePreflightManagers(t)
	if _, _, err := core.Propose(ctx, []byte("shared certified prefix")); err != nil {
		t.Fatal(err)
	}
	through := core.Tip()
	for _, value := range []string{"waiter-only suffix 1", "waiter-only suffix 2"} {
		if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	fullTip := core.Tip()

	writerWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writerWAL.Close() })
	writerCore, err := quepaxa.New(quepaxa.Config{
		NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: writerWAL,
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err := core.DecisionsFromBounded(through, 1, maxExtentPayload)
	if err != nil || len(prefix) != 1 {
		t.Fatalf("read shared certified prefix: decisions=%d err=%v", len(prefix), err)
	}
	if err := writerCore.AcceptCertifiedValues(prefix); err != nil {
		t.Fatalf("install shared certified prefix: %v", err)
	}
	if writerCore.Tip() != through {
		t.Fatalf("writer core tip=%d want=%d", writerCore.Tip(), through)
	}
	writerPrefix, ok := writerCore.PrefixHash(through)
	corePrefix, coreHasPrefix := core.PrefixHash(through)
	if !ok || !coreHasPrefix || writerPrefix != corePrefix {
		t.Fatal("writer and waiter cores do not share the certified prefix")
	}

	waiterGates := &archiveStatsBucket{
		Bucket: base, lockRead: newArchiveStatsGate(), admission: newArchiveStatsGate(),
		admissionRetry: newArchiveStatsGate(), extent: newArchiveStatsGate(),
		head: newArchiveStatsGate(), release: newArchiveStatsGate()}
	for _, gate := range []*archiveStatsGate{waiterGates.extent, waiterGates.head, waiterGates.release} {
		gate.open()
	}
	waiter.bucket = waiterGates
	waiterGates.syncStarted.Store(true)

	writerGates := &archiveStatsBucket{
		Bucket: base, admission: newArchiveStatsGate(), admissionRetry: newArchiveStatsGate(),
		extent: newArchiveStatsGate(), head: newArchiveStatsGate(), release: newArchiveStatsGate(),
	}
	for _, gate := range []*archiveStatsGate{writerGates.admission, writerGates.admissionRetry, writerGates.extent, writerGates.head} {
		gate.open()
	}
	writer.bucket = writerGates
	writerGates.syncStarted.Store(true)

	before := waiter.ArchiveStats()
	waiterDone := make(chan error, 1)
	waiterFinished := make(chan struct{})
	go func() {
		defer close(waiterFinished)
		waiterDone <- waiter.SyncThrough(ctx, core, through)
	}()
	t.Cleanup(func() {
		waiterGates.lockRead.open()
		waiterGates.admission.open()
		waiterGates.admissionRetry.open()
		select {
		case <-waiterFinished:
		case <-time.After(time.Second):
			t.Error("waiter did not stop during cleanup")
		}
	})
	waitArchiveStatsGate(t, ctx, waiterGates.lockRead)
	waiter.batchMu.Lock()
	lowerBatch := waiter.batch
	waiter.batchMu.Unlock()
	if lowerBatch == nil {
		t.Fatal("lower request did not create a batch before reaching admission")
	}

	// This caller joins after flushBatch has snapshotted target 1. Its target
	// must be retried after the lower request completes independently.
	highDone := make(chan error, 1)
	highFinished := make(chan struct{})
	go func() {
		defer close(highFinished)
		highDone <- waiter.SyncThrough(ctx, core, fullTip)
	}()
	t.Cleanup(func() {
		waiterGates.admissionRetry.open()
		select {
		case <-highFinished:
		case <-time.After(time.Second):
			t.Error("higher waiter did not stop during cleanup")
		}
	})
	deadline := time.After(5 * time.Second)
	for {
		waiter.batchMu.Lock()
		joined := waiter.batch != nil && waiter.batch.target >= fullTip
		waiter.batchMu.Unlock()
		if joined {
			break
		}
		select {
		case <-deadline:
			t.Fatal("higher target did not join the snapshotted lower batch")
		default:
			runtime.Gosched()
		}
	}

	writerDone := make(chan error, 1)
	writerFinished := make(chan struct{})
	go func() {
		defer close(writerFinished)
		writerDone <- writer.SyncThrough(ctx, writerCore, through)
	}()
	t.Cleanup(func() {
		writerGates.release.open()
		select {
		case <-writerFinished:
		case <-time.After(time.Second):
			t.Error("writer did not release its publication lease during cleanup")
		}
	})
	waitArchiveStatsGate(t, ctx, writerGates.release)

	// The waiter completed its initial uncovered load before the writer began.
	// It now reads the writer's live lease, then must observe the published tip
	// without waiting for the writer's gated release.
	waiterGates.lockRead.open()
	waitArchiveStatsGate(t, ctx, waiterGates.admission)
	waiterGates.admission.open()
	lowerReturned := false
	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("waiter recheck: %v", err)
		}
		lowerReturned = true
	case <-waiterGates.admissionRetry.entered:
		waiter.batchMu.Lock()
		stillLowerBatch := waiter.batch == lowerBatch
		waiter.batchMu.Unlock()
		if stillLowerBatch {
			t.Fatal("lower request retried admission for a higher late joiner")
		}
	case <-ctx.Done():
		t.Fatalf("waiter did not acknowledge the requested target while the lease remained held: %v", ctx.Err())
	}
	if !lowerReturned {
		select {
		case err := <-waiterDone:
			if err != nil {
				t.Fatalf("waiter recheck: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("waiter did not acknowledge the requested target while the lease remained held: %v", ctx.Err())
		}
	}
	select {
	case err := <-writerDone:
		t.Fatalf("writer released before its release gate opened: %v", err)
	default:
	}
	select {
	case <-waiterGates.admissionRetry.entered:
	case err := <-highDone:
		t.Fatalf("higher caller acknowledged before its target was published: %v", err)
	case <-ctx.Done():
		t.Fatalf("higher caller did not retry while publication lease remained held: %v", ctx.Err())
	}
	select {
	case err := <-highDone:
		t.Fatalf("higher caller acknowledged before its target was published: %v", err)
	default:
	}

	reader := NewManager(base, "cluster", 1)
	t.Cleanup(reader.Close)
	if err := reader.Load(ctx); err != nil || reader.Tip() != through {
		t.Fatalf("independent durable tip=%d want=%d err=%v", reader.Tip(), through, err)
	}
	if reader.Tip() >= fullTip {
		t.Fatalf("unrequested suffix was published with the lower ACK: durable tip=%d full tip=%d", reader.Tip(), fullTip)
	}

	after := waiter.ArchiveStats()
	assertArchiveStageCount(t, after.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+3)
	assertArchiveStageCount(t, after.Stages.PublicationAdmission, stageCount(t, before.Stages.PublicationAdmission)+1)
	assertArchiveStageCount(t, after.Stages.ExtentBuildUpload, stageCount(t, before.Stages.ExtentBuildUpload))
	assertArchiveStageCount(t, after.Stages.HeadPublish, stageCount(t, before.Stages.HeadPublish))
	assertArchiveStageCount(t, after.Stages.PublicationRelease, stageCount(t, before.Stages.PublicationRelease))

	writerGates.release.open()
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("writer publication: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("writer did not finish: %v", ctx.Err())
	}
	waiterGates.admissionRetry.open()
	select {
	case err := <-highDone:
		if err != nil {
			t.Fatalf("higher waiter retry: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("higher waiter did not publish its target: %v", ctx.Err())
	}
	if err := reader.Load(ctx); err != nil || reader.Tip() != fullTip {
		t.Fatalf("independent suffix tip=%d want=%d err=%v", reader.Tip(), fullTip, err)
	}
}

func TestArchiveSyncThroughBusyRecheckRequiresSnapshottedTarget(t *testing.T) {
	ctx, base, core, waiter, writer := newArchivePreflightManagers(t)
	if _, _, err := core.Propose(ctx, []byte("shared certified prefix")); err != nil {
		t.Fatal(err)
	}
	through := core.Tip()
	for _, value := range []string{"higher target suffix 1", "higher target suffix 2"} {
		if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	fullTip := core.Tip()

	waiterGates := &archiveStatsBucket{
		Bucket: base, lockRead: newArchiveStatsGate(), admission: newArchiveStatsGate(),
		admissionRetry: newArchiveStatsGate(), extent: newArchiveStatsGate(),
		head: newArchiveStatsGate(), release: newArchiveStatsGate(),
	}
	for _, gate := range []*archiveStatsGate{waiterGates.extent, waiterGates.head, waiterGates.release} {
		gate.open()
	}
	waiter.bucket = waiterGates
	waiterGates.syncStarted.Store(true)

	writerGates := &archiveStatsBucket{
		Bucket: base, admission: newArchiveStatsGate(), admissionRetry: newArchiveStatsGate(),
		extent: newArchiveStatsGate(), head: newArchiveStatsGate(), release: newArchiveStatsGate(),
	}
	for _, gate := range []*archiveStatsGate{writerGates.admission, writerGates.admissionRetry, writerGates.extent, writerGates.head} {
		gate.open()
	}
	writer.bucket = writerGates
	writerGates.syncStarted.Store(true)

	waiterDone := make(chan error, 1)
	waiterFinished := make(chan struct{})
	go func() {
		defer close(waiterFinished)
		waiterDone <- waiter.SyncThrough(ctx, core, fullTip)
	}()
	t.Cleanup(func() {
		waiterGates.lockRead.open()
		waiterGates.admission.open()
		waiterGates.admissionRetry.open()
		select {
		case <-waiterFinished:
		case <-time.After(time.Second):
			t.Error("snapshotted higher request did not stop during cleanup")
		}
	})
	waitArchiveStatsGate(t, ctx, waiterGates.lockRead)

	writerDone := make(chan error, 1)
	writerFinished := make(chan struct{})
	go func() {
		defer close(writerFinished)
		writerDone <- writer.syncNow(ctx, core, through)
	}()
	t.Cleanup(func() {
		writerGates.release.open()
		select {
		case <-writerFinished:
		case <-time.After(time.Second):
			t.Error("lower-target writer did not stop during cleanup")
		}
	})
	waitArchiveStatsGate(t, ctx, writerGates.release)

	waiterGates.lockRead.open()
	waitArchiveStatsGate(t, ctx, waiterGates.admission)
	waiterGates.admission.open()
	select {
	case err := <-waiterDone:
		t.Fatalf("snapshotted higher target returned before it was durable: %v", err)
	case <-waiterGates.admissionRetry.entered:
	case <-ctx.Done():
		t.Fatalf("higher target did not recheck the lower publication: %v", ctx.Err())
	}
	select {
	case err := <-waiterDone:
		t.Fatalf("snapshotted higher target returned before it was durable: %v", err)
	default:
	}
	reader := NewManager(base, "cluster", 1)
	t.Cleanup(reader.Close)
	if err := reader.Load(ctx); err != nil || reader.Tip() != through {
		t.Fatalf("independent lower durable tip=%d want=%d err=%v", reader.Tip(), through, err)
	}

	writerGates.release.open()
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("lower-target writer: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("lower-target writer did not finish: %v", ctx.Err())
	}
	waiterGates.admissionRetry.open()
	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("snapshotted higher target retry: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("snapshotted higher target did not publish: %v", ctx.Err())
	}
	if err := reader.Load(ctx); err != nil || reader.Tip() != fullTip {
		t.Fatalf("independent higher durable tip=%d want=%d err=%v", reader.Tip(), fullTip, err)
	}
}

func TestArchiveSyncThroughPreflightPreservesSealedAndLateTargets(t *testing.T) {
	t.Run("covered sealed tip remains an error", func(t *testing.T) {
		ctx, base, core, stale, writer := newArchivePreflightManagers(t)
		if _, _, err := core.Propose(ctx, []byte("covered before seal")); err != nil {
			t.Fatal(err)
		}
		through := core.Tip()
		if err := writer.SyncThrough(ctx, core, through); err != nil {
			t.Fatal(err)
		}
		if err := Seal(ctx, base, "cluster", "preflight-sealed-tip"); err != nil {
			t.Fatal(err)
		}
		if err := stale.SyncThrough(ctx, core, through); !errors.Is(err, ErrArchiveSealed) {
			t.Fatalf("covered sealed target error=%v, want ErrArchiveSealed", err)
		}
	})

	t.Run("higher late waiter retries after covered lower batch", func(t *testing.T) {
		ctx, base, core, stale, writer := newArchivePreflightManagers(t)
		if _, _, err := core.Propose(ctx, []byte("already published lower target")); err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
		gates := &archiveStatsBucket{
			Bucket: base, admission: newArchiveStatsGate(), admissionRetry: newArchiveStatsGate(),
			preflightLoad: newArchiveStatsGate(), load: newArchiveStatsGate(),
			extent: newArchiveStatsGate(), head: newArchiveStatsGate(), release: newArchiveStatsGate(),
		}
		stale.bucket = gates
		gates.syncStarted.Store(true)
		for _, gate := range []*archiveStatsGate{gates.admission, gates.admissionRetry, gates.preflightLoad, gates.load, gates.extent, gates.head, gates.release} {
			t.Cleanup(gate.open)
		}
		gates.admission.open()
		gates.admissionRetry.open()

		lowDone := make(chan error, 1)
		go func() { lowDone <- stale.SyncThrough(ctx, core, 1) }()
		waitArchiveStatsGate(t, ctx, gates.preflightLoad)
		if _, _, err := core.Propose(ctx, []byte("late higher target")); err != nil {
			t.Fatal(err)
		}
		highTarget := core.Tip()
		highDone := make(chan error, 1)
		go func() { highDone <- stale.SyncThrough(ctx, core, highTarget) }()
		deadline := time.After(5 * time.Second)
		for {
			stale.batchMu.Lock()
			joined := stale.batch != nil && stale.batch.target >= highTarget
			stale.batchMu.Unlock()
			if joined {
				break
			}
			select {
			case <-deadline:
				t.Fatal("higher target did not join the in-flight batch")
			default:
				runtime.Gosched()
			}
		}
		// The lower target is already durable, so releasing preflight completes
		// that batch without admission. Install a gate for the higher waiter's
		// recursive retry before allowing the lower batch to finish.
		gates.lockRead = newArchiveStatsGate()
		t.Cleanup(gates.lockRead.open)
		gates.preflightLoad.open()
		gates.load.open()
		select {
		case err := <-lowDone:
			if err != nil {
				t.Fatalf("covered lower target: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("covered lower batch did not finish: %v", ctx.Err())
		}

		// Observe the recursive SyncThrough admission. If a successful lower
		// batch is returned directly to its late waiter, highDone wins instead.
		select {
		case <-gates.lockRead.entered:
		case err := <-highDone:
			t.Fatalf("higher waiter completed before retry publication: %v", err)
		case <-ctx.Done():
			t.Fatalf("higher waiter did not retry publication: %v", ctx.Err())
		}
		select {
		case err := <-highDone:
			t.Fatalf("higher target acknowledged before its own publication: %v", err)
		default:
		}
		gates.lockRead.open()
		waitArchiveStatsGate(t, ctx, gates.extent)
		gates.extent.open()
		waitArchiveStatsGate(t, ctx, gates.head)
		gates.head.open()
		waitArchiveStatsGate(t, ctx, gates.release)
		gates.release.open()
		select {
		case err := <-highDone:
			if err != nil {
				t.Fatalf("higher target: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("higher target did not complete: %v", ctx.Err())
		}
		reader := NewManager(base, "cluster", 1)
		t.Cleanup(reader.Close)
		if err := reader.Load(ctx); err != nil || reader.Tip() != highTarget {
			t.Fatalf("independent tip=%d want=%d err=%v", reader.Tip(), highTarget, err)
		}
	})
}

func TestArchiveStatsPreflightLoadFailureFallsBackToFencedLoad(t *testing.T) {
	ctx, bucket, core, manager := newSealableArchive(t)
	defer manager.Close()
	gates := &archiveStatsBucket{
		Bucket: bucket, admission: newArchiveStatsGate(), admissionRetry: newArchiveStatsGate(),
		extent: newArchiveStatsGate(), head: newArchiveStatsGate(), release: newArchiveStatsGate(),
	}
	for _, gate := range []*archiveStatsGate{gates.admission, gates.admissionRetry, gates.extent, gates.head, gates.release} {
		gate.open()
	}
	manager.bucket = gates
	if _, _, err := core.Propose(ctx, []byte("preflight fallback target")); err != nil {
		t.Fatal(err)
	}
	before := manager.ArchiveStats()
	gates.failPreflight.Store(true)
	gates.syncStarted.Store(true)
	through := core.Tip()
	if err := manager.SyncThrough(ctx, core, through); err != nil {
		t.Fatalf("SyncThrough after failed preflight: %v", err)
	}
	if manager.Tip() != through {
		t.Fatalf("published tip=%d want=%d", manager.Tip(), through)
	}
	after := manager.ArchiveStats()
	assertArchiveStageCount(t, after.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+2)
	assertArchiveStageCount(t, after.Stages.PublicationAdmission, stageCount(t, before.Stages.PublicationAdmission)+1)
	reader := NewManager(bucket, "cluster", 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil || reader.Tip() != through {
		t.Fatalf("independent tip=%d want=%d err=%v", reader.Tip(), through, err)
	}
}

func TestArchiveStatsCheckedExactIntegerBounds(t *testing.T) {
	var stats archiveDurationStats
	stats.record(archiveHeadPublish, time.Duration(maxExactJSONInteger))
	got := archiveStageSnapshot(stats.stages[archiveHeadPublish])
	if !got.Available || got.Count == nil || *got.Count != 1 || got.DurationNSSum == nil || *got.DurationNSSum != maxExactJSONInteger {
		t.Fatalf("exact bound snapshot=%+v", got)
	}
	stats.record(archiveHeadPublish, time.Nanosecond)
	got = archiveStageSnapshot(stats.stages[archiveHeadPublish])
	if got.Available || got.Count != nil || got.DurationNSSum != nil || stats.stages[archiveHeadPublish].durationNSSum != maxExactJSONInteger {
		t.Fatalf("overflow snapshot=%+v accumulator=%+v", got, stats.stages[archiveHeadPublish])
	}

	stats.stages[archiveLoad] = archiveDurationAccumulator{count: maxExactJSONInteger, durationNSSum: 7}
	stats.record(archiveLoad, 0)
	got = archiveStageSnapshot(stats.stages[archiveLoad])
	if got.Available || stats.stages[archiveLoad].count != maxExactJSONInteger {
		t.Fatalf("count overflow snapshot=%+v accumulator=%+v", got, stats.stages[archiveLoad])
	}
}

func stageCount(t *testing.T, stage ArchiveStageStats) uint64 {
	t.Helper()
	if !stage.Available || stage.Count == nil || stage.DurationNSSum == nil {
		t.Fatalf("archive stage is unavailable: %+v", stage)
	}
	return *stage.Count
}

func assertArchiveStageCount(t *testing.T, stage ArchiveStageStats, want uint64) {
	t.Helper()
	if got := stageCount(t, stage); got != want {
		t.Fatalf("archive stage count=%d want=%d", got, want)
	}
}

func waitArchiveStatsGate(t *testing.T, ctx context.Context, gate *archiveStatsGate) {
	t.Helper()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("archive phase did not reach its deterministic gate")
	}
}
