package recovery

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	admission      *archiveStatsGate
	admissionRetry *archiveStatsGate
	load           *archiveStatsGate
	extent         *archiveStatsGate
	head           *archiveStatsGate
	release        *archiveStatsGate
}

func (b *archiveStatsBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.syncStarted.Load() && strings.HasSuffix(name, "/archive/PUBLISH_LOCK") {
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
	if b.syncStarted.Load() && strings.HasSuffix(name, "/archive/head.bin") {
		if err := b.load.wait(ctx); err != nil {
			return objstore.ObjectAttributes{}, err
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
		Bucket: bucket, admission: newArchiveStatsGate(), admissionRetry: newArchiveStatsGate(), load: newArchiveStatsGate(),
		extent: newArchiveStatsGate(), head: newArchiveStatsGate(), release: newArchiveStatsGate(),
	}
	manager.bucket = gates
	t.Cleanup(func() {
		gates.admission.open()
		gates.admissionRetry.open()
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

	waitArchiveStatsGate(t, ctx, gates.admission)
	gates.admission.open()
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
	waitArchiveStatsGate(t, ctx, gates.load)
	afterAdmission := manager.ArchiveStats()
	assertArchiveStageCount(t, afterAdmission.Stages.PublicationAdmission, stageCount(t, before.Stages.PublicationAdmission)+1)
	if got := *afterAdmission.Stages.PublicationAdmission.DurationNSSum - *before.Stages.PublicationAdmission.DurationNSSum; got < uint64(50*time.Millisecond) {
		t.Fatalf("admission duration=%dns, want at least one bounded retry interval", got)
	}
	assertArchiveStageCount(t, afterAdmission.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad))
	assertArchiveStageCount(t, afterAdmission.Stages.ExtentBuildUpload, stageCount(t, before.Stages.ExtentBuildUpload))
	assertArchiveStageCount(t, afterAdmission.Stages.HeadPublish, stageCount(t, before.Stages.HeadPublish))
	assertArchiveStageCount(t, afterAdmission.Stages.PublicationRelease, stageCount(t, before.Stages.PublicationRelease))

	gates.load.open()
	waitArchiveStatsGate(t, ctx, gates.extent)
	afterLoad := manager.ArchiveStats()
	assertArchiveStageCount(t, afterLoad.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+1)
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
	assertArchiveStageCount(t, after.Stages.ArchiveLoad, stageCount(t, before.Stages.ArchiveLoad)+1)
	assertArchiveStageCount(t, after.Stages.ExtentBuildUpload, stageCount(t, before.Stages.ExtentBuildUpload)+1)
	assertArchiveStageCount(t, after.Stages.HeadPublish, stageCount(t, before.Stages.HeadPublish)+1)
	assertArchiveStageCount(t, after.Stages.PublicationRelease, stageCount(t, before.Stages.PublicationRelease)+1)
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
