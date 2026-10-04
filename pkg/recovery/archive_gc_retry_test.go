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
	"strings"
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
	bucket.appendTip = func() error { return writer.SyncThrough(ctx, core, wantTip) }
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
	bucket.appendTip = func() error { return writer.SyncThrough(ctx, core, wantTip) }
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
		events["archive-gc:compaction-choice:full"] != 1 || events["archive-gc:compaction-choice:reuse"] != 1 {
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
	if events["archive-gc:publication-result:other"] != 1 || events["archive-gc:compaction-choice:full"] != 2 || events["archive-gc:compaction-choice:reuse"] != 0 {
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
	bucket.appendTip = func() error { return writer.SyncThrough(ctx, core, core.Tip()) }
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
