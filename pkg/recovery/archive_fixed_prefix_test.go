package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type fixedPrefixRaceBucket struct {
	objstore.Bucket
	extentStarted chan struct{}
	releaseExtent chan struct{}
	blockExtent   atomic.Bool
	watchHead     atomic.Bool
	headRead      chan struct{}
	headReadOnce  sync.Once
	headTips      chan quepaxa.Slot
}

type fixedPrefixCloseBucket struct {
	objstore.Bucket
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (b *fixedPrefixCloseBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.block.CompareAndSwap(true, false) && strings.Contains(name, "/archive/blocks/") {
		close(b.started)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Get(ctx, name)
}

type fixedPrefixCloseSource struct {
	source
	manager *Manager
	bucket  *fixedPrefixCloseBucket
	slot    quepaxa.Slot
	once    sync.Once
}

type fixedPrefixExistingHeadCASBucket struct {
	objstore.Bucket
	alternateHead  []byte
	alternateBlock string
	blockPayload   []byte
	competed       atomic.Bool
	nativeConflict atomic.Bool
}

func (b *fixedPrefixExistingHeadCASBucket) Upload(ctx context.Context, name string, body io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/archive/head.bin") && b.competed.CompareAndSwap(false, true) {
		payload, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		if err := b.Bucket.Upload(ctx, b.alternateBlock, bytes.NewReader(b.blockPayload), objstore.WithIfNotExists()); err != nil {
			return err
		}
		attrs, err := b.Bucket.Attributes(ctx, name)
		if err != nil {
			return err
		}
		if attrs.Version == nil {
			return errors.New("existing HEAD has no conditional-write version")
		}
		if err := b.Bucket.Upload(ctx, name, bytes.NewReader(b.alternateHead), objstore.WithIfMatch(attrs.Version)); err != nil {
			return err
		}
		err = b.Bucket.Upload(ctx, name, bytes.NewReader(payload), options...)
		if err != nil && b.Bucket.IsConditionNotMetErr(err) {
			b.nativeConflict.Store(true)
		}
		return err
	}
	return b.Bucket.Upload(ctx, name, body, options...)
}

type fixedPrefixCASForkSource struct {
	*archiveMemorySource
}

func (s *fixedPrefixCASForkSource) DecisionsFromBounded(from quepaxa.Slot, itemLimit, payloadByteLimit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	decisions, tip, err := s.archiveMemorySource.DecisionsFromBounded(from, itemLimit, payloadByteLimit)
	if err != nil {
		return nil, tip, err
	}
	for i := range decisions {
		if decisions[i].Slot != 2 {
			continue
		}
		decisions[i].Value = bytes.Clone(decisions[i].Value)
		decisions[i].Value[len(decisions[i].Value)-1] ^= 0x80
		decisions[i].Hash = quepaxa.ValueHash(sha256.Sum256(decisions[i].Value))
		decisions[i].Certificate, err = archiveMemoryCertificateBytes(decisions[i].Slot, decisions[i].Hash)
		if err != nil {
			return nil, tip, err
		}
	}
	return decisions, tip, nil
}

func (s *fixedPrefixCASForkSource) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	if slot < 2 {
		return s.archiveMemorySource.PrefixHash(slot)
	}
	if slot != 2 {
		return [32]byte{}, false
	}
	value := make([]byte, s.valueSize)
	fillArchiveMemoryValue(value, 2)
	value[len(value)-1] ^= 0x80
	hash := sha256.Sum256(value)
	return quepaxa.AdvancePrefixHash(s.prefixes[1], 2, hash), true
}

func (s *fixedPrefixCloseSource) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	if slot == s.slot {
		s.once.Do(func() {
			s.manager.cacheMu.Lock()
			s.manager.cache = make(map[extentObject]Extent)
			s.manager.cacheMu.Unlock()
			s.bucket.block.Store(true)
		})
	}
	return s.source.PrefixHash(slot)
}

func (b *fixedPrefixRaceBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.watchHead.Load() && strings.HasSuffix(name, "/archive/head.bin") {
		b.headReadOnce.Do(func() { close(b.headRead) })
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *fixedPrefixRaceBucket) Upload(ctx context.Context, name string, body io.Reader, options ...objstore.ObjectUploadOption) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if strings.Contains(name, "/archive/blocks/") && b.blockExtent.CompareAndSwap(true, false) {
		if err := b.Bucket.Upload(ctx, name, bytes.NewReader(data), options...); err != nil {
			return err
		}
		close(b.extentStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.releaseExtent:
			return nil
		}
	}
	if strings.HasSuffix(name, "/archive/head.bin") {
		head, err := decodeHead(data)
		if err != nil {
			return err
		}
		b.headTips <- head.Tip
	}
	return b.Bucket.Upload(ctx, name, bytes.NewReader(data), options...)
}

func TestSyncPrefixThroughKeepsFixedTargetDuringOrdinarySync(t *testing.T) {
	ctx, bucket, core, seed := newSealableArchive(t)
	defer seed.Close()
	defer bucket.Close()

	if _, _, err := core.Propose(ctx, []byte("fixed prefix decision")); err != nil {
		t.Fatal(err)
	}
	prefixThrough := core.Tip()
	wrapped := &fixedPrefixRaceBucket{
		Bucket:        bucket,
		extentStarted: make(chan struct{}),
		releaseExtent: make(chan struct{}),
		headRead:      make(chan struct{}),
		headTips:      make(chan quepaxa.Slot, 2),
	}
	wrapped.blockExtent.Store(true)
	manager := NewManager(wrapped, "cluster", ArchiveConfigID)
	manager.SetGroupDelay(time.Millisecond)
	defer manager.Close()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fixedDone := make(chan error, 1)
	go func() { fixedDone <- manager.SyncPrefixThrough(ctx, core, prefixThrough) }()
	select {
	case <-wrapped.extentStarted:
	case err := <-fixedDone:
		t.Fatalf("fixed-prefix sync finished before its extent window: %v", err)
	case <-ctx.Done():
		t.Fatal("fixed-prefix sync did not reach its extent window")
	}

	if _, _, err := core.Propose(ctx, []byte("terminal decision after prefix snapshot")); err != nil {
		t.Fatal(err)
	}
	terminal := core.Tip()
	wrapped.watchHead.Store(true)
	ordinaryDone := make(chan error, 1)
	ordinaryStarted := make(chan struct{})
	go func() {
		close(ordinaryStarted)
		ordinaryDone <- manager.SyncThrough(ctx, core, terminal)
	}()
	<-ordinaryStarted
	select {
	case <-wrapped.headRead:
	case <-ctx.Done():
		t.Fatal("ordinary SyncThrough did not begin its fresh HEAD read")
	}

	close(wrapped.releaseExtent)
	select {
	case err := <-fixedDone:
		if err != nil {
			t.Fatalf("fixed-prefix sync: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("fixed-prefix sync did not finish")
	}
	select {
	case err := <-ordinaryDone:
		if err != nil {
			t.Fatalf("ordinary terminal SyncThrough: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("ordinary SyncThrough did not finish")
	}

	first, second := <-wrapped.headTips, <-wrapped.headTips
	if first != prefixThrough || second != terminal {
		t.Fatalf("published HEAD tips=(%d,%d), want fixed prefix %d then ordinary terminal %d", first, second, prefixThrough, terminal)
	}
	if manager.Tip() != terminal {
		t.Fatalf("final archive tip=%d, want ordinary SyncThrough terminal %d", manager.Tip(), terminal)
	}
}

func TestSyncPrefixThroughPreservesFreshSealAndClosedChecks(t *testing.T) {
	ctx, bucket, core, cached := newSealableArchive(t)
	defer cached.Close()
	defer bucket.Close()

	covered := cached.Tip()
	if err := Seal(ctx, bucket, "cluster", "fixed-prefix-seal"); err != nil {
		t.Fatal(err)
	}
	if err := cached.SyncPrefixThrough(ctx, core, covered); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("sealed archive error=%v, want ErrArchiveSealed", err)
	}

	closed := NewManager(bucket, "cluster", ArchiveConfigID)
	closed.Close()
	if err := closed.SyncPrefixThrough(ctx, core, core.Tip()+1); !errors.Is(err, ErrArchiveClosed) {
		t.Fatalf("closed archive error=%v, want ErrArchiveClosed", err)
	}
}

func TestSyncPrefixThroughCloseCancelsInFlightPrefixRead(t *testing.T) {
	for _, extend := range []bool{false, true} {
		name := "already-ahead-prefix"
		if extend {
			name = "existing-boundary-prefix"
		}
		t.Run(name, func(t *testing.T) {
			ctx, bucket, core, seeded := newSealableArchive(t)
			seeded.Close()
			if extend {
				if _, _, err := core.Propose(ctx, []byte("fixed-prefix extension")); err != nil {
					t.Fatal(err)
				}
			}

			wrapped := &fixedPrefixCloseBucket{
				Bucket:  bucket,
				started: make(chan struct{}),
				release: make(chan struct{}),
			}
			manager := NewManager(wrapped, "cluster", ArchiveConfigID)
			source := &fixedPrefixCloseSource{source: core, manager: manager, bucket: wrapped, slot: core.Tip()}
			syncDone := make(chan error, 1)
			go func() { syncDone <- manager.SyncPrefixThrough(ctx, source, core.Tip()) }()

			select {
			case <-wrapped.started:
			case <-time.After(5 * time.Second):
				close(wrapped.release)
				manager.Close()
				t.Fatal("fixed-prefix read did not reach the blocked extent")
			}

			closeDone := make(chan struct{})
			go func() {
				manager.Close()
				close(closeDone)
			}()
			select {
			case err := <-syncDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("fixed-prefix read error=%v, want context cancellation", err)
				}
			case <-time.After(5 * time.Second):
				close(wrapped.release)
				<-syncDone
				t.Fatal("Close did not cancel the in-flight fixed-prefix read")
			}
			select {
			case <-closeDone:
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not wait for the fixed-prefix operation to finish")
			}
			close(wrapped.release)
		})
	}
}

func TestSyncPrefixThroughRevalidatesAfterNativeCASConflict(t *testing.T) {
	ctx := context.Background()
	bucket := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
	seed, err := newArchiveMemorySource(1, 64)
	if err != nil {
		t.Fatal(err)
	}
	seedManager := NewManager(bucket, "fixed-prefix-cas", ArchiveConfigID)
	if err := seedManager.SyncThrough(ctx, seed, seed.Tip()); err != nil {
		t.Fatalf("seed existing HEAD: %v", err)
	}
	seedManager.Close()

	target, err := newArchiveMemorySource(2, 64)
	if err != nil {
		t.Fatal(err)
	}
	fork := &fixedPrefixCASForkSource{archiveMemorySource: target}
	competitorBucket := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "competitor-objects"))
	competitorSeed := NewManager(competitorBucket, "fixed-prefix-cas", ArchiveConfigID)
	if err := competitorSeed.SyncThrough(ctx, seed, seed.Tip()); err != nil {
		t.Fatalf("seed competing archive: %v", err)
	}
	competitor := NewManager(competitorBucket, "fixed-prefix-cas", ArchiveConfigID)
	if err := competitor.SyncThrough(ctx, fork, 2); err != nil {
		t.Fatalf("publish competing valid prefix: %v", err)
	}
	competitor.mu.Lock()
	if len(competitor.extents) < 2 {
		competitor.mu.Unlock()
		t.Fatalf("competing archive extents=%d, want seed and divergent suffix", len(competitor.extents))
	}
	newRef := competitor.extents[len(competitor.extents)-1]
	alternateHead, err := encodeHead(competitor.head)
	competitor.mu.Unlock()
	if err != nil {
		t.Fatalf("encode competing HEAD: %v", err)
	}
	extentReader, err := competitorBucket.Get(ctx, competitor.key(extentObjectKey(newRef.hash, newRef.object)))
	if err != nil {
		t.Fatalf("read competing extent: %v", err)
	}
	blockPayload, err := io.ReadAll(extentReader)
	closeErr := extentReader.Close()
	if err != nil {
		t.Fatalf("read competing extent body: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("close competing extent reader: %v", closeErr)
	}
	competitor.Close()
	competitorSeed.Close()
	keyManager := NewManager(bucket, "fixed-prefix-cas", ArchiveConfigID)
	alternateBlock := keyManager.key(extentObjectKey(newRef.hash, newRef.object))
	keyManager.Close()
	wrapped := &fixedPrefixExistingHeadCASBucket{
		Bucket:         bucket,
		alternateHead:  alternateHead,
		alternateBlock: alternateBlock,
		blockPayload:   blockPayload,
	}
	manager := NewManager(wrapped, "fixed-prefix-cas", ArchiveConfigID)
	defer manager.Close()
	if !manager.CASSupported() {
		t.Fatal("filesystem bucket must support conditional HEAD publication")
	}
	if err := manager.SyncPrefixThrough(ctx, target, 2); err == nil || !strings.Contains(err.Error(), "archive prefix mismatch at slot 2") {
		t.Fatalf("fixed-prefix sync error=%v, want divergent winner rejected at slot 2", err)
	}
	if !wrapped.competed.Load() || !wrapped.nativeConflict.Load() {
		t.Fatalf("competing publication=%t native conditional conflict=%t; want both", wrapped.competed.Load(), wrapped.nativeConflict.Load())
	}

	reader := NewManager(bucket, "fixed-prefix-cas", ArchiveConfigID)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("read competing HEAD: %v", err)
	}
	wantPrefix, ok := fork.PrefixHash(2)
	if !ok {
		t.Fatal("competing source prefix is unavailable")
	}
	gotPrefix, err := reader.prefixAt(ctx, 2)
	if err != nil {
		t.Fatalf("read competing prefix: %v", err)
	}
	if reader.Tip() != 2 || gotPrefix != wantPrefix {
		t.Fatalf("competing HEAD tip=%d prefix=%x; want tip=2 prefix=%x", reader.Tip(), gotPrefix, wantPrefix)
	}
}

func TestSyncPrefixThroughRejectsDivergentAlreadyPublishedPrefix(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", ArchiveConfigID)
	defer manager.Close()

	archived, err := newArchiveMemorySource(3, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SyncThrough(ctx, archived, 3); err != nil {
		t.Fatalf("seed archive: %v", err)
	}
	beforeHead, beforeVersion := fixedPrefixHead(manager)

	divergent, err := newArchiveMemorySource(3, 65)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SyncPrefixThrough(ctx, divergent, 2); err == nil {
		t.Fatal("accepted a divergent prefix already covered by the archive")
	}
	afterHead, afterVersion := fixedPrefixHead(manager)
	if afterHead != beforeHead || !sameObjectVersion(&beforeVersion, &afterVersion) {
		t.Fatalf("divergent prefix changed HEAD: before=%+v after=%+v", beforeHead, afterHead)
	}
}

func TestSyncPrefixThroughRejectsDivergentExtensionBoundary(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", ArchiveConfigID)
	defer manager.Close()

	archived, err := newArchiveMemorySource(1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SyncThrough(ctx, archived, 1); err != nil {
		t.Fatalf("seed archive: %v", err)
	}
	beforeHead, beforeVersion := fixedPrefixHead(manager)

	divergent, err := newArchiveMemorySource(3, 65)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SyncPrefixThrough(ctx, divergent, 3); err == nil {
		t.Fatal("extended archive from a divergent existing prefix")
	}
	afterHead, afterVersion := fixedPrefixHead(manager)
	if afterHead != beforeHead || !sameObjectVersion(&beforeVersion, &afterVersion) {
		t.Fatalf("divergent extension changed HEAD: before=%+v after=%+v", beforeHead, afterHead)
	}
}

func TestSyncPrefixThroughHonorsTrimmedCheckpointBoundary(t *testing.T) {
	ctx, bucket, core, manager := newSealableArchive(t)
	defer bucket.Close()
	defer manager.Close()
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
		t.Fatalf("prepare checkpoint: %v", err)
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
	if err := manager.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("publish before trim: %v", err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("latest checkpoint seal: ok=%v err=%v", ok, err)
	}
	if err := manager.TrimThrough(ctx, sealed, decision); err != nil {
		t.Fatalf("trim through checkpoint: %v", err)
	}

	if err := manager.SyncPrefixThrough(ctx, core, sealed.Index); err != nil {
		t.Fatalf("matching prefix at checkpoint base: %v", err)
	}
	if err := manager.SyncPrefixThrough(ctx, core, sealed.Index-1); !errors.Is(err, quepaxa.ErrCompacted) {
		t.Fatalf("prefix below checkpoint base error=%v, want ErrCompacted", err)
	}

	divergent, err := newArchiveMemorySource(int(sealed.Index), 65)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SyncPrefixThrough(ctx, divergent, sealed.Index); err == nil {
		t.Fatal("accepted a divergent prefix at the checkpoint base")
	}
}

func fixedPrefixHead(manager *Manager) (fixedPrefixHeadSnapshot, objstore.ObjectVersion) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	var version objstore.ObjectVersion
	if manager.headCAS != nil {
		version = *manager.headCAS
	}
	return fixedPrefixHeadSnapshot{generation: manager.head.Generation, base: manager.head.Base, tip: manager.head.Tip}, version
}

type fixedPrefixHeadSnapshot struct {
	generation uint64
	base       quepaxa.Slot
	tip        quepaxa.Slot
}
