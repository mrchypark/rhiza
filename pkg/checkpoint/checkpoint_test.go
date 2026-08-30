package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

type getCountingBucket struct {
	objstore.Bucket
	gets      atomic.Uint64
	rootGets  atomic.Uint64
	blockGets atomic.Uint64
	uploads   atomic.Uint64
}

type blockingIterBucket struct {
	objstore.Bucket
	started  chan struct{}
	release  chan struct{}
	blockDir string
}

type racingRecoveryPinBucket struct {
	objstore.Bucket
	armed atomic.Bool
	run   func()
}

type blockingDeleteBucket struct {
	objstore.Bucket
	name    string
	started chan struct{}
	release chan struct{}
	once    atomic.Bool
}

type streamingIterBucket struct {
	objstore.Bucket
	extra *atomic.Value
	once  atomic.Bool
}

func (b *streamingIterBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if err := b.Bucket.IterWithAttributes(ctx, dir, f, options...); err != nil || !strings.HasSuffix(dir, "checkpoint/blocks") || !b.once.CompareAndSwap(false, true) {
		return err
	}
	name, _ := b.extra.Load().(string)
	if _, err := b.Bucket.Attributes(ctx, name); err != nil {
		return err
	}
	extra := objstore.IterObjectAttributes{Name: name}
	extra.SetLastModified(time.Unix(0, 0))
	return f(extra)
}

func (b *blockingDeleteBucket) Delete(ctx context.Context, name string) error {
	if name == b.name && b.once.CompareAndSwap(false, true) {
		close(b.started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *racingRecoveryPinBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	r, err := b.Bucket.Get(ctx, name)
	if err == nil && strings.Contains(name, "/checkpoint/recovery-pins/") && b.armed.CompareAndSwap(true, false) {
		b.run()
	}
	return r, err
}

func (b *blockingIterBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if b.blockDir != "" && !strings.HasSuffix(dir, b.blockDir) {
		return b.Bucket.Iter(ctx, dir, f, options...)
	}
	close(b.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
	}
	return b.Bucket.Iter(ctx, dir, f, options...)
}

func (b *getCountingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
	if strings.Contains(name, "/checkpoint/roots/") {
		b.rootGets.Add(1)
	}
	if strings.Contains(name, "/checkpoint/blocks/") {
		b.blockGets.Add(1)
	}
	return b.Bucket.Get(ctx, name)
}

func (b *getCountingBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	b.uploads.Add(1)
	return b.Bucket.Upload(ctx, name, r, options...)
}

func source(t *testing.T, role, value string) Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), role+".db")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return Source{Role: role, Path: path}
}

func createFiles(t *testing.T, manager *Manager, ctx context.Context, sources []Source, index uint64) *Checkpoint {
	t.Helper()
	claim, err := manager.AcquirePublisherClaim(ctx, "test-publisher", index-1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	root.claim = nil // tests below model roots left by completed publications.
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestFixedRoleCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "sql"), source(t, RoleGraphData, "graph")}, 11)
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	if len(root.Files) != 2 {
		t.Fatalf("files=%d", len(root.Files))
	}
	loaded := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	files, err := loaded.DownloadRootFiles(ctx, 11, root.RootHash, dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		got, err := os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		want := "sql"
		if file.Role == RoleGraphData {
			want = "graph"
		}
		if string(got) != want {
			t.Fatalf("role %s=%q", file.Role, got)
		}
	}
}

func TestCorruptBlockIsRejected(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "node", t.TempDir())
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "valid")}, 7)
	hash, _ := decodeHash(root.Files[0].Blocks[0].Hash)
	if err := bucket.Upload(ctx, manager.key(blockKey(hash)), bytes.NewReader([]byte("bad!!"))); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.DownloadRootFiles(ctx, 7, root.RootHash, t.TempDir()); err == nil {
		t.Fatal("corrupt block was accepted")
	}
}

func TestVerifyReusesDurableBlockCache(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	localDir := t.TempDir()
	manager := NewManager(bucket, "node", localDir)
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "stable")}, 7)
	bucket.gets.Store(0)
	if err := manager.Verify(ctx, root.Index, root.RootHash, root.Hash); err != nil {
		t.Fatal(err)
	}
	if gets := bucket.gets.Load(); gets != 2 {
		t.Fatalf("initial verification GETs=%d, want root and block", gets)
	}
	bucket.gets.Store(0)
	restarted := NewManager(bucket, "node", localDir)
	if err := restarted.Verify(ctx, root.Index, root.RootHash, root.Hash); err != nil {
		t.Fatal(err)
	}
	if gets := bucket.gets.Load(); gets != 1 {
		t.Fatalf("cached verification GETs=%d, want root only", gets)
	}
}

func TestPinnedRecoveryDownloadsRootAndBlockOnce(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "node", t.TempDir())
	created := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "stable")}, 7)
	bucket.gets.Store(0)
	bucket.rootGets.Store(0)
	bucket.blockGets.Store(0)
	root, err := manager.OpenRoot(ctx, created.Index, created.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := manager.PinRecoveryRoot(ctx, root, "recovery-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close(ctx)
	claim, err := manager.AcquirePublisherClaim(ctx, "active-publisher", root.Index, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := pin.Renew(ctx, time.Minute); err != nil {
		t.Fatalf("renew recovery pin during publication: %v", err)
	}
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.DownloadAndVerifyRootFiles(ctx, root, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Verify(ctx, root.Index, root.RootHash, root.Hash); err != nil {
		t.Fatal(err)
	}
	if got := bucket.rootGets.Load(); got != 1 {
		t.Fatalf("recovery root GETs=%d, want 1", got)
	}
	if got := bucket.blockGets.Load(); got != 1 {
		t.Fatalf("recovery block GETs=%d, want 1", got)
	}
}

func TestRecoveryPinReclaimsExpiredOwnerRecord(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "node", t.TempDir())
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "stable")}, 7)
	owner := "crashed-recovery"
	if err := manager.uploadRecoveryPin(ctx, manager.recoveryPinKey(owner), recoveryPinRecord{
		ConfigID: manager.configID, OwnerID: owner, Token: "expired", Index: root.Index,
		RootHash: hex.EncodeToString(root.RootHash[:]), Root: *root, LeaseUntilMS: time.Now().Add(-time.Second).UnixMilli(),
	}, objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	pin, err := manager.PinRecoveryRoot(ctx, root, owner, time.Minute)
	if err != nil {
		t.Fatalf("expired crash orphan was not reclaimed: %v", err)
	}
	defer pin.Close(ctx)
	if pin.record.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatal("reclaimed pin remained expired")
	}
}

func TestRecoveryPinGCDoesNotDeleteConcurrentRenewal(t *testing.T) {
	ctx := context.Background()
	bucket := &racingRecoveryPinBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "node", t.TempDir())
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "stable")}, 7)
	key := manager.recoveryPinKey("recovery")
	expired := recoveryPinRecord{ConfigID: manager.configID, OwnerID: "recovery", Token: "token", Index: root.Index, RootHash: hex.EncodeToString(root.RootHash[:]), Root: *root, LeaseUntilMS: time.Now().Add(-time.Second).UnixMilli()}
	if err := manager.uploadRecoveryPin(ctx, key, expired, objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	stale, err := manager.readRecoveryPin(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	bucket.run = func() {
		renewed := *stale
		renewed.LeaseUntilMS = time.Now().Add(time.Minute).UnixMilli()
		if err := manager.uploadRecoveryPin(ctx, key, renewed, objstore.WithIfMatch(stale.version)); err != nil {
			t.Errorf("inject renewal: %v", err)
		}
	}
	bucket.armed.Store(true)
	if _, err := manager.activeRecoveryRoots(ctx); !errors.Is(err, ErrPublisherBusy) {
		t.Fatalf("GC error=%v, want publisher busy", err)
	}
	current, err := manager.readRecoveryPin(ctx, key)
	if err != nil || current.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatalf("renewed pin was lost: pin=%+v err=%v", current, err)
	}
}

func TestRecoveryPinStaleCloseCannotExpireReplacement(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "node", t.TempDir())
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "stable")}, 7)
	first, err := manager.PinRecoveryRoot(ctx, root, "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.PinRecoveryRoot(ctx, root, "same-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close(ctx)
	if err := first.Close(ctx); !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("stale close error=%v, want fenced", err)
	}
	current, err := manager.readRecoveryPin(ctx, replacement.key)
	if err != nil {
		t.Fatal(err)
	}
	if current.Token != replacement.record.Token || current.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatal("stale close expired the replacement pin")
	}
}

func TestCheckpointSkipsCertifiedBlocks(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	snapshot := source(t, RoleSQLite, "stable")
	root := createFiles(t, manager, ctx, []Source{snapshot}, 7)
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	claim, err := manager.AcquirePublisherClaim(ctx, "test-publisher", 7, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bucket.uploads.Store(0)
	if _, err := manager.CreateFiles(ctx, claim, []Source{snapshot}, 8); err != nil {
		t.Fatal(err)
	}
	if uploads := bucket.uploads.Load(); uploads != 1 {
		t.Fatalf("unchanged checkpoint uploads=%d, want root only", uploads)
	}
	if err := manager.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func TestCreateFilesRefreshesKnownBlocksFromCurrent(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	stale := NewManager(bucket, "cluster", t.TempDir(), 9)
	shared := source(t, RoleSQLite, "shared")
	first := createFiles(t, stale, ctx, []Source{shared}, 1)
	if err := stale.PromoteCertifiedCurrent(ctx, first); err != nil {
		t.Fatal(err)
	}
	advance := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := advance.Load(ctx); err != nil {
		t.Fatal(err)
	}
	second := createFiles(t, advance, ctx, []Source{source(t, RoleSQLite, "new-current")}, 2)
	if err := advance.PromoteCertifiedCurrent(ctx, second); err != nil {
		t.Fatal(err)
	}
	claim, err := stale.AcquirePublisherClaim(ctx, "stale-publisher", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := stale.CreateFiles(ctx, claim, []Source{shared}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got := candidate.Files[0].Blocks[0].Generation; got != claim.Generation {
		t.Fatalf("stale certified block generation=%d, want new physical generation %d", got, claim.Generation)
	}
	if err := stale.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointListingDoesNotBlockCreation(t *testing.T) {
	bucket := &blockingIterBucket{Bucket: objstore.NewInMemBucket(), started: make(chan struct{}), release: make(chan struct{}), blockDir: "checkpoint/roots"}
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	loaded := make(chan error, 1)
	go func() { loaded <- manager.loadAll(context.Background()) }()
	<-bucket.started
	snapshot := source(t, RoleSQLite, "state")
	created := make(chan error, 1)
	claim, err := manager.AcquirePublisherClaim(context.Background(), "test-publisher", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := manager.CreateFiles(context.Background(), claim, []Source{snapshot}, 1)
		created <- err
	}()
	select {
	case err := <-created:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote checkpoint listing blocked checkpoint creation")
	}
	if err := manager.ReleasePublisherClaim(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	close(bucket.release)
	if err := <-loaded; err != nil {
		t.Fatal(err)
	}
	if len(manager.checkpoints) != 1 {
		t.Fatalf("checkpoint merge lost concurrent root: %d", len(manager.checkpoints))
	}
}

func TestGarbageCollectDropsRemoteDeletedLocalRoot(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	first := NewManager(bucket, "cluster", t.TempDir(), 9)
	old := createFiles(t, first, ctx, []Source{source(t, RoleSQLite, "old")}, 1)
	if err := first.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	current := createFiles(t, first, ctx, []Source{source(t, RoleSQLite, "current")}, 2)
	if err := first.PromoteCertifiedCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}
	second := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := second.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, err := bucket.Exists(ctx, first.key(rootName(old.Index, old.RootHash))); err != nil || exists {
		t.Fatalf("old root exists=%v err=%v", exists, err)
	}
	if err := first.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatalf("stale local root was resurrected: %v", err)
	}
	if len(first.checkpoints) != 1 || first.checkpoints[0].RootHash != current.RootHash {
		t.Fatalf("remote roots=%+v, want only current", first.checkpoints)
	}
}

func TestGarbageCollectFencesPublisherBeforeListSnapshot(t *testing.T) {
	ctx := context.Background()
	bucket := &blockingIterBucket{Bucket: objstore.NewInMemBucket(), started: make(chan struct{}), release: make(chan struct{}), blockDir: "checkpoint/roots"}
	publisher := NewManager(bucket, "cluster", t.TempDir(), 9)
	root := createFiles(t, publisher, ctx, []Source{source(t, RoleSQLite, "old")}, 1)
	if err := publisher.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	collector := NewManager(bucket, "cluster", t.TempDir(), 9)
	done := make(chan error, 1)
	go func() { done <- collector.GarbageCollect(ctx, nil, 1, 0) }()
	<-bucket.started // Iter has taken its snapshot only after GC acquired maintenance.
	if _, err := publisher.AcquirePublisherClaim(ctx, "publisher", 1, time.Minute); !errors.Is(err, ErrPublisherBusy) {
		t.Fatalf("publisher acquired during GC snapshot: %v", err)
	}
	close(bucket.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestGarbageCollectSkipsPublishedCandidate(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	publisher := NewManager(bucket, "cluster", t.TempDir(), 9)
	shared := source(t, RoleSQLite, "shared")
	old := createFiles(t, publisher, ctx, []Source{shared}, 1)
	if err := publisher.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	current := createFiles(t, publisher, ctx, []Source{source(t, RoleSQLite, "current")}, 2)
	if err := publisher.PromoteCertifiedCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}
	claim, err := publisher.AcquirePublisherClaim(ctx, "publisher", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := publisher.CreateFiles(ctx, claim, []Source{shared}, 3)
	if err != nil {
		t.Fatal(err)
	}
	collector := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := collector.GarbageCollect(ctx, nil, 1, 0); !errors.Is(err, ErrPublisherBusy) {
		t.Fatalf("GC ran during candidate publication: %v", err)
	}
	hash, err := decodeHash(candidate.Files[0].Blocks[0].Hash)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := bucket.Exists(ctx, publisher.key(blockKey(hash))); err != nil || !exists {
		t.Fatalf("candidate block exists=%v err=%v", exists, err)
	}
	if err := publisher.PromoteCertifiedCurrent(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	if err := publisher.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
}

func TestLoadUsesCurrentAndExactRoot(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	creator := NewManager(bucket, "cluster", t.TempDir(), 9)
	for index := uint64(1); index <= 3; index++ {
		root := createFiles(t, creator, ctx, []Source{source(t, RoleSQLite, string(rune('0'+index)))}, index)
		if err := creator.PromoteCertifiedCurrent(ctx, root); err != nil {
			t.Fatal(err)
		}
	}
	bucket.gets.Store(0)
	loaded := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if gets := bucket.gets.Load(); gets != 2 {
		t.Fatalf("startup GETs=%d, want 2", gets)
	}
}

func TestGarbageCollectRetiresRootBeforeBlocks(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	old := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "old")}, 10)
	if err := manager.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	newRoot := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "new")}, 11)
	if err := manager.PromoteCertifiedCurrent(ctx, newRoot); err != nil {
		t.Fatal(err)
	}
	hash, _ := decodeHash(old.Files[0].Blocks[0].Hash)
	key := manager.key(blockKey(hash))
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, key); !exists {
		t.Fatal("retired root block deleted in same GC pass")
	}
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if exists, _ := bucket.Exists(ctx, key); exists {
		t.Fatal("orphaned block survived following GC pass")
	}
}

func TestGarbageCollectPrunesVerifiedBlockCache(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "cluster", t.TempDir(), 9)
	old := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "old")}, 10)
	if err := manager.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := manager.Verify(ctx, old.Index, old.RootHash, old.Hash); err != nil {
		t.Fatal(err)
	}
	newRoot := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "new")}, 11)
	if err := manager.PromoteCertifiedCurrent(ctx, newRoot); err != nil {
		t.Fatal(err)
	}
	if err := manager.Verify(ctx, newRoot.Index, newRoot.RootHash, newRoot.Hash); err != nil {
		t.Fatal(err)
	}
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.verified[blockObjectKey(old.Files[0].Blocks[0])]; ok {
		t.Fatal("verified cache retained retired checkpoint block")
	}
	if _, ok := manager.verified[blockObjectKey(newRoot.Files[0].Blocks[0])]; !ok {
		t.Fatal("verified cache removed retained checkpoint block")
	}
}

func TestGarbageCollectAlwaysRetainsCurrentRoot(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	current := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "current")}, 1)
	if err := manager.PromoteCertifiedCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}
	_ = createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "candidate")}, 2)
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	restarted := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := restarted.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if latest := restarted.Latest(); latest == nil || latest.RootHash != current.RootHash {
		t.Fatalf("CURRENT root was not retained: %#v", latest)
	}
}

func TestGarbageCollectFromRetainsArchiveBaseFloor(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	old := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "old")}, 1)
	if err := manager.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	base := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "base")}, 2)
	if err := manager.PromoteCertifiedCurrent(ctx, base); err != nil {
		t.Fatal(err)
	}
	_ = createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "candidate")}, 3)
	if err := manager.GarbageCollectFrom(ctx, nil, 1, base.Index, 0); err != nil {
		t.Fatal(err)
	}
	if exists, err := bucket.Exists(ctx, manager.key(rootName(base.Index, base.RootHash))); err != nil || !exists {
		t.Fatalf("archive base root exists=%v err=%v", exists, err)
	}
}

func TestCreateFilesDoesNotReuseBlocksFromStaleCertifiedRoot(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	stale := NewManager(bucket, "cluster", t.TempDir(), 9)
	old := createFiles(t, stale, ctx, []Source{source(t, RoleSQLite, "same")}, 1)
	if err := stale.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}

	fresh := NewManager(bucket, "cluster", t.TempDir(), 9)
	current := createFiles(t, fresh, ctx, []Source{source(t, RoleSQLite, "different")}, 2)
	if err := fresh.PromoteCertifiedCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}

	republished := createFiles(t, stale, ctx, []Source{source(t, RoleSQLite, "same")}, 3)
	if republished.Files[0].Blocks[0].Generation == old.Files[0].Blocks[0].Generation {
		t.Fatal("publisher reused a block from its stale certified root")
	}
	key := stale.key(blockObjectKey(republished.Files[0].Blocks[0]))
	if exists, err := bucket.Exists(ctx, key); err != nil || !exists {
		t.Fatalf("republished physical block exists=%v err=%v", exists, err)
	}
}

func TestRecoveryPinDescriptorSurvivesCanonicalRootDeletion(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	old := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "pinned")}, 1)
	if err := manager.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	current := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "current")}, 2)
	if err := manager.PromoteCertifiedCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}
	root, err := manager.OpenRoot(ctx, old.Index, old.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := manager.PinRecoveryRoot(ctx, root, "root-delete-race", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close(ctx)
	if err := bucket.Delete(ctx, manager.key(rootName(old.Index, old.RootHash))); err != nil {
		t.Fatal(err)
	}
	descriptor, err := pin.Root()
	if err != nil || descriptor.RootHash != old.RootHash {
		t.Fatalf("pinned descriptor=%+v err=%v", descriptor, err)
	}
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	block := manager.key(blockObjectKey(old.Files[0].Blocks[0]))
	if exists, err := bucket.Exists(ctx, block); err != nil || !exists {
		t.Fatalf("pinned descriptor block exists=%v err=%v", exists, err)
	}
	if _, err := manager.DownloadAndVerifyRootFiles(ctx, descriptor, t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestStaleCheckpointGCCannotDeleteRepublishedBlock(t *testing.T) {
	ctx := context.Background()
	content := []byte("reused block")
	hash := sha256.Sum256(content)
	base := objstore.NewInMemBucket()
	blocked := &blockingDeleteBucket{
		Bucket:  base,
		name:    "cluster/" + blockKey(hash),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var suffix atomic.Value
	bucket := &streamingIterBucket{Bucket: blocked, extra: &suffix}
	if err := bucket.Upload(ctx, blocked.name, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	gc := NewManager(bucket, "cluster", t.TempDir(), 9)
	gcDone := make(chan error, 1)
	go func() { gcDone <- gc.GarbageCollect(ctx, nil, 1, 0) }()
	<-blocked.started
	stale, err := gc.readPublisherClaim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stale.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
	if err := gc.uploadPublisherClaim(ctx, gc.key("checkpoint/PUBLISHER"), stale, objstore.WithIfMatch(stale.version)); err != nil {
		t.Fatal(err)
	}
	publisher := NewManager(bucket, "cluster", t.TempDir(), 9)
	claim, err := publisher.AcquirePublisherClaim(ctx, "publisher", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := publisher.CreateFiles(ctx, claim, []Source{source(t, RoleSQLite, string(content))}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if blockObjectKey(root.Files[0].Blocks[0]) == blockKey(hash) {
		t.Fatal("publisher reused a stale GC candidate physical object")
	}
	suffix.Store(publisher.key(blockObjectKey(root.Files[0].Blocks[0])))
	if err := publisher.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	if err := <-gcDone; !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("stale streaming GC error=%v, want %v", err, ErrPublisherFenced)
	}
	if exists, err := bucket.Exists(ctx, suffix.Load().(string)); err != nil || !exists {
		t.Fatalf("republished physical block exists=%v err=%v", exists, err)
	}
	verifier := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := verifier.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(ctx, root.Index, root.RootHash, root.Hash); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDoesNotRewriteUnchangedBlockCache(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "cluster", t.TempDir(), 9)
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "state")}, 10)
	if err := manager.Verify(ctx, root.Index, root.RootHash, root.Hash); err != nil {
		t.Fatal(err)
	}
	manager.localDir = t.TempDir() + "/missing"
	if err := manager.Verify(ctx, root.Index, root.RootHash, root.Hash); err != nil {
		t.Fatalf("unchanged verification rewrote cache: %v", err)
	}
}

func TestCandidateDoesNotAdvanceCurrent(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	root := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "candidate")}, 10)
	loaded := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if loaded.Latest() != nil {
		t.Fatal("uncertified candidate became CURRENT")
	}
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := loaded.Load(ctx); err != nil || loaded.Latest() == nil || loaded.Latest().RootHash != root.RootHash {
		t.Fatalf("load promoted CURRENT: latest=%+v err=%v", loaded.Latest(), err)
	}
}

func TestCurrentRejectsStaleAndConflictingWriters(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	root10 := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "ten")}, 10)
	if err := manager.PromoteCertifiedCurrent(ctx, root10); err != nil {
		t.Fatal(err)
	}
	root11 := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "eleven")}, 11)
	if err := manager.PromoteCertifiedCurrent(ctx, root11); err != nil {
		t.Fatal(err)
	}
	stale := NewManager(bucket, "cluster", t.TempDir(), 9)
	if err := stale.PromoteCertifiedCurrent(ctx, root10); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("stale writer error=%v", err)
	}
	if latest := stale.Latest(); latest == nil || latest.Index != 11 || latest.RootHash != root11.RootHash {
		t.Fatalf("stale writer did not learn CURRENT: %+v", latest)
	}
	conflict := createFiles(t, manager, ctx, []Source{source(t, RoleSQLite, "other eleven")}, 11)
	if err := manager.PromoteCertifiedCurrent(ctx, conflict); err == nil {
		t.Fatal("conflicting root replaced CURRENT at the same index")
	}
}

func TestCertificationRetryReusesCandidate(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "checkpoint", Operation: "put", Key: "key", Value: []byte("value")})
	if err != nil || material.Apply(ctx, 1, value) != nil {
		t.Fatalf("apply: encode=%v", err)
	}
	auto := NewAutoCheckpointer(manager, material, 1, 0)
	auto.ConfigurePublisher("n1", func() uint64 { return 0 }, nil)
	attempts := 0
	auto.ConfigurePublication(nil, func(ctx context.Context, root *Checkpoint) error {
		attempts++
		if attempts == 1 {
			return errors.New("temporary verifier failure")
		}
		return manager.PromoteCertifiedCurrent(ctx, root)
	})
	if _, err := auto.create(ctx); err == nil {
		t.Fatal("first certification unexpectedly succeeded")
	}
	before := bucket.uploads.Load()
	if index, err := auto.create(ctx); err != nil || index != 1 {
		t.Fatalf("retry index=%d err=%v", index, err)
	}
	if delta := bucket.uploads.Load() - before; delta != 2 {
		t.Fatalf("retry uploads=%d, want CURRENT promotion and claim release", delta)
	}
}

func TestPublisherClaimFencesStaleOwnerAndIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "cluster", t.TempDir(), 9)
	first, err := manager.AcquirePublisherClaim(ctx, "n1", 10, time.Minute)
	if err != nil || first.ReservedIndex != 11 {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	if _, err := manager.AcquirePublisherClaim(ctx, "n2", 10, time.Minute); !errors.Is(err, ErrPublisherBusy) {
		t.Fatalf("concurrent owner error=%v", err)
	}
	root1 := sha256.Sum256([]byte("root-1"))
	first, err = manager.BindPublisherClaim(ctx, first, 11, root1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ValidatePublisherClaim(ctx, "n1", 11, root1); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleasePublisherClaim(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, err := manager.AcquirePublisherClaim(ctx, "n2", 11, time.Minute)
	if err != nil || second.Generation != first.Generation+1 || second.ReservedIndex != 12 {
		t.Fatalf("takeover claim=%+v err=%v", second, err)
	}
	if err := manager.ValidatePublisherClaim(ctx, "n1", 11, root1); !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("stale owner validation=%v", err)
	}
	if _, err := manager.BindPublisherClaim(ctx, second, 11, root1, time.Minute); !errors.Is(err, ErrPublisherFenced) {
		t.Fatalf("stale index bind=%v", err)
	}
	root2 := sha256.Sum256([]byte("root-2"))
	if _, err := manager.BindPublisherClaim(ctx, second, 12, root2, time.Minute); err != nil {
		t.Fatal(err)
	}
}
