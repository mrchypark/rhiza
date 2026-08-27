package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

type getCountingBucket struct {
	objstore.Bucket
	gets    atomic.Uint64
	uploads atomic.Uint64
}

func (b *getCountingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.gets.Add(1)
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

func TestFixedRoleCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	root, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "sql"), source(t, RoleGraphData, "graph")}, 11)
	if err != nil {
		t.Fatal(err)
	}
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
	root, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "valid")}, 7)
	if err != nil {
		t.Fatal(err)
	}
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
	root, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "stable")}, 7)
	if err != nil {
		t.Fatal(err)
	}
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

func TestCheckpointSkipsCertifiedBlocks(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "cluster", t.TempDir(), 9)
	snapshot := source(t, RoleSQLite, "stable")
	root, err := manager.CreateFiles(ctx, []Source{snapshot}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	bucket.uploads.Store(0)
	if _, err := manager.CreateFiles(ctx, []Source{snapshot}, 8); err != nil {
		t.Fatal(err)
	}
	if uploads := bucket.uploads.Load(); uploads != 1 {
		t.Fatalf("unchanged checkpoint uploads=%d, want root only", uploads)
	}
}

func TestLoadUsesCurrentAndExactRoot(t *testing.T) {
	ctx := context.Background()
	bucket := &getCountingBucket{Bucket: objstore.NewInMemBucket()}
	creator := NewManager(bucket, "cluster", t.TempDir(), 9)
	for index := uint64(1); index <= 3; index++ {
		root, err := creator.CreateFiles(ctx, []Source{source(t, RoleSQLite, string(rune('0'+index)))}, index)
		if err != nil {
			t.Fatal(err)
		}
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
	old, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "old")}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	newRoot, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "new")}, 11)
	if err != nil {
		t.Fatal(err)
	}
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
	old, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "old")}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := manager.Verify(ctx, old.Index, old.RootHash, old.Hash); err != nil {
		t.Fatal(err)
	}
	newRoot, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "new")}, 11)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteCertifiedCurrent(ctx, newRoot); err != nil {
		t.Fatal(err)
	}
	if err := manager.Verify(ctx, newRoot.Index, newRoot.RootHash, newRoot.Hash); err != nil {
		t.Fatal(err)
	}
	if err := manager.GarbageCollect(ctx, nil, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.verified[old.Files[0].Blocks[0].Hash]; ok {
		t.Fatal("verified cache retained retired checkpoint block")
	}
	if _, ok := manager.verified[newRoot.Files[0].Blocks[0].Hash]; !ok {
		t.Fatal("verified cache removed retained checkpoint block")
	}
}

func TestVerifyDoesNotRewriteUnchangedBlockCache(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "cluster", t.TempDir(), 9)
	root, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "state")}, 10)
	if err != nil {
		t.Fatal(err)
	}
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
	root, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "candidate")}, 10)
	if err != nil {
		t.Fatal(err)
	}
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
	root10, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "ten")}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteCertifiedCurrent(ctx, root10); err != nil {
		t.Fatal(err)
	}
	root11, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "eleven")}, 11)
	if err != nil {
		t.Fatal(err)
	}
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
	conflict, err := manager.CreateFiles(ctx, []Source{source(t, RoleSQLite, "other eleven")}, 11)
	if err != nil {
		t.Fatal(err)
	}
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
	if delta := bucket.uploads.Load() - before; delta != 1 {
		t.Fatalf("retry uploads=%d, want only CURRENT promotion", delta)
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
