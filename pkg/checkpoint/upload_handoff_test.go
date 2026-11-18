package checkpoint

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

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/ncruces/go-sqlite3/driver"
	"github.com/thanos-io/objstore"
)

type issue310Upload struct {
	key   string
	bytes int64
	err   error
}

type issue310RecordingBucket struct {
	*appendOnlyBucket
	mu      sync.Mutex
	uploads []issue310Upload
}

var errIssue310PostCASRead = errors.New("test: read after successful CURRENT CAS")

type issue310PostCASReadFailureBucket struct {
	*issue310RecordingBucket
	fail       atomic.Bool
	readErrors atomic.Int64
}

func (b *issue310PostCASReadFailureBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if b.fail.Load() {
		b.readErrors.Add(1)
		return nil, errIssue310PostCASRead
	}
	return b.issue310RecordingBucket.Get(ctx, key)
}

func (b *issue310PostCASReadFailureBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	if b.fail.Load() {
		b.readErrors.Add(1)
		return objstore.ObjectAttributes{}, errIssue310PostCASRead
	}
	return b.issue310RecordingBucket.Attributes(ctx, key)
}

func (b *issue310PostCASReadFailureBucket) Upload(ctx context.Context, key string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	err := b.issue310RecordingBucket.Upload(ctx, key, r, options...)
	if err == nil && strings.HasSuffix(key, "/checkpoint/CURRENT") {
		b.fail.Store(true)
	}
	return err
}

type issue310DelayedACKBucket struct {
	*issue310RecordingBucket
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

type issue310RootReadFailureBucket struct {
	*issue310RecordingBucket
	failKey atomic.Value
}

func (b *issue310RootReadFailureBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if failed, _ := b.failKey.Load().(string); failed != "" && key == failed {
		return nil, errIssue310PostCASRead
	}
	return b.issue310RecordingBucket.Get(ctx, key)
}

func (b *issue310DelayedACKBucket) Upload(ctx context.Context, key string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	err := b.issue310RecordingBucket.Upload(ctx, key, r, options...)
	if err == nil && strings.HasSuffix(key, "/checkpoint/CURRENT") && b.block.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return err
}

func (b *issue310RecordingBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appendOnlyBucket.Get(ctx, key)
}

func (b *issue310RecordingBucket) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appendOnlyBucket.GetRange(ctx, key, off, length)
}

func (b *issue310RecordingBucket) Exists(ctx context.Context, key string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appendOnlyBucket.Exists(ctx, key)
}

func (b *issue310RecordingBucket) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appendOnlyBucket.Attributes(ctx, key)
}

func (b *issue310RecordingBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.appendOnlyBucket.Iter(ctx, dir, f, options...)
}

func (b *issue310RecordingBucket) Upload(ctx context.Context, key string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	err = b.appendOnlyBucket.Upload(ctx, key, bytes.NewReader(data), options...)
	b.uploads = append(b.uploads, issue310Upload{key: key, bytes: int64(len(data)), err: err})
	b.mu.Unlock()
	return err
}

func (b *issue310RecordingBucket) uploadsAfter(index int) []issue310Upload {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]issue310Upload(nil), b.uploads[index:]...)
}

func (b *issue310RecordingBucket) uploadCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.uploads)
}

func issue310BlockUploads(uploads []issue310Upload) []issue310Upload {
	var blocks []issue310Upload
	for _, upload := range uploads {
		if strings.Contains(upload.key, "/checkpoint/blocks/") {
			blocks = append(blocks, upload)
		}
	}
	return blocks
}

func issue310RootUploads(uploads []issue310Upload) []issue310Upload {
	var roots []issue310Upload
	for _, upload := range uploads {
		if strings.Contains(upload.key, "/checkpoint/roots/") {
			roots = append(roots, upload)
		}
	}
	return roots
}

func issue310UploadBytes(uploads []issue310Upload) (attempted, acknowledged int64) {
	for _, upload := range uploads {
		attempted += upload.bytes
		if upload.err == nil {
			acknowledged += upload.bytes
		}
	}
	return attempted, acknowledged
}

func issue310Policy9Source(t *testing.T) Source {
	return issue310Policy9SourceValue(t, "unchanged")
}

func issue310Policy9SourceValue(t *testing.T, value string) Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), "request.db")
	db, err := driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY, value TEXT); INSERT INTO _rhiza_meta VALUES ('sql_execution_policy','9'); CREATE TABLE payload(value TEXT)`)
	if err == nil {
		_, err = db.Exec(`INSERT INTO payload VALUES (?)`, value)
	}
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return Source{Role: RoleSQLite, Path: path}
}

func issue310Policy9Locator(cut uint64, root [32]byte) ManagedAppendOnlyLocator {
	return ManagedAppendOnlyLocator{
		Policy:           int(sqlpolicy.ManagedRequestMaterializationPolicy),
		Index:            cut,
		RootHash:         root,
		SealDecisionSlot: cut + 1,
		SealDecisionHash: sha256.Sum256([]byte{byte(cut)}),
	}
}

func issue310Manager(t *testing.T, bucket objstore.Bucket, mode string) *Manager {
	t.Helper()
	if mode == "append-only" {
		m, err := NewAppendOnlyManager(bucket, "issue310", "", 9)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	return NewManager(bucket, "issue310", "", 9)
}

func issue310Policy9Manager(t *testing.T, bucket objstore.Bucket) *Manager {
	t.Helper()
	m, err := NewAppendOnlyManagerForPolicy(bucket, "issue310", t.TempDir(), sqlpolicy.ManagedRequestMaterializationPolicy, 9)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestIssue310ManagedPolicy9PromotionAndRestartDedup(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same-manager-after-promotion"
		if restart {
			name = "restart-after-promotion"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			bucket := &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}
			snapshot := issue310Policy9Source(t)
			publisher := issue310Policy9Manager(t, bucket)
			first, err := publisher.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := publisher.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(1, first.RootHash)); err != nil {
				t.Fatal(err)
			}

			next := publisher
			cuts := []uint64{2}
			if restart {
				next = issue310Policy9Manager(t, bucket)
				if err := next.Load(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				cuts = []uint64{2, 3}
			}
			for _, cut := range cuts {
				before := bucket.uploadCount()
				candidate, err := next.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, cut)
				if err != nil {
					t.Fatal(err)
				}
				if candidate.Index != cut || candidate.Generation != cut {
					t.Fatalf("cut %d candidate index=%d generation=%d", cut, candidate.Index, candidate.Generation)
				}
				uploads := bucket.uploadsAfter(before)
				blocks := issue310BlockUploads(uploads)
				blockAttempted, blockAcknowledged := issue310UploadBytes(blocks)
				roots := issue310RootUploads(uploads)
				rootAttempted, rootAcknowledged := issue310UploadBytes(roots)
				t.Logf("cut=%d block_calls=%d block_attempted_bytes=%d block_acknowledged_bytes=%d root_calls=%d root_attempted_bytes=%d root_acknowledged_bytes=%d", cut, len(blocks), blockAttempted, blockAcknowledged, len(roots), rootAttempted, rootAcknowledged)
				if len(blocks) != 0 {
					keys := make([]string, 0, len(blocks))
					for _, upload := range blocks {
						keys = append(keys, upload.key)
					}
					t.Errorf("cut %d certified block reuploaded after promotion: calls=%d attempted_bytes=%d acknowledged_bytes=%d keys=%v", cut, len(blocks), blockAttempted, blockAcknowledged, keys)
				}
				if len(roots) != 1 || rootAcknowledged == 0 {
					t.Errorf("cut %d candidate root uploads=%d attempted_bytes=%d acknowledged_bytes=%d, want one nonzero root", cut, len(roots), rootAttempted, rootAcknowledged)
				}
				if !restart {
					if err := next.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(cut, candidate.RootHash)); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestIssue310ManagedPolicy9AlreadyCurrentPromotionPopulatesCertified(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}
	snapshot := issue310Policy9Source(t)
	seed := issue310Policy9Manager(t, bucket)
	first, err := seed.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 1)
	if err != nil {
		t.Fatal(err)
	}
	locator := issue310Policy9Locator(1, first.RootHash)
	if err := seed.PromoteManagedAppendOnlyLocator(ctx, locator); err != nil {
		t.Fatal(err)
	}

	// A new manager without Load exercises the already-current promotion path.
	next := issue310Policy9Manager(t, bucket)
	if err := next.PromoteManagedAppendOnlyLocator(ctx, locator); err != nil {
		t.Fatal(err)
	}
	if latest := next.Latest(); latest == nil || latest.Index != 1 || latest.RootHash != first.RootHash {
		t.Fatalf("already-current promotion cached %+v", latest)
	}
	before := bucket.uploadCount()
	if _, err := next.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 2); err != nil {
		t.Fatal(err)
	}
	uploads := bucket.uploadsAfter(before)
	if blocks := issue310BlockUploads(uploads); len(blocks) != 0 {
		t.Fatalf("already-current promotion did not deduplicate: %+v", blocks)
	}
	if roots := issue310RootUploads(uploads); len(roots) != 1 {
		t.Fatalf("candidate root uploads=%d, want 1", len(roots))
	}
}

func TestIssue310ManagedPolicy9SuccessfulCASAcceptsNoPostCommitReads(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310PostCASReadFailureBucket{issue310RecordingBucket: &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}}
	snapshot := issue310Policy9Source(t)
	manager := issue310Policy9Manager(t, bucket)
	root, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(1, root.RootHash)); err != nil {
		t.Fatalf("successful CAS depended on later reads: %v", err)
	}
	if reads := bucket.readErrors.Load(); reads != 0 {
		t.Fatalf("successful CAS performed %d post-commit reads", reads)
	}
	if latest := manager.Latest(); latest == nil || latest.RootHash != root.RootHash {
		t.Fatalf("successful CAS cached %+v", latest)
	}
}

func TestIssue310ManagedPolicy9LateOutOfOrderCompletionCannotDowngrade(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310DelayedACKBucket{
		issue310RecordingBucket: &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}},
		entered:                 make(chan struct{}),
		release:                 make(chan struct{}),
	}
	snapshot := issue310Policy9Source(t)
	manager := issue310Policy9Manager(t, bucket)
	first, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(1, first.RootHash)); err != nil {
		t.Fatal(err)
	}
	second, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 2)
	if err != nil {
		t.Fatal(err)
	}
	third, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 3)
	if err != nil {
		t.Fatal(err)
	}

	bucket.block.Store(true)
	olderDone := make(chan error, 1)
	go func() {
		olderDone <- manager.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(2, second.RootHash))
	}()
	<-bucket.entered
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(3, third.RootHash)); err != nil {
		t.Fatal(err)
	}
	close(bucket.release)
	if err := <-olderDone; err != nil {
		t.Fatalf("delayed successful CAS: %v", err)
	}
	if latest := manager.Latest(); latest == nil || latest.Index != 3 || latest.RootHash != third.RootHash {
		t.Fatalf("late older completion downgraded cache to %+v", latest)
	}
	before := bucket.uploadCount()
	if _, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 4); err != nil {
		t.Fatal(err)
	}
	if blocks := issue310BlockUploads(bucket.uploadsAfter(before)); len(blocks) != 0 {
		t.Fatalf("late completion left stale block cache: %+v", blocks)
	}
}

func TestIssue310ManagedPolicy9StaleEmptyManagerObservesCurrentBestEffort(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}
	snapshot := issue310Policy9Source(t)
	seed := issue310Policy9Manager(t, bucket)
	var latest, stale *Checkpoint
	for cut := uint64(1); cut <= 3; cut++ {
		root, err := seed.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, cut)
		if err != nil {
			t.Fatal(err)
		}
		if err := seed.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(cut, root.RootHash)); err != nil {
			t.Fatal(err)
		}
		latest = root
		if cut == 2 {
			stale = root
		}
	}
	staleManager := issue310Policy9Manager(t, bucket)
	if staleManager.Latest() != nil {
		t.Fatal("new manager did not start with an empty cache")
	}
	if err := staleManager.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(2, stale.RootHash)); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("stale promotion error=%v, want stale", err)
	}
	if cached := staleManager.Latest(); cached == nil || cached.Index != latest.Index || cached.RootHash != latest.RootHash {
		t.Fatalf("stale empty manager cached %+v, want cut %d", cached, latest.Index)
	}
	before := bucket.uploadCount()
	if _, err := staleManager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 4); err != nil {
		t.Fatal(err)
	}
	if blocks := issue310BlockUploads(bucket.uploadsAfter(before)); len(blocks) != 0 {
		t.Fatalf("stale empty manager did not observe current blocks: %+v", blocks)
	}
}

func TestIssue310ManagedPolicy9ObserverClearsSameCutDifferentRoot(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}
	manager := issue310Policy9Manager(t, bucket)
	sourceA := issue310Policy9SourceValue(t, "same-cut-a")
	sourceB := issue310Policy9SourceValue(t, "same-cut-b")
	sourceC := issue310Policy9SourceValue(t, "same-cut-c")
	first, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceA}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(1, first.RootHash)); err != nil {
		t.Fatal(err)
	}
	staleRoot, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceA}, 2)
	if err != nil {
		t.Fatal(err)
	}
	sameCutA, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceB}, 3)
	if err != nil {
		t.Fatal(err)
	}
	locatorA := issue310Policy9Locator(3, sameCutA.RootHash)
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, locatorA); err != nil {
		t.Fatal(err)
	}

	cached := issue310Policy9Manager(t, bucket)
	if err := cached.PromoteManagedAppendOnlyLocator(ctx, locatorA); err != nil {
		t.Fatal(err)
	}
	sameCutB, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceC}, 3)
	if err != nil {
		t.Fatal(err)
	}
	locatorB := issue310Policy9Locator(3, sameCutB.RootHash)
	locatorB.SealDecisionSlot = locatorA.SealDecisionSlot + 1
	if locatorB.RootHash == locatorA.RootHash {
		t.Fatal("same-cut fixture roots are identical")
	}
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, locatorB); err != nil {
		t.Fatal(err)
	}
	if latest := cached.Latest(); latest == nil || latest.RootHash != locatorA.RootHash {
		t.Fatalf("cached manager did not retain its older same-cut observation: %+v", latest)
	}

	// The stale result observes locatorB at the same cut as cached locatorA.
	if err := cached.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(2, staleRoot.RootHash)); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("stale promotion error=%v, want stale", err)
	}
	if latest := cached.Latest(); latest != nil {
		t.Fatalf("observer retained ambiguous same-cut root %+v", latest)
	}
}

func TestIssue310ManagedPolicy9ObserverReadFailureClearsSameCutCache(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310RootReadFailureBucket{issue310RecordingBucket: &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}}
	manager := issue310Policy9Manager(t, bucket)
	sourceA := issue310Policy9SourceValue(t, "read-failure-a")
	sourceB := issue310Policy9SourceValue(t, "read-failure-b")
	first, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceA}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(1, first.RootHash)); err != nil {
		t.Fatal(err)
	}
	staleRoot, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceA}, 2)
	if err != nil {
		t.Fatal(err)
	}
	sameCutA, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceA}, 3)
	if err != nil {
		t.Fatal(err)
	}
	locatorA := issue310Policy9Locator(3, sameCutA.RootHash)
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, locatorA); err != nil {
		t.Fatal(err)
	}
	cached := issue310Policy9Manager(t, bucket)
	if err := cached.PromoteManagedAppendOnlyLocator(ctx, locatorA); err != nil {
		t.Fatal(err)
	}
	sameCutB, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceB}, 3)
	if err != nil {
		t.Fatal(err)
	}
	locatorB := issue310Policy9Locator(3, sameCutB.RootHash)
	locatorB.SealDecisionSlot = locatorA.SealDecisionSlot + 1
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, locatorB); err != nil {
		t.Fatal(err)
	}

	bucket.failKey.Store(manager.key(rootName(locatorB.Index, locatorB.RootHash)))
	if err := cached.PromoteManagedAppendOnlyLocator(ctx, issue310Policy9Locator(2, staleRoot.RootHash)); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("stale promotion error=%v, want stale", err)
	}
	if latest := cached.Latest(); latest != nil {
		t.Fatalf("root read failure retained ambiguous same-cut cache %+v", latest)
	}
	bucket.failKey.Store("")

	next, err := cached.CreateManagedAppendOnlyCandidate(ctx, []Source{sourceB}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if next.Files[0].Blocks[0].Hash != sameCutB.Files[0].Blocks[0].Hash {
		t.Fatalf("next candidate used ambiguous block %s, want %s", next.Files[0].Blocks[0].Hash, sameCutB.Files[0].Blocks[0].Hash)
	}
	if _, err := cached.DownloadAndVerifyRootFiles(ctx, next, t.TempDir()); err != nil {
		t.Fatalf("next candidate failed verification: %v", err)
	}
}

func TestIssue310ManagedPolicy9PromotionOrderingKeepsLatestCertified(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}
	snapshot := issue310Policy9Source(t)
	manager := issue310Policy9Manager(t, bucket)
	promoteCandidate := func(cut uint64) (*Checkpoint, ManagedAppendOnlyLocator) {
		t.Helper()
		root, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, cut)
		if err != nil {
			t.Fatal(err)
		}
		return root, issue310Policy9Locator(cut, root.RootHash)
	}
	first, firstLocator := promoteCandidate(1)
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, firstLocator); err != nil {
		t.Fatal(err)
	}
	second, secondLocator := promoteCandidate(2)
	third, thirdLocator := promoteCandidate(3)

	start := make(chan struct{})
	results := make(chan struct {
		locator ManagedAppendOnlyLocator
		err     error
	}, 2)
	for _, locator := range []ManagedAppendOnlyLocator{secondLocator, thirdLocator} {
		go func(locator ManagedAppendOnlyLocator) {
			<-start
			results <- struct {
				locator ManagedAppendOnlyLocator
				err     error
			}{locator: locator, err: manager.PromoteManagedAppendOnlyLocator(ctx, locator)}
		}(locator)
	}
	close(start)
	for range 2 {
		result := <-results
		if result.locator == thirdLocator && result.err != nil {
			t.Fatalf("latest concurrent promotion failed: %v", result.err)
		}
		if result.locator == secondLocator && result.err != nil && !errors.Is(result.err, ErrStaleCheckpoint) {
			t.Fatalf("older concurrent promotion returned %v, want success or stale", result.err)
		}
	}
	latest := manager.Latest()
	if latest == nil || latest.Index != third.Index || latest.RootHash != third.RootHash {
		t.Fatalf("concurrent promotions cached %+v, want cut %d", latest, third.Index)
	}
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, secondLocator); !errors.Is(err, ErrStaleCheckpoint) {
		t.Fatalf("late older promotion error=%v, want stale", err)
	}
	if latest := manager.Latest(); latest == nil || latest.RootHash != third.RootHash {
		t.Fatalf("stale promotion changed cache to %+v", latest)
	}
	conflict := thirdLocator
	conflict.SealDecisionHash[0] ^= 1
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, conflict); err == nil {
		t.Fatal("conflicting equal-rank promotion succeeded")
	}
	if latest := manager.Latest(); latest == nil || latest.RootHash != third.RootHash {
		t.Fatalf("conflicting promotion changed cache to %+v", latest)
	}
	if err := manager.PromoteManagedAppendOnlyLocator(ctx, thirdLocator); err != nil {
		t.Fatalf("idempotent current promotion: %v", err)
	}
	if latest := manager.Latest(); latest == nil || latest.RootHash != third.RootHash {
		t.Fatalf("idempotent promotion changed cache to %+v", latest)
	}

	before := bucket.uploadCount()
	fourth, err := manager.CreateManagedAppendOnlyCandidate(ctx, []Source{snapshot}, 4)
	if err != nil {
		t.Fatal(err)
	}
	uploads := bucket.uploadsAfter(before)
	if blocks := issue310BlockUploads(uploads); len(blocks) != 0 {
		t.Fatalf("latest ordered promotion did not deduplicate: %+v", blocks)
	}
	if roots := issue310RootUploads(uploads); len(roots) != 1 {
		t.Fatalf("candidate root uploads=%d, want 1", len(roots))
	}
	if first.Index != 1 || second.Index != 2 || third.Index != 3 || fourth.Index != 4 {
		t.Fatalf("unexpected cuts %d,%d,%d,%d", first.Index, second.Index, third.Index, fourth.Index)
	}
}

func TestIssue310CertifiedLatestBlockSkipsUploadAcrossRestartAndHandoff(t *testing.T) {
	for _, mode := range []string{"legacy", "append-only"} {
		for _, load := range []bool{true, false} {
			name := "handoff"
			if load {
				name = "restart"
			}
			t.Run(mode+"/"+name, func(t *testing.T) {
				ctx := context.Background()
				bucket := &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}
				publisher := issue310Manager(t, bucket, mode)
				snapshot := source(t, RoleSQLite, "latest")
				certified := createFiles(t, publisher, ctx, []Source{snapshot}, 1)
				if err := publisher.PromoteCertifiedCurrent(ctx, certified); err != nil {
					t.Fatal(err)
				}

				next := issue310Manager(t, bucket, mode)
				if load {
					if err := next.Load(ctx); err != nil {
						t.Fatal(err)
					}
				}
				before := bucket.uploadCount()
				claim, err := next.AcquirePublisherClaim(ctx, "next-publisher", certified.Index, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				candidate, err := next.CreateFiles(ctx, claim, []Source{snapshot}, 2)
				if err != nil {
					t.Fatal(err)
				}
				if blocks := issue310BlockUploads(bucket.uploadsAfter(before)); len(blocks) != 0 {
					t.Fatalf("latest certified block uploads: %+v", blocks)
				}
				if got, want := candidate.Files[0].Blocks[0].Generation, certified.Files[0].Blocks[0].Generation; got != want {
					t.Fatalf("candidate generation=%d, want certified generation %d", got, want)
				}
				if err := next.ReleasePublisherClaim(ctx, claim); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestIssue310AppendOnlyOlderCanonicalBlockDuplicatesAcrossHandoffs(t *testing.T) {
	ctx := context.Background()
	bucket := &issue310RecordingBucket{appendOnlyBucket: &appendOnlyBucket{Bucket: objstore.NewInMemBucket()}}
	publisher := issue310Manager(t, bucket, "append-only")
	older := source(t, RoleSQLite, "older")
	old := createFiles(t, publisher, ctx, []Source{older}, 1)
	if err := publisher.PromoteCertifiedCurrent(ctx, old); err != nil {
		t.Fatal(err)
	}
	current := createFiles(t, publisher, ctx, []Source{source(t, RoleSQLite, "current")}, 2)
	if err := publisher.PromoteCertifiedCurrent(ctx, current); err != nil {
		t.Fatal(err)
	}
	oldBlock := old.Files[0].Blocks[0]
	hash, err := decodeHash(oldBlock.Hash)
	if err != nil {
		t.Fatal(err)
	}
	canonicalKey := publisher.key(blockKey(hash))

	var generationKeys []string
	for attempt, index := range []uint64{3, 4} {
		next := issue310Manager(t, bucket, "append-only")
		if err := next.Load(ctx); err != nil {
			t.Fatal(err)
		}
		before := bucket.uploadCount()
		claim, err := next.AcquirePublisherClaim(ctx, "handoff-"+string(rune('A'+attempt)), current.Index+uint64(attempt), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := next.CreateFiles(ctx, claim, []Source{older}, index)
		if err != nil {
			t.Fatal(err)
		}
		if err := next.ReleasePublisherClaim(ctx, claim); err != nil {
			t.Fatal(err)
		}

		generationKey := next.key(blockGenerationKey(hash, candidate.Files[0].Blocks[0].Generation))
		blocks := issue310BlockUploads(bucket.uploadsAfter(before))
		if len(blocks) != 2 {
			t.Fatalf("attempt %d block uploads=%+v, want canonical probe and generation upload", attempt, blocks)
		}
		canonical, ok := issue310UploadByKey(blocks, canonicalKey)
		if !ok || canonical.bytes != oldBlock.Size || !next.bucket.IsConditionNotMetErr(canonical.err) {
			t.Fatalf("attempt %d canonical upload=%+v, want %d condition-not-met bytes", attempt, canonical, oldBlock.Size)
		}
		generation, ok := issue310UploadByKey(blocks, generationKey)
		if !ok || generation.bytes != oldBlock.Size || generation.err != nil {
			t.Fatalf("attempt %d generation upload=%+v, want %d acknowledged bytes", attempt, generation, oldBlock.Size)
		}
		var attempted, acknowledged int64
		for _, upload := range blocks {
			attempted += upload.bytes
			if upload.err == nil {
				acknowledged += upload.bytes
			}
		}
		if attempted != 2*oldBlock.Size || acknowledged != oldBlock.Size {
			t.Fatalf("attempt %d attempted=%d acknowledged=%d bytes, want %d and %d", attempt, attempted, acknowledged, 2*oldBlock.Size, oldBlock.Size)
		}
		if candidate.Files[0].Blocks[0].Generation == 0 || blockObjectKey(candidate.Files[0].Blocks[0]) != blockGenerationKey(hash, candidate.Files[0].Blocks[0].Generation) {
			t.Fatalf("attempt %d did not use a generation block: %+v", attempt, candidate.Files[0].Blocks[0])
		}
		generationKeys = append(generationKeys, generationKey)
	}

	if generationKeys[0] == generationKeys[1] {
		t.Fatalf("handoffs reused generation key %s", generationKeys[0])
	}
	for _, key := range append([]string{canonicalKey}, generationKeys...) {
		r, err := bucket.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read %s: %v", key, errors.Join(readErr, closeErr))
		}
		if int64(len(data)) != oldBlock.Size || sha256.Sum256(data) != hash {
			t.Fatalf("object %s bytes=%d hash=%x, want %d %x", key, len(data), sha256.Sum256(data), oldBlock.Size, hash)
		}
	}
}

func issue310UploadByKey(uploads []issue310Upload, key string) (issue310Upload, bool) {
	for _, upload := range uploads {
		if upload.key == key {
			return upload, true
		}
	}
	return issue310Upload{}, false
}
