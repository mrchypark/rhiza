package recovery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestArchiveMemoryNativeCASConflictPublishesReadableWinner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	source, err := newArchiveMemorySource(7, 512)
	if err != nil {
		t.Fatal(err)
	}
	base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
	counter := &archiveMemoryMetricsBucket{Bucket: base}
	conflict := &archiveMemoryCASConflictBucket{Bucket: counter}
	conflict.armed.Store(true)
	manager := NewManager(conflict, "native-cas", 1)
	t.Cleanup(manager.Close)
	if !manager.CASSupported() {
		t.Fatal("filesystem bucket must support conditional HEAD publication")
	}

	if err := manager.syncNow(ctx, source, source.Tip()); err != nil {
		t.Fatalf("sync through native CAS conflict: %v", err)
	}
	if got := conflict.winnerUploads.Load(); got != 1 {
		t.Fatalf("valid HEAD winner uploads=%d, want=1", got)
	}
	if got := conflict.nativeConflicts.Load(); got != 1 {
		t.Fatalf("backend-native conditional conflicts=%d, want=1", got)
	}
	if got := conflict.violations.Load(); got != 0 {
		t.Fatalf("CAS fixture violations=%d, want=0", got)
	}
	if got := manager.Tip(); got != source.Tip() {
		t.Fatalf("outer manager tip=%d, want=%d", got, source.Tip())
	}
	manager.mu.Lock()
	outerExtents := len(manager.extents)
	outerVersion := manager.headCAS
	manager.mu.Unlock()
	if outerExtents != 1 {
		t.Fatalf("outer manager extents=%d, want=1", outerExtents)
	}
	if outerVersion == nil {
		t.Fatal("outer manager did not retain the published HEAD version")
	}

	// Read through the raw filesystem bucket, not the conflict injector or the
	// manager's cache, to verify the persisted winner and its complete contents.
	reader := NewManager(base, "native-cas", 1)
	t.Cleanup(reader.Close)
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("fresh reader load: %v", err)
	}
	if got := reader.Tip(); got != source.Tip() {
		t.Fatalf("fresh reader tip=%d, want=%d", got, source.Tip())
	}
	reader.mu.Lock()
	readerExtents := len(reader.extents)
	readerVersion := reader.headCAS
	reader.mu.Unlock()
	if readerExtents != 1 {
		t.Fatalf("fresh reader extents=%d, want=1", readerExtents)
	}
	if !sameNullableObjectVersion(outerVersion, readerVersion) {
		t.Fatalf("outer HEAD version=%v differs from fresh persisted version=%v", outerVersion, readerVersion)
	}

	want, wantTip, err := source.DecisionsFromBounded(1, int(source.Tip()), 1<<20)
	if err != nil || wantTip != source.Tip() || len(want) != int(source.Tip()) {
		t.Fatalf("build expected decisions tip=%d count=%d err=%v", wantTip, len(want), err)
	}
	got, gotTip, err := reader.DecisionsFrom(ctx, 1, int(source.Tip()))
	if err != nil || gotTip != source.Tip() || len(got) != len(want) {
		t.Fatalf("fresh decisions tip=%d count=%d want=%d err=%v", gotTip, len(got), len(want), err)
	}
	var prefix [32]byte
	for i := range want {
		if got[i].Slot != want[i].Slot || got[i].Hash != want[i].Hash ||
			!bytes.Equal(got[i].Value, want[i].Value) || !bytes.Equal(got[i].Certificate, want[i].Certificate) {
			t.Fatalf("fresh decision %d differs from source", i)
		}
		prefix = quepaxa.AdvancePrefixHash(prefix, got[i].Slot, got[i].Hash)
	}
	wantPrefix, ok := source.PrefixHash(source.Tip())
	if !ok || prefix != wantPrefix {
		t.Fatalf("fresh prefix=%x want=%x present=%t", prefix, wantPrefix, ok)
	}
	if counter.headGets.Load() == 0 {
		t.Fatal("outer manager did not re-read the published HEAD after the conditional conflict")
	}
	if counter.puts.Load() < 2 || counter.gets.Load() == 0 || counter.attributes.Load() == 0 {
		t.Fatalf("outer metrics puts=%d gets=%d attributes=%d; expected publication and conflict refresh I/O", counter.puts.Load(), counter.gets.Load(), counter.attributes.Load())
	}
}

func TestArchiveMemoryNativeCASFixtureRejectsInvalidOptionsAndOutcomes(t *testing.T) {
	t.Run("missing-original-condition-is-not-counted-as-conflict", func(t *testing.T) {
		base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
		backend := &archiveMemoryMetricsBucket{Bucket: base}
		conflict := &archiveMemoryCASConflictBucket{Bucket: backend}
		conflict.armed.Store(true)
		err := conflict.Upload(context.Background(), "prefix/archive/head.bin", bytes.NewReader([]byte("head")))
		if err == nil {
			t.Fatal("missing condition unexpectedly passed fixture")
		}
		if conflict.winnerUploads.Load() != 0 || conflict.nativeConflicts.Load() != 0 || conflict.violations.Load() != 1 || backend.puts.Load() != 0 {
			t.Fatalf("missing-condition winner=%d conflict=%d violations=%d backend uploads=%d; want 0, 0, 1, 0", conflict.winnerUploads.Load(), conflict.nativeConflicts.Load(), conflict.violations.Load(), backend.puts.Load())
		}
	})

	t.Run("wrong-original-condition-is-not-counted-as-conflict", func(t *testing.T) {
		base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
		backend := &archiveMemoryMetricsBucket{Bucket: base}
		conflict := &archiveMemoryCASConflictBucket{Bucket: backend}
		conflict.armed.Store(true)
		err := conflict.Upload(context.Background(), "prefix/archive/head.bin", bytes.NewReader([]byte("head")), objstore.WithIfMatch(nil))
		if err == nil {
			t.Fatal("invalid original condition unexpectedly passed fixture")
		}
		if conflict.winnerUploads.Load() != 0 || conflict.nativeConflicts.Load() != 0 || conflict.violations.Load() != 1 || backend.puts.Load() != 0 {
			t.Fatalf("wrong-condition winner=%d conflict=%d violations=%d backend uploads=%d; want 0, 0, 1, 0", conflict.winnerUploads.Load(), conflict.nativeConflicts.Load(), conflict.violations.Load(), backend.puts.Load())
		}
	})

	t.Run("first-upload-failure-is-not-counted-as-conflict", func(t *testing.T) {
		base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
		conflict := &archiveMemoryCASConflictBucket{Bucket: base}
		conflict.armed.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := conflict.Upload(ctx, "prefix/archive/head.bin", bytes.NewReader([]byte("head")), objstore.WithIfNotExists())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first upload error=%v, want context cancellation", err)
		}
		if conflict.winnerUploads.Load() != 0 || conflict.nativeConflicts.Load() != 0 || conflict.violations.Load() == 0 {
			t.Fatalf("canceled upload winner=%d conflict=%d violations=%d", conflict.winnerUploads.Load(), conflict.nativeConflicts.Load(), conflict.violations.Load())
		}
	})

	t.Run("non-head-and-disarmed-requests-pass-through", func(t *testing.T) {
		base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
		conflict := &archiveMemoryCASConflictBucket{Bucket: base}
		if err := conflict.Upload(context.Background(), "prefix/archive/extent.bin", bytes.NewReader([]byte("extent")), objstore.WithIfNotExists()); err != nil {
			t.Fatal(err)
		}
		if err := conflict.Upload(context.Background(), "prefix/archive/head.bin", bytes.NewReader([]byte("head")), objstore.WithIfNotExists()); err != nil {
			t.Fatal(err)
		}
		if conflict.winnerUploads.Load() != 0 || conflict.nativeConflicts.Load() != 0 || conflict.violations.Load() != 0 {
			t.Fatalf("pass-through counters winner=%d conflict=%d violations=%d", conflict.winnerUploads.Load(), conflict.nativeConflicts.Load(), conflict.violations.Load())
		}
	})

	t.Run("nonconditional-second-error-is-not-normalized", func(t *testing.T) {
		base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
		expectedErr := errors.New("fixture I/O failure")
		failed := &archiveMemoryFailUploadBucket{Bucket: base, failCall: 2, err: expectedErr}
		conflict := &archiveMemoryCASConflictBucket{Bucket: failed}
		conflict.armed.Store(true)
		err := conflict.Upload(context.Background(), "prefix/archive/head.bin", bytes.NewReader([]byte("head")), objstore.WithIfNotExists())
		if !errors.Is(err, expectedErr) {
			t.Fatalf("second upload error=%v, want original nonconditional failure", err)
		}
		if conflict.nativeConflicts.Load() != 0 || conflict.violations.Load() == 0 {
			t.Fatalf("nonconditional error conflict=%d violations=%d", conflict.nativeConflicts.Load(), conflict.violations.Load())
		}
	})

	t.Run("unexpected-success-on-second-upload-is-a-fixture-violation", func(t *testing.T) {
		base := newArchiveMemoryBucket(t, filepath.Join(t.TempDir(), "objects"))
		backend := &archiveMemoryMetricsBucket{Bucket: base}
		acceptUnexpectedSuccess := &archiveMemoryFailUploadBucket{Bucket: backend, nilUploadCall: 2}
		conflict := &archiveMemoryCASConflictBucket{Bucket: acceptUnexpectedSuccess}
		conflict.armed.Store(true)
		err := conflict.Upload(context.Background(), "prefix/archive/head.bin", bytes.NewReader([]byte("head")), objstore.WithIfNotExists())
		if err == nil || !strings.Contains(err.Error(), "expected the backend to reject duplicate HEAD publication") {
			t.Fatalf("second upload error=%v, want explicit fixture violation", err)
		}
		if acceptUnexpectedSuccess.calls != 2 || conflict.winnerUploads.Load() != 1 || conflict.nativeConflicts.Load() != 0 || conflict.violations.Load() != 1 || backend.puts.Load() != 1 {
			t.Fatalf("calls=%d winner=%d conflict=%d violations=%d backend uploads=%d; want 2, 1, 0, 1, 1", acceptUnexpectedSuccess.calls, conflict.winnerUploads.Load(), conflict.nativeConflicts.Load(), conflict.violations.Load(), backend.puts.Load())
		}
	})
}

type archiveMemoryFailUploadBucket struct {
	objstore.Bucket
	calls         int
	failCall      int
	nilUploadCall int
	err           error
}

func (b *archiveMemoryFailUploadBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	b.calls++
	if b.calls == b.nilUploadCall {
		return nil
	}
	if b.calls == b.failCall {
		return b.err
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}
