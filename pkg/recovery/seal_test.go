package recovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type sealAdmissionBucket struct {
	objstore.Bucket
	observed chan struct{}
	once     sync.Once
}

func (b *sealAdmissionBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if strings.HasSuffix(name, "/archive/PUBLISH_LOCK") {
		b.once.Do(func() { close(b.observed) })
	}
	return b.Bucket.Attributes(ctx, name)
}

// publishLegacyArchiveTip models a valid, non-participating publisher already
// in flight at HEAD CAS. It preserves the real conditional-storage conflict
// cases without recursively waiting on the new cooperative admission lease.
func publishLegacyArchiveTip(ctx context.Context, bucket objstore.Bucket, writer *Manager, core *quepaxa.Core, through quepaxa.Slot) error {
	writer.mu.Lock()
	tip, head, version := writer.tip, writer.head, writer.headCAS
	writer.mu.Unlock()
	if tip >= through {
		return nil
	}
	if head.Generation == ^uint64(0) {
		return fmt.Errorf("legacy writer generation exhausted")
	}
	nextGeneration := head.Generation + 1
	previous, previousObject := head.TailHash, head.TailObject
	for from := tip + 1; from <= through; {
		extent, data, _, err := writer.buildExtent(core, from, through, previous, previousObject)
		if err != nil {
			return err
		}
		if err := writer.uploadExtent(ctx, extent.hash, data, nextGeneration); err != nil {
			return err
		}
		head.Tip, head.TailHash, head.TailObject = extent.End, extent.hash, nextGeneration
		previous, previousObject = extent.hash, nextGeneration
		if extent.End == through {
			break
		}
		from = extent.End + 1
	}
	head.Generation = nextGeneration
	encoded, err := encodeHead(head)
	if err != nil {
		return err
	}
	option := objstore.WithIfNotExists()
	if version != nil {
		option = objstore.WithIfMatch(version)
	}
	if err := bucket.Upload(ctx, writer.key("archive/head.bin"), bytes.NewReader(encoded), option); err != nil {
		return err
	}
	return writer.Load(ctx)
}

func newSealableArchive(t *testing.T) (context.Context, objstore.Bucket, *quepaxa.Core, *Manager) {
	t.Helper()
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("sealed history")); err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	archive := NewManager(bucket, "cluster", 1)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	return ctx, bucket, core, archive
}

func TestSealStopsPublicationAndKeepsSnapshotReadable(t *testing.T) {
	ctx, bucket, core, _ := newSealableArchive(t)
	if err := Seal(ctx, bucket, "cluster", "recovery-1"); err != nil {
		t.Fatal(err)
	}
	if err := Seal(ctx, bucket, "cluster", "recovery-1"); err != nil {
		t.Fatalf("same-operation retry: %v", err)
	}
	if err := Seal(ctx, bucket, "cluster", "recovery-2"); err == nil {
		t.Fatal("different operation resealed archive")
	}

	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.BeginRecoverySnapshot(ctx, "fork", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close(ctx)
	values, tip, err := snapshot.DecisionsFrom(ctx, 1, 1)
	if err != nil || tip != core.Tip() || len(values) != 1 {
		t.Fatalf("sealed snapshot values=%d tip=%d err=%v", len(values), tip, err)
	}
	if _, _, err := core.Propose(ctx, []byte("late")); err != nil {
		t.Fatal(err)
	}
	if err := reader.SyncThrough(ctx, core, core.Tip()); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("post-seal publish error=%v, want ErrArchiveSealed", err)
	}
	if err := reader.Cleanup(ctx, 0); !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("post-seal cleanup error=%v, want ErrArchiveSealed", err)
	}
}

func TestSealFencesStalePublisherHeadCAS(t *testing.T) {
	ctx, base, core, _ := newSealableArchive(t)
	wrapped := &blockingHeadUploadBucket{Bucket: base, started: make(chan struct{}), release: make(chan struct{})}
	stale := NewManager(wrapped, "cluster", 1)
	if err := stale.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("racing late decision")); err != nil {
		t.Fatal(err)
	}
	wrapped.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		err := publishLegacyArchiveTip(ctx, wrapped, stale, core, core.Tip())
		if err != nil && wrapped.IsConditionNotMetErr(err) {
			if loadErr := stale.Load(ctx); loadErr != nil {
				err = loadErr
			} else if stale.Sealed() {
				err = ErrArchiveSealed
			}
		}
		done <- err
	}()
	select {
	case <-wrapped.started:
	case <-time.After(time.Second):
		t.Fatal("stale publisher did not reach head CAS")
	}
	if err := Seal(ctx, base, "cluster", "recovery-race"); err != nil {
		t.Fatal(err)
	}
	close(wrapped.release)
	if err := <-done; !errors.Is(err, ErrArchiveSealed) {
		t.Fatalf("stale publisher error=%v, want ErrArchiveSealed", err)
	}
	reader := NewManager(base, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if reader.Tip() != 1 || !reader.head.Sealed {
		t.Fatalf("sealed head tip=%d sealed=%v", reader.Tip(), reader.head.Sealed)
	}
}

func TestSealAndFencedInitializeWaitForPublicationAdmission(t *testing.T) {
	t.Run("seal", func(t *testing.T) {
		ctx, base, _, seed := newSealableArchive(t)
		defer seed.Close()
		holder := NewManager(base, "cluster", 1)
		defer holder.Close()
		key := holder.publicationLockKey()
		lease, err := holder.acquireArchiveLock(ctx, key, "test-holder", archivePinLease)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.releaseArchiveLock(ctx, key, lease) }()
		observed := &sealAdmissionBucket{Bucket: base, observed: make(chan struct{})}
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- Seal(waitCtx, observed, "cluster", "seal-admission") }()
		select {
		case <-observed.observed:
		case err := <-done:
			t.Fatalf("Seal never reached publication admission: %v", err)
		case <-waitCtx.Done():
			t.Fatal(waitCtx.Err())
		}
		select {
		case err := <-done:
			t.Fatalf("Seal completed before admission release: %v", err)
		default:
		}
		if err := holder.releaseArchiveLock(ctx, key, lease); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		final := NewManager(base, "cluster", 1)
		defer final.Close()
		if err := final.Load(ctx); err != nil || !final.Sealed() {
			t.Fatalf("sealed final head=%t err=%v", final.Sealed(), err)
		}
	})
	t.Run("fenced_initialization", func(t *testing.T) {
		ctx := context.Background()
		base := objstore.NewInMemBucket()
		holder := NewManager(base, "cluster", 1)
		defer holder.Close()
		key := holder.publicationLockKey()
		lease, err := holder.acquireArchiveLock(ctx, key, "test-holder", archivePinLease)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.releaseArchiveLock(ctx, key, lease) }()
		observed := &sealAdmissionBucket{Bucket: base, observed: make(chan struct{})}
		initializer := NewManager(observed, "cluster", 1)
		defer initializer.Close()
		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- initializer.InitializeFencedGeneration(waitCtx, 1, [32]byte{1}, [32]byte{2}) }()
		select {
		case <-observed.observed:
		case err := <-done:
			t.Fatalf("initialization never reached admission: %v", err)
		case <-waitCtx.Done():
			t.Fatal(waitCtx.Err())
		}
		if exists, err := base.Exists(ctx, holder.key("archive/head.bin")); err != nil || exists {
			t.Fatalf("fenced HEAD before admission release: exists=%t err=%v", exists, err)
		}
		if err := holder.releaseArchiveLock(ctx, key, lease); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		final := NewManager(base, "cluster", 1)
		defer final.Close()
		if err := final.Load(ctx); err != nil || final.head.Base != 1 || final.head.Tip != 1 {
			t.Fatalf("fenced final base=%d tip=%d err=%v", final.head.Base, final.head.Tip, err)
		}
	})
}
