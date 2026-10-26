package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

func TestLocalMaintenanceClashAndUncertainWork(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	first := NewManager(bucket, "local", "", 1)
	second := NewManager(bucket, "local", "", 1)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- first.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	if err := second.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { t.Error("concurrent admission"); return nil }); err == nil {
		t.Error("clash admitted")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	uncertain := errors.New("delete acknowledgement lost")
	if err := first.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		return localVoid(ctx, true, func(context.Context) error { return uncertain })
	}); !errors.Is(err, uncertain) {
		t.Fatalf("uncertainty: %v", err)
	}
	if err := second.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { t.Error("uncertain work taken over"); return nil }); err == nil {
		t.Fatal("uncertainty gate released")
	}
}

func TestLocalGCRetainsPinsPreparedRootsAndSharedBlocks(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "local", "", 1)
	makeRoot := func(s Source, index uint64) *Checkpoint {
		t.Helper()
		root, err := manager.CreateLocalFiles(ctx, []Source{s}, index)
		if err != nil {
			t.Fatal(err)
		}
		return root
	}
	old := makeRoot(source(t, RoleSQLite, "old"), 1)
	shared := source(t, RoleSQLite, "shared")
	middle := makeRoot(shared, 2)
	current := makeRoot(shared, 3)
	prepared := makeRoot(source(t, RoleSQLite, "prepared"), 4)
	var pin *RecoveryPin
	if err := manager.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		var err error
		pin, err = manager.PinRecoveryRoot(ctx, old, "restore", time.Hour)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	orphan := manager.key(blockGenerationKey([32]byte{99}, 1))
	if err := bucket.Upload(ctx, orphan, bytes.NewReader([]byte("orphan"))); err != nil {
		t.Fatal(err)
	}
	retired := map[[32]byte]struct{}{old.RootHash: {}, middle.RootHash: {}}
	retain := map[[32]byte]struct{}{current.RootHash: {}}
	collect := func() {
		t.Helper()
		if err := manager.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { return manager.GarbageCollectLocal(ctx, retired, retain, 0) }); err != nil {
			t.Fatal(err)
		}
	}
	collect()
	if _, err := manager.OpenRoot(ctx, old.Index, old.RootHash); err != nil {
		t.Fatal("pinned root removed", err)
	}
	if _, err := manager.OpenRoot(ctx, middle.Index, middle.RootHash); err == nil {
		t.Fatal("retired root retained")
	}
	if _, err := manager.OpenRoot(ctx, prepared.Index, prepared.RootHash); err != nil {
		t.Fatal("prepared root removed", err)
	}
	if _, err := manager.DownloadAndVerifyRootFiles(ctx, current, t.TempDir()); err != nil {
		t.Fatal("shared blocks removed", err)
	}
	if exists, _ := bucket.Exists(ctx, orphan); exists {
		t.Fatal("orphan block retained")
	}
	if err := pin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	collect()
	if _, err := manager.OpenRoot(ctx, old.Index, old.RootHash); err == nil {
		t.Fatal("closed pin root retained")
	}
}

func TestLocalGCUnknownRootFailsBeforeDelete(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	manager := NewManager(bucket, "local", "", 1)
	root, err := manager.CreateLocalFiles(ctx, []Source{source(t, RoleSQLite, "old")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, "local/checkpoint/roots/unknown", strings.NewReader("bad")); err != nil {
		t.Fatal(err)
	}
	if err := manager.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		return manager.GarbageCollectLocal(ctx, map[[32]byte]struct{}{root.RootHash: {}}, nil, 0)
	}); err == nil {
		t.Fatal("unknown metadata accepted")
	}
	if _, err := manager.OpenRoot(ctx, root.Index, root.RootHash); err != nil {
		t.Fatal("deleted before validation", err)
	}
}

type localDelayedDeleteBucket struct {
	objstore.Bucket
	pending string
}

func (b *localDelayedDeleteBucket) Delete(_ context.Context, name string) error {
	b.pending = name
	return errors.New("request still executing after transport failure")
}
func TestLocalGCUnresolvedDeleteBlocksNewAdmission(t *testing.T) {
	ctx := context.Background()
	underlying := objstore.NewInMemBucket()
	bucket := &localDelayedDeleteBucket{Bucket: underlying}
	manager := NewManager(bucket, "local", "", 1)
	root, err := manager.CreateLocalFiles(ctx, []Source{source(t, RoleSQLite, "old")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = manager.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error {
		return manager.GarbageCollectLocal(ctx, map[[32]byte]struct{}{root.RootHash: {}}, nil, 0)
	})
	if err == nil || bucket.pending == "" {
		t.Fatalf("expected unresolved deletion: %v", err)
	}
	contender := NewManager(underlying, "local", "", 1)
	if err := contender.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { t.Error("pin/writer admitted before delete resolution"); return nil }); err == nil {
		t.Fatal("unresolved delete gate taken over")
	}
	if err := underlying.Delete(ctx, bucket.pending); err != nil {
		t.Fatal(err)
	}
	// Even known completion is insufficient for automatic takeover: recovery is explicit.
	if err := contender.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { return nil }); err == nil {
		t.Fatal("automatic recovery of poisoned gate")
	}
}

type localCancelAdmissionBucket struct {
	objstore.Bucket
	cancel context.CancelFunc
}

func (b *localCancelAdmissionBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	err := b.Bucket.Upload(ctx, name, r, options...)
	if err == nil && b.cancel != nil && strings.HasSuffix(name, "/local-gc-control") {
		cancel := b.cancel
		b.cancel = nil
		cancel()
	}
	return err
}
func TestLocalMaintenanceCancellationBeforeMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := NewManager(nil, "local", "", 1).WithLocalMaintenanceContext(ctx, func(context.Context) error { t.Error("cancelled callback ran"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	bucket := &localCancelAdmissionBucket{Bucket: objstore.NewInMemBucket(), cancel: cancel}
	manager := NewManager(bucket, "local", "", 1)
	if err := manager.WithLocalMaintenanceContext(ctx, func(context.Context) error { t.Error("callback ran after admission cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := manager.WithLocalMaintenanceContext(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal("cancelled admission stranded gate", err)
	}
}

type localAcknowledgedDeleteBucket struct {
	objstore.Bucket
	started, release chan struct{}
	once             sync.Once
}

func (b *localAcknowledgedDeleteBucket) Delete(ctx context.Context, name string) error {
	b.once.Do(func() { close(b.started); <-b.release })
	return b.Bucket.Delete(ctx, name)
}
func TestLocalGCCancellationDrainsDeleteAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bucket := &localAcknowledgedDeleteBucket{Bucket: objstore.NewInMemBucket(), started: make(chan struct{}), release: make(chan struct{})}
	manager := NewManager(bucket, "local", "", 1)
	root, err := manager.CreateLocalFiles(ctx, []Source{source(t, RoleSQLite, "old")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- manager.WithLocalMaintenanceContext(ctx, func(workCtx context.Context) error {
			return manager.GarbageCollectLocal(workCtx, map[[32]byte]struct{}{root.RootHash: {}}, nil, 0)
		})
	}()
	<-bucket.started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned before delete ACK: %v", err)
	default:
	}
	if err := NewManager(bucket, "local", "", 1).WithLocalMaintenanceContext(context.Background(), func(context.Context) error { t.Error("admitted before delete ACK"); return nil }); err == nil {
		t.Error("gate opened early")
	}
	close(bucket.release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := manager.WithLocalMaintenanceContext(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal("healthy cancellation stranded gate", err)
	}
}

func TestLocalMaintenanceHungMutationBoundedAndBlocked(t *testing.T) {
	manager := NewManager(objstore.NewInMemBucket(), "local", "", 1)
	release, finished := make(chan struct{}), make(chan struct{})
	started := time.Now()
	err := manager.WithLocalMaintenanceContext(context.Background(), func(ctx context.Context) error {
		state := ctx.Value(localOperationKey{}).(*localOperation)
		state.timeout = 20 * time.Millisecond
		return localVoid(ctx, true, func(context.Context) error { defer close(finished); <-release; return nil })
	})
	close(release)
	<-finished
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("hung mutation not bounded: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := manager.WithLocalMaintenanceContext(ctx, func(context.Context) error { t.Error("hung mutation gate admitted"); return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestLocalMaintenanceReadFailureAndContentionRecover(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "local", "", 1)
	rejected := errors.New("invalid metadata before mutation")
	if err := manager.WithLocalMaintenanceContext(ctx, func(context.Context) error { return rejected }); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- manager.WithLocalMaintenanceContext(ctx, func(context.Context) error { close(entered); <-release; return nil })
	}()
	<-entered
	cancelCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if err := manager.WithLocalMaintenanceContext(cancelCtx, func(context.Context) error { t.Error("contender admitted"); return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	waitingDone := make(chan error, 1)
	go func() {
		waitingDone <- manager.WithLocalMaintenanceContext(ctx, func(context.Context) error { return nil })
	}()
	close(release)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	if err := <-waitingDone; err != nil {
		t.Fatal("healthy contention poisoned gate", err)
	}
}

type localInjectedListingBucket struct {
	objstore.Bucket
	dir, extra string
}

func (b localInjectedListingBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if err := b.Bucket.Iter(ctx, dir, f, options...); err != nil {
		return err
	}
	if dir == b.dir {
		return f(b.extra)
	}
	return nil
}
func TestLocalGCRejectsNoncanonicalCandidateKeys(t *testing.T) {
	for _, kind := range []string{"nested-block", "sibling-block", "foreign-block", "nested-root", "sibling-root"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			raw := objstore.NewInMemBucket()
			hash := [32]byte{71}
			canonical := blockGenerationKey(hash, 1)
			name := "local/checkpoint/blocks/unknown-subdir/" + path.Base(canonical)
			dir := "local/checkpoint/blocks"
			switch kind {
			case "sibling-block":
				name = "local/checkpoint/blocks-sibling/" + path.Base(canonical)
			case "foreign-block":
				name = "foreign/" + canonical
			case "nested-root":
				name = "local/checkpoint/roots/unknown-subdir/" + path.Base(rootName(1, hash))
				dir = "local/checkpoint/roots"
			case "sibling-root":
				name = "local/checkpoint/roots-sibling/" + path.Base(rootName(1, hash))
				dir = "local/checkpoint/roots"
			}
			orphan := "local/" + blockGenerationKey([32]byte{72}, 1)
			for _, key := range []string{name, orphan} {
				if err := raw.Upload(ctx, key, strings.NewReader("untouched")); err != nil {
					t.Fatal(err)
				}
			}
			manager := NewManager(localInjectedListingBucket{Bucket: raw, dir: dir, extra: name}, "local", "", 1)
			err := manager.WithLocalMaintenanceContext(ctx, func(ctx context.Context) error { return manager.GarbageCollectLocal(ctx, nil, nil, 0) })
			if strings.Contains(kind, "root") && err == nil {
				t.Fatal("noncanonical root accepted")
			}
			if exists, _ := raw.Exists(ctx, name); !exists {
				t.Fatal("noncanonical object deleted")
			}
			if err != nil {
				if exists, _ := raw.Exists(ctx, orphan); !exists {
					t.Fatal("deleted before validation completed")
				}
			}
		})
	}
}
