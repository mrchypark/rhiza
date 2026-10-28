package rhiza_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type delayedEmbeddedArchiveHeadBucket struct {
	objstore.Bucket
	headKey string
	started chan struct{}
	release chan struct{}
	result  chan error
	arm     sync.Once
	resume  sync.Once
}

func (b *delayedEmbeddedArchiveHeadBucket) unblock() {
	b.resume.Do(func() { close(b.release) })
}

func (b *delayedEmbeddedArchiveHeadBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if name == b.headKey {
		paused := false
		b.arm.Do(func() {
			paused = true
			close(b.started)
		})
		if paused {
			<-b.release
			// Submit the saved request even if the original caller was canceled:
			// this models a conditional provider request already in flight.
			lateCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := b.Bucket.Upload(lateCtx, name, reader, options...)
			b.result <- err
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

type embeddedArchivePublicationLock struct {
	OwnerID      string `json:"owner_id"`
	Generation   uint64 `json:"generation"`
	LeaseUntilMS int64  `json:"lease_until_unix_ms"`
}

func readEmbeddedArchivePublicationLock(ctx context.Context, bucket objstore.Bucket, key string) (embeddedArchivePublicationLock, *objstore.ObjectVersion, error) {
	before, err := bucket.Attributes(ctx, key)
	if err != nil {
		return embeddedArchivePublicationLock{}, nil, err
	}
	if before.Version == nil {
		return embeddedArchivePublicationLock{}, nil, fmt.Errorf("archive publication lock has no conditional-write version")
	}
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return embeddedArchivePublicationLock{}, nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, 64<<10+1))
	closeErr := r.Close()
	if readErr != nil {
		return embeddedArchivePublicationLock{}, nil, readErr
	}
	if closeErr != nil {
		return embeddedArchivePublicationLock{}, nil, closeErr
	}
	if len(data) > 64<<10 {
		return embeddedArchivePublicationLock{}, nil, fmt.Errorf("archive publication lock exceeds size limit")
	}
	after, err := bucket.Attributes(ctx, key)
	if err != nil {
		return embeddedArchivePublicationLock{}, nil, err
	}
	if after.Version == nil || *before.Version != *after.Version {
		return embeddedArchivePublicationLock{}, nil, fmt.Errorf("archive publication lock changed during read")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var lock embeddedArchivePublicationLock
	if err := decoder.Decode(&lock); err != nil {
		return embeddedArchivePublicationLock{}, nil, fmt.Errorf("decode archive publication lock: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return embeddedArchivePublicationLock{}, nil, fmt.Errorf("archive publication lock has trailing data")
	}
	if lock.OwnerID == "" || lock.Generation == 0 || lock.LeaseUntilMS <= 0 {
		return embeddedArchivePublicationLock{}, nil, fmt.Errorf("archive publication lock is incomplete")
	}
	return lock, before.Version, nil
}

func TestEmbeddedObjectStorageStalePublisherHeadCAS(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider == "" || provider == "filesystem" {
		t.Skip("stale publisher contract requires an explicitly selected Floci provider")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := embeddedStoreConfig(t)
	bucket := embeddedBucket(t, cfg)
	root := path.Join("stale-publisher", fmt.Sprintf("%d", time.Now().UnixNano()))
	seed := recovery.NewManager(bucket, root, 1)
	defer seed.Close()
	if !seed.CASSupported() {
		t.Skipf("provider %s does not advertise archive head conditional writes", provider)
	}

	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close test WAL: %v", err)
		}
	}()
	core, err := quepaxa.New(quepaxa.Config{
		NodeID:  "archive-cas-voter",
		Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "archive-cas-voter"}}},
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("initial certified decision")); err != nil {
		t.Fatal(err)
	}
	if err := seed.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("publish initial certified archive: %v", err)
	}
	initialHeadVersion, ok := seed.HeadVersion()
	if !ok {
		t.Fatal("initial archive head has no conditional-write version")
	}

	if _, _, err := core.Propose(ctx, []byte("old publisher's certified decision")); err != nil {
		t.Fatal(err)
	}
	delayed := &delayedEmbeddedArchiveHeadBucket{
		Bucket: bucket, headKey: path.Join(root, "archive/head.bin"),
		started: make(chan struct{}), release: make(chan struct{}), result: make(chan error, 1),
	}
	old := recovery.NewManager(delayed, root, 1)
	defer old.Close()
	defer delayed.unblock()
	if err := old.Load(ctx); err != nil {
		t.Fatal(err)
	}
	oldDone := make(chan error, 1)
	go func() { oldDone <- old.SyncThrough(ctx, core, core.Tip()) }()
	select {
	case <-delayed.started:
	case <-ctx.Done():
		t.Fatalf("old publisher did not reach HEAD upload: %v", ctx.Err())
	}

	// Expire PUBLISH_LOCK with a real conditional object update. No lease timer
	// is shortened or replaced; the saved HEAD request keeps its original ETag.
	lockKey := path.Join(root, "archive/PUBLISH_LOCK")
	lock, lockVersion, err := readEmbeddedArchivePublicationLock(ctx, bucket, lockKey)
	if err != nil {
		t.Fatalf("read publication lock: %v", err)
	}
	lock.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
	lockBytes, err := json.Marshal(lock)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, lockKey, bytes.NewReader(lockBytes), objstore.WithIfMatch(lockVersion)); err != nil {
		t.Fatalf("conditionally expire publication lock: %v", err)
	}

	if _, _, err := core.Propose(ctx, []byte("new publisher's certified decision")); err != nil {
		t.Fatal(err)
	}
	newer := recovery.NewManager(bucket, root, 1)
	defer newer.Close()
	if err := newer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("new publisher advance: %v", err)
	}
	newTip := core.Tip()
	newHeadVersion, ok := newer.HeadVersion()
	if !ok || newHeadVersion == initialHeadVersion || newer.Tip() != newTip {
		t.Fatalf("new head version changed=%t present=%t tip=%d want=%d", newHeadVersion != initialHeadVersion, ok, newer.Tip(), newTip)
	}

	delayed.unblock()
	var oldErr error
	select {
	case oldErr = <-oldDone:
	case <-ctx.Done():
		t.Fatalf("old publisher did not return after delayed upload: %v", ctx.Err())
	}
	if !errors.Is(oldErr, recovery.ErrArchiveBusy) {
		t.Fatalf("old publisher error=%v, want ErrArchiveBusy after losing publication admission", oldErr)
	}
	select {
	case err := <-delayed.result:
		if !bucket.IsConditionNotMetErr(err) {
			t.Fatalf("delayed old HEAD upload error=%v, want provider precondition rejection", err)
		}
	case <-ctx.Done():
		t.Fatalf("delayed old HEAD upload did not finish: %v", ctx.Err())
	}

	reader := recovery.NewManager(bucket, root, 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("read back newer archive head: %v", err)
	}
	if reader.Tip() != newTip {
		t.Fatalf("read-back tip=%d, want newer certified tip %d", reader.Tip(), newTip)
	}
	decisions, tip, err := reader.DecisionsFrom(ctx, 1, int(newTip))
	if err != nil || tip != newTip || len(decisions) != int(newTip) {
		t.Fatalf("read-back decisions=%d tip=%d err=%v", len(decisions), tip, err)
	}
	for slot, decision := range decisions {
		certified, exists := core.CertifiedValue(quepaxa.Slot(slot + 1))
		if !exists || certified.Slot != decision.Slot || !bytes.Equal(certified.Value, decision.Value) || !bytes.Equal(certified.Certificate, decision.Certificate) {
			t.Fatalf("read-back slot %d differs from certified core decision", slot+1)
		}
	}
}
