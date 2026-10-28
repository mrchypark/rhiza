package recovery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// delayedArchiveHeadUploadBucket holds a HEAD request after Manager has built
// its conditional upload, then submits that same request after a newer writer
// has advanced the head. The background context models a provider request
// already in flight when the original caller is canceled.
type delayedArchiveHeadUploadBucket struct {
	objstore.Bucket
	headKey string
	started chan struct{}
	release chan struct{}
	result  chan error
	arm     sync.Once
	resume  sync.Once
}

func (b *delayedArchiveHeadUploadBucket) unblock() {
	b.resume.Do(func() { close(b.release) })
}

func (b *delayedArchiveHeadUploadBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if name == b.headKey {
		paused := false
		b.arm.Do(func() {
			paused = true
			close(b.started)
		})
		if paused {
			<-b.release
			err := b.Bucket.Upload(context.Background(), name, reader, options...)
			b.result <- err
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func TestDelayedStalePublisherHeadCASCannotReplaceNewerCertifiedHead(t *testing.T) {
	ctx := context.Background()
	_, base, core, seed := newSealableArchive(t)
	defer seed.Close()

	if _, _, err := core.Propose(ctx, []byte("stale writer's certified decision")); err != nil {
		t.Fatal(err)
	}
	oldBucket := &delayedArchiveHeadUploadBucket{
		Bucket: base, headKey: seed.key("archive/head.bin"),
		started: make(chan struct{}), release: make(chan struct{}), result: make(chan error, 1),
	}
	old := NewManager(oldBucket, "cluster", 1)
	defer old.Close()
	defer oldBucket.unblock()
	if err := old.Load(ctx); err != nil {
		t.Fatal(err)
	}
	oldDone := make(chan error, 1)
	go func() { oldDone <- old.SyncThrough(ctx, core, core.Tip()) }()
	select {
	case <-oldBucket.started:
	case <-time.After(2 * time.Second):
		t.Fatal("old publisher did not reach its conditional HEAD upload")
	}

	// Expire the old publication lock through a real conditional object update
	// while its already-built HEAD request stays paused in the wrapper.
	lock, err := seed.readArchiveLock(ctx, seed.publicationLockKey())
	if err != nil {
		t.Fatal(err)
	}
	lock.LeaseUntilMS = time.Now().Add(-time.Second).UnixMilli()
	if err := seed.writeArchiveLock(ctx, seed.publicationLockKey(), *lock, objstore.WithIfMatch(lock.version)); err != nil {
		t.Fatalf("expire old publication lock: %v", err)
	}

	if _, _, err := core.Propose(ctx, []byte("new publisher's certified decision")); err != nil {
		t.Fatal(err)
	}
	newer := NewManager(base, "cluster", 1)
	defer newer.Close()
	if err := newer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("new publisher advance: %v", err)
	}
	newTip := core.Tip()
	newHeadVersion, ok := newer.HeadVersion()
	if !ok || newer.Tip() != newTip {
		t.Fatalf("new publisher head version=%v present=%t tip=%d want=%d", newHeadVersion, ok, newer.Tip(), newTip)
	}

	oldBucket.unblock()
	var oldErr error
	select {
	case oldErr = <-oldDone:
	case <-time.After(2 * time.Second):
		t.Fatal("old publisher did not finish after delayed upload was released")
	}
	if !errors.Is(oldErr, ErrArchiveBusy) {
		t.Fatalf("old publisher error=%v, want ErrArchiveBusy after losing publication admission", oldErr)
	}
	if err := <-oldBucket.result; !base.IsConditionNotMetErr(err) {
		t.Fatalf("delayed old HEAD upload error=%v, want provider precondition rejection", err)
	}

	reader := NewManager(base, "cluster", 1)
	defer reader.Close()
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("read back newer head: %v", err)
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
