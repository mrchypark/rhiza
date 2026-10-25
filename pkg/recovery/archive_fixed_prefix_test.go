package recovery

import (
	"bytes"
	"context"
	"errors"
	"io"
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
