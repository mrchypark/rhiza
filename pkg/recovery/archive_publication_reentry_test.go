package recovery

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

// archivePublicationLockProbe records the competitor's attempts to inspect
// the publication lease after the outer writer enters its HEAD upload hook.
type archivePublicationLockProbe struct {
	objstore.Bucket
	armed atomic.Bool
	reads atomic.Uint64
}

type archivePublicationReentrantBucket struct {
	objstore.Bucket
	armed atomic.Bool
	run   func() error
}

func (b *archivePublicationReentrantBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "archive/head.bin") && b.armed.CompareAndSwap(true, false) {
		if err := b.run(); err != nil {
			return err
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func (b *archivePublicationLockProbe) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.armed.Load() && strings.HasSuffix(name, "archive/PUBLISH_LOCK") {
		b.reads.Add(1)
	}
	return b.Bucket.Get(ctx, name)
}

func TestArchivePublicationReentrantCompetitorWaitIsContextBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	source, err := newArchiveMemorySource(1, 512)
	if err != nil {
		t.Fatal(err)
	}
	base := newArchiveMemoryBucket(t, t.TempDir()+"/objects")
	probe := &archivePublicationLockProbe{Bucket: base}
	competitor := NewManager(probe, "reentrant-publication", 1)
	t.Cleanup(competitor.Close)
	var competitorErr error
	outerBucket := &archivePublicationReentrantBucket{Bucket: probe, run: func() error {
		probe.armed.Store(true)
		competitorCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		defer stop()
		competitorErr = competitor.syncNow(competitorCtx, source, source.Tip())
		return competitorErr
	}}
	outerBucket.armed.Store(true)
	outer := NewManager(outerBucket, "reentrant-publication", 1)
	t.Cleanup(outer.Close)
	if !outer.CASSupported() {
		t.Fatal("filesystem bucket must support conditional HEAD publication")
	}

	outerDone := make(chan error, 1)
	finished := false
	go func() { outerDone <- outer.syncNow(ctx, source, source.Tip()) }()
	defer func() {
		if !finished {
			cancel()
			<-outerDone
		}
	}()

	var outerErr error
	select {
	case outerErr = <-outerDone:
		finished = true
	case <-ctx.Done():
		t.Fatalf("outer publication did not leave the nested callback: %v", ctx.Err())
	}
	if !errors.Is(competitorErr, context.DeadlineExceeded) {
		t.Fatalf("nested competing publication error=%v, want bounded context deadline", competitorErr)
	}
	if outerErr == nil {
		t.Fatal("outer publication succeeded even though its nested competitor timed out")
	}
	if got := probe.reads.Load(); got < 2 {
		t.Fatalf("nested manager inspected publication lease %d times, want repeated pending checks", got)
	}
}
