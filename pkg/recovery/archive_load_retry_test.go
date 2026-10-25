package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// Hooks affect only the reader; the real writer uses the underlying bucket.
type archiveLoadHookBucket struct {
	objstore.Bucket
	gets         map[string]int
	onGet        func(string) error
	onAttributes func(string) error
}

func (b *archiveLoadHookBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.Contains(name, "/archive/blocks/") {
		b.gets[name]++
	}
	if b.onGet != nil {
		if err := b.onGet(name); err != nil {
			return nil, err
		}
	}
	return b.Bucket.Get(ctx, name)
}

func (b *archiveLoadHookBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.onAttributes != nil {
		if err := b.onAttributes(name); err != nil {
			return objstore.ObjectAttributes{}, err
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

func TestArchiveLoadRetriesHeadAppendWithoutRereadingExtents(t *testing.T) {
	ctx, bucket, core, writer := newSealableArchive(t)
	defer writer.Close()
	appendExtent := func() {
		t.Helper()
		if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("write-%d", core.Tip()+1))); err != nil {
			t.Fatal(err)
		}
		if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
			t.Fatal(err)
		}
	}
	appendExtent()
	appendExtent()
	hook := &archiveLoadHookBucket{Bucket: bucket, gets: map[string]int{}}
	reader := NewManager(hook, "cluster", 1)
	defer reader.Close()
	blocks := 0
	hook.onGet = func(name string) error {
		if strings.Contains(name, "/archive/blocks/") {
			blocks++
			if blocks == 2 {
				// The stable HEAD and newest extent have already been read.
				if reader.Tip() != 0 {
					t.Fatal("partial head installed before retry")
				}
				appendExtent()
			}
		}
		return nil
	}
	if err := reader.Load(ctx); err != nil {
		t.Fatalf("load across append: %v", err)
	}
	if reader.Tip() != 4 {
		t.Fatalf("tip=%d, want 4", reader.Tip())
	}
	if len(hook.gets) != 4 {
		t.Fatalf("read %d distinct extents, want 4", len(hook.gets))
	}
	for name, count := range hook.gets {
		if count != 1 {
			t.Errorf("%s GETs=%d, want 1 across retries", name, count)
		}
	}
	gotVersion, ok := reader.HeadVersion()
	wantVersion, _ := writer.HeadVersion()
	if !ok || gotVersion != wantVersion {
		t.Fatal("reader did not install the final HEAD version")
	}
}

func TestArchiveLoadOnceDoesNotWaitForFirstPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, []byte("first publication")); err != nil {
		t.Fatal(err)
	}
	base := objstore.NewInMemBucket()
	writerBucket := &firstExtentPublicationWindowBucket{Bucket: base, started: make(chan struct{}), release: make(chan struct{})}
	writer := NewManager(writerBucket, "cluster", 1)
	defer writer.Close()
	reader := NewManager(base, "cluster", 1)
	defer reader.Close()
	writerDone := make(chan error, 1)
	go func() { writerDone <- writer.SyncThrough(ctx, core, core.Tip()) }()
	select {
	case <-writerBucket.started:
	case err := <-writerDone:
		t.Fatalf("writer completed before publication window: %v", err)
	case <-ctx.Done():
		t.Fatal("writer did not expose first-publication window")
	}
	if err := reader.LoadOnce(ctx); !errors.Is(err, errArchivePublicationPending) {
		t.Fatalf("LoadOnce error=%v, want first-publication-pending", err)
	}
	close(writerBucket.release)
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("finish writer publication: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("writer did not finish after release")
	}
	if err := reader.LoadOnce(ctx); err != nil {
		t.Fatalf("LoadOnce after publication: %v", err)
	}
	if reader.Tip() != core.Tip() {
		t.Fatalf("reader tip=%d, want %d", reader.Tip(), core.Tip())
	}
}

func TestArchiveLoadOnceReturnsOnChangedHeadWithoutRetry(t *testing.T) {
	ctx, bucket, core, writer := newSealableArchive(t)
	defer writer.Close()
	readerBucket := &archiveLoadHookBucket{Bucket: bucket, gets: map[string]int{}}
	reader := NewManager(readerBucket, "cluster", 1)
	defer reader.Close()
	if err := reader.LoadOnce(ctx); err != nil {
		t.Fatalf("initial LoadOnce: %v", err)
	}
	oldTip := reader.Tip()
	if _, _, err := core.Propose(ctx, []byte("published before refresh")); err != nil {
		t.Fatal(err)
	}
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	headReads := 0
	writerAdvanced := false
	readerBucket.onAttributes = func(name string) error {
		if strings.HasSuffix(name, "/archive/head.bin") {
			headReads++
			if headReads == 3 && !writerAdvanced {
				writerAdvanced = true
				if _, _, err := core.Propose(ctx, []byte("concurrent head change")); err != nil {
					return err
				}
				return writer.SyncThrough(ctx, core, core.Tip())
			}
		}
		return nil
	}
	if err := reader.LoadOnce(ctx); !errors.Is(err, errArchiveStateChanged) {
		t.Fatalf("LoadOnce error=%v, want changed-head result", err)
	}
	if !writerAdvanced || reader.Tip() != oldTip {
		t.Fatalf("writerAdvanced=%t readerTip=%d, want unchanged tip %d", writerAdvanced, reader.Tip(), oldTip)
	}
}

func TestArchiveLoadOnceDoesNotRetryUnstableHeadRead(t *testing.T) {
	for _, mode := range []string{"once", "load"} {
		t.Run(mode, func(t *testing.T) {
			ctx, bucket, core, writer := newSealableArchive(t)
			defer writer.Close()
			readerBucket := &archiveLoadHookBucket{Bucket: bucket, gets: map[string]int{}}
			reader := NewManager(readerBucket, "cluster", 1)
			defer reader.Close()
			if err := reader.Load(ctx); err != nil {
				t.Fatalf("initial load: %v", err)
			}
			oldTip := reader.Tip()
			appendExtent := func(value string) {
				t.Helper()
				if _, _, err := core.Propose(ctx, []byte(value)); err != nil {
					t.Fatal(err)
				}
				if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
					t.Fatal(err)
				}
			}
			appendExtent("published before unstable read")

			headGets, headAttributes := 0, 0
			readerBucket.onGet = func(name string) error {
				if strings.HasSuffix(name, "/archive/head.bin") {
					headGets++
				}
				return nil
			}
			readerBucket.onAttributes = func(name string) error {
				if strings.HasSuffix(name, "/archive/head.bin") {
					headAttributes++
					if headAttributes == 2 {
						appendExtent("changed between head attributes")
					}
				}
				return nil
			}

			var err error
			if mode == "once" {
				err = reader.LoadOnce(ctx)
				if !errors.Is(err, errArchiveStateChanged) {
					t.Fatalf("LoadOnce error=%v, want one-pass changed-head result", err)
				}
				if reader.Tip() != oldTip {
					t.Fatalf("LoadOnce installed tip %d after unstable read, want unchanged %d", reader.Tip(), oldTip)
				}
				if headGets != 1 {
					t.Fatalf("LoadOnce fetched HEAD %d times, want one", headGets)
				}
				return
			}

			err = reader.Load(ctx)
			if err != nil {
				t.Fatalf("Load across unstable head read: %v", err)
			}
			if reader.Tip() != core.Tip() {
				t.Fatalf("Load tip=%d, want latest tip=%d", reader.Tip(), core.Tip())
			}
			if headGets < 2 {
				t.Fatalf("Load fetched HEAD %d times, want retry after changed read", headGets)
			}
		})
	}
}

func TestArchiveLoadOnceHonorsCanceledContextAndLoadsStableHead(t *testing.T) {
	ctx, bucket, _, writer := newSealableArchive(t)
	defer writer.Close()
	reader := NewManager(bucket, "cluster", 1)
	defer reader.Close()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := reader.LoadOnce(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled LoadOnce error=%v, want context cancellation", err)
	}
	if reader.Tip() != 0 {
		t.Fatalf("canceled LoadOnce changed tip to %d", reader.Tip())
	}
	if err := reader.LoadOnce(ctx); err != nil {
		t.Fatalf("stable LoadOnce: %v", err)
	}
	if reader.Tip() != writer.Tip() {
		t.Fatalf("reader tip=%d, want stable writer tip=%d", reader.Tip(), writer.Tip())
	}
}

func TestArchiveLoadFailurePreservesInstalledState(t *testing.T) {
	for _, mode := range []string{"churn", "cancellation", "corruption"} {
		t.Run(mode, func(t *testing.T) {
			ctx, bucket, core, writer := newSealableArchive(t)
			defer writer.Close()
			hook := &archiveLoadHookBucket{Bucket: bucket, gets: map[string]int{}}
			reader := NewManager(hook, "cluster", 1)
			defer reader.Close()
			if err := reader.Load(ctx); err != nil {
				t.Fatal(err)
			}
			oldHead := reader.head
			oldVersion, _ := reader.HeadVersion()
			oldExtentCount := len(reader.extents)
			appendExtent := func() {
				t.Helper()
				if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("write-%d", core.Tip()+1))); err != nil {
					t.Fatal(err)
				}
				if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
					t.Fatal(err)
				}
			}
			appendExtent()
			appendExtent()
			loadCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			finalChecks := 0
			headAttributes := 0
			hook.onAttributes = func(name string) error {
				if strings.HasSuffix(name, "/archive/head.bin") {
					headAttributes++
					if headAttributes%3 == 0 {
						finalChecks++
						if reader.Tip() != 1 {
							t.Fatal("partial tip exposed during load")
						}
						appendExtent()
					}
				}
				return nil
			}
			if mode != "churn" {
				hook.onAttributes = nil
				blocks := 0
				hook.onGet = func(name string) error {
					if strings.Contains(name, "/archive/blocks/") {
						blocks++
						if blocks == 2 {
							if mode == "cancellation" {
								cancel()
								return context.Canceled
							}
							if err := bucket.Upload(ctx, name, strings.NewReader("corrupt extent")); err != nil {
								t.Fatal(err)
							}
						}
					}
					return nil
				}
			}
			err := reader.Load(loadCtx)
			if err == nil {
				t.Fatal("unstable or invalid archive was accepted")
			}
			if mode == "churn" && (finalChecks != maxPublishRetries || !strings.Contains(err.Error(), "changed too often")) {
				t.Errorf("final checks=%d error=%v, want bounded retry exhaustion", finalChecks, err)
			}
			if mode == "cancellation" && !errors.Is(err, context.Canceled) {
				t.Errorf("error=%v, want cancellation", err)
			}
			version, ok := reader.HeadVersion()
			if reader.Tip() != 1 || !ok || version != oldVersion || !archiveHeadsEqual(reader.head, oldHead) || len(reader.extents) != oldExtentCount {
				t.Fatal("failed load published partial manager state")
			}
		})
	}
}
