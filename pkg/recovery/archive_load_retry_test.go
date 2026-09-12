package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

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
