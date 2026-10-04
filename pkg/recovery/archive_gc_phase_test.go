//go:build rhiza_local_testhooks

package recovery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

type archiveGCPhaseBucket struct {
	*archiveLoadHookBucket
	unsafeDeletes int
}

func (b *archiveGCPhaseBucket) Delete(ctx context.Context, name string) error {
	if strings.Contains(name, "/archive/blocks/") || strings.Contains(name, "/archive/manifests/") {
		b.unsafeDeletes++
	}
	return b.archiveLoadHookBucket.Delete(ctx, name)
}

func TestArchiveCleanupPhaseTracePreservesCanceledFailure(t *testing.T) {
	for _, phase := range []string{"load", "compaction"} {
		t.Run(phase, func(t *testing.T) {
			ctx, bucket, core, writer := newSealableArchive(t)
			defer writer.Close()
			if phase == "compaction" {
				for i := 0; i < 2; i++ {
					if _, _, err := core.Propose(ctx, []byte(fmt.Sprintf("extra-%d", i))); err != nil {
						t.Fatal(err)
					}
					if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
						t.Fatal(err)
					}
				}
			}
			hookBucket := &archiveGCPhaseBucket{archiveLoadHookBucket: &archiveLoadHookBucket{Bucket: bucket, gets: make(map[string]int)}}
			manager := NewManager(hookBucket, "cluster", 1)
			defer manager.Close()
			if phase == "compaction" {
				if err := manager.Load(ctx); err != nil {
					t.Fatal(err)
				}
			}
			beforeHead, beforeOK := writer.HeadVersion()
			cleanupCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			phaseStarted := false
			injectedBlockGETs := 0
			hookBucket.onGet = func(name string) error {
				if phaseStarted && strings.Contains(name, "/archive/blocks/") {
					injectedBlockGETs++
					cancel()
					return cleanupCtx.Err()
				}
				return nil
			}
			var events []string
			tracedCtx := localtesthooks.WithArchiveGCPhaseTrace(cleanupCtx, func(event string) {
				events = append(events, event)
				if event == "archive-gc:"+phase+":begin" {
					phaseStarted = true
				}
			})
			err := manager.Cleanup(tracedCtx, 0)
			if injectedBlockGETs != 1 {
				t.Fatalf("phase %s injected archive-block GETs=%d, want exactly one actual GET", phase, injectedBlockGETs)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Cleanup error=%v, want original canceled cause", err)
			}
			if !slices.Contains(events, "archive-gc:"+phase+":begin") || !slices.Contains(events, "archive-gc:"+phase+":error") {
				t.Fatalf("phase %s lacks begin/error terminals: %v", phase, events)
			}
			for _, forbidden := range []string{"archive-gc:publication:begin", "archive-gc:candidate-scan:begin", "archive-gc:object-scan:begin"} {
				if slices.Contains(events, forbidden) {
					t.Fatalf("canceled phase %s reached %s", phase, forbidden)
				}
			}
			remote := NewManager(bucket, "cluster", 1)
			defer remote.Close()
			if err := remote.Load(ctx); err != nil {
				t.Fatalf("load remote head after canceled cleanup: %v", err)
			}
			afterHead, afterOK := remote.HeadVersion()
			if beforeOK != afterOK || beforeHead != afterHead {
				t.Fatalf("canceled phase %s changed archive head", phase)
			}
			if remote.Tip() != core.Tip() || hookBucket.unsafeDeletes != 0 {
				t.Fatalf("canceled phase %s changed remote tip or deleted retained objects: tip=%d want=%d unsafe_deletes=%d", phase, remote.Tip(), core.Tip(), hookBucket.unsafeDeletes)
			}
		})
	}
}

func TestArchiveCleanupPhaseTraceExcludesOtherContext(t *testing.T) {
	ctx, bucket, _, writer := newSealableArchive(t)
	defer writer.Close()
	otherCtx, otherBucket, _, otherWriter := newSealableArchive(t)
	defer otherWriter.Close()
	manager := NewManager(bucket, "cluster", 1)
	defer manager.Close()
	other := NewManager(otherBucket, "cluster", 1)
	defer other.Close()
	var events []string
	var otherErr error
	tracedCtx := localtesthooks.WithArchiveGCPhaseTrace(ctx, func(event string) {
		events = append(events, event)
		if event == "archive-gc:load:begin" {
			done := make(chan error, 1)
			go func() { done <- other.Cleanup(otherCtx, 0) }()
			otherErr = <-done
		}
	})
	if err := manager.Cleanup(tracedCtx, 0); err != nil || otherErr != nil {
		t.Fatalf("scoped cleanup=%v other cleanup=%v", err, otherErr)
	}
	if got := countArchiveGCEvent(events, "archive-gc:manager-lock:begin"); got != 1 {
		t.Fatalf("scoped manager-lock callbacks=%d, want 1; events=%v", got, events)
	}
	if got := countArchiveGCEvent(events, "archive-gc:load:begin"); got != 1 {
		t.Fatalf("scoped Load callbacks=%d, want 1; events=%v", got, events)
	}
}

func countArchiveGCEvent(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
}
