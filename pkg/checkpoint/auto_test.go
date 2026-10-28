package checkpoint

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

type terminalReleaseTestBucket struct {
	objstore.Bucket
	armed         bool
	releaseErr    error
	afterRelease  func()
	publisherPuts atomic.Int64
}

func (b *terminalReleaseTestBucket) Upload(ctx context.Context, name string, value io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/checkpoint/PUBLISHER") {
		b.publisherPuts.Add(1)
	}
	if b.armed && strings.HasSuffix(name, "/checkpoint/PUBLISHER") {
		if err := b.Bucket.Upload(ctx, name, value, options...); err != nil {
			return err
		}
		if b.afterRelease != nil {
			b.afterRelease()
		}
		return b.releaseErr
	}
	return b.Bucket.Upload(ctx, name, value, options...)
}

func TestAutoCheckpointerStopCancelsAndJoinsBefore(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	auto := NewAutoCheckpointer(NewManager(objstore.NewInMemBucket(), "stop-before", t.TempDir(), 1), nil, 1, time.Millisecond)
	entered := make(chan context.Context, 1)
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		auto.Stop()
	})
	auto.Start(parent, func() uint64 { return 1 }, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entered <- ctx
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic before callback did not start")
	}
	stopped := make(chan struct{})
	go func() { auto.Stop(); close(stopped) }()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not cancel the active before callback")
	}
	if parent.Err() != nil {
		t.Fatal("Stop canceled the parent context")
	}
	select {
	case <-stopped:
		t.Fatal("Stop returned before the callback finished")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join the completed callback")
	}
	auto.Stop()
}

func TestAutoCheckpointerStopCancelsAndJoinsClaimWork(t *testing.T) {
	for _, phase := range []string{"advance", "publish"} {
		t.Run(phase, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			manager := NewManager(objstore.NewInMemBucket(), "stop-claim", t.TempDir(), 1)
			material, err := materializer.Open(filepath.Join(t.TempDir(), "state.db"), 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := material.Close(); err != nil {
					t.Error(err)
				}
			})
			value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "stop-claim", Operation: "put", Key: "key", Value: []byte("value")})
			if err != nil {
				t.Fatal(err)
			}
			if err := material.Apply(parent, 1, value); err != nil {
				t.Fatal(err)
			}
			auto := NewAutoCheckpointer(manager, material, 1, time.Millisecond)
			entered := make(chan struct{})
			canceled := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() {
				cancel()
				releaseOnce.Do(func() { close(release) })
				auto.Stop()
			})
			work := func(ctx context.Context) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
				return ctx.Err()
			}
			advance := func(ctx context.Context, _ uint64) error {
				if phase == "advance" {
					return work(ctx)
				}
				return nil
			}
			auto.ConfigurePublisher("node", func() uint64 { return 0 }, advance)
			auto.ConfigurePublication(nil, func(ctx context.Context, _ *Checkpoint) error { return work(ctx) })
			auto.Start(parent, material.Tip, nil)
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("claim-backed maintenance callback did not start")
			}
			stopped := make(chan struct{})
			go func() { auto.Stop(); close(stopped) }()
			select {
			case <-canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("Stop did not cancel claim-backed maintenance")
			}
			select {
			case <-stopped:
				t.Fatal("Stop returned before claim work and its renewal worker joined")
			default:
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case <-stopped:
			case <-time.After(3 * time.Second):
				t.Fatal("Stop did not join claim work")
			}
			if parent.Err() != nil {
				t.Fatal("Stop canceled the parent context")
			}
			if phase == "advance" {
				claim, err := manager.AcquirePublisherClaim(parent, "next-owner", 0, publisherLease)
				if err != nil {
					t.Fatalf("canceled advance did not release its unbound claim: %v", err)
				}
				if err := manager.ReleasePublisherClaim(parent, claim); err != nil {
					t.Fatal(err)
				}
			} else if auto.candidate == nil || auto.candidate.claim == nil {
				t.Fatal("canceled certification lost its pending candidate claim")
			} else {
				if err := auto.FinalizeTerminal(parent, nil); err == nil {
					t.Fatal("terminal finalization retired an unpublished candidate")
				}
				if _, err := manager.AcquirePublisherClaim(parent, "unrelated", 0, publisherLease); !errors.Is(err, ErrPublisherBusy) {
					t.Fatalf("unpublished candidate claim after terminal finalization=%v, want busy", err)
				}
			}
		})
	}
}

func TestFinalizeTerminalRequiresCertificationBeyondCurrent(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(objstore.NewInMemBucket(), "terminal-unproven", t.TempDir(), 1)
	claim, err := manager.AcquirePublisherClaim(ctx, "publisher", 0, publisherLease)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(ctx, claim, []Source{source(t, RoleSQLite, "candidate")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = manager.BindPublisherClaim(ctx, claim, root.Index, root.RootHash, publisherLease)
	if err != nil {
		t.Fatal(err)
	}
	root.claim = claim
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	auto := NewAutoCheckpointer(manager, nil, 1, 0)
	auto.candidate = root
	if err := auto.FinalizeTerminal(ctx, nil); err == nil {
		t.Fatal("CURRENT without durable certification proof retired the publisher claim")
	}
	if _, err := manager.AcquirePublisherClaim(ctx, "other", 0, publisherLease); !errors.Is(err, ErrPublisherBusy) {
		t.Fatalf("claim after unproven terminal finalization=%v, want busy", err)
	}
}

func makeTerminalCandidate(t *testing.T, manager *Manager, owner string) (*Checkpoint, *PublisherClaim) {
	t.Helper()
	ctx := context.Background()
	claim, err := manager.AcquirePublisherClaim(ctx, owner, 0, publisherLease)
	if err != nil {
		t.Fatal(err)
	}
	root, err := manager.CreateFiles(ctx, claim, []Source{source(t, RoleSQLite, "terminal-candidate")}, 1)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = manager.BindPublisherClaim(ctx, claim, root.Index, root.RootHash, publisherLease)
	if err != nil {
		t.Fatal(err)
	}
	root.claim = claim
	if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	return root, claim
}

func TestFinalizeTerminalPreservesUnknownReleaseOutcomeAfterExpiredReadback(t *testing.T) {
	ctx := context.Background()
	unknown := errors.New("publisher release outcome unknown")
	bucket := &terminalReleaseTestBucket{Bucket: objstore.NewInMemBucket(), releaseErr: unknown}
	manager := NewManager(bucket, "terminal-unknown-expired", t.TempDir(), 1)
	root, claim := makeTerminalCandidate(t, manager, "publisher")
	auto := NewAutoCheckpointer(manager, nil, 1, 0)
	auto.candidate = root
	bucket.armed = true
	err := auto.FinalizeTerminal(ctx, func(context.Context, *Checkpoint) error { return nil })
	if !errors.Is(err, unknown) {
		t.Fatalf("terminal finalization error=%v, want preserved unknown release result", err)
	}
	if auto.candidate == nil || auto.candidate.claim != claim {
		t.Fatal("unknown terminal release outcome discarded the candidate")
	}
}

func TestFinalizeTerminalPreservesUnknownReleaseOutcomeAfterSuccessorReadback(t *testing.T) {
	ctx := context.Background()
	unknown := errors.New("publisher release outcome unknown")
	bucket := &terminalReleaseTestBucket{Bucket: objstore.NewInMemBucket(), releaseErr: unknown}
	manager := NewManager(bucket, "terminal-unknown-successor", t.TempDir(), 1)
	root, claim := makeTerminalCandidate(t, manager, "publisher")
	successorManager := NewManager(bucket.Bucket, "terminal-unknown-successor", t.TempDir(), 1)
	var successor *PublisherClaim
	bucket.afterRelease = func() {
		var err error
		successor, err = successorManager.AcquirePublisherClaim(ctx, "successor", claim.ReservedIndex, publisherLease)
		if err != nil {
			t.Errorf("acquire successor after committed release: %v", err)
		}
	}
	auto := NewAutoCheckpointer(manager, nil, 1, 0)
	auto.candidate = root
	bucket.armed = true
	err := auto.FinalizeTerminal(ctx, func(context.Context, *Checkpoint) error { return nil })
	if !errors.Is(err, unknown) {
		t.Fatalf("terminal finalization error=%v, want preserved unknown release result", err)
	}
	if successor == nil {
		t.Fatal("release callback did not establish successor")
	}
	current, err := successorManager.readPublisherClaim(ctx)
	if err != nil || current.Generation != successor.Generation || current.OwnerID != successor.OwnerID {
		t.Fatalf("successor after unknown release: current=%+v err=%v", current, err)
	}
	if err := successorManager.ReleasePublisherClaim(ctx, successor); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizeTerminalAcceptsSuccessorBeforeCurrentAndSealProof(t *testing.T) {
	for _, advanceCurrent := range []bool{false, true} {
		name := "unchanged-current"
		if advanceCurrent {
			name = "advanced-current"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			bucket := &terminalReleaseTestBucket{Bucket: objstore.NewInMemBucket()}
			manager := NewManager(bucket, "terminal-successor-first", t.TempDir(), 1)
			oldRoot, oldClaim := makeTerminalCandidate(t, manager, "old-publisher")
			if err := manager.ReleasePublisherClaim(ctx, oldClaim); err != nil {
				t.Fatal(err)
			}
			successorManager := NewManager(bucket.Bucket, "terminal-successor-first", t.TempDir(), 1)
			successor, err := successorManager.AcquirePublisherClaim(ctx, "successor", oldClaim.ReservedIndex, publisherLease)
			if err != nil {
				t.Fatal(err)
			}
			wantCurrent := oldRoot
			if advanceCurrent {
				nextRoot, err := successorManager.CreateFiles(ctx, successor, []Source{source(t, RoleSQLite, "successor-candidate")}, successor.ReservedIndex)
				if err != nil {
					t.Fatal(err)
				}
				successor, err = successorManager.BindPublisherClaim(ctx, successor, nextRoot.Index, nextRoot.RootHash, publisherLease)
				if err != nil {
					t.Fatal(err)
				}
				nextRoot.claim = successor
				if err := successorManager.PromoteCertifiedCurrent(ctx, nextRoot); err != nil {
					t.Fatal(err)
				}
				wantCurrent = nextRoot
			}
			auto := NewAutoCheckpointer(manager, nil, 1, 0)
			auto.candidate = oldRoot
			putsBefore := bucket.publisherPuts.Load()
			if err := auto.FinalizeTerminal(ctx, nil); err != nil {
				t.Fatalf("finalize fenced predecessor: %v", err)
			}
			if got := bucket.publisherPuts.Load(); got != putsBefore {
				t.Fatalf("terminal finalization wrote PUBLISHER for successor: before=%d after=%d", putsBefore, got)
			}
			if err := manager.Load(ctx); err != nil {
				t.Fatal(err)
			}
			currentRoot := manager.Latest()
			if currentRoot == nil || currentRoot.Index != wantCurrent.Index || currentRoot.RootHash != wantCurrent.RootHash {
				t.Fatalf("terminal finalization changed CURRENT: got=%+v want=%+v", currentRoot, wantCurrent)
			}
			current, err := successorManager.readPublisherClaim(ctx)
			if err != nil || current.Generation != successor.Generation || current.OwnerID != successor.OwnerID {
				t.Fatalf("successor claim changed: current=%+v err=%v", current, err)
			}
			if err := successorManager.ReleasePublisherClaim(ctx, successor); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAutoCheckpointerStopBeforeStartAndConcurrentStart(t *testing.T) {
	for i := 0; i < 50; i++ {
		auto := NewAutoCheckpointer(NewManager(objstore.NewInMemBucket(), "stop-start", t.TempDir(), 1), nil, 1, time.Hour)
		if i == 0 {
			auto.Stop()
		}
		var callers sync.WaitGroup
		callers.Add(2)
		go func() { defer callers.Done(); auto.Start(context.Background(), func() uint64 { return 0 }, nil) }()
		go func() { defer callers.Done(); auto.Stop() }()
		callers.Wait()
		auto.Stop()
	}
	disabled := NewAutoCheckpointer(nil, nil, 1, 0)
	disabled.Start(context.Background(), nil, nil)
	disabled.Stop()
	disabled.Stop()
}

func TestAutoCheckpointerExplicitCheckpointAfterStopRemainsStrict(t *testing.T) {
	manager := NewManager(objstore.NewInMemBucket(), "explicit-after-stop", t.TempDir(), 1)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "state.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	auto := NewAutoCheckpointer(manager, material, 1, time.Millisecond)
	auto.Stop()
	auto.candidate = createFiles(t, manager, context.Background(), []Source{source(t, RoleSQLite, "explicit")}, 1)
	failure := errors.New("checkpoint quorum unavailable")
	for _, want := range []error{ErrPublisherBusy, failure, context.Canceled} {
		auto.ConfigurePublication(nil, func(ctx context.Context, _ *Checkpoint) error {
			if ctx.Err() != nil {
				t.Fatalf("automatic Stop canceled the explicit checkpoint context: %v", ctx.Err())
			}
			return want
		})
		if err := auto.CheckpointOnShutdown(context.Background(), 1); !errors.Is(err, want) {
			t.Fatalf("explicit checkpoint error=%v, want %v", err, want)
		}
	}
	auto.ConfigurePublication(nil, manager.PromoteCertifiedCurrent)
	if err := auto.CheckpointOnShutdown(context.Background(), 1); err != nil {
		t.Fatalf("explicit checkpoint after Stop: %v", err)
	}
	if latest := manager.Latest(); latest == nil || latest.Index != 1 {
		t.Fatalf("explicit checkpoint was not promoted: %+v", latest)
	}
}
