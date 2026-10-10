package checkpoint

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

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
