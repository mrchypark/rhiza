//go:build rhiza_local_testhooks

package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

type claimRetryHeldRootBucket struct {
	objstore.Bucket
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *claimRetryHeldRootBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.armed.Load() && strings.Contains(name, "checkpoint/roots/") {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-ctx.Done():
			return objstore.ObjectAttributes{}, ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

func TestNodeForegroundCheckpointClaimRetryRealPinRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bucket := &claimRetryHeldRootBucket{Bucket: objstore.NewInMemBucket(), entered: make(chan struct{}), release: make(chan struct{})}
	var releaseHold sync.Once
	defer releaseHold.Do(func() { close(bucket.release) })
	holder := checkpoint.NewManager(bucket, "claim-retry", t.TempDir(), 1)
	claim, err := holder.AcquirePublisherClaim(ctx, "initial", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "initial.db")
	policySnapshot(t, file, "initial")
	root, err := holder.CreateFiles(ctx, claim, []checkpoint.Source{{Role: checkpoint.RoleSQLite, Path: file}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.PromoteCertifiedCurrent(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := holder.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	contender := checkpoint.NewManager(bucket, "claim-retry", t.TempDir(), 1)
	if err := contender.Load(ctx); err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "material.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	for index := uint64(1); index <= 2; index++ {
		value, err := types.EncodeKVCommand(types.KVCommand{RequestID: fmt.Sprintf("claim-retry-%d", index), Operation: "put", Key: "key", Value: []byte("value")})
		if err != nil {
			t.Fatal(err)
		}
		if err := material.Apply(ctx, index, value); err != nil {
			t.Fatal(err)
		}
	}
	auto := checkpoint.NewAutoCheckpointer(contender, material, 1, 0)
	auto.ConfigurePublisher("contender", func() uint64 { return 1 }, nil)
	auto.ConfigurePublication(nil, func(ctx context.Context, candidate *checkpoint.Checkpoint) error {
		return contender.PromoteCertifiedCurrent(ctx, candidate)
	})
	bucket.armed.Store(true)
	pinDone := make(chan error, 1)
	go func() {
		pin, err := holder.PinRecoveryRoot(ctx, root, "held-pin", time.Minute)
		if err == nil {
			err = pin.Close(ctx)
		}
		pinDone <- err
	}()
	select {
	case <-bucket.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The former single-call fixture really rejects this live maintenance claim.
	var oldObserved checkpoint.PublisherBusyObservation
	oldCtx := checkpoint.WithPublisherBusyObserver(ctx, func(event checkpoint.PublisherBusyObservation) { oldObserved = event })
	if err := auto.CheckpointOnShutdown(oldCtx, 2); err != checkpoint.ErrPublisherBusy || oldObserved.Source != "publisher_claim_active" {
		t.Fatalf("single call error=%v observation=%+v", err, oldObserved)
	}
	// An unreleased claim must remain a hard error after the finite budget;
	// neither a candidate nor CURRENT may be credited as completed work.
	exhausted, err := retryNodeForegroundCheckpointClaim(ctx, func(context.Context) error { return nil }, func(attemptCtx context.Context) error {
		return auto.CheckpointOnShutdown(attemptCtx, 2)
	}, nil)
	if !errors.Is(err, checkpoint.ErrPublisherBusy) || exhausted.attempts != 50 || exhausted.refusals != 50 || exhausted.waits != 49 {
		t.Fatalf("held claim error=%v counts=%+v", err, exhausted)
	}
	if latest := contender.Latest(); latest == nil || latest.Index != 1 {
		t.Fatalf("CURRENT advanced despite 50 refusals: %+v", latest)
	}
	canceledAttemptCtx, cancelAttempt := context.WithCancel(ctx)
	canceledCounts, canceledErr := retryNodeForegroundCheckpointClaim(canceledAttemptCtx, func(context.Context) error { return nil }, func(attemptCtx context.Context) error {
		return auto.CheckpointOnShutdown(attemptCtx, 2)
	}, func(event checkpoint.PublisherBusyObservation) {
		if event.Source == "publisher_claim_active" {
			cancelAttempt()
		}
	})
	if !errors.Is(canceledErr, context.Canceled) || canceledCounts.attempts != 1 || canceledCounts.refusals != 1 || canceledCounts.waits != 0 {
		t.Fatalf("canceled after real refusal error=%v counts=%+v", canceledErr, canceledCounts)
	}
	currentTip := uint64(1)
	var syncedTips []uint64
	counts, err := retryNodeForegroundCheckpointClaim(ctx, func(context.Context) error {
		syncedTips = append(syncedTips, currentTip)
		return nil
	}, func(attemptCtx context.Context) error {
		return auto.CheckpointOnShutdown(attemptCtx, 2)
	}, func(event checkpoint.PublisherBusyObservation) {
		if event.Source != "publisher_claim_active" {
			return
		}
		currentTip = 2
		releaseHold.Do(func() { close(bucket.release) })
		select {
		case pinErr := <-pinDone:
			if pinErr != nil {
				t.Errorf("pin close: %v", pinErr)
			}
		case <-ctx.Done():
			t.Errorf("pin close: %v", ctx.Err())
		}
	})
	if err != nil || counts.attempts != 2 || counts.refusals != 1 || counts.waits != 1 || len(syncedTips) != 2 || syncedTips[0] != 1 || syncedTips[1] != 2 {
		t.Fatalf("retry error=%v counts=%+v coverage_checks=%v", err, counts, syncedTips)
	}
	reader := checkpoint.NewManager(bucket, "claim-retry", t.TempDir(), 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	latest := reader.Latest()
	if latest == nil || latest.Index < 2 {
		t.Fatalf("independent CURRENT=%v, want index >=2", latest)
	}
	if err := reader.Verify(ctx, latest.Index, latest.RootHash, latest.Hash); err != nil {
		t.Fatalf("independent certified checkpoint readback: %v", err)
	}
}

func TestNodeForegroundCheckpointClaimRetryTerminalCases(t *testing.T) {
	active := checkpoint.PublisherBusyObservation{Source: "publisher_claim_active", RequestedPurpose: "publisher", ActiveClaim: true, VersionPresent: true, LeaseRemainingMillis: 1000}
	if !nodeForegroundLiveClaimRefusal(active) {
		t.Fatal("real active observation was not eligible")
	}
	for _, event := range []checkpoint.PublisherBusyObservation{
		{Source: "publisher_claim_conditional_exhausted", RequestedPurpose: "publisher", ActiveClaim: true, VersionPresent: true, LeaseRemainingMillis: 1000},
		{Source: "acquire_upload_ack", RequestedPurpose: "publisher", ActiveClaim: true, VersionPresent: true, LeaseRemainingMillis: 1000},
		{Source: "publisher_claim_active", RequestedPurpose: "maintenance", ActiveClaim: true, VersionPresent: true, LeaseRemainingMillis: 1000},
		{Source: "publisher_claim_active", RequestedPurpose: "publisher", ActiveClaim: true, LeaseRemainingMillis: 1000},
		{Source: "publisher_claim_active", RequestedPurpose: "publisher", ActiveClaim: true, VersionPresent: true},
	} {
		if nodeForegroundLiveClaimRefusal(event) {
			t.Fatalf("non-pre-admission event was eligible: %+v", event)
		}
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"source_absent", checkpoint.ErrPublisherBusy},
		{"wrapped_busy", fmt.Errorf("wrapped: %w", checkpoint.ErrPublisherBusy)},
		{"joined_busy", errors.Join(checkpoint.ErrPublisherBusy, errors.New("other cause"))},
		{"storage_error", errors.New("storage failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			counts, err := retryNodeForegroundCheckpointClaim(context.Background(), func(context.Context) error { return nil }, func(context.Context) error {
				calls++
				return tc.err
			}, nil)
			if err == nil || calls != 1 || counts.waits != 0 {
				t.Fatalf("error=%v calls=%d counts=%+v", err, calls, counts)
			}
		})
	}
	t.Run("canceled_before_first_attempt", func(t *testing.T) {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		counts, err := retryNodeForegroundCheckpointClaim(canceled, func(context.Context) error {
			calls++
			return nil
		}, func(context.Context) error {
			calls++
			return nil
		}, nil)
		if !errors.Is(err, context.Canceled) || calls != 0 || counts.attempts != 0 || counts.waits != 0 {
			t.Fatalf("error=%v calls=%d counts=%+v", err, calls, counts)
		}
	})
	t.Run("archive_error_is_terminal", func(t *testing.T) {
		archiveErr := errors.New("archive unavailable")
		calls := 0
		counts, err := retryNodeForegroundCheckpointClaim(context.Background(), func(context.Context) error {
			return archiveErr
		}, func(context.Context) error {
			calls++
			return nil
		}, nil)
		if !errors.Is(err, archiveErr) || calls != 0 || counts.attempts != 0 || counts.archiveSyncs != 1 {
			t.Fatalf("error=%v calls=%d counts=%+v", err, calls, counts)
		}
	})
	t.Run("post_admission_busy_is_terminal", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		manager := checkpoint.NewManager(objstore.NewInMemBucket(), "post-admission", t.TempDir(), 1)
		calls := 0
		counts, err := retryNodeForegroundCheckpointClaim(ctx, func(context.Context) error { return nil }, func(attemptCtx context.Context) error {
			calls++
			claim, err := manager.AcquirePublisherClaim(attemptCtx, "controlled-work", 0, time.Minute)
			if err != nil {
				return err
			}
			if err := manager.ReleasePublisherClaim(attemptCtx, claim); err != nil {
				return err
			}
			return checkpoint.ErrPublisherBusy
		}, nil)
		if !errors.Is(err, checkpoint.ErrPublisherBusy) || calls != 1 || counts.refusals != 0 || counts.waits != 0 {
			t.Fatalf("post-admission error=%v calls=%d counts=%+v", err, calls, counts)
		}
	})
}
