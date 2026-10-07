//go:build rhiza_local_testhooks

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

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

type holdClaimRootAttributesBucket struct {
	objstore.Bucket
	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type failClaimReleaseBucket struct {
	objstore.Bucket
	fail atomic.Bool
}

func (b *failClaimReleaseBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/checkpoint/PUBLISHER") && b.fail.Swap(false) {
		return errors.New("injected release upload failure")
	}
	return b.Bucket.Upload(ctx, name, r, options...)
}

func (b *holdClaimRootAttributesBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if name == b.name {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-ctx.Done():
			return objstore.ObjectAttributes{}, ctx.Err()
		case <-b.release:
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

func TestCheckpointClaimLifecycleIdentifiesRealMaintenanceHolder(t *testing.T) {
	for _, kind := range []string{"pin", "gc"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			base := objstore.NewInMemBucket()
			var bucket objstore.Bucket = base
			var pinHold *holdClaimRootAttributesBucket
			var gcHold *blockingIterBucket
			if kind == "pin" {
				pinHold = &holdClaimRootAttributesBucket{Bucket: base, entered: make(chan struct{}), release: make(chan struct{})}
				bucket = pinHold
			} else {
				gcHold = &blockingIterBucket{Bucket: base, started: make(chan struct{}), release: make(chan struct{}), blockDir: "checkpoint/roots"}
				bucket = gcHold
			}
			publisher := NewManager(bucket, "holder-join", t.TempDir(), 1)
			root := createFiles(t, publisher, ctx, []Source{source(t, RoleSQLite, "initial")}, 1)
			if err := publisher.PromoteCertifiedCurrent(ctx, root); err != nil {
				t.Fatal(err)
			}
			if pinHold != nil {
				pinHold.name = publisher.key(rootName(root.Index, root.RootHash))
			}
			contender := NewManager(bucket, "holder-join", t.TempDir(), 1)
			if err := contender.Load(ctx); err != nil {
				t.Fatal(err)
			}
			material, err := materializer.Open(filepath.Join(t.TempDir(), "state.db"), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer material.Close()
			for index := uint64(1); index <= 2; index++ {
				value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "claim-holder", Operation: "put", Key: "key", Value: []byte("value")})
				if err != nil {
					t.Fatal(err)
				}
				if err := material.Apply(ctx, index, value); err != nil {
					t.Fatalf("apply index=%d: encode=%v", index, err)
				}
			}
			auto := NewAutoCheckpointer(contender, material, 1, 0)
			auto.ConfigurePublisher("contender", func() uint64 { return 1 }, nil)
			auto.ConfigurePublication(nil, func(ctx context.Context, candidate *Checkpoint) error {
				return contender.PromoteCertifiedCurrent(ctx, candidate)
			})
			var mu sync.Mutex
			var events []PublisherBusyObservation
			observe := func(event PublisherBusyObservation) {
				mu.Lock()
				events = append(events, event)
				mu.Unlock()
			}
			holderCtx := WithPublisherBusyObserver(ctx, observe)
			if kind == "pin" {
				holderCtx = localtesthooks.WithRecoveryPinCategory(holderCtx, localtesthooks.RecoveryPinOwnerStartup)
			}
			done := make(chan error, 1)
			if kind == "pin" {
				go func() {
					pin, err := publisher.PinRecoveryRoot(holderCtx, root, "pin-holder", time.Minute)
					if err == nil {
						err = pin.Close(ctx)
					}
					done <- err
				}()
			} else {
				go func() { done <- publisher.GarbageCollectFrom(holderCtx, nil, 1, root.Index, 0) }()
			}
			if kind == "pin" {
				select {
				case <-pinHold.entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			} else {
				select {
				case <-gcHold.started:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if err := auto.CheckpointOnShutdown(WithPublisherBusyObserver(ctx, observe), 2); !errors.Is(err, ErrPublisherBusy) {
				t.Fatalf("checkpoint under real %s maintenance claim: %v", kind, err)
			}
			if latest := contender.Latest(); latest == nil || latest.Index != 1 {
				t.Fatalf("CURRENT changed while contender was refused: %+v", latest)
			}
			if pinHold != nil {
				close(pinHold.release)
			} else {
				close(gcHold.release)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			claim, err := contender.readPublisherClaim(ctx)
			if err != nil || claim.LeaseUntilMS > time.Now().UnixMilli() {
				t.Fatalf("maintenance release was not independently visible: claim=%+v err=%v", claim, err)
			}
			mu.Lock()
			seen := append([]PublisherBusyObservation(nil), events...)
			mu.Unlock()
			var acquired, guarded, released *PublisherBusyObservation
			for _, event := range seen {
				switch event.Source {
				case "acquire_confirmed":
					copy := event
					acquired = &copy
				case "publisher_claim_active":
					copy := event
					guarded = &copy
				case "release_upload_ack":
					copy := event
					released = &copy
				}
			}
			wantCategory := claimCategoryGC
			if kind == "pin" {
				wantCategory = claimCategoryStartup
			}
			if acquired == nil || guarded == nil || released == nil || acquired.HolderCategory != wantCategory ||
				acquired.Purpose != "maintenance" || acquired.Generation != claim.Generation || !acquired.VersionPresent ||
				guarded.Purpose != "maintenance" || guarded.Generation != acquired.Generation || !guarded.VersionPresent ||
				guarded.NamespaceDigest != acquired.NamespaceDigest || guarded.VersionDigest != acquired.VersionDigest ||
				released.Generation != acquired.Generation || released.NamespaceDigest != acquired.NamespaceDigest {
				t.Fatalf("claim lifecycle join missing: acquired=%+v guarded=%+v released=%+v", acquired, guarded, released)
			}
			foreign := NewManager(base, "other-namespace", t.TempDir(), 1)
			unrelated, err := foreign.AcquirePublisherClaim(ctx, "unrelated", 0, time.Minute)
			if err != nil {
				t.Fatalf("unrelated namespace was blocked: %v", err)
			}
			if err := foreign.ReleasePublisherClaim(ctx, unrelated); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckpointClaimLifecycleWorkAndReleaseFailureStayDistinct(t *testing.T) {
	ctx := context.Background()
	bucket := &failClaimReleaseBucket{Bucket: objstore.NewInMemBucket()}
	manager := NewManager(bucket, "failed-release", t.TempDir(), 1)
	var events []PublisherBusyObservation
	ctx = WithPublisherBusyObserver(ctx, func(event PublisherBusyObservation) { events = append(events, event) })
	wantWork := errors.New("injected work failure")
	err := manager.withMaintenanceClaim(ctx, claimCategoryGC, func(context.Context, *PublisherClaim) error {
		bucket.fail.Store(true)
		return wantWork
	})
	if !errors.Is(err, wantWork) {
		t.Fatalf("work error changed by release failure: %v", err)
	}
	claim, err := manager.readPublisherClaim(ctx)
	if err != nil || claim.LeaseUntilMS <= time.Now().UnixMilli() {
		t.Fatalf("failed release unexpectedly cleared live claim: claim=%+v err=%v", claim, err)
	}
	var confirmed, releaseError bool
	for _, event := range events {
		confirmed = confirmed || event.Source == "acquire_confirmed"
		releaseError = releaseError || event.Source == "release_error"
	}
	if !confirmed || !releaseError {
		t.Fatalf("missing confirmed acquire or failed release: %+v", events)
	}
}
