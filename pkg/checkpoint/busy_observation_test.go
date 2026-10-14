package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

type publisherBusyProbeBucket struct {
	objstore.Bucket
	mu          sync.Mutex
	forceCAS    bool
	liveOnCAS   bool
	publisherIO []string
	currentPut  int
}

func (b *publisherBusyProbeBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if strings.HasSuffix(name, "/checkpoint/PUBLISHER") {
		b.mu.Lock()
		b.publisherIO = append(b.publisherIO, "attributes")
		b.mu.Unlock()
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *publisherBusyProbeBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if strings.HasSuffix(name, "/checkpoint/PUBLISHER") {
		b.mu.Lock()
		b.publisherIO = append(b.publisherIO, "get")
		b.mu.Unlock()
	}
	return b.Bucket.Get(ctx, name)
}

func (b *publisherBusyProbeBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.HasSuffix(name, "/checkpoint/CURRENT") {
		b.mu.Lock()
		b.currentPut++
		b.mu.Unlock()
	}
	b.mu.Lock()
	force := b.forceCAS && strings.HasSuffix(name, "/checkpoint/PUBLISHER")
	b.mu.Unlock()
	if !force {
		return b.Bucket.Upload(ctx, name, reader, options...)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	var stale PublisherClaim
	if err := json.Unmarshal(data, &stale); err != nil {
		return err
	}
	stale.LeaseUntilMS = 1
	if b.liveOnCAS {
		stale.LeaseUntilMS = time.Now().Add(time.Minute).UnixMilli()
		stale.OwnerID, stale.Purpose = "foreign-live-publisher", "publisher"
		stale.ReservedIndex = 1
		stale.Generation++
	}
	stale.version = nil
	staleData, err := json.Marshal(stale)
	if err != nil {
		return err
	}
	if err := b.Bucket.Upload(ctx, name, bytes.NewReader(staleData)); err != nil {
		return err
	}
	return b.Bucket.Upload(ctx, name, bytes.NewReader(data), options...)
}

func (b *publisherBusyProbeBucket) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publisherIO = nil
	b.currentPut = 0
}

func (b *publisherBusyProbeBucket) snapshot() ([]string, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.publisherIO...), b.currentPut
}

func TestPublisherBusyObservationActiveClaimsAreExactAndReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name, requestedPurpose, heldPurpose, source string
		hold                                        func(*Manager, context.Context) (*PublisherClaim, error)
		attempt                                     func(*Manager, context.Context) error
	}{
		{
			name: "publisher", requestedPurpose: "publisher", heldPurpose: "publisher", source: "publisher_claim_active",
			hold: func(m *Manager, ctx context.Context) (*PublisherClaim, error) {
				return m.AcquirePublisherClaim(ctx, strings.Repeat("é", 100), 0, time.Minute)
			},
			attempt: func(m *Manager, ctx context.Context) error {
				_, err := m.AcquirePublisherClaim(ctx, "contender", 0, time.Minute)
				return err
			},
		},
		{
			name: "maintenance", requestedPurpose: "publisher", heldPurpose: "maintenance", source: "publisher_claim_active",
			hold: func(m *Manager, ctx context.Context) (*PublisherClaim, error) {
				return m.acquireMaintenanceClaim(ctx, "holder", time.Minute)
			},
			attempt: func(m *Manager, ctx context.Context) error {
				_, err := m.AcquirePublisherClaim(ctx, "contender", 0, time.Minute)
				return err
			},
		},
		{
			name: "recovery-admission", requestedPurpose: "maintenance", heldPurpose: "publisher", source: "publisher_claim_active",
			hold: func(m *Manager, ctx context.Context) (*PublisherClaim, error) {
				return m.AcquirePublisherClaim(ctx, "holder", 0, time.Minute)
			},
			attempt: func(m *Manager, ctx context.Context) error {
				_, err := m.acquireMaintenanceClaim(ctx, "contender", time.Minute)
				return err
			},
		},
		{
			name: "generation", requestedPurpose: "generation", heldPurpose: "generation", source: "generation_claim_active",
			hold: func(m *Manager, ctx context.Context) (*PublisherClaim, error) {
				return m.AcquireGenerationClaim(ctx, "holder", 1, time.Minute)
			},
			attempt: func(m *Manager, ctx context.Context) error {
				_, err := m.AcquireGenerationClaim(ctx, "holder", 1, time.Minute)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := &publisherBusyProbeBucket{Bucket: objstore.NewInMemBucket()}
			manager := NewManager(bucket, tc.name, t.TempDir(), 1)
			held, err := tc.hold(manager, context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = manager.ReleasePublisherClaim(context.Background(), held) }()
			bucket.reset()
			var observations []PublisherBusyObservation
			ctx := WithPublisherBusyObserver(context.Background(), func(observation PublisherBusyObservation) { observations = append(observations, observation) })
			if err := tc.attempt(manager, ctx); !errors.Is(err, ErrPublisherBusy) || errors.Is(err, ErrRecoveryAdmissionBusy) != (tc.requestedPurpose == "maintenance") {
				t.Fatalf("attempt error=%v", err)
			}
			ioWithObserver, currentWrites := bucket.snapshot()
			if currentWrites != 0 {
				t.Fatalf("CURRENT writes=%d, want 0", currentWrites)
			}
			if len(observations) != 1 {
				t.Fatalf("observations=%+v", observations)
			}
			observation := observations[0]
			if observation.Source != tc.source || observation.RequestedPurpose != tc.requestedPurpose || !observation.ActiveClaim || observation.Purpose != tc.heldPurpose || observation.Attempt != 1 || observation.LeaseRemainingMillis <= 0 {
				t.Fatalf("observation=%+v", observation)
			}
			if observation.PurposeTruncated {
				t.Fatalf("known claim purpose was unexpectedly bounded: %+v", observation)
			}
			if tc.name == "publisher" && (!observation.OwnerIDTruncated || len(observation.OwnerID) > 128 || !strings.HasSuffix(observation.OwnerID, "é")) {
				t.Fatalf("owner bound=%q truncated=%t", observation.OwnerID, observation.OwnerIDTruncated)
			}

			bucket.reset()
			if err := tc.attempt(manager, context.Background()); !errors.Is(err, ErrPublisherBusy) {
				t.Fatalf("unobserved attempt error=%v", err)
			}
			ioWithoutObserver, currentWritesWithout := bucket.snapshot()
			if currentWritesWithout != 0 || strings.Join(ioWithObserver, ",") != strings.Join(ioWithoutObserver, ",") {
				t.Fatalf("observer changed I/O: with=%v/%d without=%v/%d", ioWithObserver, currentWrites, ioWithoutObserver, currentWritesWithout)
			}
		})
	}
}

func TestPublisherBusyObservationBoundsMalformedClaimFields(t *testing.T) {
	claim := &PublisherClaim{
		Purpose:       strings.Repeat("p", 127) + "\xff" + strings.Repeat("q", 128),
		OwnerID:       strings.Repeat("é", 80) + "\xff" + strings.Repeat("owner", 64),
		Generation:    7,
		ReservedIndex: 11,
		BoundIndex:    12,
		LeaseUntilMS:  200,
	}
	observation := activePublisherBusyObservation("publisher_claim_active", "publisher", 1, claim, 100)
	if !observation.PurposeTruncated || !observation.OwnerIDTruncated || len(observation.Purpose) > 128 || len(observation.OwnerID) > 128 || !utf8.ValidString(observation.Purpose) || !utf8.ValidString(observation.OwnerID) {
		t.Fatalf("bounded observation=%+v", observation)
	}
	if observation.Generation != claim.Generation || observation.ReservedIndex != claim.ReservedIndex || observation.BoundIndex != claim.BoundIndex || observation.LeaseRemainingMillis != 100 {
		t.Fatalf("scalar observation=%+v", observation)
	}
}

func TestPublisherBusyObservationConditionalExhaustionHasNoClaim(t *testing.T) {
	for _, tc := range []struct {
		name, source, purpose string
		attempt               func(*Manager, context.Context) error
	}{
		{name: "publisher", source: "publisher_claim_conditional_exhausted", purpose: "publisher", attempt: func(m *Manager, ctx context.Context) error {
			_, err := m.AcquirePublisherClaim(ctx, "owner", 0, time.Minute)
			return err
		}},
		{name: "generation", source: "generation_claim_conditional_exhausted", purpose: "generation", attempt: func(m *Manager, ctx context.Context) error {
			_, err := m.AcquireGenerationClaim(ctx, "owner", 1, time.Minute)
			return err
		}},
		{name: "maintenance", source: "publisher_claim_conditional_exhausted", purpose: "maintenance", attempt: func(m *Manager, ctx context.Context) error {
			_, err := m.acquireMaintenanceClaim(ctx, "owner", time.Minute)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := &publisherBusyProbeBucket{Bucket: objstore.NewInMemBucket(), forceCAS: true}
			manager := NewManager(bucket, tc.name, t.TempDir(), 1)
			var observations []PublisherBusyObservation
			ctx := WithPublisherBusyObserver(context.Background(), func(observation PublisherBusyObservation) { observations = append(observations, observation) })
			if err := tc.attempt(manager, ctx); !errors.Is(err, ErrPublisherBusy) || errors.Is(err, ErrRecoveryAdmissionBusy) {
				t.Fatalf("attempt error=%v", err)
			}
			if len(observations) != 1 {
				t.Fatalf("observations=%+v", observations)
			}
			observation := observations[0]
			if observation.Source != tc.source || observation.RequestedPurpose != tc.purpose || observation.Attempt != 4 || observation.ActiveClaim || observation.Purpose != "" || observation.OwnerID != "" || observation.Generation != 0 || observation.ReservedIndex != 0 || observation.BoundIndex != 0 || observation.LeaseRemainingMillis != 0 {
				t.Fatalf("exhaustion observation=%+v", observation)
			}
			_, currentWrites := bucket.snapshot()
			if currentWrites != 0 {
				t.Fatalf("CURRENT writes=%d, want 0", currentWrites)
			}
		})
	}
}

func TestRecoveryAdmissionDoesNotClassifyLiveClaimAfterLostCAS(t *testing.T) {
	bucket := &publisherBusyProbeBucket{Bucket: objstore.NewInMemBucket(), forceCAS: true, liveOnCAS: true}
	manager := NewManager(bucket, "lost-cas", t.TempDir(), 1)
	var observations []PublisherBusyObservation
	ctx := WithPublisherBusyObserver(context.Background(), func(event PublisherBusyObservation) { observations = append(observations, event) })
	_, err := manager.acquireMaintenanceClaim(ctx, "contender", time.Minute)
	if !errors.Is(err, ErrPublisherBusy) || errors.Is(err, ErrRecoveryAdmissionBusy) {
		t.Fatalf("post-CAS refusal incorrectly classified: %v", err)
	}
	if len(observations) != 1 || observations[0].Attempt != 2 || observations[0].Source != "publisher_claim_active" || observations[0].OwnerID != "foreign-live-publisher" {
		t.Fatalf("wrong lost-CAS branch: %+v", observations)
	}
}

func TestPublisherBusyObserverLeavesCustomCallbackUnattributed(t *testing.T) {
	material, err := materializer.Open(filepath.Join(t.TempDir(), "state.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	auto := NewAutoCheckpointer(NewManager(objstore.NewInMemBucket(), "custom-callback", t.TempDir(), 1), material, 1, 0)
	auto.candidate = &Checkpoint{Index: 1}
	auto.ConfigurePublication(nil, func(context.Context, *Checkpoint) error { return ErrPublisherBusy })
	var observations []PublisherBusyObservation
	ctx := WithPublisherBusyObserver(context.Background(), func(observation PublisherBusyObservation) { observations = append(observations, observation) })
	if _, err := auto.create(ctx); !errors.Is(err, ErrPublisherBusy) {
		t.Fatalf("custom callback error=%v", err)
	}
	if len(observations) != 0 {
		t.Fatalf("custom callback unexpectedly attributed busy=%+v", observations)
	}
}
