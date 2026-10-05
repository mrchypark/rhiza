package checkpoint

import (
	"context"
	"crypto/sha256"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/thanos-io/objstore"
)

// PublisherBusyObservation describes one claim-acquisition branch that
// returned ErrPublisherBusy. It is emitted only to an opt-in, context-local
// diagnostic observer; normal checkpoint behavior does not retain it.
type PublisherBusyObservation struct {
	Source               string
	RequestedPurpose     string
	Attempt              int
	ActiveClaim          bool
	Purpose              string
	PurposeTruncated     bool
	OwnerID              string
	OwnerIDTruncated     bool
	Generation           uint64
	ReservedIndex        uint64
	BoundIndex           uint64
	LeaseRemainingMillis int64
	HolderCategory       string
	NamespaceDigest      [32]byte
	VersionDigest        [32]byte
	VersionPresent       bool
	ObservedAtMillis     int64
}

type publisherBusyObserverKey struct{}
type publisherClaimCategoryKey struct{}

const (
	claimCategoryStartup       = "startup"
	claimCategoryNodeCatchup   = "node_catchup"
	claimCategoryGC            = "checkpoint_gc"
	claimCategorySourceUnknown = "source_context_unknown"
)

// WithPublisherBusyObserver enables diagnostic observations for claim
// acquisition in ctx. A nil observer is equivalent to no observer.
func WithPublisherBusyObserver(ctx context.Context, observe func(PublisherBusyObservation)) context.Context {
	if observe == nil {
		return ctx
	}
	return context.WithValue(ctx, publisherBusyObserverKey{}, observe)
}

func observePublisherBusy(ctx context.Context, observation PublisherBusyObservation) {
	observe, _ := ctx.Value(publisherBusyObserverKey{}).(func(PublisherBusyObservation))
	if observe != nil {
		observe(observation)
	}
}

func carryPublisherObserver(dst, src context.Context) context.Context {
	observe, _ := src.Value(publisherBusyObserverKey{}).(func(PublisherBusyObservation))
	if observe == nil {
		return dst
	}
	return context.WithValue(dst, publisherBusyObserverKey{}, observe)
}

func withPublisherClaimCategory(ctx context.Context, category string) context.Context {
	if ctx.Value(publisherBusyObserverKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, publisherClaimCategoryKey{}, category)
}

func publisherClaimCategory(ctx context.Context) string {
	category, _ := ctx.Value(publisherClaimCategoryKey{}).(string)
	switch category {
	case claimCategoryStartup, claimCategoryNodeCatchup, claimCategoryGC:
		return category
	default:
		return claimCategorySourceUnknown
	}
}

// observeClaimLifecycle reuses the existing opt-in publisher observer. It
// computes identity digests only when a subscriber exists and performs no I/O.
func observeClaimLifecycle(ctx context.Context, phase, prefix string, claim *PublisherClaim) {
	observe, _ := ctx.Value(publisherBusyObserverKey{}).(func(PublisherBusyObservation))
	if observe == nil || claim == nil {
		return
	}
	now := time.Now().UnixMilli()
	event := PublisherBusyObservation{
		Source: phase, Purpose: claim.Purpose, Generation: claim.Generation,
		HolderCategory: publisherClaimCategory(ctx), ObservedAtMillis: now,
		LeaseRemainingMillis: claim.LeaseUntilMS - now,
	}
	fillClaimIdentity(&event, prefix, claim.version)
	observe(event)
}

func fillClaimIdentity(event *PublisherBusyObservation, prefix string, version *objstore.ObjectVersion) {
	event.NamespaceDigest = sha256.Sum256([]byte(prefix))
	if version == nil {
		return
	}
	event.VersionPresent = true
	encoded := strconv.AppendInt(nil, int64(version.Type), 10)
	encoded = append(encoded, ':')
	encoded = append(encoded, version.Value...)
	event.VersionDigest = sha256.Sum256(encoded)
}

func activePublisherBusyObservation(source, requestedPurpose string, attempt int, claim *PublisherClaim, nowMillis int64) PublisherBusyObservation {
	purpose, purposeTruncated := boundedPublisherBusyField(claim.Purpose)
	owner, ownerTruncated := boundedPublisherBusyField(claim.OwnerID)
	return PublisherBusyObservation{
		Source: source, RequestedPurpose: requestedPurpose, Attempt: attempt,
		ActiveClaim: true, Purpose: purpose, PurposeTruncated: purposeTruncated,
		OwnerID: owner, OwnerIDTruncated: ownerTruncated, Generation: claim.Generation,
		ReservedIndex: claim.ReservedIndex, BoundIndex: claim.BoundIndex,
		LeaseRemainingMillis: claim.LeaseUntilMS - nowMillis,
	}
}

func exhaustedPublisherBusyObservation(source, requestedPurpose string, attempt int) PublisherBusyObservation {
	return PublisherBusyObservation{Source: source, RequestedPurpose: requestedPurpose, Attempt: attempt}
}

func boundedPublisherBusyField(value string) (string, bool) {
	const limit = 128
	if len(value) <= limit && utf8.ValidString(value) {
		return value, false
	}
	var bounded strings.Builder
	bounded.Grow(limit)
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && size == 1 {
			r, size = utf8.RuneError, 1
		}
		encodedSize := utf8.RuneLen(r)
		if bounded.Len()+encodedSize > limit {
			return bounded.String(), true
		}
		if size == 1 && r == utf8.RuneError {
			bounded.WriteRune(r)
		} else {
			bounded.WriteString(value[:size])
		}
		value = value[size:]
	}
	return bounded.String(), true
}
