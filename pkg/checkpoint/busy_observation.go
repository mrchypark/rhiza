package checkpoint

import (
	"context"
	"strings"
	"unicode/utf8"
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
}

type publisherBusyObserverKey struct{}

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
