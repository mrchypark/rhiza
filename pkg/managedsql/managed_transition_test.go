package managedsql

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestShouldRetryMembershipTransitionContract(t *testing.T) {
	if shouldRetryMembershipTransition(nil, 0) {
		t.Fatal("nil error must not retry")
	}
	if shouldRetryMembershipTransition(network.ErrNotReady, 0) {
		t.Fatal("plain ErrNotReady must not retry")
	}
	drain := errors.Join(network.ErrNotReady, quepaxa.ErrReconfigurationDrain)
	if !shouldRetryMembershipTransition(drain, 0) || !shouldRetryMembershipTransition(drain, maxMembershipTransitionRetries-1) {
		t.Fatal("ErrReconfigurationDrain below retry bound must retry")
	}
	if shouldRetryMembershipTransition(drain, maxMembershipTransitionRetries) {
		t.Fatal("ErrReconfigurationDrain at retry bound must stop retrying")
	}
	unknownDrain := fmt.Errorf("%w: %w", network.ErrCommitUnknown, quepaxa.ErrReconfigurationDrain)
	if shouldRetryMembershipTransition(unknownDrain, 0) {
		t.Fatal("ErrCommitUnknown must never retry even when paired with ErrReconfigurationDrain")
	}
	exhausted := exhaustedMembershipTransitionError(quepaxa.ErrReconfigurationDrain)
	if !errors.Is(exhausted, network.ErrNotReady) || !errors.Is(exhausted, quepaxa.ErrReconfigurationDrain) {
		t.Fatalf("exhausted drain err=%v, want both ErrNotReady and ErrReconfigurationDrain", exhausted)
	}
	if got := exhaustedMembershipTransitionError(unknownDrain); got != unknownDrain || errors.Is(got, network.ErrNotReady) {
		t.Fatalf("exhausted ErrCommitUnknown modified=%v", got)
	}
}
