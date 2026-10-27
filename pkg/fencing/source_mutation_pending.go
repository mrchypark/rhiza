package fencing

import (
	"errors"
	"fmt"
)

const (
	MutationPendingReasonHeader         = "X-Rhiza-Mutation-Pending-Reason"
	MutationPreDispatchContentionReason = "foreign-key-pre-dispatch-v1"
)

// ErrMutationPreDispatchContention is the narrowly typed pending condition
// returned when this exact admitted tuple is waiting for a foreign key owner
// before provider dispatch. It is not proof that the key is free or that any
// dispatch permission can be released.
var ErrMutationPreDispatchContention = fmt.Errorf("%w: %s", ErrPending, MutationPreDispatchContentionReason)

// IsMutationPreDispatchContention accepts only this exact marker in a linear
// error chain. Joined errors are deliberately excluded because they may carry
// an unrelated failure that must not be hidden by settlement or transport.
func IsMutationPreDispatchContention(err error) bool {
	for err != nil {
		if err == ErrMutationPreDispatchContention {
			return true
		}
		if _, joined := err.(interface{ Unwrap() []error }); joined {
			return false
		}
		err = errors.Unwrap(err)
	}
	return false
}
