package node

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedMembershipCauseClassAllowlist(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "canceled", err: context.Canceled, want: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, want: "deadline"},
		{name: "not-ready", err: network.ErrNotReady, want: "not_ready"},
		{name: "reconfiguration-drain", err: quepaxa.ErrReconfigurationDrain, want: "reconfiguration_drain"},
		{name: "invalid", err: network.ErrInvalidRequest, want: "invalid_request"},
		{name: "conflict", err: network.ErrRequestConflict, want: "request_conflict"},
		{name: "unknown-secret", err: errors.New("private object path and payload"), want: "other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := managedMembershipCauseClass(test.err); got != test.want {
				t.Fatalf("cause class=%q, want %q", got, test.want)
			}
		})
	}
}
