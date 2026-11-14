//go:build !rhiza_local_testhooks

package quepaxa

import (
	"context"
	"testing"
)

func verifyUnchangedISRMembershipDrift(*testing.T, context.Context, *Core, Slot, MembershipRecord, MembershipRecord) {
}

func verifyUnchangedISRUnavailableBecomesAvailable(*testing.T, context.Context, *Core, Cluster, Slot) {
}
