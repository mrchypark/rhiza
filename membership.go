package rhiza

import (
	"context"

	"github.com/mrchypark/rhiza/pkg/network"
)

// Public type aliases for membership management types.
type MembershipChange = network.MembershipChange
type MembershipFence = network.MembershipFence
type MembershipStatus = network.MembershipStatus

// ChangeMembership joins the server lifecycle so shutdown cannot close its WAL
// while a management operation is still using it.
func (db *DB) ChangeMembership(ctx context.Context, change MembershipChange) error {
	return db.api.ChangeMembership(ctx, change)
}

// AbortMembership terminates a pending addition under its original voter quorum.
func (db *DB) AbortMembership(ctx context.Context, change MembershipChange) error {
	return db.api.AbortMembership(ctx, change)
}

// MembershipStatus is an observational snapshot, never a fencing authorization.
func (db *DB) MembershipStatus() (MembershipStatus, error) {
	return db.api.MembershipStatus()
}
