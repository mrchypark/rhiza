//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

// ArchiveBusyEvent has no production fields; all calls are compiled out.
type ArchiveBusyEvent struct {
	Operation string
	Resource  string
	Branch    string
	Stage     string
	Entered   bool
}

func WithArchiveBusyTrace(ctx context.Context, _ func(ArchiveBusyEvent)) context.Context { return ctx }

func HitArchiveBusy(context.Context, ArchiveBusyEvent) {}

// CarryArchiveBusyObserver is an identity when the hooks are compiled out.
func CarryArchiveBusyObserver(dst, _ context.Context) context.Context { return dst }

// RecoveryPinEvent has no production fields; all calls are compiled out.
type RecoveryPinEvent struct {
	Phase          string
	KeyHash        string
	OwnerCategory  string
	LeaseState     string
	LeaseDeltaMS   int64
	ReadStatus     string
	WriteStatus    string
	ReadbackStatus string
	VersionPresent bool
	ObservedAtMS   int64
}

const (
	RecoveryPinCreateAttempt        = "create_attempt"
	RecoveryPinCreateConfirmed      = "create_confirmed"
	RecoveryPinRenewAttempt         = "renew_attempt"
	RecoveryPinRenewConfirmed       = "renew_confirmed"
	RecoveryPinCloseAttempt         = "close_attempt"
	RecoveryPinCloseConfirmed       = "close_confirmed"
	RecoveryPinCloseConflict        = "close_conflict"
	RecoveryPinCloseError           = "close_error"
	RecoveryPinGuardRead            = "guard_read"
	RecoveryPinLeaseActive          = "active"
	RecoveryPinLeaseExpired         = "expired"
	RecoveryPinLeaseZero            = "zero"
	RecoveryPinLeaseUnknown         = "unknown"
	RecoveryPinReadOK               = "ok"
	RecoveryPinReadMissing          = "not_found"
	RecoveryPinReadInvalid          = "invalid"
	RecoveryPinReadIdentityMismatch = "identity_mismatch"
	RecoveryPinReadUnknown          = "unknown"
	RecoveryPinWriteOK              = "ok"
	RecoveryPinWriteCondMet         = "condition_not_met"
	RecoveryPinWriteNotAttempted    = "not_attempted"
	RecoveryPinWriteUnknown         = "unknown"
	RecoveryPinReadbackDone         = "verified"
	RecoveryPinReadbackNone         = "not_attempted"
	RecoveryPinOwnerStartup         = "startup"
	RecoveryPinOwnerNodeCatchup     = "node_catchup"
	RecoveryPinOwnerGuardUnknown    = "guard_unknown"
)

func WithRecoveryPinTrace(ctx context.Context, _ func(RecoveryPinEvent)) context.Context {
	return ctx
}

func HitRecoveryPin(context.Context, RecoveryPinEvent) {}

func CarryRecoveryPinObserver(dst, _ context.Context) context.Context { return dst }

func WithRecoveryPinCategory(ctx context.Context, _ string) context.Context { return ctx }

func RecoveryPinCategory(context.Context) string { return RecoveryPinOwnerGuardUnknown }

// WithArchiveGCPhaseTrace is an allocation-free identity outside local tests.
func WithArchiveGCPhaseTrace(ctx context.Context, _ func(string)) context.Context { return ctx }

func HitArchiveGCPhase(context.Context, string) {}

// Enabled is false when test boundary callbacks are compiled out.
const Enabled = false

// Hit is a production no-op. Boundary callbacks are only available in test
// binaries built with the rhiza_local_testhooks tag.
func Hit(string) {}
