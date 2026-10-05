//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"sync"
)

type archiveGCPhaseKey struct{}
type archiveBusyKey struct{}
type recoveryPinKey struct{}
type recoveryPinCategoryKey struct{}

// ArchiveBusyEvent contains only finite classifications of an archive lock
// boundary. No lock identity or object path is retained.
type ArchiveBusyEvent struct {
	Operation string
	Resource  string
	Branch    string
	Stage     string
	Entered   bool
}

// WithArchiveBusyTrace scopes a nonblocking observer to one operation.
func WithArchiveBusyTrace(ctx context.Context, hook func(ArchiveBusyEvent)) context.Context {
	return context.WithValue(ctx, archiveBusyKey{}, hook)
}

func HitArchiveBusy(ctx context.Context, event ArchiveBusyEvent) {
	if hook, ok := ctx.Value(archiveBusyKey{}).(func(ArchiveBusyEvent)); ok && hook != nil {
		hook(event)
	}
}

// CarryArchiveBusyObserver returns a context that keeps exactly this archive
// Busy observer and nothing else from ctx. It exists for a release path that
// must run on a fresh context with its own deadline; WithoutCancel is not used
// because it would also carry unrelated values.
func CarryArchiveBusyObserver(dst, src context.Context) context.Context {
	hook, ok := src.Value(archiveBusyKey{}).(func(ArchiveBusyEvent))
	if !ok || hook == nil {
		return dst
	}
	return context.WithValue(dst, archiveBusyKey{}, hook)
}

// RecoveryPinEvent contains only finite classifications of one archive
// recovery-pin lifecycle boundary. It retains no owner string, token, object
// key, or credential material. LeaseDeltaMS is the relative expiry observed at
// capture time.
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

// Recovery-pin lifecycle phases. Every value is a fixed category.
const (
	RecoveryPinCreateAttempt   = "create_attempt"
	RecoveryPinCreateConfirmed = "create_confirmed"
	RecoveryPinRenewAttempt    = "renew_attempt"
	RecoveryPinRenewConfirmed  = "renew_confirmed"
	RecoveryPinCloseAttempt    = "close_attempt"
	RecoveryPinCloseConfirmed  = "close_confirmed"
	RecoveryPinCloseConflict   = "close_conflict"
	RecoveryPinCloseError      = "close_error"
	RecoveryPinGuardRead       = "guard_read"
)

// Recovery-pin lease and status categories.
const (
	RecoveryPinLeaseActive  = "active"
	RecoveryPinLeaseExpired = "expired"
	RecoveryPinLeaseZero    = "zero"
	RecoveryPinLeaseUnknown = "unknown"
	RecoveryPinReadOK       = "ok"
	RecoveryPinReadMissing  = "not_found"
	RecoveryPinReadInvalid  = "invalid"
	// RecoveryPinReadIdentityMismatch marks a read whose stored owner/token/
	// generation fields do not match the snapshot. No upload is attempted on
	// this path, so it must never be reported as a conditional conflict.
	RecoveryPinReadIdentityMismatch = "identity_mismatch"
	RecoveryPinReadUnknown          = "unknown"
	RecoveryPinWriteOK              = "ok"
	RecoveryPinWriteCondMet         = "condition_not_met"
	// RecoveryPinWriteNotAttempted marks a boundary where no conditional upload
	// was issued at all.
	RecoveryPinWriteNotAttempted = "not_attempted"
	RecoveryPinWriteUnknown      = "unknown"
	RecoveryPinReadbackDone      = "verified"
	RecoveryPinReadbackNone      = "not_attempted"
)

// Recovery-pin owner categories. guard_unknown is used where the observing
// site cannot know which caller created the pin.
const (
	RecoveryPinOwnerStartup      = "startup"
	RecoveryPinOwnerNodeCatchup  = "node_catchup"
	RecoveryPinOwnerGuardUnknown = "guard_unknown"
)

// WithRecoveryPinTrace scopes a nonblocking observer to one operation.
func WithRecoveryPinTrace(ctx context.Context, hook func(RecoveryPinEvent)) context.Context {
	return context.WithValue(ctx, recoveryPinKey{}, hook)
}

// HitRecoveryPin delivers one lifecycle event to a scoped observer.
func HitRecoveryPin(ctx context.Context, event RecoveryPinEvent) {
	if hook, ok := ctx.Value(recoveryPinKey{}).(func(RecoveryPinEvent)); ok && hook != nil {
		hook(event)
	}
}

// CarryRecoveryPinObserver returns a context that keeps exactly this recovery
// pin observer and nothing else from ctx. It exists for close paths that must
// run on a fresh context with its own deadline; WithoutCancel is not used
// because it would also carry unrelated values.
func CarryRecoveryPinObserver(dst, src context.Context) context.Context {
	hook, ok := src.Value(recoveryPinKey{}).(func(RecoveryPinEvent))
	if !ok || hook == nil {
		return dst
	}
	return context.WithValue(dst, recoveryPinKey{}, hook)
}

// WithRecoveryPinCategory stamps a finite caller category onto ctx so events
// can name which caller owns a pin without ever retaining the owner string.
func WithRecoveryPinCategory(ctx context.Context, category string) context.Context {
	return context.WithValue(ctx, recoveryPinCategoryKey{}, category)
}

// RecoveryPinCategory reports the stamped caller category, or guard_unknown
// when the observing site cannot know the creator.
func RecoveryPinCategory(ctx context.Context) string {
	if category, ok := ctx.Value(recoveryPinCategoryKey{}).(string); ok && category != "" {
		return category
	}
	return RecoveryPinOwnerGuardUnknown
}

// WithArchiveGCPhaseTrace scopes fixed archive cleanup phase events to one
// operation context. It retains no object identifiers or request history.
func WithArchiveGCPhaseTrace(ctx context.Context, hook func(string)) context.Context {
	return context.WithValue(ctx, archiveGCPhaseKey{}, hook)
}

func HitArchiveGCPhase(ctx context.Context, phase string) {
	if hook, ok := ctx.Value(archiveGCPhaseKey{}).(func(string)); ok && hook != nil {
		hook(phase)
	}
}

// Enabled is true when test boundary callbacks are compiled in.
const Enabled = true

var state struct {
	sync.RWMutex
	hook func(string)
}

// Set installs one process-local test callback and returns a restore function.
func Set(hook func(string)) func() {
	state.Lock()
	previous := state.hook
	state.hook = hook
	state.Unlock()
	return func() {
		state.Lock()
		state.hook = previous
		state.Unlock()
	}
}

// Hit invokes the test callback at a durable protocol boundary.
func Hit(name string) {
	state.RLock()
	hook := state.hook
	state.RUnlock()
	if hook != nil {
		hook(name)
	}
}
