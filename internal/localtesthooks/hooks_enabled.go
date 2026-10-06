//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"sync"
	"time"
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

// BatchAdmissionEvent contains only bounded aggregate fields for an internal
// mutation batch admission attempt. It retains no request, key, value, slot,
// or error identity.
type BatchAdmissionEvent struct {
	Reason                 string
	Waited                 bool
	WaitNanos              int64
	AcceptedToDurableNanos int64
	Durable                bool
}

// BatchAdmissionTrace holds the test-only timestamps for one B batch
// admission. It is created only by the internal batch path, never by ordinary
// proposals.
type BatchAdmissionTrace struct {
	started   time.Time
	waited    bool
	waitAt    time.Time
	waitNanos int64
	accepted  time.Time
}

// NewBatchAdmissionTrace begins one test-only internal batch admission trace.
func NewBatchAdmissionTrace() BatchAdmissionTrace {
	return BatchAdmissionTrace{started: time.Now()}
}

// MarkWait records the first capacity wait for this batch admission.
func (t *BatchAdmissionTrace) MarkWait() {
	if !t.waited {
		t.waited = true
		t.waitAt = time.Now()
	}
}

// MarkAccepted records the instant the batch obtained proposal admission.
func (t *BatchAdmissionTrace) MarkAccepted() {
	if t.waited {
		t.waitNanos = time.Since(t.waitAt).Nanoseconds()
	}
	t.accepted = time.Now()
}

// Event returns the bounded terminal observation for this batch. Accepted
// traces report accepted-to-durable time only for a durable completion.
func (t BatchAdmissionTrace) Event(reason string, durable bool) (BatchAdmissionEvent, bool) {
	if t.started.IsZero() {
		return BatchAdmissionEvent{}, false
	}
	event := BatchAdmissionEvent{Reason: reason, Waited: t.waited, Durable: durable}
	if t.waited {
		event.WaitNanos = t.waitNanos
		if t.accepted.IsZero() {
			event.WaitNanos = time.Since(t.waitAt).Nanoseconds()
		}
	}
	if reason == BatchAdmissionAccepted && durable && !t.accepted.IsZero() {
		event.AcceptedToDurableNanos = time.Since(t.accepted).Nanoseconds()
	}
	return event, true
}

const (
	BatchAdmissionAccepted            = "accepted"
	BatchAdmissionWaitBudgetExhausted = "wait_budget_exhausted"
	BatchAdmissionContextCanceled     = "context_canceled"
	BatchAdmissionServerNotReady      = "server_not_ready"
	BatchAdmissionByteRejected        = "byte_rejected"
	BatchAdmissionJoinedExisting      = "joined_existing"
)

var batchAdmissionState struct {
	sync.RWMutex
	hook func(BatchAdmissionEvent)
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

// SetBatchAdmission installs one process-local batch admission observer and
// returns a restore function. The callback is copied while locked, then always
// invoked after the lock is released.
func SetBatchAdmission(hook func(BatchAdmissionEvent)) func() {
	batchAdmissionState.Lock()
	previous := batchAdmissionState.hook
	batchAdmissionState.hook = hook
	batchAdmissionState.Unlock()
	return func() {
		batchAdmissionState.Lock()
		batchAdmissionState.hook = previous
		batchAdmissionState.Unlock()
	}
}

// HitBatchAdmission delivers one bounded aggregate admission event.
func HitBatchAdmission(event BatchAdmissionEvent) {
	batchAdmissionState.RLock()
	hook := batchAdmissionState.hook
	batchAdmissionState.RUnlock()
	if hook != nil {
		hook(event)
	}
}
