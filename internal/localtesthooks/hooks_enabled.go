//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"sync"
)

type archiveGCPhaseKey struct{}
type archiveBusyKey struct{}

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
