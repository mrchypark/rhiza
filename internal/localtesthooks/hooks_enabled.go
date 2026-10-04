//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"sync"
)

type archiveGCPhaseKey struct{}

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
