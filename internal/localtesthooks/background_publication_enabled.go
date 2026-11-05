//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"sync"
)

var backgroundPublicationState struct {
	sync.RWMutex
	hook func(context.Context, string)
}

// SetBackgroundPublication installs a test-only background worker boundary.
// A blocking callback must select on ctx.Done so shutdown can join the worker.
func SetBackgroundPublication(hook func(context.Context, string)) func() {
	backgroundPublicationState.Lock()
	previous := backgroundPublicationState.hook
	backgroundPublicationState.hook = hook
	backgroundPublicationState.Unlock()
	return func() {
		backgroundPublicationState.Lock()
		backgroundPublicationState.hook = previous
		backgroundPublicationState.Unlock()
	}
}

// HitBackgroundPublication never holds the registry lock during the callback.
// Cancellation must stop the worker before it starts any publication.
func HitBackgroundPublication(ctx context.Context, phase string) error {
	backgroundPublicationState.RLock()
	hook := backgroundPublicationState.hook
	backgroundPublicationState.RUnlock()
	if hook != nil {
		hook(ctx, phase)
	}
	return ctx.Err()
}
