//go:build rhiza_local_testhooks

package localtesthooks

import "sync"

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
