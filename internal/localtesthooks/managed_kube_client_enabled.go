//go:build rhiza_local_testhooks

package localtesthooks

import (
	"net/http"
	"sync"
)

var managedKubeClientState struct {
	sync.RWMutex
	hook func(*http.Client)
}

// SetManagedKubeClient observes only newly constructed managed clients, before
// they are exposed to runtime goroutines. Tests may wrap their transport for
// counts; they must not retain/log request payloads, credentials or responses.
func SetManagedKubeClient(hook func(*http.Client)) func() {
	managedKubeClientState.Lock()
	previous := managedKubeClientState.hook
	managedKubeClientState.hook = hook
	managedKubeClientState.Unlock()
	return func() {
		managedKubeClientState.Lock()
		managedKubeClientState.hook = previous
		managedKubeClientState.Unlock()
	}
}

func HitManagedKubeClient(client *http.Client) {
	managedKubeClientState.RLock()
	hook := managedKubeClientState.hook
	managedKubeClientState.RUnlock()
	if hook != nil {
		hook(client)
	}
}
