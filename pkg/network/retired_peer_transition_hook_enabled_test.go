//go:build rhiza_local_testhooks

package network

import "github.com/mrchypark/rhiza/internal/localtesthooks"

func installRetiredPeerTestHook(hook func(string)) func() {
	return localtesthooks.Set(hook)
}
