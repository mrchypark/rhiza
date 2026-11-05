//go:build rhiza_local_testhooks

package localtesthooks

import "sync"

var proposalEntryState struct {
	sync.RWMutex
	hook func([]byte)
}

// SetProposalEntry installs one test observer. The borrowed value is valid only
// during the synchronous callback and must not be changed, retained or logged.
func SetProposalEntry(hook func([]byte)) func() {
	proposalEntryState.Lock()
	previous := proposalEntryState.hook
	proposalEntryState.hook = hook
	proposalEntryState.Unlock()
	return func() {
		proposalEntryState.Lock()
		proposalEntryState.hook = previous
		proposalEntryState.Unlock()
	}
}

func HitProposalEntry(value []byte) {
	proposalEntryState.RLock()
	hook := proposalEntryState.hook
	proposalEntryState.RUnlock()
	if hook != nil {
		hook(value)
	}
}
