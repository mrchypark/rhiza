package network

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestArchiveCatchUpHintIsOptionalAndPassesExactKey(t *testing.T) {
	server := &Server{}
	key := quepaxa.TerminalGapKey{
		ConfigID:   7,
		Freeze:     11,
		Terminal:   23,
		FreezeHash: quepaxa.ValueHash{0x42},
	}

	// An unset callback is a no-op.
	server.hintArchiveCatchUp(key)

	called := false
	server.SetArchiveCatchUpHint(func(got quepaxa.TerminalGapKey) {
		called = true
		if got != key {
			t.Errorf("callback key=%+v, want %+v", got, key)
		}
		// Re-entering the setter must not deadlock; the callback runs outside
		// the server's callback-protection lock.
		server.SetArchiveCatchUpHint(nil)
	})
	server.hintArchiveCatchUp(key)
	if !called {
		t.Fatal("configured callback was not invoked")
	}
	server.hintArchiveCatchUp(key)
}
