//go:build rhiza_local_testhooks

package qlog

import "sync"

var testSyncErrors sync.Map

// TestInjectSyncError targets only this WAL and must be restored after a test.
func (w *WAL) TestInjectSyncError(err error) func() {
	old, had := testSyncErrors.Swap(w, err)
	return func() {
		if had {
			testSyncErrors.Store(w, old)
		} else {
			testSyncErrors.Delete(w)
		}
	}
}

func injectedSyncError(w *WAL) error {
	if err, ok := testSyncErrors.Load(w); ok {
		return err.(error)
	}
	return nil
}
