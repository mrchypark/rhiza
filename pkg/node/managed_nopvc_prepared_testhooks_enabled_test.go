//go:build rhiza_local_testhooks

package node

import "github.com/mrchypark/rhiza/internal/localtesthooks"

func setManagedPreparedStartupTestHook(hook func(string)) func() {
	return localtesthooks.Set(hook)
}
