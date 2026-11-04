//go:build !rhiza_local_testhooks

package node

func setManagedPreparedStartupTestHook(func(string)) func() {
	return func() {}
}
