//go:build !rhiza_local_testhooks

package materializer

func failUserPolicyRestoreForTest() bool { return false }
