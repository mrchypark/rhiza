//go:build rhiza_local_testhooks

package materializer

import "sync/atomic"

var failUserPolicyRestore atomic.Bool

func failUserPolicyRestoreForTest() bool { return failUserPolicyRestore.Load() }
