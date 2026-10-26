//go:build !rhiza_local_testhooks

package network

func installRetiredPeerTestHook(func(string)) func() { return func() {} }
