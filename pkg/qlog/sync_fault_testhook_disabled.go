//go:build !rhiza_local_testhooks

package qlog

func injectedSyncError(*WAL) error { return nil }
