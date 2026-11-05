//go:build cgo && !rhiza_local_testhooks

package main

import "testing"

func observeManagedGraphStartup() (last func() string, stop func()) {
	return func() string { return "not_observed" }, func() {}
}

func TestManagedGraphStartupObserverDisabled(t *testing.T) {
	last, stop := observeManagedGraphStartup()
	stop()
	if last() != "not_observed" {
		t.Fatal("disabled observer reported instrumentation")
	}
}
