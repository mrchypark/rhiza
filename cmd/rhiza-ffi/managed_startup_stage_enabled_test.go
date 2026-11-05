//go:build cgo && rhiza_local_testhooks

package main

import (
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func observeManagedGraphStartup() (last func() string, stop func()) {
	stages := [...]string{"not_observed", "target_inspect", "core_open", "archive_recover", "pair_select", "pair_reconstruct", "pair_install", "runtime_verify", "open_complete"}
	var current atomic.Uint32
	stop = localtesthooks.Set(func(event string) {
		var stage uint32
		switch event {
		case "node:managed-graph-startup:target_inspect":
			stage = 1
		case "node:managed-graph-startup:core_open":
			stage = 2
		case "node:managed-graph-startup:archive_recover":
			stage = 3
		case "node:managed-graph-startup:pair_select":
			stage = 4
		case "node:managed-graph-startup:pair_reconstruct":
			stage = 5
		case "node:managed-graph-startup:pair_install":
			stage = 6
		case "node:managed-graph-startup:runtime_verify":
			stage = 7
		case "node:managed-graph-startup:open_complete":
			stage = 8
		default:
			return
		}
		current.Store(stage)
	})
	return func() string { return stages[current.Load()] }, stop
}

func TestManagedGraphStartupObserverPreservesFailureBoundary(t *testing.T) {
	last, stop := observeManagedGraphStartup()
	t.Cleanup(stop)
	if last() != "not_observed" {
		t.Fatal("observer invented an initial stage")
	}
	for _, stage := range []string{"target_inspect", "core_open", "archive_recover", "pair_select", "pair_reconstruct", "pair_install", "runtime_verify", "open_complete"} {
		localtesthooks.Hit("node:managed-graph-startup:" + stage)
		if last() != stage {
			t.Fatal("fixed stage not observed", stage)
		}
	}
	localtesthooks.Hit("node:managed-graph-startup:pair_reconstruct")
	for _, event := range []string{"cleanup", "node:shutdown", "node:managed-graph-startup:unknown", "node:managed-graph-startup:pair_install secret=/private/value", "node:managed-graph-startup:pair_reconstruct\nsecret"} {
		localtesthooks.Hit(event)
		if last() != "pair_reconstruct" {
			t.Fatal("cleanup or untrusted event overwrote failure boundary")
		}
	}
	stop()
	localtesthooks.Hit("node:managed-graph-startup:open_complete")
	if last() != "pair_reconstruct" {
		t.Fatal("observer retained callback after stop")
	}
}
