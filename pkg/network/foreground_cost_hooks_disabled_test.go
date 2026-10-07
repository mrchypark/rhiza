//go:build !rhiza_local_testhooks

package network

import "testing"

func installForegroundLearnHook(testing.TB, *foregroundLearnObservation) {}
