//go:build !ci_profile

package main

import "testing"

func TestCPUProfileDisabledInDefaultBuild(t *testing.T) {
	t.Setenv("RHIZA_CI_CPU_PROFILE_PORT", "invalid")
	stop, err := startCPUProfile()
	if err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}
