//go:build !ci_profile

package main

func startCPUProfile() (func() error, error) {
	return func() error { return nil }, nil
}
