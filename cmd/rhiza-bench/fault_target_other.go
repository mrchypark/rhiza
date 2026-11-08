//go:build !linux

package main

import (
	"fmt"
	"time"
)

type faultTarget struct{}

func openFaultTarget(int, string, int) (*faultTarget, error) {
	return nil, fmt.Errorf("fault lanes require Linux pidfd support")
}
func (*faultTarget) signal() (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, fmt.Errorf("fault lanes require Linux pidfd support")
}
func (*faultTarget) close() {}
