//go:build !linux

package fencing

import (
	"context"
	"fmt"
)

func VerifyPrivatePIDNamespace() error {
	return fmt.Errorf("fencing: private PID namespace requires Linux")
}

func CheckInitNamespaceQuiescence(context.Context) error {
	return fmt.Errorf("fencing: init namespace quiescence requires Linux")
}

func terminatePrivateNamespace(context.Context) error {
	return fmt.Errorf("fencing: private namespace termination requires Linux")
}
