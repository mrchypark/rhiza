//go:build !linux

package fencing

import (
	"context"
	"fmt"
)

func terminatePrivateNamespace(context.Context) error {
	return fmt.Errorf("fencing: private namespace termination requires Linux")
}
