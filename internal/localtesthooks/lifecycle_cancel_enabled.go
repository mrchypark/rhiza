//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"errors"
)

// ErrLifecycleStopped is test-only evidence that Node invoked its own cancel.
var ErrLifecycleStopped = errors.New("test: node lifecycle stopped")

func WithLifecycleCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancelCause(ctx)
	return child, func() { cancel(ErrLifecycleStopped) }
}
