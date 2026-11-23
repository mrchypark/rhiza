//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

func WithLifecycleCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(ctx)
}
