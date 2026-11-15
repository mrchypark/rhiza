//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

func GraphApplyContext(ctx context.Context) context.Context { return ctx }
