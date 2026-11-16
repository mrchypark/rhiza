//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

func WithGraphApplyNode(ctx context.Context, _ string) context.Context { return ctx }

func GraphApplyNode(context.Context) string { return "" }

func GraphApplyContext(ctx context.Context) context.Context { return ctx }
