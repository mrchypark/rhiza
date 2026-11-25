//go:build rhiza_local_testhooks

package network

import "context"

type postSubmitMissingKey struct{}

func postSubmitMissingForTest(ctx context.Context, kind, requestID string) bool {
	fn, ok := ctx.Value(postSubmitMissingKey{}).(func(string, string) bool)
	return ok && fn(kind, requestID)
}
