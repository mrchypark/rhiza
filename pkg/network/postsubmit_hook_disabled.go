//go:build !rhiza_local_testhooks

package network

import "context"

func postSubmitMissingForTest(context.Context, string, string) bool { return false }
