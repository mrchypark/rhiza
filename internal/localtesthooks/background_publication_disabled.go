//go:build !rhiza_local_testhooks

package localtesthooks

import "context"

func SetBackgroundPublication(func(context.Context, string)) func() { return func() {} }
func HitBackgroundPublication(context.Context, string) error       { return nil }
