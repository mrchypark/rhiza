//go:build !rhiza_local_testhooks

package localtesthooks

import "net/http"

func SetManagedKubeClient(func(*http.Client)) func() { return func() {} }
func HitManagedKubeClient(*http.Client)              {}
