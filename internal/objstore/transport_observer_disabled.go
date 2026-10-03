//go:build !rhiza_local_testhooks

package objstore

import "net/http"

func wrapTestObserver(_ string, next http.RoundTripper) http.RoundTripper { return next }
