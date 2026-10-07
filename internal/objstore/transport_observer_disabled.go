//go:build !rhiza_local_testhooks

package objstore

import (
	"context"
	"net/http"
)

// ExtentUploadAttribution is inert outside tagged local diagnostic builds.
type ExtentUploadAttribution struct{}

func BeginExtentUploadAttribution(ctx context.Context) (context.Context, *ExtentUploadAttribution) {
	return ctx, nil
}

func (*ExtentUploadAttribution) Complete(error, bool, string) {}

func wrapTestObserver(_ string, next http.RoundTripper) http.RoundTripper { return next }
