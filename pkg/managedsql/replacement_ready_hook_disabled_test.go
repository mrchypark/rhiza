//go:build !rhiza_local_testhooks

package managedsql

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

// Normal builds still wait for actual readiness before the next injected phase;
// only the explicitly hooked build can prove the held-not-ready boundary.
func holdReplacementBeforeReady(t *testing.T, _ context.Context, _ types.NodeID) (<-chan struct{}, <-chan struct{}, func(), bool) {
	t.Helper()
	return nil, nil, func() {}, false
}
