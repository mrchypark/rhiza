//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestManagedGraphDownloadFailureDiagnosticPreservesError(t *testing.T) {
	fixture := newPreparedLearnerFixture(t)
	stopPreparedLearnerBackgroundWork(t, fixture.node)
	ctx, cancel := context.WithCancelCause(fixture.ctx)
	defer cancel(nil)
	const secret = "private-diagnostic-canary"
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	restore := localtesthooks.Set(func(event string) {
		if event == "node:managed-graph-startup:before-checkpoint-download" {
			cancel(errors.New(secret))
		}
	})
	defer restore()
	projection, err := fixture.node.reconstructManagedGraphStartup(ctx, fixture.node.core, &fixture.sealB)
	if projection != nil {
		_ = projection.cleanup()
		t.Fatal("canceled download unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled download lost context error: %v", err)
	}
	if got := output.String(); !strings.Contains(got, "MANAGED_GRAPH_RECONSTRUCTION_FAILURE stage=download_checkpoint") || strings.Contains(got, secret) {
		t.Fatalf("unexpected failure diagnostic: %q", got)
	}
}
