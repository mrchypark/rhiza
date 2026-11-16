//go:build !rhiza_latency_diag

package latencydiag

import (
	"context"
	"testing"
	"time"
)

func TestProposalDiagnosticsAreNoopWhenDisabled(t *testing.T) {
	ctx := context.Background()
	if Enabled || WithTags(ctx, []uint64{16, 32}) != ctx || CarryTags(ctx, WithTag(ctx, 16)) != ctx {
		t.Fatal("proposal tag helpers changed the context")
	}
	if len(Tags(ctx)) != 0 || Tag(ctx) != 0 || !StartTagged(ctx).IsZero() {
		t.Fatal("proposal diagnostics became observable")
	}
	TraceProposalStage(ctx, StageProposalPermitWait, ProposalWaitLocalCap, nil, time.Now())
}
