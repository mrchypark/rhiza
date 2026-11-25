package quepaxa

import (
	"context"
	"testing"
	"time"
)

func TestIdenticalBarrierProposalsAreNotGloballyDeduplicated(t *testing.T) {
	cores, _ := newTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	barrier := []byte("same-control-value")
	first, _, err := cores["n1"].Propose(ctx, barrier)
	if err != nil {
		t.Fatal(err)
	}
	if err := cores["n2"].WaitTip(ctx, first); err != nil {
		t.Fatal(err)
	}
	second, _, err := cores["n2"].Propose(ctx, barrier)
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("identical barrier did not advance independently: first=%d second=%d", first, second)
	}
}
