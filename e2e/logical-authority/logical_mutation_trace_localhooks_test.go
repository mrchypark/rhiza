//go:build localhooks && rhiza_local_testhooks

package logicalauthority_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestLogicalMutationTraceJSONReportsNoReturnWithoutClaimingStuck(t *testing.T) {
	trace := localtesthooks.NewLogicalMutationExecutionTrace(8)
	ctx := trace.Context(context.Background())
	localtesthooks.HitLogicalMutationExecution(ctx, "producer-private", 11,
		localtesthooks.LogicalMutationAttemptStarted, localtesthooks.LogicalMutationStarted)
	localtesthooks.HitLogicalMutationExecution(ctx, "producer-private", 11,
		localtesthooks.LogicalMutationDispatchGranted, localtesthooks.LogicalMutationSucceeded)
	var snapshot struct {
		Attempts []struct {
			ProducerHash string `json:"producer_hash"`
			Sequence     uint64 `json:"sequence"`
			State        string `json:"state"`
		} `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(logicalMutationTraceJSON(trace)), &snapshot); err != nil {
		t.Fatalf("decode bounded trace snapshot: %v", err)
	}
	if len(snapshot.Attempts) != 1 || snapshot.Attempts[0].State != "started-no-return-observed" ||
		snapshot.Attempts[0].ProducerHash == "producer-private" || snapshot.Attempts[0].Sequence != 11 {
		t.Fatalf("attempt state=%+v, want one redacted started-no-return-observed attempt", snapshot.Attempts)
	}
	localtesthooks.HitLogicalMutationExecution(ctx, "producer-private", 11,
		localtesthooks.LogicalMutationAttemptReturned, localtesthooks.LogicalMutationPending)
	if err := json.Unmarshal([]byte(logicalMutationTraceJSON(trace)), &snapshot); err != nil {
		t.Fatalf("decode returned trace snapshot: %v", err)
	}
	if len(snapshot.Attempts) != 1 || snapshot.Attempts[0].State != "returned" {
		t.Fatalf("returned attempt state=%+v, want returned", snapshot.Attempts)
	}
	events, _ := trace.Snapshot()
	if len(events) < 3 || events[0].Stage != localtesthooks.LogicalMutationAttemptStarted ||
		events[len(events)-1].Stage != localtesthooks.LogicalMutationAttemptReturned {
		t.Fatalf("attempt start/return event order=%+v", events)
	}
}
