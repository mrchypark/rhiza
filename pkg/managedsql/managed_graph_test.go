package managedsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphActualPublicHost(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		h, err := fixture.open(ctx, cfg)
		if h != nil {
			t.Cleanup(func() { _ = h.Close() })
		}
		if err != nil {
			t.Fatalf("open public Graph Host %d: %v", i, err)
		}
		hosts[i] = h
	}
	// The first public mutation itself must complete initial-root admission.
	// No Ready, checkpoint polling or private admission precedes this call.
	original := types.GraphCommand{RequestID: "\xff", Cypher: "CREATE (:Item {n: 9})"}
	result, err := hosts[0].GraphExecute(ctx, original)
	if err != nil || result.Slot == 0 || result.Status != types.MutationCommitted {
		t.Fatalf("public Graph write: receipt=%+v error=%v", result, err)
	}
	if err := hosts[0].Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := fixture.open(ctx, configs[0])
	if reopened != nil {
		t.Cleanup(func() { _ = reopened.Close() })
	}
	if err != nil {
		t.Fatalf("same-directory nonempty WAL Graph reopen: %v", err)
	}
	waitGraphHostPublicReady(t, ctx, reopened)
	rows, err := reopened.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:Item) RETURN n.n", Consistency: "linearizable"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("reopened Graph effect changed: rows=%+v error=%v", rows, err)
	}
	recovered, err := reopened.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "graph", RequestID: original.RequestID})
	if err != nil || recovered.Graph == nil || recovered.Graph.MutationReceipt != result.MutationReceipt {
		t.Fatalf("reopened original receipt changed: result=%+v error=%v", recovered, err)
	}
}

// This checks the existing outcome helper, not an actual submitted Graph Host.
func TestManagedGraphOutcomeContextPreservesRawIdentity(t *testing.T) {
	for _, id := range []string{"\xff", "\xfe", "\ufffd"} {
		for _, slot := range []quepaxa.Slot{0, 17} {
			original := applicationOriginal{kind: "graph", graph: types.GraphCommand{RequestID: id}}
			err := applicationOutcomeUnknown(original, types.MutationReceipt{}, &network.CommitUnknownError{RequestID: id, Slot: slot, RetryThroughSlot: 1040, Cause: context.Canceled})
			captured, found := MutationErrorContext(err)
			if !found || captured.Kind != "graph" || string(captured.RequestID) != id || captured.Slot != uint64(slot) || captured.RetryThroughSlot != 1040 || !errors.Is(err, context.Canceled) || !errors.Is(err, network.ErrCommitUnknown) {
				t.Fatalf("Graph outcome context changed: %+v %v", captured, err)
			}
			captured.RequestID[0] = 0
			again, _ := MutationErrorContext(err)
			if string(again.RequestID) != id {
				t.Fatal("returned context aliases stored identity")
			}
		}
	}
}

func TestManagedGraphClosedHostRejectsEveryRead(t *testing.T) {
	h := &Host{closed: true}
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
	}{
		{"query", func() error { _, err := h.GraphQuery(ctx, network.GraphQueryRequest{}); return err }},
		{"reachable", func() error { _, err := h.GraphReachable(ctx, types.GraphReachableRequest{}); return err }},
		{"changes", func() error { _, err := h.GraphChanges(ctx, network.GraphStreamReadRequest{WaitMS: 30000}); return err }},
		{"stream", func() error {
			_, err := h.GraphStreamRead(ctx, network.GraphStreamReadRequest{WaitMS: 30000})
			return err
		}},
		{"offset", func() error { _, err := h.GraphStreamOffset(ctx, network.GraphStreamOffsetRequest{}); return err }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); err == nil {
				t.Fatal("closed Host accepted Graph read")
			}
		})
	}
}
