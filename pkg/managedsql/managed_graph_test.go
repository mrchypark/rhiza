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
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
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

func TestManagedGraphRustOffsetMaxOnPublicHost(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
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
	event := func(n int64) types.GraphStreamEvent {
		return types.GraphStreamEvent{Stream: "graph-ci-events", Kind: "ordered", Payload: n}
	}
	commands := []types.GraphCommand{
		{RequestID: string([]byte{0xff, 1}), Cypher: "CREATE (:GraphCI {id:'a', label:$label, n:$n})", Args: map[string]any{"label": "alpha", "n": float32(0.1)}, Events: []types.GraphStreamEvent{event(1), {Stream: "graph-ci-trim", Kind: "trimmed", Payload: int64(1)}}},
		{RequestID: string([]byte{0xfe, 2}), Cypher: "CREATE (:GraphCI {id:'b', label:'beta', n:2})", Events: []types.GraphStreamEvent{event(2), {Stream: "graph-ci-trim", Kind: "trimmed", Payload: int64(2)}}},
		{RequestID: "\ufffd", Cypher: "MATCH (a:GraphCI {id:'a'}), (b:GraphCI {id:'b'}) CREATE (a)-[:LINK]->(b)", Events: []types.GraphStreamEvent{event(3)}},
		{RequestID: "graph-offset-max", StreamOffset: &types.GraphStreamOffsetMutation{Stream: "graph-ci-events", Consumer: "graph-ci-consumer", Sequence: ^uint64(0)}},
		{RequestID: "graph-trim-max", StreamTrim: &types.GraphStreamTrimMutation{Stream: "graph-ci-trim", ThroughSequence: ^uint64(0)}},
	}
	for i, command := range commands[:4] {
		result, err := hosts[0].GraphExecute(ctx, command)
		if err != nil || result.Slot == 0 || result.Status != types.MutationCommitted || !result.Applied {
			t.Fatalf("Rust Graph command %d did not return a committed receipt", i+1)
		}
	}
	trimStream, err := hosts[0].GraphStreamRead(ctx, network.GraphStreamReadRequest{Stream: "graph-ci-trim", Limit: 10, Consistency: "linearizable"})
	if err != nil || len(trimStream.Records) != 2 || trimStream.Records[0].Sequence >= trimStream.Records[1].Sequence || trimStream.Records[0].Kind != "trimmed" || trimStream.Records[1].Kind != "trimmed" || trimStream.Records[0].Payload != int64(1) || trimStream.Records[1].Payload != int64(2) {
		t.Fatalf("Rust trim precondition: records=%+v error=%v", trimStream.Records, err)
	}
	trim, err := hosts[0].GraphExecute(ctx, commands[4])
	if err != nil || trim.Slot == 0 || trim.Status != types.MutationCommitted || !trim.Applied {
		t.Fatal("Rust Graph trim-max did not return a committed receipt")
	}
	offset, err := hosts[0].GraphStreamOffset(ctx, network.GraphStreamOffsetRequest{Stream: "graph-ci-events", Consumer: "graph-ci-consumer"})
	if err != nil || !offset.Found || offset.Sequence != ^uint64(0) {
		t.Fatal("max stream offset readback differs")
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
