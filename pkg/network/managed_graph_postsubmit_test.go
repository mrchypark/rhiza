package network

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphActualServerRetainsRawOriginal(t *testing.T) {
	ctx := context.Background()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	m, err := materializer.OpenManagedSQLKVGraphWithOriginals(t.TempDir()+"/state.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	s, err := NewServerForPolicy(core, m, "cluster", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	for _, id := range []string{"\xff", "\xfe", "\ufffd"} {
		original := types.GraphCommand{RequestID: id, Cypher: "CREATE (:Item {n: 1})"}
		result, err := s.GraphExecute(ctx, original)
		if err != nil || result.Slot == 0 || result.Status != types.MutationCommitted {
			t.Fatalf("actual Graph execution failed: receipt=%+v error=%v", result, err)
		}
		retained, found, err := m.RetainedRequestOriginal(ctx, types.MutationGraph, id)
		if err != nil || !found || retained.Graph == nil || retained.Graph.RequestID != id || retained.Receipt != result.MutationReceipt {
			t.Fatalf("raw Graph original/receipt changed: found=%t error=%v", found, err)
		}
		before := core.Tip()
		resolved, err := s.ResolveGraph(ctx, original)
		if err != nil || resolved.Receipt == nil || *resolved.Receipt != result.MutationReceipt || core.Tip() != before {
			t.Fatalf("Graph resolution changed receipt or proposed: result=%+v error=%v", resolved, err)
		}
	}
	rows, err := s.GraphQuery(ctx, GraphQueryRequest{Cypher: "MATCH (n:Item) RETURN n.n"})
	if err != nil || len(rows.Rows) != 3 {
		t.Fatalf("actual Graph effects missing: rows=%+v error=%v", rows, err)
	}
}

// Actual ordinary Core/WAL/Graph commit, followed by loss of observation. This
// does not qualify managed profile10, whose dependencies are still separate.
func TestGraphPostSubmitLookupFailurePreservesUnknown(t *testing.T) {
	ctx := context.Background()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	path := t.TempDir() + "/state.sqlite"
	m, err := materializer.Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	s := NewServer(core, m, "cluster", true, nil)
	t.Cleanup(s.Close)
	var submitted atomic.Uint64
	var calls atomic.Int32
	actual := s.graphBatcher.propose
	s.graphBatcher.propose = func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
		slot, err := actual(ctx, value)
		if err != nil || slot == 0 {
			return slot, err
		}
		submitted.Store(uint64(slot))
		calls.Add(1)
		return slot, m.Close()
	}
	id := "graph-original"
	_, err = s.GraphExecute(ctx, types.GraphCommand{RequestID: id, Cypher: "CREATE (:Item {n: 1})"})
	var unknown *CommitUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, ErrCommitUnknown) || errors.Is(err, ErrRequestConflict) || unknown.RequestID != id || uint64(unknown.Slot) != submitted.Load() || unknown.Slot == 0 || unknown.Cause == nil {
		t.Fatalf("post-submit observation lost actual outcome: slot=%d error=%v", submitted.Load(), err)
	}
	if !strings.Contains(fmt.Sprint(unknown.Cause), "LatticeDB is not open") {
		t.Fatalf("actual closed Graph observation cause not preserved: %v", err)
	}
	assertPostSubmitHTTP(t, err, http.StatusServiceUnavailable, "commit_unknown", id, submitted.Load())
	s.Close()
	reopened, err := materializer.Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	receipt, found, err := reopened.GraphMutationReceipt(ctx, id)
	if err != nil || !found || receipt.Slot != submitted.Load() || receipt.Status != types.MutationCommitted {
		t.Fatalf("original Graph commit not recoverable: found=%t receipt=%+v error=%v", found, receipt, err)
	}
	rows, err := reopened.GraphQuery(ctx, "MATCH (n:Item) RETURN n.n", nil)
	if err != nil || len(rows.Rows) != 1 || calls.Load() != 1 {
		t.Fatalf("actual Graph effect missing/duplicated: rows=%+v calls=%d error=%v", rows, calls.Load(), err)
	}
}
