//go:build graph

package network

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestGraphRequestIDRejectedBeforeConsensus(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := quepaxa.Member{ID: "n1"}
	core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{Members: []quepaxa.Member{member}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, []quepaxa.Member{member}, 0)
	defer server.Close()

	for _, length := range []int{65, 255, 256} {
		_, err := server.GraphExecute(context.Background(), types.GraphCommand{RequestID: strings.Repeat("x", length), Cypher: "CREATE (:Item)"})
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("request ID length %d: error=%v", length, err)
		}
		if core.Tip() != 0 {
			t.Fatalf("request ID length %d advanced consensus tip to %d", length, core.Tip())
		}
	}

	response, err := server.GraphExecute(context.Background(), types.GraphCommand{RequestID: strings.Repeat("x", types.MaxRequestIDBytes), Cypher: "CREATE (:Item)"})
	if err != nil || response.Slot != 1 || core.Tip() != 1 {
		t.Fatalf("valid request: response=%+v tip=%d err=%v", response, core.Tip(), err)
	}
}

func TestReservedGraphStreamRejectedBeforeConsensus(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := quepaxa.Member{ID: "n1"}
	core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{Members: []quepaxa.Member{member}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, []quepaxa.Member{member}, 0)
	defer server.Close()

	_, err = server.GraphExecute(context.Background(), types.GraphCommand{
		RequestID: "reserved", Cypher: "CREATE (:Item)",
		Events: []types.GraphStreamEvent{{Stream: "__lattice_changes", Kind: "invalid", Payload: "value"}},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error=%v, want invalid request", err)
	}
	if core.Tip() != 0 {
		t.Fatalf("rejected stream advanced consensus tip to %d", core.Tip())
	}
}

func TestConcurrentGraphRequestIDConflict(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := quepaxa.Member{ID: "n1"}
	core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{Members: []quepaxa.Member{member}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, []quepaxa.Member{member}, 0)
	defer server.Close()

	const contenders = 32
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := server.GraphExecute(context.Background(), types.GraphCommand{
				RequestID: "shared", Cypher: `CREATE (:Item {id: $id})`, Args: map[string]any{"id": float64(i)},
			})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded, conflicted := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrRequestConflict):
			conflicted++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != contenders-1 {
		t.Fatalf("succeeded=%d conflicted=%d", succeeded, conflicted)
	}
}

func TestGraphBuildRejectsNewSQLBeforeConsensus(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := quepaxa.Member{ID: "n1"}
	core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{Members: []quepaxa.Member{member}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, []quepaxa.Member{member}, 0)
	defer server.Close()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "sql", SQL: "CREATE TABLE forbidden (id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.proposeLocal(context.Background(), value); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error=%v, want invalid request", err)
	}
	if core.Tip() != 0 {
		t.Fatalf("rejected SQL advanced consensus tip to %d", core.Tip())
	}
}

func TestGraphReadMetadataBoundsItsSnapshot(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := quepaxa.Member{ID: "n1"}
	core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{Members: []quepaxa.Member{member}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, []quepaxa.Member{member}, 0)
	defer server.Close()
	if _, err := server.GraphExecute(context.Background(), types.GraphCommand{
		RequestID: "seed", Cypher: `CREATE (:Item {id: '1'})`,
		Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "1"}},
	}); err != nil {
		t.Fatal(err)
	}
	query, err := server.GraphQuery(context.Background(), GraphQueryRequest{Cypher: `MATCH (n:Item) RETURN n.id`})
	if err != nil || query.AppliedSlot != 1 || query.ConsensusTip < query.AppliedSlot {
		t.Fatalf("query metadata=%d/%d err=%v", query.AppliedSlot, query.ConsensusTip, err)
	}
	stream, err := server.GraphStreamRead(context.Background(), GraphStreamReadRequest{Stream: "events", Limit: 1})
	if err != nil || stream.AppliedSlot != 1 || stream.ConsensusTip < stream.AppliedSlot || len(stream.Records) != 1 {
		t.Fatalf("stream metadata=%d/%d records=%d err=%v", stream.AppliedSlot, stream.ConsensusTip, len(stream.Records), err)
	}
	offset, err := server.GraphStreamOffset(context.Background(), GraphStreamOffsetRequest{Stream: "events", Consumer: "c"})
	if err != nil || offset.AppliedSlot != 1 || offset.ConsensusTip < offset.AppliedSlot {
		t.Fatalf("offset metadata=%d/%d err=%v", offset.AppliedSlot, offset.ConsensusTip, err)
	}
}
