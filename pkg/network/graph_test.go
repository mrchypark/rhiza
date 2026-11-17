package network

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()

	for _, cypher := range []string{
		`CREATE (:Person)-[:KNOWS]->(:Person)`,
		`CREATE (:Person), (:Person)`,
		`CREATE (a:Person)-[e:KNOWS]->(b:Person)`,
	} {
		_, err := server.GraphExecute(context.Background(), types.GraphCommand{RequestID: "unsupported", Cypher: cypher})
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("cypher %q: error=%v", cypher, err)
		}
		if core.Tip() != 0 {
			t.Fatalf("cypher %q advanced consensus tip to %d", cypher, core.Tip())
		}
	}

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
	server := NewServer(core, material, "cluster", true, nil)
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
	server := NewServer(core, material, "cluster", true, nil)
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
	server := NewServer(core, material, "cluster", true, nil)
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

func TestGraphQueryExactSlotAndResourceLimitTransport(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	if _, err := server.GraphExecute(context.Background(), types.GraphCommand{RequestID: "query-pin-seed", Cypher: `CREATE (:PinnedQuery {id: '1'})`}); err != nil {
		t.Fatal(err)
	}

	exact := uint64(1)
	result, err := server.GraphQuery(context.Background(), GraphQueryRequest{Cypher: `MATCH (n:PinnedQuery) RETURN n.id`, RequireAppliedSlot: &exact})
	if err != nil || result.AppliedSlot != exact || len(result.Rows) != 1 || result.Rows[0][0] != "1" {
		t.Fatalf("pinned query result=%+v error=%v", result, err)
	}

	stale := uint64(0)
	result, err = server.GraphQuery(context.Background(), GraphQueryRequest{Cypher: `MATCH (n:PinnedQuery) RETURN n.id`, RequireAppliedSlot: &stale})
	if !errors.Is(err, ErrReadVersionMismatch) || !errors.Is(err, materializer.ErrReadVersionMismatch) || result.AppliedSlot != exact || len(result.Rows) != 0 {
		t.Fatalf("stale query result=%+v error=%v", result, err)
	}
	code, body := graphHTTP(t, server, http.MethodPost, "/graph/query", GraphQueryRequest{Cypher: `MATCH (n:PinnedQuery) RETURN n.id`, RequireAppliedSlot: &stale})
	if code != http.StatusConflict || body.Code != "read_version_mismatch" {
		t.Fatalf("stale status=%d body=%+v", code, body)
	}

	limited := GraphQueryRequest{Cypher: `UNWIND range(0, 10000) AS n RETURN n`}
	result, err = server.GraphQuery(context.Background(), limited)
	if !errors.Is(err, ErrGraphResourceLimit) || !errors.Is(err, materializer.ErrGraphResourceLimit) || errors.Is(err, ErrInvalidRequest) || result.AppliedSlot != exact {
		t.Fatalf("limited query result=%+v error=%v", result, err)
	}
	code, body = graphHTTP(t, server, http.MethodPost, "/graph/query", limited)
	if code != http.StatusBadRequest || body.Code != "graph_resource_limit" {
		t.Fatalf("limited status=%d body=%+v", code, body)
	}

	escapedCellQuery := GraphQueryRequest{Cypher: fmt.Sprintf(`RETURN replace('%s', 'a', '\u0001') AS value`, strings.Repeat("a", 180_000))}
	if _, err := server.GraphQuery(context.Background(), escapedCellQuery); !errors.Is(err, ErrGraphResourceLimit) {
		t.Fatalf("escaped cell query error=%v", err)
	}
	code, body = graphHTTP(t, server, http.MethodPost, "/graph/query", escapedCellQuery)
	if code != http.StatusBadRequest || body.Code != "graph_resource_limit" {
		t.Fatalf("escaped cell status=%d body=%+v", code, body)
	}

	escapedRowsQuery := GraphQueryRequest{Cypher: fmt.Sprintf(`UNWIND range(0, 27) AS i RETURN replace('%s', 'a', '\u0001') AS value`, strings.Repeat("a", 100_000))}
	if _, err := server.GraphQuery(context.Background(), escapedRowsQuery); !errors.Is(err, ErrGraphResourceLimit) {
		t.Fatalf("escaped rows query error=%v", err)
	}
	code, body = graphHTTP(t, server, http.MethodPost, "/graph/query", escapedRowsQuery)
	if code != http.StatusBadRequest || body.Code != "graph_resource_limit" {
		t.Fatalf("escaped rows status=%d body=%+v", code, body)
	}
}

func TestGraphReachableHTTPPostAndGet(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if err := material.ConfigureLocalGraphNodePropertyIndexes([]types.GraphNodePropertyIndex{{Label: "Reach", Property: "key"}}); err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	for i, cypher := range []string{
		`CREATE (:Reach {key: 'a'})`,
		`CREATE (:Reach {key: 'b'})`,
		`CREATE (:Reach {key: 'c'})`,
		`MATCH (a:Reach {key: 'a'}), (b:Reach {key: 'b'}) CREATE (a)-[:REL]->(b)`,
		`MATCH (b:Reach {key: 'b'}), (c:Reach {key: 'c'}) CREATE (b)-[:REL]->(c)`,
	} {
		if _, err := server.GraphExecute(context.Background(), types.GraphCommand{RequestID: "reachable-http-" + string(rune('0'+i)), Cypher: cypher}); err != nil {
			t.Fatal(err)
		}
	}
	request := GraphReachableRequest{
		StartLabel: "Reach", StartProperty: "key", StartValue: "a", EdgeType: "REL",
		NodeLabel: "Reach", ResultProperty: "key", MaxDepth: 2, MaxResults: 10, MaxScannedEdges: 10, MaxBytes: 1024,
	}
	result, err := server.GraphReachable(context.Background(), request)
	if err != nil || result.AppliedSlot != 5 || len(result.Nodes) != 2 {
		t.Fatalf("reachable result=%+v error=%v", result, err)
	}
	request.RequireAppliedSlot = &result.AppliedSlot

	for _, method := range []string{http.MethodPost, http.MethodGet} {
		code, contentType, raw := graphHTTPBytes(t, server, method, "/graph/reachable", request)
		if code != http.StatusOK {
			t.Fatalf("%s success status=%d body=%s", method, code, raw)
		}
		if contentType != "application/json" {
			t.Fatalf("%s success content-type=%q", method, contentType)
		}
		var got GraphReachableResult
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("%s decode result: %v", method, err)
		}
		if !got.StartFound || got.AppliedSlot != result.AppliedSlot || got.ConsensusTip != result.ConsensusTip || got.ScannedEdges != result.ScannedEdges || !reflect.DeepEqual(got.Nodes, result.Nodes) {
			t.Fatalf("%s result=%+v want=%+v", method, got, result)
		}
	}

	stale := uint64(0)
	request.RequireAppliedSlot = &stale
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		code, body := graphHTTP(t, server, method, "/graph/reachable", request)
		if code != http.StatusConflict || body.Code != "read_version_mismatch" {
			t.Fatalf("%s stale status=%d body=%+v", method, code, body)
		}
	}

	request.RequireAppliedSlot = nil
	request.NodeFilters = map[string]any{"unknown": true}
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		code, contentType, raw := graphHTTPBytes(t, server, method, "/graph/reachable", request)
		if code != http.StatusOK {
			t.Fatalf("%s filtered status=%d body=%s", method, code, raw)
		}
		if contentType != "application/json" {
			t.Fatalf("%s filtered content-type=%q", method, contentType)
		}
		var got GraphReachableResult
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("%s decode filtered result: %v", method, err)
		}
		if !got.StartFound || got.AppliedSlot != result.AppliedSlot || got.ConsensusTip != result.ConsensusTip || len(got.Nodes) != 0 {
			t.Fatalf("%s filtered result=%+v", method, got)
		}
	}

	request.NodeFilters = nil
	request.MaxResults = 1
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		code, body := graphHTTP(t, server, method, "/graph/reachable", request)
		if code != http.StatusBadRequest || body.Code != "graph_resource_limit" {
			t.Fatalf("%s limited status=%d body=%+v", method, code, body)
		}
	}

	request.MaxResults = 0
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		code, body := graphHTTP(t, server, method, "/graph/reachable", request)
		if code != http.StatusBadRequest || body.Code != "invalid_request" {
			t.Fatalf("%s invalid status=%d body=%+v", method, code, body)
		}
	}
}

func graphHTTP(t *testing.T, server *Server, method, path string, value any) (int, ErrorResponse) {
	t.Helper()
	code, _, body := graphHTTPBytes(t, server, method, path, value)
	var apiError ErrorResponse
	if len(body) != 0 {
		_ = json.Unmarshal(body, &apiError)
	}
	return code, apiError
}

func graphHTTPBytes(t *testing.T, server *Server, method, path string, value any) (int, string, []byte) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(method, path, bytes.NewReader(body)))
	return response.Code, response.Header().Get("Content-Type"), response.Body.Bytes()
}
